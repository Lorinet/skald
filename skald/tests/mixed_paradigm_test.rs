use skald::client::{SkaldClient, ServiceBuilder};
use skald::server::SkaldServer;
use skald::transport::{TcpTransport, TcpTransportListener};
use skald::saga::builder::{SagaDefinitionBuilder, MessageTemplateBuilder};
use skald::Message;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};
use serde_json::Value;

// --- Messages for Event Sourcing ---
#[derive(Serialize, Deserialize, Debug, Clone, Message)]
#[skald(topic = "user.created")]
struct UserCreatedEvent {
    user_id: String,
    username: String,
}

// --- Messages for Durable Execution ---
#[derive(Serialize, Deserialize, Debug)]
struct CreateOrderRequest {
    user_id: String,
    item_id: String,
    quantity: u32,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct CreateOrderResponse {
    order_id: String,
    total_amount: f64,
}

#[derive(Serialize, Deserialize, Debug)]
struct OrderError {
    reason: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct ProcessPaymentRequest {
    order_id: String,
    amount: f64,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct ProcessPaymentResponse {
    transaction_id: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct PaymentError {
    reason: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct CancelOrderRequest {
    order_id: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct RefundPaymentRequest {
    transaction_id: String,
}

// --- Shared Log ---
type SharedLog = Arc<Mutex<Vec<String>>>;

// --- Service Implementations ---

async fn start_user_service(skald_addr: SocketAddr, log: SharedLog) -> Result<()> {
    let transport = TcpTransport::connect(skald_addr).await?;
    ServiceBuilder::new("UserService", transport).await?
        .on_event("user.created", move |event: UserCreatedEvent| {
            let log = log.clone();
            async move {
                log.lock().unwrap().push(format!("UserService: User {} created", event.username));
                Ok(())
            }
        })
        .start();
    Ok(())
}

async fn start_order_service(skald_addr: SocketAddr, log: SharedLog) -> Result<()> {
    let log_create = log.clone();
    let log_cancel = log.clone();
    let transport = TcpTransport::connect(skald_addr).await?;

    ServiceBuilder::new("OrderService", transport).await?
        .on_invoke("OrderService.create", move |req: CreateOrderRequest| {
            let log = log_create.clone();
            async move {
                log.lock().unwrap().push(format!("OrderService: Creating order for user {}", req.user_id));
                // Simulate order creation
                Ok::<_, OrderError>(CreateOrderResponse {
                    order_id: format!("order-{}", uuid::Uuid::new_v4()),
                    total_amount: (req.quantity as f64) * 10.0, // $10 per item
                })
            }
        })
        .on_invoke("OrderService.cancel", move |req: CancelOrderRequest| {
            let log = log_cancel.clone();
            async move {
                log.lock().unwrap().push(format!("OrderService: Cancelling order {}", req.order_id));
                Ok::<_, OrderError>(Value::Null) // Compensation usually returns nothing specific
            }
        })
        .start();
    Ok(())
}

async fn start_payment_service(skald_addr: SocketAddr, log: SharedLog) -> Result<()> {
    let log_process = log.clone();
    let log_refund = log.clone();
    let transport = TcpTransport::connect(skald_addr).await?;

    ServiceBuilder::new("PaymentService", transport).await?
        .on_invoke("PaymentService.process", move |req: ProcessPaymentRequest| {
            let log = log_process.clone();
            async move {
                log.lock().unwrap().push(format!("PaymentService: Processing payment for order {} amount {}", req.order_id, req.amount));
                // Simulate payment processing
                Ok::<_, PaymentError>(ProcessPaymentResponse {
                    transaction_id: format!("txn-{}", uuid::Uuid::new_v4()),
                })
            }
        })
        .on_invoke("PaymentService.refund", move |req: RefundPaymentRequest| {
            let log = log_refund.clone();
            async move {
                log.lock().unwrap().push(format!("PaymentService: Refunding payment {}", req.transaction_id));
                Ok::<_, PaymentError>(Value::Null) // Compensation usually returns nothing specific
            }
        })
        .start();
    Ok(())
}

// --- Test ---

#[tokio::test]
async fn test_mixed_paradigm_scenario() -> Result<()> {
    let execution_log = Arc::new(Mutex::new(Vec::new()));

    // 1. Start Skald Server
    let skald_port = 9102;
    let skald_addr: SocketAddr = format!("127.0.0.1:{}", skald_port).parse()?;
    let server = Arc::new(SkaldServer::new());
    let server_clone = server.clone();
    tokio::spawn(async move {
        let listener = TcpTransportListener::bind(skald_addr).await.unwrap();
        server_clone.listen(Box::new(listener)).await.unwrap();
    });
    sleep(Duration::from_millis(100)).await;

    // 2. Start Services
    start_user_service(skald_addr, execution_log.clone()).await?;
    start_order_service(skald_addr, execution_log.clone()).await?;
    start_payment_service(skald_addr, execution_log.clone()).await?;
    sleep(Duration::from_millis(100)).await;

    // 3. Client: Send an Event (Event Sourcing)
    let event_client_transport = TcpTransport::connect(skald_addr).await?;
    let mut event_client = SkaldClient::new(event_client_transport);
    event_client.send_event(&UserCreatedEvent {
        user_id: "user-1".to_string(),
        username: "Alice".to_string(),
    }).await?;
    sleep(Duration::from_millis(100)).await;

    // 4. Client: Trigger a Saga (Durable Execution + Orchestration)
    let saga_client_transport = TcpTransport::connect(skald_addr).await?;
    let mut saga_client = SkaldClient::new(saga_client_transport);

    // Spawn a listener for the saga client (though submit_saga is fire-and-forget, it's good practice)
    let mut listen_client = saga_client.clone();
    tokio::spawn(async move {
        if let Err(e) = listen_client.listen().await {
            eprintln!("Saga client listener failed: {:?}", e);
        }
    });

    let saga_recipe = SagaDefinitionBuilder::new("PurchaseFlowSaga")
        .step("CreateOrder")
            .service("OrderService")
            .execute("OrderService.create", MessageTemplateBuilder::new()
                .field("user_id", "user-1")
                .field("item_id", "item-A")
                .field("quantity", 2)
                .build())
            .compensate("OrderService.cancel", MessageTemplateBuilder::new()
                .from_result("order_id", "{{CreateOrder.order_id}}")
                .build())
            .add()
        .step("ProcessPayment")
            .service("PaymentService")
            .execute("PaymentService.process", MessageTemplateBuilder::new()
                .from_result("order_id", "{{CreateOrder.order_id}}")
                .from_result("amount", "{{CreateOrder.total_amount}}")
                .build())
            .compensate("PaymentService.refund", MessageTemplateBuilder::new()
                .from_result("transaction_id", "{{ProcessPayment.transaction_id}}")
                .build())
            .add()
        .build();

    saga_client.submit_saga(saga_recipe).await?;
    sleep(Duration::from_millis(500)).await;

    // 5. Verify Log
    let log = execution_log.lock().unwrap();
    println!("Execution Log: {:?}", log);

    // Event Sourcing verification
    assert!(log.iter().any(|s| s == "UserService: User Alice created"));

    // Saga execution verification
    assert!(log.iter().any(|s| s.contains("OrderService: Creating order for user user-1")));
    assert!(log.iter().any(|s| s.contains("PaymentService: Processing payment for order")));

    // Verify result passing (indirectly via log messages)
    let _order_creation_log = log.iter().find(|s| s.contains("OrderService: Creating order for user user-1")).unwrap();
    let payment_processing_log = log.iter().find(|s| s.contains("PaymentService: Processing payment for order")).unwrap();
    assert!(payment_processing_log.contains("amount 20")); // 2 items * $10

    Ok(())
}
