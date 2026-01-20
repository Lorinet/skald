use skald::transport::{TcpTransportListener, TcpTransport};
use skald::server::SkaldServer;
use skald::client::{SkaldClient, ServiceBuilder};
use skald::saga::builder::{SagaDefinitionBuilder, MessageTemplateBuilder};
use anyhow::Result;
use std::net::SocketAddr;
use tokio::time::{sleep, Duration};
use std::sync::{Arc, Mutex};
use serde::{Deserialize, Serialize};
use serde_json::Value;

// --- Message Structs for Testing ---

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Service2ExecuteRequest {
    initial_id: u32,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
struct Service2ExecuteResponse {
    processed_id: u32,
    order_id: String, // New field to pass to next step
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Service2ExecuteError {
    reason: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Service2CompensateRequest {
    order_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Service3ExecuteRequest {
    data: String,
    received_order_id: String, // Expecting this from previous step
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
struct Service3ExecuteResponse {
    status: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Service3ExecuteError {
    reason: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Service3CompensateRequest {
    data: String,
    order_id: String, // For compensation, might need original order_id
}

// --- Shared Log ---
type SharedLog = Arc<Mutex<Vec<String>>>;

// --- Service Functions ---

/// Simulates a microservice that connects to Skald, registers handlers, and listens.
async fn start_service_node(name: String, skald_addr: SocketAddr, log: SharedLog) -> Result<()> {
    let transport = TcpTransport::connect(skald_addr).await?;

    // Handler for s2.exec invocation
    let log_clone1 = log.clone();
    let name_clone1 = name.clone();

    // Handler for s2.comp invocation
    let log_clone_s2_comp = log.clone();
    let name_clone_s2_comp = name.clone();

    // Handler for s3.exec invocation
    let log_clone2 = log.clone();
    let name_clone2 = name.clone();

    // Handler for s3.comp invocation
    let log_clone_s3_comp = log.clone();
    let name_clone_s3_comp = name.clone();

    ServiceBuilder::new(&name, transport).await?
        .connect().await?
        .on_invoke("Service2.s2.exec", move |req: Service2ExecuteRequest| {
            let log = log_clone1.clone();
            let name = name_clone1.clone();
            async move {
                let order_id = format!("order-{}", req.initial_id);
                let log_msg = format!("{}: Received s2.exec with initial_id {} -> returning order_id {}", name, req.initial_id, order_id);
                println!("{}", log_msg);
                log.lock().unwrap().push(log_msg);
                Ok::<_, Service2ExecuteError>(Service2ExecuteResponse {
                    processed_id: req.initial_id + 100,
                    order_id,
                })
            }
        })
        .on_invoke("Service2.s2.comp", move |req: Service2CompensateRequest| {
            let log = log_clone_s2_comp.clone();
            let name = name_clone_s2_comp.clone();
            async move {
                let log_msg = format!("{}: Compensating s2.comp for order_id {}", name, req.order_id);
                println!("{}", log_msg);
                log.lock().unwrap().push(log_msg);
                Ok::<_, Service2ExecuteError>(Value::Null) // Compensation usually returns nothing specific
            }
        })
        .on_invoke("Service3.s3.exec", move |req: Service3ExecuteRequest| {
            let log = log_clone2.clone();
            let name = name_clone2.clone();
            async move {
                let log_msg = format!("{}: Received s3.exec with data '{}' and order_id '{}'", name, req.data, req.received_order_id);
                println!("{}", log_msg);
                log.lock().unwrap().push(log_msg);
                Ok::<_, Service3ExecuteError>(Service3ExecuteResponse { status: "processed".to_string() })
            }
        })
        .on_invoke("Service3.s3.comp", move |req: Service3CompensateRequest| {
            let log = log_clone_s3_comp.clone();
            let name = name_clone_s3_comp.clone();
            async move {
                let log_msg = format!("{}: Compensating s3.comp for data '{}' and order_id '{}'", name, req.data, req.order_id);
                println!("{}", log_msg);
                log.lock().unwrap().push(log_msg);
                Ok::<_, Service3ExecuteError>(Value::Null) // Compensation usually returns nothing specific
            }
        })
        .start();

    println!("{} started and registered", name);
    Ok(())
}

/// Simulates a client that triggers a saga.
async fn trigger_saga(skald_addr: SocketAddr) -> Result<()> {
    let transport = TcpTransport::connect(skald_addr).await?;
    let mut client = SkaldClient::new(transport);

    let recipe = SagaDefinitionBuilder::new("DistributedTestSaga")
        .step("Step1_Service2")
            .service("Service2")
            .execute("Service2.s2.exec", MessageTemplateBuilder::new()
                .field("initial_id", 101)
                .build())
            .compensate("Service2.s2.comp", MessageTemplateBuilder::new()
                .from_result("order_id", "{{Step1_Service2.order_id}}") // Pass result from previous step
                .build())
            .add()
        .step("Step2_Service3")
            .service("Service3")
            .execute("Service3.s3.exec", MessageTemplateBuilder::new()
                .field("data", "Hello from S3")
                .from_result("received_order_id", "{{Step1_Service2.order_id}}") // Use result from Step1
                .build())
            .compensate("Service3.s3.comp", MessageTemplateBuilder::new()
                .field("data", "Undo S3")
                .from_result("order_id", "{{Step1_Service2.order_id}}") // Use result from Step1 for compensation
                .build())
            .add()
        .build();

    client.submit_saga(recipe).await?;

    // Keep connection open briefly to ensure message is sent/received
    sleep(Duration::from_millis(100)).await;

    Ok(())
}

// --- Test ---

#[tokio::test]
async fn test_central_orchestration_with_client_handlers() -> Result<()> {
    let execution_log = Arc::new(Mutex::new(Vec::new()));

    // 1. Start Skald Server
    let skald_port = 9093; // Use a different port
    let skald_addr: SocketAddr = format!("127.0.0.1:{}", skald_port).parse()?;
    let skald_server = Arc::new(SkaldServer::new(vec![]));

    let server_clone = skald_server.clone();
    tokio::spawn(async move {
        let listener = TcpTransportListener::bind(skald_addr).await.unwrap();
        server_clone.listen(Box::new(listener)).await.unwrap();
    });

    sleep(Duration::from_millis(100)).await;

    // 2. Start Service Nodes
    start_service_node("Service2".to_string(), skald_addr, execution_log.clone()).await?;
    start_service_node("Service3".to_string(), skald_addr, execution_log.clone()).await?;

    sleep(Duration::from_millis(100)).await;

    // 3. Trigger the Saga
    trigger_saga(skald_addr).await?;

    // 4. Wait for execution to complete
    sleep(Duration::from_millis(500)).await;

    // 5. Verify the execution log
    let log = execution_log.lock().unwrap();
    println!("Execution Log: {:?}", log);

    // Verify Step1 execution and its result passing
    let s2_exec_log = log.iter().find(|s| s.contains("Service2: Received s2.exec with initial_id 101")).expect("Service2 s2.exec not found");
    assert!(s2_exec_log.contains("returning order_id order-101"));

    // Verify Step2 execution and that it received the order_id from Step1
    let s3_exec_log = log.iter().find(|s| s.contains("Service3: Received s3.exec with data 'Hello from S3'")).expect("Service3 s3.exec not found");
    assert!(s3_exec_log.contains("and order_id 'order-101'"));

    Ok(())
}
