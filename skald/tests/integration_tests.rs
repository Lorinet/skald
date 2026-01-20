use skald::transport::{TcpTransportListener, TcpTransport};
use skald::server::SkaldServer;
use skald::client::SkaldClient;
use skald::messaging::Message;
use skald::saga::{SagaBuilder, SagaStep, SagaState};
use async_trait::async_trait;
use anyhow::Result;
use std::net::SocketAddr;
use tokio::time::{sleep, Duration};
use std::sync::{Arc, Mutex};
use serde::{Serialize, Deserialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct TestEvent {
    id: u32,
    content: String,
}

impl Message for TestEvent {
    fn topic(&self) -> String {
        "test.topic".to_string()
    }
}

#[tokio::test]
async fn test_tcp_transport_and_messaging() -> Result<()> {
    let addr: SocketAddr = "127.0.0.1:8081".parse()?;
    let server = Arc::new(SkaldServer::new());
    let received_events = Arc::new(Mutex::new(Vec::<TestEvent>::new()));

    // 1. Start Server
    let server_clone = server.clone();
    tokio::spawn(async move {
        let listener = TcpTransportListener::bind(addr).await.unwrap();
        server_clone.listen(Box::new(listener)).await.unwrap();
    });
    sleep(Duration::from_millis(50)).await;

    // 2. Start a Listener Client
    let received_clone = received_events.clone();
    let listener_transport = TcpTransport::connect(addr).await?;
    let mut listener_client = SkaldClient::new(listener_transport);
    listener_client.on("test.topic", move |event: TestEvent| {
        let events = received_clone.clone();
        async move {
            events.lock().unwrap().push(event);
            Ok(())
        }
    });
    tokio::spawn(async move { listener_client.listen().await.unwrap(); });
    sleep(Duration::from_millis(50)).await;

    // 3. Start a Sender Client and Send an Event
    let sender_transport = TcpTransport::connect(addr).await?;
    let mut sender_client = SkaldClient::new(sender_transport);
    let event_to_send = TestEvent { id: 1, content: "Hello World".to_string() };
    sender_client.send_event(&event_to_send).await?;

    // 4. Wait and Verify
    sleep(Duration::from_millis(200)).await;
    let received = received_events.lock().unwrap();
    assert_eq!(received.len(), 1);
    assert_eq!(received[0], event_to_send);

    Ok(())
}

struct MockStep {
    name: String,
    should_fail: bool,
    executed: Arc<Mutex<bool>>,
    compensated: Arc<Mutex<bool>>,
    result_data: Option<Value>,
}

#[async_trait]
impl SagaStep for MockStep {
    fn name(&self) -> String {
        self.name.clone()
    }

    async fn execute(&self, _context: &HashMap<String, Value>) -> Result<Value> {
        if self.should_fail {
            anyhow::bail!("Step failed intentionally");
        }
        *self.executed.lock().unwrap() = true;
        Ok(self.result_data.clone().unwrap_or(Value::Null))
    }

    async fn compensate(&self, _context: &HashMap<String, Value>, _error: Option<&Value>) -> Result<()> {
        *self.compensated.lock().unwrap() = true;
        Ok(())
    }
}

#[tokio::test]
async fn test_saga_success() -> Result<()> {
    let executed1 = Arc::new(Mutex::new(false));
    let executed2 = Arc::new(Mutex::new(false));

    let step1 = Box::new(MockStep {
        name: "step1".to_string(),
        should_fail: false,
        executed: executed1.clone(),
        compensated: Arc::new(Mutex::new(false)),
        result_data: Some(serde_json::json!({"status": "ok"})),
    });

    let step2 = Box::new(MockStep {
        name: "step2".to_string(),
        should_fail: false,
        executed: executed2.clone(),
        compensated: Arc::new(Mutex::new(false)),
        result_data: None,
    });

    let mut saga = SagaBuilder::new()
        .step(step1)
        .step(step2)
        .build();

    saga.execute().await?;

    assert!(matches!(saga.get_state(), SagaState::Completed));
    assert!(*executed1.lock().unwrap());
    assert!(*executed2.lock().unwrap());

    Ok(())
}

#[tokio::test]
async fn test_saga_failure_and_compensation() -> Result<()> {
    let executed1 = Arc::new(Mutex::new(false));
    let compensated1 = Arc::new(Mutex::new(false));

    let step1 = Box::new(MockStep {
        name: "step1".to_string(),
        should_fail: false,
        executed: executed1.clone(),
        compensated: compensated1.clone(),
        result_data: None,
    });

    let step2 = Box::new(MockStep {
        name: "step2".to_string(),
        should_fail: true,
        executed: Arc::new(Mutex::new(false)),
        compensated: Arc::new(Mutex::new(false)),
        result_data: None,
    });

    let mut saga = SagaBuilder::new()
        .step(step1)
        .step(step2)
        .build();

    let result = saga.execute().await;

    assert!(result.is_err());
    assert!(matches!(saga.get_state(), SagaState::Aborted));
    assert!(*executed1.lock().unwrap()); // Step 1 executed
    assert!(*compensated1.lock().unwrap()); // Step 1 compensated

    Ok(())
}

#[tokio::test]
async fn test_saga_state_inspection() -> Result<()> {
    let executed1 = Arc::new(Mutex::new(false));

    let step1 = Box::new(MockStep {
        name: "step1".to_string(),
        should_fail: true,
        executed: executed1.clone(),
        compensated: Arc::new(Mutex::new(false)),
        result_data: None,
    });

    let mut saga = SagaBuilder::new()
        .step(step1)
        .build();

    let _ = saga.execute().await;

    let state = saga.get_state();
    match state {
        SagaState::Aborted => {
             // Expected final state
        },
        _ => panic!("Expected Aborted state, got {:?}", state),
    }

    Ok(())
}

#[tokio::test]
async fn test_saga_step_result_passing() -> Result<()> {
    let executed1 = Arc::new(Mutex::new(false));
    let expected_data = serde_json::json!({"key": "value"});

    let step1 = Box::new(MockStep {
        name: "step1".to_string(),
        should_fail: false,
        executed: executed1.clone(),
        compensated: Arc::new(Mutex::new(false)),
        result_data: Some(expected_data.clone()),
    });

    let mut saga = SagaBuilder::new()
        .step(step1)
        .build();

    saga.execute().await?;

    assert!(matches!(saga.get_state(), SagaState::Completed));

    Ok(())
}
