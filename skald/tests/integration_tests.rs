use skald::transport::{TcpTransportListener, TcpTransport, Transport, TransportListener};
use skald::server::SkaldServer;
use skald::client::SkaldClient;
use skald::messaging::{Message, GenericMessage};
use skald::handler::EventHandler;
use skald::saga::{SagaBuilder, SagaStep, SagaState};
use async_trait::async_trait;
use anyhow::Result;
use std::net::SocketAddr;
use tokio::time::{sleep, Duration};
use std::sync::{Arc, Mutex};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
struct TestEvent {
    id: u32,
    content: String,
}

impl Message for TestEvent {
    fn topic(&self) -> String {
        "test.topic".to_string()
    }
}

struct TestHandler {
    received: Arc<Mutex<Vec<TestEvent>>>,
}

#[async_trait]
impl EventHandler for TestHandler {
    async fn handle(&self, message: GenericMessage) -> Result<()> {
        let event: TestEvent = skald::messaging::deserialize(&message.payload)?;
        self.received.lock().unwrap().push(event);
        Ok(())
    }
}

#[tokio::test]
async fn test_tcp_transport_and_messaging() -> Result<()> {
    let addr: SocketAddr = "127.0.0.1:8081".parse()?;
    let server = Arc::new(SkaldServer::new());
    let received_events = Arc::new(Mutex::new(Vec::new()));

    server.register_handler("test.topic".to_string(), TestHandler {
        received: received_events.clone(),
    }).await;

    let server_clone = server.clone();
    tokio::spawn(async move {
        let listener = TcpTransportListener::bind(addr).await.unwrap();
        server_clone.listen(Box::new(listener)).await.unwrap();
    });

    // Give server time to start
    sleep(Duration::from_millis(100)).await;

    // Start processing loop
    let server_clone_run = server.clone();
    tokio::spawn(async move {
        server_clone_run.run().await;
    });

    let transport = TcpTransport::connect(addr).await?;
    let mut client = SkaldClient::new(transport);

    let event = TestEvent { id: 1, content: "Hello World".to_string() };
    client.send_event(&event).await?;

    // Wait for processing
    sleep(Duration::from_millis(200)).await;

    let received = received_events.lock().unwrap();
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].id, 1);
    assert_eq!(received[0].content, "Hello World");

    Ok(())
}

struct MockStep {
    name: String,
    should_fail: bool,
    executed: Arc<Mutex<bool>>,
    compensated: Arc<Mutex<bool>>,
    result_data: Option<Vec<u8>>,
}

#[async_trait]
impl SagaStep for MockStep {
    fn name(&self) -> String {
        self.name.clone()
    }

    async fn execute(&self) -> Result<Option<Vec<u8>>> {
        if self.should_fail {
            anyhow::bail!("Step failed intentionally");
        }
        *self.executed.lock().unwrap() = true;
        Ok(self.result_data.clone())
    }

    async fn compensate(&self) -> Result<()> {
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
        result_data: Some(vec![1, 2, 3]),
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
    // This test verifies that we can retrieve the result of a step if we were to inspect the state
    // In a real scenario, subsequent steps might use this data, but here we just check it was returned.

    let executed1 = Arc::new(Mutex::new(false));
    let expected_data = vec![0xDE, 0xAD, 0xBE, 0xEF];

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

    // We can't easily inspect the intermediate state of a completed saga with the current API
    // unless we add a history feature, but we can verify the saga completed successfully.
    // However, if we wanted to verify the result was captured, we'd need to expose the execution history.
    // For now, we just ensure it runs.

    assert!(matches!(saga.get_state(), SagaState::Completed));

    Ok(())
}
