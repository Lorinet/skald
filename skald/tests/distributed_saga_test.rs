use skald::transport::{TcpTransportListener, TcpTransport};
use skald::server::SkaldServer;
use skald::client::SkaldClient;
use skald::messaging::{Message, serialize};
use skald::saga::{SagaRecipe, RemoteStepDefinition};
use anyhow::Result;
use std::net::SocketAddr;
use tokio::time::{sleep, Duration};
use std::sync::{Arc, Mutex};
use serde::{Serialize, Deserialize};

// --- Events for Services ---

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Service2Execute { id: u32 }
impl Message for Service2Execute { fn topic(&self) -> String { "s2.exec".to_string() } }

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Service2Compensate { id: u32 }
impl Message for Service2Compensate { fn topic(&self) -> String { "s2.comp".to_string() } }

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Service3Execute { data: String }
impl Message for Service3Execute { fn topic(&self) -> String { "s3.exec".to_string() } }

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Service3Compensate { data: String }
impl Message for Service3Compensate { fn topic(&self) -> String { "s3.comp".to_string() } }

// --- Shared Log ---
type SharedLog = Arc<Mutex<Vec<String>>>;

// --- Service Functions ---

/// Simulates a microservice that connects to Skald, registers, and waits for commands.
async fn start_service_node(name: String, skald_addr: SocketAddr, log: SharedLog) -> Result<()> {
    let transport = TcpTransport::connect(skald_addr).await?;
    let mut client = SkaldClient::new(transport);

    // Use the high-level API to register
    client.register_service(&name).await?;

    // Listen for commands from the orchestrator
    let name_clone = name.clone();
    tokio::spawn(async move {
        loop {
            match client.receive_message().await {
                Ok(msg) => {
                    let log_msg = format!("{}: Received {}", name_clone, msg.topic);
                    println!("{}", log_msg);
                    log.lock().unwrap().push(log_msg);
                }
                Err(_) => break, // Connection closed
            }
        }
    });

    Ok(())
}

/// Simulates a client/service that triggers a complex business process.
async fn trigger_saga(skald_addr: SocketAddr) -> Result<()> {
    let transport = TcpTransport::connect(skald_addr).await?;
    let mut client = SkaldClient::new(transport);

    // Define the business process as a data-only recipe
    let recipe = SagaRecipe {
        steps: vec![
            RemoteStepDefinition {
                name: "Step1_Service2".to_string(),
                service_name: "Service2".to_string(),
                execute_topic: Service2Execute{id: 0}.topic(),
                execute_payload: serialize(&Service2Execute { id: 101 })?,
                compensate_topic: Service2Compensate{id: 0}.topic(),
                compensate_payload: serialize(&Service2Compensate { id: 101 })?,
            },
            RemoteStepDefinition {
                name: "Step2_Service3".to_string(),
                service_name: "Service3".to_string(),
                execute_topic: Service3Execute{data: "".to_string()}.topic(),
                execute_payload: serialize(&Service3Execute { data: "Hello".to_string() })?,
                compensate_topic: Service3Compensate{data: "".to_string()}.topic(),
                compensate_payload: serialize(&Service3Compensate { data: "Undo".to_string() })?,
            },
        ],
    };

    // Use the high-level API to submit the saga
    client.submit_saga(recipe).await?;

    Ok(())
}

// --- Test ---

#[tokio::test]
async fn test_central_orchestration_with_clean_api() -> Result<()> {
    let execution_log = Arc::new(Mutex::new(Vec::new()));

    // 1. Start Skald Server (The Orchestrator)
    let skald_port = 9091; // Use a different port to avoid conflicts
    let skald_addr: SocketAddr = format!("127.0.0.1:{}", skald_port).parse()?;
    let skald_server = Arc::new(SkaldServer::new());

    let server_clone = skald_server.clone();
    tokio::spawn(async move {
        let listener = TcpTransportListener::bind(skald_addr).await.unwrap();
        // The listen loop now handles registration internally
        server_clone.listen(Box::new(listener)).await.unwrap();
    });
    let server_run = skald_server.clone();
    tokio::spawn(async move {
        // The run loop now handles saga submissions internally
        server_run.run().await;
    });

    sleep(Duration::from_millis(100)).await;

    // 2. Start Service 2 and Service 3
    start_service_node("Service2".to_string(), skald_addr, execution_log.clone()).await?;
    start_service_node("Service3".to_string(), skald_addr, execution_log.clone()).await?;

    sleep(Duration::from_millis(100)).await;

    // 3. Trigger the Saga via a client
    trigger_saga(skald_addr).await?;

    // 4. Wait for execution
    sleep(Duration::from_millis(500)).await;

    // 5. Verify Log
    let log = execution_log.lock().unwrap();
    println!("Execution Log: {:?}", log);

    assert!(log.iter().any(|s| s.contains("Service2: Received s2.exec")));
    assert!(log.iter().any(|s| s.contains("Service3: Received s3.exec")));

    // We can't easily check for "Saga Completed" in the shared log because that logic is now internal to the server.
    // But we can see the effects (the service logs). This is a more realistic integration test.

    Ok(())
}
