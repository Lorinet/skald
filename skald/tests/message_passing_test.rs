use skald::client::{SkaldClient, ServiceBuilder};
use skald::server::SkaldServer;
use skald::transport::{TcpTransport, TcpTransportListener};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};

#[derive(Serialize, Deserialize, Debug, Clone)]
struct GreetingEvent {
    message: String,
}

#[tokio::test]
async fn test_basic_message_passing() -> Result<()> {
    let received_messages = Arc::new(Mutex::new(Vec::new()));

    // 1. Start Skald Server
    let addr: SocketAddr = "127.0.0.1:9100".parse()?;
    let server = Arc::new(SkaldServer::new(vec![]));
    let server_clone = server.clone();
    tokio::spawn(async move {
        let listener = TcpTransportListener::bind(addr).await.unwrap();
        server_clone.listen(Box::new(listener)).await.unwrap();
    });
    sleep(Duration::from_millis(50)).await;

    // 2. Start a Listener Service
    let listener_transport = TcpTransport::connect(addr).await?;
    let received_clone = received_messages.clone();

    ServiceBuilder::new("ListenerService", listener_transport).await?
        .connect().await?
        .on_event("greetings", move |payload: GreetingEvent| {
            let messages = received_clone.clone();
            async move {
                messages.lock().unwrap().push(payload.message);
                Ok(())
            }
        })
        .start();

    sleep(Duration::from_millis(50)).await;

    // 3. Start a Sender Client and Send a Message
    let sender_transport = TcpTransport::connect(addr).await?;
    let mut sender_client = SkaldClient::new(sender_transport);
    sender_client.send_event(&GreetingEvent {
        message: "Hello, Skald!".to_string(),
    }).await?;

    // 4. Wait and Verify
    sleep(Duration::from_millis(200)).await;
    let messages = received_messages.lock().unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0], "Hello, Skald!");

    Ok(())
}

// Implement the Message trait for GreetingEvent
impl skald::messaging::Message for GreetingEvent {
    fn topic(&self) -> String {
        "greetings".to_string()
    }
}
