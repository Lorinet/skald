use skald::client::SkaldClient;
use skald::server::SkaldServer;
use skald::transport::{TcpTransport, TcpTransportListener};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tokio::time::{sleep, Duration};
use std::sync::Arc;

#[derive(Serialize, Deserialize, Debug)]
struct AddRequest {
    a: i32,
    b: i32,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct AddResponse {
    sum: i32,
}

#[derive(Serialize, Deserialize, Debug)]
struct MathError {
    reason: String,
}

#[tokio::test]
async fn test_durable_execution() -> Result<()> {
    // 1. Start Skald Server
    let addr: SocketAddr = "127.0.0.1:9101".parse()?;
    let server = Arc::new(SkaldServer::new());
    let server_clone = server.clone();
    tokio::spawn(async move {
        let listener = TcpTransportListener::bind(addr).await.unwrap();
        server_clone.listen(Box::new(listener)).await.unwrap();
    });
    sleep(Duration::from_millis(50)).await;

    // 2. Start the Calculator Service
    let calc_transport = TcpTransport::connect(addr).await?;
    let mut calc_client = SkaldClient::new(calc_transport);
    calc_client.register_service("CalculatorService").await?;

    calc_client.on_invoke("CalculatorService.add", |req: AddRequest| async move {
        let sum = req.a + req.b;
        Ok::<_, MathError>(AddResponse { sum })
    });

    tokio::spawn(async move {
        calc_client.listen().await.unwrap();
    });
    sleep(Duration::from_millis(50)).await;

    // 3. Start the Requester Client and Invoke the Method
    let req_transport = TcpTransport::connect(addr).await?;
    let mut req_client = SkaldClient::new(req_transport);

    // Spawn a listener for the requester client to receive the response
    let mut listen_client = req_client.clone();
    tokio::spawn(async move {
        listen_client.listen().await.unwrap();
    });

    let response: Result<AddResponse, MathError> = req_client
        .invoke("CalculatorService.add", AddRequest { a: 5, b: 10 })
        .await?;

    // 4. Verify the result
    assert!(response.is_ok());
    assert_eq!(response.unwrap(), AddResponse { sum: 15 });

    Ok(())
}
