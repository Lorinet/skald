use crate::messaging::{SkaldMessage, deserialize, serialize, ResponseMessage, InvocationMessage};
use crate::transport::{TransportListener, CloneableTransport};
use crate::saga::SagaBuilder;
use anyhow::Result;
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use tokio::sync::{Mutex, oneshot};
use std::collections::HashMap;
use uuid::Uuid;

// --- Registries for Managing Connections and Services ---

#[derive(Clone)]
pub struct ConnectionRegistry {
    connections: Arc<Mutex<HashMap<usize, Box<dyn CloneableTransport>>>>,
}

impl ConnectionRegistry {
    fn new() -> Self {
        Self {
            connections: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn add(&self, id: usize, transport: Box<dyn CloneableTransport>) {
        self.connections.lock().await.insert(id, transport);
    }

    async fn get(&self, id: usize) -> Option<Box<dyn CloneableTransport>> {
        self.connections.lock().await.get(&id).map(|t| t.clone_box())
    }

    async fn broadcast(&self, data: &[u8], exclude_id: usize) {
        let connections = self.connections.lock().await;
        for (&id, transport) in connections.iter() {
            if id != exclude_id {
                let mut transport_clone = transport.clone_box();
                let data_clone = data.to_vec();
                tokio::spawn(async move {
                    if let Err(e) = transport_clone.send(&data_clone).await {
                        eprintln!("Failed to broadcast to {}: {}", id, e);
                    }
                });
            }
        }
    }
}

#[derive(Clone)]
pub struct ServiceRegistry {
    services: Arc<Mutex<HashMap<String, usize>>>,
}

impl ServiceRegistry {
    pub fn new() -> Self {
        Self {
            services: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn register(&self, name: String, connection_id: usize) {
        self.services.lock().await.insert(name, connection_id);
    }

    pub async fn get_connection_id(&self, name: &str) -> Option<usize> {
        self.services.lock().await.get(name).cloned()
    }
}

// --- Orchestrator & Routing ---

enum InvocationOrigin {
    External(usize),
    Internal(oneshot::Sender<ResponseMessage>),
}

#[derive(Clone)]
pub struct SagaOrchestrator {
    service_registry: ServiceRegistry,
    connection_registry: ConnectionRegistry,
    pending_invocations: Arc<Mutex<HashMap<String, InvocationOrigin>>>,
}

impl SagaOrchestrator {
    fn new(service_registry: ServiceRegistry, connection_registry: ConnectionRegistry) -> Self {
        Self {
            service_registry,
            connection_registry,
            pending_invocations: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Initiates an invocation from within the server (e.g., a Saga step).
    pub async fn invoke(&self, service_name: &str, topic: &str, payload: serde_json::Value) -> Result<ResponseMessage> {
        let conn_id = self.service_registry.get_connection_id(service_name).await
            .ok_or_else(|| anyhow::anyhow!("Service '{}' not found for saga invocation", service_name))?;

        let mut transport = self.connection_registry.get(conn_id).await
            .ok_or_else(|| anyhow::anyhow!("Transport for service '{}' not found", service_name))?;

        let correlation_id = Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();

        self.pending_invocations.lock().await.insert(
            correlation_id.clone(),
            InvocationOrigin::Internal(tx)
        );

        let invocation = SkaldMessage::Invocation(InvocationMessage {
            correlation_id,
            topic: topic.to_string(),
            payload,
        });

        transport.send(&serialize(&invocation)?).await?;

        rx.await.map_err(|e| anyhow::anyhow!("Saga invocation failed: {}", e))
    }

    /// Handles an invocation received from an external service, routing it to the target.
    pub async fn handle_invocation(&self, invocation: InvocationMessage, origin_conn_id: usize) -> Result<()> {
        self.pending_invocations.lock().await.insert(
            invocation.correlation_id.clone(),
            InvocationOrigin::External(origin_conn_id)
        );

        let topic_parts: Vec<&str> = invocation.topic.splitn(2, '.').collect();
        if topic_parts.len() == 2 {
            let service_name = topic_parts[0];
            if let Some(target_conn_id) = self.service_registry.get_connection_id(service_name).await {
                if let Some(mut target_transport) = self.connection_registry.get(target_conn_id).await {
                    let data = serialize(&SkaldMessage::Invocation(invocation))?;
                    target_transport.send(&data).await?;
                }
            }
        }
        Ok(())
    }

    /// Handles a response received from a service, routing it back to the origin.
    pub async fn handle_response(&self, response: ResponseMessage) -> Result<()> {
        if let Some(origin) = self.pending_invocations.lock().await.remove(&response.correlation_id) {
            match origin {
                InvocationOrigin::Internal(tx) => {
                    let _ = tx.send(response);
                }
                InvocationOrigin::External(origin_conn_id) => {
                    if let Some(mut origin_transport) = self.connection_registry.get(origin_conn_id).await {
                        let data = serialize(&SkaldMessage::Response(response))?;
                        origin_transport.send(&data).await?;
                    }
                }
            }
        }
        Ok(())
    }
}


// --- Skald Server ---

#[derive(Clone)]
pub struct SkaldServer {
    service_registry: ServiceRegistry,
    connection_registry: ConnectionRegistry,
    next_connection_id: Arc<AtomicUsize>,
    orchestrator: SagaOrchestrator,
}

impl SkaldServer {
    pub fn new() -> Self {
        let service_registry = ServiceRegistry::new();
        let connection_registry = ConnectionRegistry::new();
        let orchestrator = SagaOrchestrator::new(service_registry.clone(), connection_registry.clone());
        Self {
            service_registry,
            connection_registry,
            next_connection_id: Arc::new(AtomicUsize::new(1)),
            orchestrator,
        }
    }

    pub async fn listen(&self, mut listener: Box<dyn TransportListener>) -> Result<()> {
        println!("Skald Server listening...");
        loop {
            match listener.accept().await {
                Ok(transport) => {
                    let server = self.clone();
                    let conn_id = server.next_connection_id.fetch_add(1, Ordering::SeqCst);
                    server.connection_registry.add(conn_id, transport).await;

                    tokio::spawn(async move {
                        if let Err(e) = server.handle_connection(conn_id).await {
                            eprintln!("Connection handler for ID {} error: {}", conn_id, e);
                        }
                    });
                }
                Err(e) => eprintln!("Accept error: {}", e),
            }
        }
    }

    async fn handle_connection(self, conn_id: usize) -> Result<()> {
        let mut transport = self.connection_registry.get(conn_id).await
            .ok_or_else(|| anyhow::anyhow!("Transport for connection {} not found", conn_id))?;

        loop {
            let data = transport.receive().await?;
            if let Ok(msg) = deserialize::<SkaldMessage>(&data) {
                let server = self.clone();
                tokio::spawn(async move {
                    if let Err(e) = server.process_message(msg, conn_id).await {
                        eprintln!("Error processing message for conn {}: {}", conn_id, e);
                    }
                });
            }
        }
    }

    async fn process_message(self, msg: SkaldMessage, conn_id: usize) -> Result<()> {
        match msg {
            SkaldMessage::RegisterService(reg_msg) => {
                println!("Registered service connection: {}", reg_msg.name);
                self.service_registry.register(reg_msg.name, conn_id).await;
            }
            SkaldMessage::SubmitSaga(submit_msg) => {
                println!("Orchestrator: Received Saga Recipe");
                let mut saga = SagaBuilder::from_recipe(submit_msg.recipe, self.orchestrator.clone()).build();
                tokio::spawn(async move {
                    println!("Orchestrator: Executing Saga...");
                    if let Err(e) = saga.execute().await {
                        eprintln!("Orchestrator: Saga Failed: {}", e);
                    } else {
                        println!("Orchestrator: Saga Completed");
                    }
                });
            }
            SkaldMessage::Invocation(invocation) => {
                self.orchestrator.handle_invocation(invocation, conn_id).await?;
            }
            SkaldMessage::Response(response) => {
                self.orchestrator.handle_response(response).await?;
            }
            SkaldMessage::Event(_) => {
                if let Ok(data) = serialize(&msg) {
                    self.connection_registry.broadcast(&data, conn_id).await;
                }
            }
        }
        Ok(())
    }
}
