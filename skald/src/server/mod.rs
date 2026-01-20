use crate::messaging::{SkaldMessage, deserialize, serialize, ResponseMessage, InvocationMessage};
use crate::transport::{TransportListener, CloneableTransport};
use crate::saga::SagaBuilder;
use anyhow::Result;
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use tokio::sync::{Mutex, oneshot};
use std::collections::HashMap;
use uuid::Uuid;
use std::process::Command;

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

    async fn remove(&self, id: usize) {
        self.connections.lock().await.remove(&id);
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
pub struct ServiceInstance {
    pub connection_id: usize,
    pub pending_invocations: Arc<AtomicUsize>,
}

#[derive(Clone)]
pub struct ServiceRegistry {
    // Map service name to a list of instances
    services: Arc<Mutex<HashMap<String, Vec<ServiceInstance>>>>,
    // Map connection ID to service name for reverse lookup on disconnect
    connection_map: Arc<Mutex<HashMap<usize, String>>>,
}

impl ServiceRegistry {
    pub fn new() -> Self {
        Self {
            services: Arc::new(Mutex::new(HashMap::new())),
            connection_map: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn register(&self, name: String, connection_id: usize, max_instances: Option<usize>) -> Result<()> {
        let mut services = self.services.lock().await;
        let instances = services.entry(name.clone()).or_insert_with(Vec::new);

        if let Some(max) = max_instances {
            if instances.len() >= max {
                return Err(anyhow::anyhow!("Service '{}' has reached max instances ({})", name, max));
            }
        }

        instances.push(ServiceInstance {
            connection_id,
            pending_invocations: Arc::new(AtomicUsize::new(0)),
        });

        self.connection_map.lock().await.insert(connection_id, name);
        Ok(())
    }

    pub async fn unregister(&self, connection_id: usize) -> Option<String> {
        let mut conn_map = self.connection_map.lock().await;
        if let Some(name) = conn_map.remove(&connection_id) {
            let mut services = self.services.lock().await;
            if let Some(instances) = services.get_mut(&name) {
                instances.retain(|i| i.connection_id != connection_id);
                if instances.is_empty() {
                    services.remove(&name);
                }
            }
            return Some(name);
        }
        None
    }

    pub async fn get_best_instance(&self, name: &str) -> Option<ServiceInstance> {
        let services = self.services.lock().await;
        if let Some(instances) = services.get(name) {
            // Load balancing: find instance with smallest pending invocations
            instances.iter()
                .min_by_key(|i| i.pending_invocations.load(Ordering::Relaxed))
                .cloned()
        } else {
            None
        }
    }

    pub async fn decrement_pending(&self, connection_id: usize) {
        let conn_map = self.connection_map.lock().await;
        if let Some(name) = conn_map.get(&connection_id) {
            let services = self.services.lock().await;
            if let Some(instances) = services.get(name) {
                if let Some(instance) = instances.iter().find(|i| i.connection_id == connection_id) {
                    let current = instance.pending_invocations.load(Ordering::Relaxed);
                    if current > 0 {
                        instance.pending_invocations.fetch_sub(1, Ordering::Relaxed);
                    }
                }
            }
        }
    }
}

// --- Service Management ---

#[derive(Clone, Debug)]
pub struct ServiceDefinition {
    pub name: String,
    pub executable: String,
    pub args: Vec<String>,
    pub pool_size: usize,
}

#[derive(Clone)]
pub struct ServiceManager {
    definitions: Arc<HashMap<String, ServiceDefinition>>,
    // Map service name to a generated token for authentication
    tokens: Arc<Mutex<HashMap<String, String>>>,
    // Map service name to an authoritative max worker count set by the first instance
    authoritative_limits: Arc<Mutex<HashMap<String, usize>>>,
}

impl ServiceManager {
    pub fn new(definitions: Vec<ServiceDefinition>) -> Self {
        let mut def_map = HashMap::new();
        for def in definitions {
            def_map.insert(def.name.clone(), def);
        }
        Self {
            definitions: Arc::new(def_map),
            tokens: Arc::new(Mutex::new(HashMap::new())),
            authoritative_limits: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn get_definition(&self, name: &str) -> Option<ServiceDefinition> {
        self.definitions.get(name).cloned()
    }

    pub async fn spawn_service(&self, name: &str) -> Result<()> {
        if let Some(def) = self.definitions.get(name) {
            let token = Uuid::new_v4().to_string();
            self.tokens.lock().await.insert(name.to_string(), token.clone());

            println!("Spawning service: {} (pool size: {})", name, def.pool_size);

            for _ in 0..def.pool_size {
                let mut cmd = Command::new(&def.executable);
                cmd.args(&def.args);
                // Pass token via environment variable
                cmd.env("SKALD_SERVICE_TOKEN", &token);
                cmd.env("SKALD_SERVICE_NAME", name);

                // Spawn detached process
                match cmd.spawn() {
                    Ok(_) => println!("Spawned instance of {}", name),
                    Err(e) => eprintln!("Failed to spawn {}: {}", name, e),
                }
            }
            Ok(())
        } else {
            Err(anyhow::anyhow!("Service definition not found for {}", name))
        }
    }

    pub async fn spawn_all(&self) {
        let definitions = self.definitions.clone();
        for name in definitions.keys() {
            let _ = self.spawn_service(name).await;
        }
    }

    pub async fn spawn_single(&self, name: &str) -> Result<()> {
        if let Some(def) = self.definitions.get(name) {
            let mut tokens = self.tokens.lock().await;
            let token = tokens.entry(name.to_string()).or_insert_with(|| Uuid::new_v4().to_string()).clone();

            let mut cmd = Command::new(&def.executable);
            cmd.args(&def.args);
            cmd.env("SKALD_SERVICE_TOKEN", &token);
            cmd.env("SKALD_SERVICE_NAME", name);

            match cmd.spawn() {
                Ok(_) => println!("Spawned single instance of {}", name),
                Err(e) => eprintln!("Failed to spawn {}: {}", name, e),
            }
            Ok(())
        } else {
            Err(anyhow::anyhow!("Service definition not found for {}", name))
        }
    }

    pub async fn validate_token(&self, name: &str, token: Option<&str>) -> bool {
        if let Some(def) = self.definitions.get(name) {
            // If it's a managed service, it must have a valid token
            let stored_tokens = self.tokens.lock().await;
            if let Some(stored_token) = stored_tokens.get(name) {
                 return token == Some(stored_token);
            }
            // If we have a definition but no token generated yet (maybe not spawned yet?), reject
            return false;
        }
        // If not managed, allow (or implement other auth policy)
        true
    }

    pub fn is_managed(&self, name: &str) -> bool {
        self.definitions.contains_key(name)
    }

    pub fn get_pool_size(&self, name: &str) -> usize {
        self.definitions.get(name).map(|d| d.pool_size).unwrap_or(0)
    }

    pub async fn get_or_set_authoritative_limit(&self, name: &str, requested_limit: Option<usize>) -> Option<usize> {
        let mut limits = self.authoritative_limits.lock().await;
        if let Some(&limit) = limits.get(name) {
            return Some(limit);
        }

        if let Some(limit) = requested_limit {
            limits.insert(name.to_string(), limit);
            return Some(limit);
        }

        None
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
        let instance = self.service_registry.get_best_instance(service_name).await
            .ok_or_else(|| anyhow::anyhow!("Service '{}' not found for saga invocation", service_name))?;

        let mut transport = self.connection_registry.get(instance.connection_id).await
            .ok_or_else(|| anyhow::anyhow!("Transport for service '{}' not found", service_name))?;

        let correlation_id = Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();

        self.pending_invocations.lock().await.insert(
            correlation_id.clone(),
            InvocationOrigin::Internal(tx)
        );

        // Increment pending invocations
        instance.pending_invocations.fetch_add(1, Ordering::Relaxed);

        let invocation = SkaldMessage::Invocation(InvocationMessage {
            correlation_id: correlation_id.clone(),
            topic: topic.to_string(),
            payload,
        });

        if let Err(e) = transport.send(&serialize(&invocation)?).await {
             // Decrement on send failure
             instance.pending_invocations.fetch_sub(1, Ordering::Relaxed);
             self.pending_invocations.lock().await.remove(&correlation_id);
             return Err(e);
        }

        let result = rx.await.map_err(|e| anyhow::anyhow!("Saga invocation failed: {}", e));
        result
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
            if let Some(instance) = self.service_registry.get_best_instance(service_name).await {
                if let Some(mut target_transport) = self.connection_registry.get(instance.connection_id).await {
                    // Increment pending
                    instance.pending_invocations.fetch_add(1, Ordering::Relaxed);

                    let data = serialize(&SkaldMessage::Invocation(invocation))?;
                    if let Err(e) = target_transport.send(&data).await {
                         instance.pending_invocations.fetch_sub(1, Ordering::Relaxed);
                         return Err(e);
                    }
                }
            }
        }
        Ok(())
    }

    /// Handles a response received from a service, routing it back to the origin.
    pub async fn handle_response(&self, response: ResponseMessage, from_conn_id: usize) -> Result<()> {
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

        // Decrement load counter
        self.service_registry.decrement_pending(from_conn_id).await;

        Ok(())
    }
}


// --- Skald Server ---

pub struct SkaldServerInner {
    service_registry: ServiceRegistry,
    connection_registry: ConnectionRegistry,
    next_connection_id: AtomicUsize,
    orchestrator: SagaOrchestrator,
    service_manager: ServiceManager,
}

#[derive(Clone)]
pub struct SkaldServer {
    inner: Arc<SkaldServerInner>,
}

impl std::ops::Deref for SkaldServer {
    type Target = SkaldServerInner;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl SkaldServer {
    pub fn new(service_definitions: Vec<ServiceDefinition>) -> Self {
        let service_registry = ServiceRegistry::new();
        let connection_registry = ConnectionRegistry::new();
        let orchestrator = SagaOrchestrator::new(service_registry.clone(), connection_registry.clone());
        let service_manager = ServiceManager::new(service_definitions);

        let inner = SkaldServerInner {
            service_registry,
            connection_registry,
            next_connection_id: AtomicUsize::new(1),
            orchestrator,
            service_manager,
        };

        let server = Self {
            inner: Arc::new(inner),
        };

        // Spawn managed services
        let server_clone = server.clone();
        tokio::spawn(async move {
            server_clone.spawn_managed_services().await;
        });

        server
    }

    async fn spawn_managed_services(&self) {
        self.service_manager.spawn_all().await;
    }

    pub async fn listen(&self, mut listener: Box<dyn TransportListener>) -> Result<()> {
        println!("Skald Server listening...");

        // Initial spawn of services
        self.service_manager.spawn_all().await;

        loop {
            match listener.accept().await {
                Ok(transport) => {
                    let server = self.clone();
                    let conn_id = server.next_connection_id.fetch_add(1, Ordering::SeqCst);
                    server.connection_registry.add(conn_id, transport).await;

                    tokio::spawn(async move {
                        if let Err(e) = server.handle_connection(conn_id).await {
                            eprintln!("Connection handler for ID {} error: {}", conn_id, e);
                            // Handle disconnect/cleanup
                            server.handle_disconnect(conn_id).await;
                        }
                    });
                }
                Err(e) => eprintln!("Accept error: {}", e),
            }
        }
    }

    async fn handle_connection(&self, conn_id: usize) -> Result<()> {
        let mut transport = self.connection_registry.get(conn_id).await
            .ok_or_else(|| anyhow::anyhow!("Transport for connection {} not found", conn_id))?;

        loop {
            // We need to handle potential disconnects here.
            // If receive fails, it likely means the connection is closed.
            match transport.receive().await {
                Ok(data) => {
                    if let Ok(msg) = deserialize::<SkaldMessage>(&data) {
                        let server = self.clone();
                        tokio::spawn(async move {
                            if let Err(e) = server.process_message(msg, conn_id).await {
                                eprintln!("Error processing message for conn {}: {}", conn_id, e);
                            }
                        });
                    }
                }
                Err(e) => {
                    // Connection closed or error
                    return Err(e);
                }
            }
        }
    }

    async fn handle_disconnect(&self, conn_id: usize) {
        println!("Handling disconnect for connection {}", conn_id);
        self.connection_registry.remove(conn_id).await;
        if let Some(service_name) = self.service_registry.unregister(conn_id).await {
            println!("Service '{}' disconnected (conn_id: {})", service_name, conn_id);

            // Check if we need to restart it
            if self.service_manager.is_managed(&service_name) {
                println!("Restarting managed service: {}", service_name);
                // We need to spawn a new instance.
                // Note: spawn_service spawns pool_size instances.
                // But here we only lost one connection (one instance).
                // We should probably only spawn one replacement.
                // But our spawn_service spawns the whole pool.
                // We need a way to spawn just one.
                // For now, let's just call spawn_single.
                if let Err(e) = self.service_manager.spawn_single(&service_name).await {
                    eprintln!("Failed to restart service {}: {}", service_name, e);
                }
            }
        }
    }

    async fn process_message(self, msg: SkaldMessage, conn_id: usize) -> Result<()> {
        match msg {
            SkaldMessage::RegisterService(reg_msg) => {
                // Validate token if managed
                if !self.service_manager.validate_token(&reg_msg.name, reg_msg.token.as_deref()).await {
                    println!("Rejected registration for service '{}': Invalid token", reg_msg.name);
                    // Optionally close connection
                    return Err(anyhow::anyhow!("Authentication failed for service {}", reg_msg.name));
                }

                // Check pool size limits
                let max_instances = if self.service_manager.is_managed(&reg_msg.name) {
                    Some(self.service_manager.get_pool_size(&reg_msg.name))
                } else {
                    // For unmanaged services, check if an authoritative limit has been set
                    // or if this registration is setting it.
                    let limit = self.service_manager.get_or_set_authoritative_limit(&reg_msg.name, reg_msg.max_workers).await;

                    if let Some(l) = limit {
                        Some(l)
                    } else if let Some(def) = self.service_manager.get_definition(&reg_msg.name) {
                        Some(def.pool_size)
                    } else {
                        // Default to 1 if no limit is known or provided
                        Some(1)
                    }
                };

                match self.service_registry.register(reg_msg.name.clone(), conn_id, max_instances).await {
                    Ok(_) => println!("Registered service connection: {}", reg_msg.name),
                    Err(e) => {
                        println!("Registration failed for {}: {}", reg_msg.name, e);
                        return Err(e);
                    }
                }
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
                self.orchestrator.handle_response(response, conn_id).await?;
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
