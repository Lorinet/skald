use crate::messaging::{GenericMessage, deserialize};
use crate::transport::{TransportListener, CloneableTransport};
use crate::handler::{EventHandler, HandlerRegistry};
use crate::saga::{SagaBuilder, SagaRecipe};
use crate::core_topics;
use tokio::sync::mpsc;
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::Mutex;
use std::collections::HashMap;
use async_trait::async_trait;

// --- Service Registry ---
pub struct ServiceRegistry {
    services: HashMap<String, Arc<Mutex<Box<dyn CloneableTransport>>>>,
}

impl ServiceRegistry {
    pub fn new() -> Self { Self { services: HashMap::new() } }
    pub fn register(&mut self, name: String, transport: Box<dyn CloneableTransport>) {
        self.services.insert(name, Arc::new(Mutex::new(transport)));
    }
    pub fn get(&self, name: &str) -> Option<Arc<Mutex<Box<dyn CloneableTransport>>>> {
        self.services.get(name).cloned()
    }
}

// --- Core Server Handlers ---

/// Handles incoming Saga Recipes and executes them.
struct SagaRecipeHandler {
    service_registry: Arc<Mutex<ServiceRegistry>>,
}

#[async_trait]
impl EventHandler for SagaRecipeHandler {
    async fn handle(&self, message: GenericMessage) -> Result<()> {
        println!("Orchestrator: Received Saga Recipe");
        // Correctly deserialize the outer message first
        let submit_msg: crate::client::SubmitSagaRecipeMessage = deserialize(&message.payload)?;
        let recipe = submit_msg.recipe;

        let mut saga = SagaBuilder::from_recipe(recipe, self.service_registry.clone()).build();

        tokio::spawn(async move {
            println!("Orchestrator: Executing Saga...");
            if let Err(e) = saga.execute().await {
                eprintln!("Orchestrator: Saga Failed: {}", e);
            } else {
                println!("Orchestrator: Saga Completed");
            }
        });
        Ok(())
    }
}

// --- Skald Server ---
pub struct SkaldServer {
    sender: mpsc::Sender<GenericMessage>,
    receiver: Arc<Mutex<mpsc::Receiver<GenericMessage>>>,
    user_registry: Arc<Mutex<HandlerRegistry>>,
    service_registry: Arc<Mutex<ServiceRegistry>>,
}

impl SkaldServer {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(100);
        Self {
            sender,
            receiver: Arc::new(Mutex::new(receiver)),
            user_registry: Arc::new(Mutex::new(HandlerRegistry::new())),
            service_registry: Arc::new(Mutex::new(ServiceRegistry::new())),
        }
    }

    pub async fn listen(&self, mut listener: Box<dyn TransportListener>) -> Result<()> {
        println!("Skald Server listening...");
        loop {
            match listener.accept().await {
                Ok(transport) => {
                    let mut read_transport = transport.clone_box();
                    let write_transport = transport.clone_box();
                    let sender = self.sender.clone();
                    let service_registry = self.service_registry.clone();

                    tokio::spawn(async move {
                        let mut pending_write_transport = Some(write_transport);
                        loop {
                            match read_transport.receive().await {
                                Ok(data) => {
                                    if let Ok(msg) = deserialize::<GenericMessage>(&data) {
                                        if msg.topic == core_topics::REGISTER_SERVICE_TOPIC {
                                            if let Ok(reg_msg) = deserialize::<crate::client::RegisterServiceMessage>(&msg.payload) {
                                                println!("Registered service connection: {}", reg_msg.name);
                                                if let Some(wt) = pending_write_transport.take() {
                                                    service_registry.lock().await.register(reg_msg.name.clone(), wt);
                                                }
                                            }
                                        } else {
                                            let _ = sender.send(msg).await;
                                        }
                                    }
                                }
                                Err(_) => break,
                            }
                        }
                    });
                }
                Err(e) => eprintln!("Accept error: {}", e),
            }
        }
    }

    pub async fn run(&self) {
        println!("Skald Server processing...");

        // Register core server handlers
        let mut core_registry = HandlerRegistry::new();
        core_registry.register(core_topics::SUBMIT_SAGA_TOPIC.to_string(), SagaRecipeHandler {
            service_registry: self.service_registry.clone(),
        });

        let receiver = self.receiver.clone();
        let user_registry = self.user_registry.clone();

        loop {
            if let Some(msg) = receiver.lock().await.recv().await {
                println!("Processing message: {:?}", msg.topic);

                // Check core handlers first
                if let Some(handler) = core_registry.get(&msg.topic) {
                    let handler_clone = handler.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handler_clone.handle(msg).await {
                            eprintln!("Core handler error: {}", e);
                        }
                    });
                }
                // Then check user-registered handlers
                else if let Some(handler) = user_registry.lock().await.get(&msg.topic) {
                    let handler_clone = handler.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handler_clone.handle(msg).await {
                            eprintln!("User handler error: {}", e);
                        }
                    });
                } else {
                    println!("No handler for topic: {}", msg.topic);
                }
            }
        }
    }

    /// Registers a custom event handler for a specific topic.
    pub async fn register_handler<H: EventHandler + 'static>(&self, topic: String, handler: H) {
        self.user_registry.lock().await.register(topic, handler);
    }

    // Internal access for saga hydration
    pub fn get_service_registry(&self) -> Arc<Mutex<ServiceRegistry>> {
        self.service_registry.clone()
    }
}
