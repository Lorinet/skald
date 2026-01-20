use async_trait::async_trait;
use crate::messaging::GenericMessage;
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;

#[async_trait]
pub trait EventHandler: Send + Sync {
    async fn handle(&self, message: GenericMessage) -> Result<()>;
}

pub struct HandlerRegistry {
    handlers: HashMap<String, Arc<dyn EventHandler>>,
}

impl HandlerRegistry {
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
        }
    }

    pub fn register<H: EventHandler + 'static>(&mut self, topic: String, handler: H) {
        self.handlers.insert(topic, Arc::new(handler));
    }

    pub fn get(&self, topic: &str) -> Option<Arc<dyn EventHandler>> {
        self.handlers.get(topic).cloned()
    }
}
