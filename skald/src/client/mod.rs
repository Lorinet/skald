use crate::transport::Transport;
use crate::messaging::{Message, GenericMessage, serialize};
use crate::saga::SagaRecipe;
use crate::core_topics;
use anyhow::Result;
use serde::{Serialize, Deserialize};

// --- API-specific Messages ---

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct RegisterServiceMessage {
    pub(crate) name: String,
}
impl Message for RegisterServiceMessage {
    fn topic(&self) -> String {
        core_topics::REGISTER_SERVICE_TOPIC.to_string()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct SubmitSagaRecipeMessage {
    pub(crate) recipe: SagaRecipe,
}
impl Message for SubmitSagaRecipeMessage {
    fn topic(&self) -> String {
        core_topics::SUBMIT_SAGA_TOPIC.to_string()
    }
}

// --- SkaldClient ---

pub struct SkaldClient<T: Transport> {
    transport: T,
}

impl<T: Transport> SkaldClient<T> {
    pub fn new(transport: T) -> Self {
        Self { transport }
    }

    /// Sends a custom, user-defined event.
    pub async fn send_event<M: Message>(&mut self, event: &M) -> Result<()> {
        let payload = serialize(event)?;
        let envelope = GenericMessage {
            topic: event.topic(),
            payload,
        };
        let data = serialize(&envelope)?;
        self.transport.send(&data).await?;
        Ok(())
    }

    /// Receives a message from the server.
    pub async fn receive_message(&mut self) -> Result<GenericMessage> {
        let data = self.transport.receive().await?;
        let msg: GenericMessage = crate::messaging::deserialize(&data)?;
        Ok(msg)
    }

    // --- High-Level API Methods ---

    /// Registers this client connection as a named service with the Skald server.
    pub async fn register_service(&mut self, name: &str) -> Result<()> {
        println!("{}: Registering with Skald...", name);
        let msg = RegisterServiceMessage { name: name.to_string() };
        self.send_event(&msg).await
    }

    /// Submits a SagaRecipe to the Skald server for orchestration.
    pub async fn submit_saga(&mut self, recipe: SagaRecipe) -> Result<()> {
        println!("Submitting Saga Recipe...");
        let msg = SubmitSagaRecipeMessage { recipe };
        self.send_event(&msg).await
    }
}
