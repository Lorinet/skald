use tokio::sync::mpsc;
use crate::messaging::GenericMessage;
use anyhow::Result;

// Simplified queue wrapper, though we are using mpsc directly in server now.
// Keeping this for potential future abstraction or persistent queue implementation.
pub struct MessageQueue {
    sender: mpsc::Sender<GenericMessage>,
    receiver: mpsc::Receiver<GenericMessage>,
}

impl MessageQueue {
    pub fn new(buffer_size: usize) -> Self {
        let (sender, receiver) = mpsc::channel(buffer_size);
        Self { sender, receiver }
    }

    pub async fn enqueue(&self, message: GenericMessage) -> Result<()> {
        self.sender.send(message).await.map_err(|e| anyhow::anyhow!("Send error: {}", e))
    }

    pub async fn dequeue(&mut self) -> Option<GenericMessage> {
        self.receiver.recv().await
    }

    pub fn get_sender(&self) -> mpsc::Sender<GenericMessage> {
        self.sender.clone()
    }
}
