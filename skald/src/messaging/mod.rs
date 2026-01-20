use serde::{Serialize, Deserialize};
use anyhow::Result;

// Re-export serde and serde_cbor so dependent services don't need to add them explicitly
// if they just want to define messages.
pub use serde;
pub use serde_cbor;

pub trait Message: Serialize + for<'de> Deserialize<'de> + Send + Sync + std::fmt::Debug + 'static {
    fn topic(&self) -> String;
}

pub fn serialize<T: Serialize>(msg: &T) -> Result<Vec<u8>> {
    Ok(serde_cbor::to_vec(msg)?)
}

pub fn deserialize<'a, T: Deserialize<'a>>(data: &'a [u8]) -> Result<T> {
    Ok(serde_cbor::from_slice(data)?)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GenericMessage {
    pub topic: String,
    pub payload: Vec<u8>,
}

impl Message for GenericMessage {
    fn topic(&self) -> String {
        self.topic.clone()
    }
}
