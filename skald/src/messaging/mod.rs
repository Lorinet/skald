use serde::{Serialize, Deserialize};
use anyhow::Result;
use serde_json::Value;

pub use serde;
pub use serde_cbor;

/// The basic trait for a message that can be sent through Skald.
/// It only requires a topic, which is used for routing.
pub trait Message: Serialize + for<'de> Deserialize<'de> + Send + Sync + std::fmt::Debug + 'static {
    fn topic(&self) -> String;
}

// --- Core Message Envelope ---

/// The top-level message envelope sent over the transport.
/// It distinguishes between different communication patterns.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SkaldMessage {
    /// A fire-and-forget event.
    Event(GenericMessage),
    /// A request that expects a response.
    Invocation(InvocationMessage),
    /// A response to an `Invocation`.
    Response(ResponseMessage),
    /// A message to register a service with the orchestrator.
    RegisterService(RegisterServiceMessage),
    /// A message to submit a saga recipe to the orchestrator.
    SubmitSaga(SubmitSagaRecipeMessage),
}

// --- Message Payloads ---

/// A generic message for fire-and-forget events.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GenericMessage {
    pub topic: String,
    pub payload: Vec<u8>,
}

/// A message for a request-response invocation.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct InvocationMessage {
    pub correlation_id: String,
    pub topic: String,
    pub payload: Value,
}

/// A message containing the result of an invocation.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ResponseMessage {
    pub correlation_id: String,
    /// The result of the operation, can be Ok(Value) or Err(Value).
    pub result: Result<Value, Value>,
}

/// A message to register a client as a named service.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RegisterServiceMessage {
    pub name: String,
    pub token: Option<String>,
    pub max_workers: Option<usize>,
}

/// A message to submit a saga definition for execution.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SubmitSagaRecipeMessage {
    pub recipe: crate::saga::SagaRecipe,
}


// --- Serialization ---

pub fn serialize<T: Serialize>(msg: &T) -> Result<Vec<u8>> {
    Ok(serde_cbor::to_vec(msg)?)
}

pub fn deserialize<'a, T: Deserialize<'a>>(data: &'a [u8]) -> Result<T> {
    Ok(serde_cbor::from_slice(data)?)
}
