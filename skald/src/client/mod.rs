use crate::transport::Transport;
use crate::messaging::{
    Message, GenericMessage, SkaldMessage, InvocationMessage, ResponseMessage,
    RegisterServiceMessage, SubmitSagaRecipeMessage, serialize, deserialize,
};
use crate::saga::SagaRecipe;
use anyhow::{Result, anyhow};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;
use async_trait::async_trait;
use uuid::Uuid;

// --- Client-side Handlers ---

#[async_trait]
pub trait ClientEventHandler: Send + Sync {
    async fn handle(&self, payload: Vec<u8>) -> Result<()>;
}

#[async_trait]
pub trait ClientInvocationHandler: Send + Sync {
    async fn handle(&self, payload: Value) -> Result<Value, Value>;
}

type BoxedClientEventHandler = Arc<dyn ClientEventHandler>;
type BoxedClientInvocationHandler = Arc<dyn ClientInvocationHandler>;

// --- Type-Erased Handlers ---

struct TypeErasedEventHandler<F, P>
where
    P: DeserializeOwned + Send + Sync + 'static,
    F: Fn(P) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync + 'static,
{
    handler: Arc<F>,
    _p: std::marker::PhantomData<P>,
}

#[async_trait]
impl<F, P> ClientEventHandler for TypeErasedEventHandler<F, P>
where
    P: DeserializeOwned + Send + Sync + 'static,
    F: Fn(P) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync + 'static,
{
    async fn handle(&self, payload_bytes: Vec<u8>) -> Result<()> {
        // Use the library's deserialize function (CBOR) to match send_event
        let payload: P = deserialize(&payload_bytes)?;
        (self.handler)(payload).await
    }
}

struct TypeErasedInvocationHandler<F, P, R, E>
where
    P: DeserializeOwned + Send + Sync + 'static,
    R: Serialize + Send + Sync + 'static,
    E: Serialize + Send + Sync + 'static,
    F: Fn(P) -> Pin<Box<dyn Future<Output = Result<R, E>> + Send>> + Send + Sync + 'static,
{
    handler: Arc<F>,
    _p: std::marker::PhantomData<(P, R, E)>,
}

#[async_trait]
impl<F, P, R, E> ClientInvocationHandler for TypeErasedInvocationHandler<F, P, R, E>
where
    P: DeserializeOwned + Send + Sync + 'static,
    R: Serialize + Send + Sync + 'static,
    E: Serialize + Send + Sync + 'static,
    F: Fn(P) -> Pin<Box<dyn Future<Output = Result<R, E>> + Send>> + Send + Sync + 'static,
{
    async fn handle(&self, payload_value: Value) -> Result<Value, Value> {
        let payload: P = serde_json::from_value(payload_value).map_err(|e| serde_json::to_value(e.to_string()).unwrap())?;
        match (self.handler)(payload).await {
            Ok(res) => Ok(serde_json::to_value(res).map_err(|e| serde_json::to_value(e.to_string()).unwrap())?),
            Err(err) => Err(serde_json::to_value(err).map_err(|e| serde_json::to_value(e.to_string()).unwrap())?),
        }
    }
}

// --- SkaldClient ---

type PendingRequests = Arc<Mutex<HashMap<String, oneshot::Sender<ResponseMessage>>>>;

#[derive(Clone)]
pub struct SkaldClient<T: Transport> {
    transport: T,
    event_handlers: HashMap<String, BoxedClientEventHandler>,
    invocation_handlers: HashMap<String, BoxedClientInvocationHandler>,
    pending_requests: PendingRequests,
}

impl<T: Transport> SkaldClient<T> {
    pub fn new(transport: T) -> Self {
        Self {
            transport,
            event_handlers: HashMap::new(),
            invocation_handlers: HashMap::new(),
            pending_requests: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn send_event<M: Message>(&mut self, event: &M) -> Result<()> {
        let payload = serialize(event)?;
        let envelope = SkaldMessage::Event(GenericMessage {
            topic: event.topic(),
            payload,
        });
        self.transport.send(&serialize(&envelope)?).await
    }

    pub async fn register_service(&mut self, name: &str) -> Result<()> {
        let msg = SkaldMessage::RegisterService(RegisterServiceMessage { name: name.to_string() });
        self.transport.send(&serialize(&msg)?).await
    }

    pub async fn submit_saga(&mut self, recipe: SagaRecipe) -> Result<()> {
        let msg = SkaldMessage::SubmitSaga(SubmitSagaRecipeMessage { recipe });
        self.transport.send(&serialize(&msg)?).await
    }
}

impl<T: Transport + Clone + Send + 'static> SkaldClient<T> {
    pub fn on<P, F, Fut>(&mut self, topic: &str, handler: F)
    where
        P: DeserializeOwned + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
        F: Fn(P) -> Fut + Send + Sync + 'static,
    {
        let wrapped_handler = Arc::new(move |payload: P| Box::pin(handler(payload)) as Pin<Box<dyn Future<Output = Result<()>> + Send>>);
        let erased_handler = Arc::new(TypeErasedEventHandler {
            handler: wrapped_handler,
            _p: std::marker::PhantomData,
        });
        self.event_handlers.insert(topic.to_string(), erased_handler);
    }

    pub fn on_invoke<P, R, E, F, Fut>(&mut self, topic: &str, handler: F)
    where
        P: DeserializeOwned + Send + Sync + 'static,
        R: Serialize + Send + Sync + 'static,
        E: Serialize + Send + Sync + 'static,
        Fut: Future<Output = Result<R, E>> + Send + 'static,
        F: Fn(P) -> Fut + Send + Sync + 'static,
    {
        let wrapped_handler = Arc::new(move |payload: P| Box::pin(handler(payload)) as Pin<Box<dyn Future<Output = Result<R, E>> + Send>>);
        let erased_handler = Arc::new(TypeErasedInvocationHandler {
            handler: wrapped_handler,
            _p: std::marker::PhantomData,
        });
        self.invocation_handlers.insert(topic.to_string(), erased_handler);
    }

    pub async fn invoke<P, R, E>(&mut self, topic: &str, payload: P) -> Result<Result<R, E>>
    where
        P: Serialize,
        R: DeserializeOwned,
        E: DeserializeOwned,
    {
        let correlation_id = Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();
        self.pending_requests.lock().unwrap().insert(correlation_id.clone(), tx);

        let invocation = SkaldMessage::Invocation(InvocationMessage {
            correlation_id,
            topic: topic.to_string(),
            payload: serde_json::to_value(payload)?,
        });

        self.transport.send(&serialize(&invocation)?).await?;

        match rx.await {
            Ok(response) => match response.result {
                Ok(val) => Ok(Ok(serde_json::from_value(val)?)),
                Err(val) => Ok(Err(serde_json::from_value(val)?)),
            },
            Err(_) => Err(anyhow!("Invocation failed: response channel closed")),
        }
    }

    pub async fn listen(&mut self) -> Result<()> {
        loop {
            match self.transport.receive().await {
                Ok(data) => {
                    match deserialize::<SkaldMessage>(&data) {
                        Ok(msg) => {
                            let mut transport_clone = self.transport.clone();
                            let pending_requests_clone = self.pending_requests.clone();
                            let event_handlers = self.event_handlers.clone();
                            let invocation_handlers = self.invocation_handlers.clone();

                            tokio::spawn(async move {
                                match msg {
                                    SkaldMessage::Event(event) => {
                                        if let Some(handler) = event_handlers.get(&event.topic) {
                                            let handler_clone = handler.clone();
                                            if let Err(e) = handler_clone.handle(event.payload).await {
                                                eprintln!("Event handler for topic {} failed: {}", event.topic, e);
                                            }
                                        } else {
                                            eprintln!("No event handler found for topic: {}", event.topic);
                                        }
                                    }
                                    SkaldMessage::Invocation(invocation) => {
                                        if let Some(handler) = invocation_handlers.get(&invocation.topic) {
                                            let handler_clone = handler.clone();
                                            let result = handler_clone.handle(invocation.payload).await;
                                            let response = SkaldMessage::Response(ResponseMessage {
                                                correlation_id: invocation.correlation_id,
                                                result,
                                            });
                                            if let Ok(res_data) = serialize(&response) {
                                                if let Err(e) = transport_clone.send(&res_data).await {
                                                    eprintln!("Failed to send invocation response: {}", e);
                                                }
                                            }
                                        } else {
                                            eprintln!("No invocation handler found for topic: {}", invocation.topic);
                                        }
                                    }
                                    SkaldMessage::Response(response) => {
                                        if let Some(tx) = pending_requests_clone.lock().unwrap().remove(&response.correlation_id) {
                                            let _ = tx.send(response);
                                        }
                                    }
                                    _ => {}
                                }
                            });
                        }
                        Err(e) => {
                            eprintln!("Failed to deserialize message: {}", e);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Client receive loop error: {}", e);
                    break;
                }
            }
        }
        Ok(())
    }
}

// --- Service Builder ---

pub struct ServiceBuilder<T: Transport> {
    name: String,
    client: Option<SkaldClient<T>>,
}

impl<T: Transport + Clone + Send + 'static> ServiceBuilder<T> {
    pub async fn new(name: &str, transport: T) -> Result<Self> {
        let mut client = SkaldClient::new(transport);
        client.register_service(name).await?;
        Ok(Self {
            name: name.to_string(),
            client: Some(client),
        })
    }

    pub fn on_event<E, F, Fut>(mut self, topic: &str, handler: F) -> Self
    where
        E: DeserializeOwned + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
        F: Fn(E) -> Fut + Send + Sync + 'static,
    {
        if let Some(client) = &mut self.client {
            client.on(topic, handler);
        }
        self
    }

    pub fn on_invoke<P, R, E, F, Fut>(mut self, topic: &str, handler: F) -> Self
    where
        P: DeserializeOwned + Send + Sync + 'static,
        R: Serialize + Send + Sync + 'static,
        E: Serialize + Send + Sync + 'static,
        Fut: Future<Output = Result<R, E>> + Send + 'static,
        F: Fn(P) -> Fut + Send + Sync + 'static,
    {
        if let Some(client) = &mut self.client {
            client.on_invoke(topic, handler);
        }
        self
    }

    pub fn start(mut self) {
        if let Some(mut client) = self.client.take() {
            let name = self.name.clone();
            tokio::spawn(async move {
                if let Err(e) = client.listen().await {
                    eprintln!("{} client listener failed: {:?}", name, e);
                }
            });
        }
    }
}
