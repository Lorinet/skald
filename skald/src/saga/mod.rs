use async_trait::async_trait;
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::client::SkaldClient;
use crate::transport::Transport;
use crate::messaging::Message;
use crate::server::ServiceRegistry;
use serde::{Serialize, Deserialize};
use serde_error::Error as SerdeError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SagaState {
    Started,
    StepCompleted { step_name: String, result: Option<Vec<u8>> },
    Failed { step_name: String, error: SerdeError },
    Compensating { step_name: String, error: Option<SerdeError> },
    Completed,
    Aborted,
}

// Implement PartialEq manually because SerdeError doesn't implement it
impl PartialEq for SagaState {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Started, Self::Started) => true,
            (Self::Completed, Self::Completed) => true,
            (Self::Aborted, Self::Aborted) => true,
            (
                Self::StepCompleted { step_name: n1, result: r1 },
                Self::StepCompleted { step_name: n2, result: r2 },
            ) => n1 == n2 && r1 == r2,
            (
                Self::Failed { step_name: n1, error: e1 },
                Self::Failed { step_name: n2, error: e2 },
            ) => n1 == n2 && format!("{:?}", e1) == format!("{:?}", e2), // Best effort comparison for error
            (
                Self::Compensating { step_name: n1, error: e1 },
                Self::Compensating { step_name: n2, error: e2 },
            ) => {
                n1 == n2 && match (e1, e2) {
                    (None, None) => true,
                    (Some(err1), Some(err2)) => format!("{:?}", err1) == format!("{:?}", err2),
                    _ => false,
                }
            }
            _ => false,
        }
    }
}

#[async_trait]
pub trait SagaStep: Send + Sync {
    fn name(&self) -> String;
    async fn execute(&self) -> Result<Option<Vec<u8>>>;
    async fn compensate(&self) -> Result<()>;
}

// Definition of a remote step (The Recipe)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RemoteStepDefinition {
    pub name: String,
    pub service_name: String,
    pub execute_topic: String,
    pub execute_payload: Vec<u8>,
    pub compensate_topic: String,
    pub compensate_payload: Vec<u8>,
}

// The executable version of the remote step, hydrated with the ServiceRegistry
pub struct ExecutableRemoteStep {
    pub definition: RemoteStepDefinition,
    pub registry: Arc<Mutex<ServiceRegistry>>,
}

#[async_trait]
impl SagaStep for ExecutableRemoteStep {
    fn name(&self) -> String {
        self.definition.name.clone()
    }

    async fn execute(&self) -> Result<Option<Vec<u8>>> {
        let reg = self.registry.lock().await;
        if let Some(transport_mutex) = reg.get(&self.definition.service_name) {
            let mut transport = transport_mutex.lock().await;
            // We need to send the message.
            // We can use SkaldClient logic here or just raw transport.
            // Let's use raw transport for now or construct a temp client.
            // But SkaldClient takes T, and we have Box<dyn Transport>.
            // We can just use transport.send().

            // Construct the envelope
            let envelope = crate::messaging::GenericMessage {
                topic: self.definition.execute_topic.clone(),
                payload: self.definition.execute_payload.clone(),
            };
            let data = crate::messaging::serialize(&envelope)?;

            transport.send(&data).await?;

            // In a real system, we'd wait for a response.
            // For now, we assume fire-and-forget or immediate success for the step dispatch.
            // Ideally, the service sends back a "StepCompleted" message which the Saga Orchestrator waits for.
            // But that requires async state machine suspension which is complex.
            // For this iteration, we'll assume the send itself is the "execution" trigger.
            Ok(None)
        } else {
            anyhow::bail!("Service not found: {}", self.definition.service_name);
        }
    }

    async fn compensate(&self) -> Result<()> {
        let reg = self.registry.lock().await;
        if let Some(transport_mutex) = reg.get(&self.definition.service_name) {
            let mut transport = transport_mutex.lock().await;

            let envelope = crate::messaging::GenericMessage {
                topic: self.definition.compensate_topic.clone(),
                payload: self.definition.compensate_payload.clone(),
            };
            let data = crate::messaging::serialize(&envelope)?;

            transport.send(&data).await?;
            Ok(())
        } else {
            anyhow::bail!("Service not found: {}", self.definition.service_name);
        }
    }
}

// The Recipe containing all steps
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SagaRecipe {
    pub steps: Vec<RemoteStepDefinition>,
}

pub struct SagaBuilder {
    steps: Vec<Box<dyn SagaStep>>,
}

impl SagaBuilder {
    pub fn new() -> Self {
        Self { steps: Vec::new() }
    }

    pub fn step(mut self, step: Box<dyn SagaStep>) -> Self {
        self.steps.push(step);
        self
    }

    // Helper to hydrate a recipe into a builder
    pub fn from_recipe(recipe: SagaRecipe, registry: Arc<Mutex<ServiceRegistry>>) -> Self {
        let mut builder = Self::new();
        for step_def in recipe.steps {
            builder = builder.step(Box::new(ExecutableRemoteStep {
                definition: step_def,
                registry: registry.clone(),
            }));
        }
        builder
    }

    pub fn build(self) -> Saga {
        Saga {
            steps: self.steps,
            current_step_index: 0,
            state: SagaState::Started,
        }
    }
}

pub struct Saga {
    steps: Vec<Box<dyn SagaStep>>,
    current_step_index: usize,
    state: SagaState,
}

impl Saga {
    pub async fn execute(&mut self) -> Result<()> {
        for (i, step) in self.steps.iter().enumerate() {
            self.current_step_index = i;
            match step.execute().await {
                Ok(result) => {
                    self.state = SagaState::StepCompleted {
                        step_name: step.name(),
                        result,
                    };
                }
                Err(e) => {
                    // Convert anyhow::Error to serde_error::Error
                    let serde_err = SerdeError::new(&*e);
                    self.state = SagaState::Failed {
                        step_name: step.name(),
                        error: serde_err,
                    };
                    self.compensate().await?;
                    return Err(e);
                }
            }
        }
        self.state = SagaState::Completed;
        Ok(())
    }

    async fn compensate(&mut self) -> Result<()> {
        // Compensate in reverse order, starting from the failed step (or the one before it depending on logic)
        // Here we assume the current step failed, so we compensate previous steps
        for i in (0..self.current_step_index).rev() {
            let step = &self.steps[i];

            // We might want to capture compensation errors too
            self.state = SagaState::Compensating {
                step_name: step.name(),
                error: None,
            };

            if let Err(e) = step.compensate().await {
                let serde_err = SerdeError::new(&*e);
                self.state = SagaState::Compensating {
                    step_name: step.name(),
                    error: Some(serde_err),
                };
                eprintln!("Failed to compensate step {}: {}", step.name(), e);
            }
        }
        self.state = SagaState::Aborted;
        Ok(())
    }

    pub fn get_state(&self) -> SagaState {
        self.state.clone()
    }
}

// Auxiliary Saga for User Input Compensation
pub struct UserInputCompensationStep {
    pub prompt: String,
    // In a real system, this might be a channel or a callback to a frontend service
    pub input_receiver: Arc<Mutex<Option<String>>>,
}

#[async_trait]
impl SagaStep for UserInputCompensationStep {
    fn name(&self) -> String {
        "UserInputCompensation".to_string()
    }

    async fn execute(&self) -> Result<Option<Vec<u8>>> {
        // This step is usually a no-op in the forward direction or setup
        Ok(None)
    }

    async fn compensate(&self) -> Result<()> {
        println!("Requesting user input: {}", self.prompt);
        // Simulate waiting for user input or triggering a frontend flow
        // For this example, we'll just log it. In a real app, this would send a message
        // to a frontend service topic.
        Ok(())
    }
}

// Re-export RemoteServiceStep for backward compatibility if needed,
// but the new model prefers RemoteStepDefinition + ExecutableRemoteStep.
// We can keep the old one for direct client usage if desired, but let's comment it out to avoid confusion
// or keep it if tests rely on it. The previous tests relied on it.
// Let's keep it but it's distinct from the "Recipe" model.
pub struct RemoteServiceStep<T: Transport + Send + Sync + 'static, M: Message + Clone, C: Message + Clone> {
    pub name: String,
    pub client: Arc<Mutex<SkaldClient<T>>>,
    pub execute_event: M,
    pub compensate_event: C,
}

#[async_trait]
impl<T: Transport + Send + Sync + 'static, M: Message + Clone, C: Message + Clone> SagaStep for RemoteServiceStep<T, M, C> {
    fn name(&self) -> String {
        self.name.clone()
    }

    async fn execute(&self) -> Result<Option<Vec<u8>>> {
        let mut client = self.client.lock().await;
        client.send_event(&self.execute_event).await?;
        Ok(None)
    }

    async fn compensate(&self) -> Result<()> {
        let mut client = self.client.lock().await;
        client.send_event(&self.compensate_event).await?;
        Ok(())
    }
}
