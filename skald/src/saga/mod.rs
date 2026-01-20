use async_trait::async_trait;
use anyhow::{Result, anyhow};
use crate::server::SagaOrchestrator;
use serde::{Serialize, Deserialize};
use serde_json::Value;
use std::collections::HashMap;

pub mod builder;

// --- Saga Context & State ---

pub type SagaContext = HashMap<String, Value>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SagaState {
    Started,
    StepCompleted { step_name: String, result: Value },
    Failed { step_name: String, error: Value },
    Compensating { step_name: String },
    Completed,
    Aborted,
}

// --- Saga Step Trait ---

#[async_trait]
pub trait SagaStep: Send + Sync {
    fn name(&self) -> String;
    async fn execute(&self, context: &SagaContext) -> Result<Value>;
    async fn compensate(&self, context: &SagaContext, error: Option<&Value>) -> Result<()>;
}

// --- Remote Step Implementation ---

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RemoteStepDefinition {
    pub name: String,
    pub service_name: String,
    pub execute_topic: String,
    pub execute_payload: Value,
    pub compensate_topic: String,
    pub compensate_payload: Value,
}

pub struct ExecutableRemoteStep {
    pub definition: RemoteStepDefinition,
    pub orchestrator: SagaOrchestrator,
}

fn resolve_payload(template: &Value, context: &SagaContext, error: Option<&Value>) -> Value {
    match template {
        Value::String(s) if s.starts_with("{{") && s.ends_with("}}") => {
            let key = s.trim_matches(|c| c == '{' || c == '}').trim();
            if key == "error" {
                return error.cloned().unwrap_or(Value::Null);
            }
            let parts: Vec<&str> = key.splitn(2, '.').collect();
            if let Some(step_result) = context.get(parts[0]) {
                if parts.len() == 1 {
                    return step_result.clone();
                }
                let pointer = format!("/{}", parts[1].replace('.', "/"));
                return step_result.pointer(&pointer).cloned().unwrap_or(Value::Null);
            }
            Value::Null
        },
        Value::Object(map) => map.iter().map(|(k, v)| (k.clone(), resolve_payload(v, context, error))).collect(),
        Value::Array(arr) => arr.iter().map(|v| resolve_payload(v, context, error)).collect(),
        _ => template.clone(),
    }
}

#[async_trait]
impl SagaStep for ExecutableRemoteStep {
    fn name(&self) -> String {
        self.definition.name.clone()
    }

    async fn execute(&self, context: &SagaContext) -> Result<Value> {
        let payload = resolve_payload(&self.definition.execute_payload, context, None);
        let response = self.orchestrator.invoke(&self.definition.service_name, &self.definition.execute_topic, payload).await?;
        match response.result {
            Ok(val) => Ok(val),
            Err(e) => Err(anyhow!("Step '{}' failed: {}", self.name(), e)),
        }
    }

    async fn compensate(&self, context: &SagaContext, error: Option<&Value>) -> Result<()> {
        let payload = resolve_payload(&self.definition.compensate_payload, context, error);
        self.orchestrator.invoke(&self.definition.service_name, &self.definition.compensate_topic, payload).await?;
        Ok(())
    }
}

// --- Saga & Builder ---

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SagaRecipe {
    pub name: String,
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

    pub fn from_recipe(recipe: SagaRecipe, orchestrator: SagaOrchestrator) -> Self {
        recipe.steps.into_iter().fold(Self::new(), |builder, step_def| {
            builder.step(Box::new(ExecutableRemoteStep {
                definition: step_def,
                orchestrator: orchestrator.clone(),
            }))
        })
    }

    pub fn build(self) -> Saga {
        Saga {
            steps: self.steps,
            state: SagaState::Started,
            context: SagaContext::new(),
        }
    }
}

pub struct Saga {
    steps: Vec<Box<dyn SagaStep>>,
    state: SagaState,
    context: SagaContext,
}

impl Saga {
    pub async fn execute(&mut self) -> Result<()> {
        let mut failed_step_index = None;
        let mut execution_error = None;

        for (i, step) in self.steps.iter().enumerate() {
            match step.execute(&self.context).await {
                Ok(result) => {
                    self.state = SagaState::StepCompleted { step_name: step.name(), result: result.clone() };
                    self.context.insert(step.name(), result);
                }
                Err(e) => {
                    let error_val = serde_json::to_value(e.to_string())?;
                    self.state = SagaState::Failed { step_name: step.name(), error: error_val.clone() };
                    failed_step_index = Some(i);
                    execution_error = Some(error_val);
                    break;
                }
            }
        }

        if let Some(failed_idx) = failed_step_index {
            self.compensate(failed_idx, execution_error.as_ref()).await;
            return Err(anyhow!("Saga failed and was aborted."));
        }

        self.state = SagaState::Completed;
        Ok(())
    }

    async fn compensate(&mut self, failed_step_index: usize, error: Option<&Value>) {
        for i in (0..=failed_step_index).rev() {
            let step = &self.steps[i];
            self.state = SagaState::Compensating { step_name: step.name() };
            if let Err(e) = step.compensate(&self.context, error).await {
                eprintln!("Failed to compensate step {}: {}", step.name(), e);
            }
        }
        self.state = SagaState::Aborted;
    }

    pub fn get_state(&self) -> SagaState {
        self.state.clone()
    }
}
