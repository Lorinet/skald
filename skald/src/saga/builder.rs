use std::marker::PhantomData;
use crate::saga::{RemoteStepDefinition, SagaRecipe};
use serde_json::Value;

/// # Saga and Step Builders
///
/// This module provides a fluent builder API for constructing `SagaRecipe` definitions.
/// It allows for a declarative and readable way to define complex sagas with multiple steps,
/// including dependencies between steps.
///
/// ## Example Usage
///
/// ```rust,ignore
/// use skald::saga::builder::{SagaDefinitionBuilder, MessageTemplateBuilder};
///
/// let saga_recipe = SagaDefinitionBuilder::new("MyOrderSaga")
///     .step("CreateOrder")
///         .service("OrderService")
///         .execute("orders.create", MessageTemplateBuilder::new()
///             .field("user_id", "user-123")
///             .field("item_id", "item-456")
///             .build())
///         .compensate("orders.cancel", MessageTemplateBuilder::new()
///             .from_result("order_id", "{{CreateOrder.result.id}}") // Use result from execute
///             .build())
///         .add()
///     .step("ProcessPayment")
///         .service("PaymentService")
///         .execute("payments.process", MessageTemplateBuilder::new()
///             .from_result("order_id", "{{CreateOrder.result.id}}")
///             .from_result("amount", "{{CreateOrder.result.amount}}")
///             .build())
///         .compensate("payments.refund", MessageTemplateBuilder::new()
///             .from_result("payment_id", "{{ProcessPayment.result.transaction_id}}")
///             .build())
///         .add()
///     .build();
/// ```

// --- MessageTemplateBuilder ---

/// A helper to build `serde_json::Value` objects for message payloads.
/// This allows defining message contracts without direct dependency on the message structs.
#[derive(Default)]
pub struct MessageTemplateBuilder {
    fields: serde_json::Map<String, Value>,
}

impl MessageTemplateBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    /// Adds a field with a concrete JSON value.
    pub fn field<V: Into<Value>>(mut self, key: &str, value: V) -> Self {
        self.fields.insert(key.to_string(), value.into());
        self
    }

    /// Adds a field whose value is a placeholder to be resolved from a previous step's result at runtime.
    /// The `from_placeholder` string should be in the format "{{StepName.result.field_path}}".
    pub fn from_result(mut self, key: &str, from_placeholder: &str) -> Self {
        self.fields.insert(key.to_string(), Value::String(from_placeholder.to_string()));
        self
    }

    /// Constructs the final `serde_json::Value`.
    pub fn build(self) -> Value {
        Value::Object(self.fields)
    }
}


// --- SagaDefinitionBuilder ---

/// Marker for a `SagaDefinitionBuilder` that has no steps yet.
pub struct NoSteps;
/// Marker for a `SagaDefinitionBuilder` that has one or more steps.
pub struct WithSteps;

/// A fluent builder for creating a `SagaRecipe`.
pub struct SagaDefinitionBuilder<State = NoSteps> {
    name: String,
    steps: Vec<RemoteStepDefinition>,
    _state: PhantomData<State>,
}

impl SagaDefinitionBuilder<NoSteps> {
    /// Creates a new saga definition builder with a given name.
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            steps: Vec::new(),
            _state: PhantomData,
        }
    }
}

impl<State> SagaDefinitionBuilder<State> {
    /// Begins the definition of a new step in the saga.
    /// This returns a `StepDefinitionBuilder` to configure the step.
    pub fn step(self, name: &str) -> StepDefinitionBuilder<Self> {
        StepDefinitionBuilder::new(name, self)
    }
}

impl SagaDefinitionBuilder<WithSteps> {
    /// Builds the final `SagaRecipe`. This method is only available
    /// if at least one step has been added to the saga.
    pub fn build(self) -> SagaRecipe {
        SagaRecipe {
            name: self.name,
            steps: self.steps,
        }
    }
}

// --- StepDefinitionBuilder ---

/// A builder for a single `RemoteStepDefinition`.
/// It holds its parent builder (`P`) to return to it when the step is complete.
pub struct StepDefinitionBuilder<P> {
    parent_builder: P,
    name: String,
    service_name: Option<String>,
    execute_topic: Option<String>,
    execute_payload: Value,
    compensate_topic: Option<String>,
    compensate_payload: Value,
}

impl<P> StepDefinitionBuilder<P> {
    fn new(name: &str, parent_builder: P) -> Self {
        Self {
            parent_builder,
            name: name.to_string(),
            service_name: None,
            execute_topic: None,
            execute_payload: Value::Null,
            compensate_topic: None,
            compensate_payload: Value::Null,
        }
    }

    /// Sets the name of the target service for this step.
    pub fn service(mut self, service_name: &str) -> Self {
        self.service_name = Some(service_name.to_string());
        self
    }

    /// Defines the execution action for this step, including the topic and payload.
    /// The payload can be constructed using `MessageTemplateBuilder`.
    pub fn execute(mut self, topic: &str, payload: Value) -> Self {
        self.execute_topic = Some(topic.to_string());
        self.execute_payload = payload;
        self
    }

    /// Defines the compensation action for this step, including the topic and payload.
    /// The payload can be constructed using `MessageTemplateBuilder`.
    pub fn compensate(mut self, topic: &str, payload: Value) -> Self {
        self.compensate_topic = Some(topic.to_string());
        self.compensate_payload = payload;
        self
    }

    /// Finalizes the current step and adds it to the saga definition.
    /// This returns the parent `SagaDefinitionBuilder` to allow for chaining more steps.
    pub fn add(self) -> P::Output where P: AddStepExt {
        let step = RemoteStepDefinition {
            name: self.name,
            service_name: self.service_name.expect("Service name must be set for a step"),
            execute_topic: self.execute_topic.expect("Execute topic must be set for a step"),
            execute_payload: self.execute_payload,
            compensate_topic: self.compensate_topic.expect("Compensate topic must be set for a step"),
            compensate_payload: self.compensate_payload,
        };
        self.parent_builder.add_step(step)
    }
}

/// A helper trait to allow `StepDefinitionBuilder` to be generic over the parent's state.
pub trait AddStepExt {
    type Output;
    fn add_step(self, step: RemoteStepDefinition) -> Self::Output;
}

impl<State> AddStepExt for SagaDefinitionBuilder<State> {
    type Output = SagaDefinitionBuilder<WithSteps>;
    fn add_step(mut self, step: RemoteStepDefinition) -> Self::Output {
        self.steps.push(step);
        SagaDefinitionBuilder {
            name: self.name,
            steps: self.steps,
            _state: PhantomData,
        }
    }
}
