//! Samsa - A simple publish/subscribe microservice (Chapter 11)
//!
//! This crate builds on Chapters 9-10 by adding functional programming patterns:
//! - Function pipelines for data transformation
//! - Generics as type classes for enhanced abstractions
//! - Advanced pattern matching for control flow
//! - Closure patterns for configurable behavior

// Core error handling - unified across all modules
pub use error::{Result, SamsaError};

pub use broker::Broker;
pub use consumer::Consumer;
pub use message::{Event, Message};
pub use producer::Producer;

// Chapter 10 additions - Type System Patterns
pub use sealed::{JsonSchema, MessageHandler, MessageSchema, TextSchema, TypedMessage};
pub use types::{ConsumerId, MessageId, TopicId};
pub use typestate_consumer::{
    ConnectedConsumer, ConnectionInfo, Consumer as TypedConsumer, DisconnectedConsumer,
    PausedConsumer, SubscribedConsumer,
};

// Chapter 11 additions - Functional Programming Patterns
pub use closures::{EventBus, MessageFilter, MessageRouter, Pipeline, SystemEvent};
pub use pattern_matching::{
    ConfigValue, ConnectionEvent, ConnectionState, DatabaseConfig, MessageContent, Priority,
    ProcessingResult, RichMessage,
};
pub use pipeline::{
    AdvancedPipeline, EventTransformation, MessagePipeline, SubscriptionEvent,
    SubscriptionProcessing, SubscriptionStats,
};
pub use type_classes::{
    Activatable, Cancellable, MessageDeliverable, Subscription, SubscriptionManager, Suspendable,
};

mod broker;
mod consumer;
mod error;
mod message;
mod producer;
mod storage;

// Chapter 10 modules
pub mod sealed;
pub mod types;
pub mod typestate_consumer;

// Chapter 11 modules - Functional Programming
pub mod closures;
pub mod pattern_matching;
pub mod pipeline;
pub mod type_classes;
