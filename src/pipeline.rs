//! Function pipeline patterns for Samsa
//!
//! This module demonstrates functional programming patterns using
//! iterator chains, custom combinators, and lazy evaluation.

use crate::message::{Event, Message, current_timestamp};
use std::collections::HashMap;
use std::ops::Deref;

/// Statistics about subscription events
#[derive(Debug, Default)]
pub struct SubscriptionStats {
    pub total_valid: usize,
    pub subscriptions_by_topic: HashMap<String, usize>, // topic -> size
}

/// A subscription event in the system
#[derive(Debug, Clone)]
pub struct SubscriptionEvent {
    pub user_id: u64,
    pub topic: String,
    pub timestamp: u64,
    pub event_type: EventType,
}

#[derive(Debug, Clone, PartialEq)]
pub enum EventType {
    Subscribe,
    Unsubscribe,
    Invalid,
}

impl SubscriptionEvent {
    pub fn is_valid(&self) -> bool {
        self.event_type != EventType::Invalid && !self.topic.is_empty() && self.user_id > 0
    }

    pub fn is_subscription(&self) -> bool {
        self.event_type == EventType::Subscribe
    }
}

/// Extension trait for subscription event processing
pub trait SubscriptionProcessing: Iterator + Sized
where
    Self::Item: Deref<Target = SubscriptionEvent>,
{
    // Iterators //
    fn recent_events(self, cutoff_timestamp: u64) -> impl Iterator<Item = Self::Item> {
        self.filter(move |event| event.timestamp >= cutoff_timestamp)
    }
    fn valid_subscriptions(self) -> impl Iterator<Item = Self::Item> {
        self.filter(|event| event.is_valid())
            .filter(|event| event.is_subscription())
    }

    // HashMap result //
    fn count_by_topic(self) -> HashMap<String, usize> {
        self.fold(HashMap::new(), |mut acc, event| {
            *acc.entry(event.topic.clone()).or_insert(0) += 1;
            acc
        })
    }
}

/// Process subscription events using function pipelines
pub fn process_subscription_events(events: &[SubscriptionEvent]) -> SubscriptionStats {
    let valid_events: Vec<_> = events
        .iter()
        .filter(|event| event.is_valid())
        .cloned()
        .collect();

    let subscriptions_by_topic = valid_events
        .iter()
        .filter(|event| event.is_subscription())
        .map(|event| &event.topic)
        .fold(HashMap::new(), |mut acc, topic| {
            *acc.entry(topic.clone()).or_insert(0) += 1;
            acc
        });

    SubscriptionStats {
        total_valid: valid_events.len(),
        subscriptions_by_topic,
    }
}

// blanket implementation
// applies to e.g., events.iter() (slice::Iter<'_, SubscriptionEvent>, Item = &SubscriptionEvent)
// -> it is a Sized iterator whose items deref to SubscriptionEvent, so it satisfies the blanket impl
// and now has the trait methods
impl<I> SubscriptionProcessing for I
where
    I: Iterator + Sized,
    I::Item: Deref<Target = SubscriptionEvent>,
{
}

/// Process recent subscriptions using custom combinators
pub fn analyze_recent_subscriptions(
    events: &[SubscriptionEvent],
    cutoff_timestamp: u64,
) -> HashMap<String, usize> {
    events
        .iter()
        .recent_events(cutoff_timestamp)
        .valid_subscriptions()
        .count_by_topic()
}

/// Message processing pipeline for filtering and transformation
pub struct MessagePipeline<F> {
    filters: Vec<F>,
}

impl<F> Default for MessagePipeline<F>
where
    F: Fn(&Message) -> bool,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<F> MessagePipeline<F>
where
    F: Fn(&Message) -> bool,
{
    pub fn new() -> Self {
        Self {
            filters: Vec::new(),
        }
    }

    pub fn add_filter(mut self, filter: F) -> Self {
        self.filters.push(filter);
        self
    }

    pub fn process(&self, messages: Vec<Message>) -> Vec<Message> {
        messages
            .into_iter()
            .filter(|msg| self.filters.iter().all(|f| f(msg)))
            .collect()
    }
}

/// Event transformation pipeline
pub trait EventTransformation: Iterator<Item = Event> + Sized {
    fn with_topic_prefix(self, prefix: &str) -> impl Iterator<Item = Event> {
        let prefix = prefix.to_string();
        self.filter(move |event| event.message.topic.starts_with(&prefix))
    }

    fn transform_values<F>(self, f: F) -> impl Iterator<Item = Event>
    where
        F: Fn(Vec<u8>) -> Vec<u8>,
    {
        self.map(move |mut event| {
            event.message.value = f(event.message.value);
            event
        })
    }

    fn batch(self, size: usize) -> BatchIterator<Self> {
        BatchIterator::new(self, size)
    }
}

impl<I> EventTransformation for I where I: Iterator<Item = Event> {}

/// Iterator for batching events
pub struct BatchIterator<I: Iterator<Item = Event>> {
    iter: I,
    batch_size: usize,
}

impl<I: Iterator<Item = Event>> BatchIterator<I> {
    fn new(iter: I, batch_size: usize) -> Self {
        Self { iter, batch_size }
    }
}

impl<I: Iterator<Item = Event>> Iterator for BatchIterator<I> {
    type Item = Vec<Event>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut batch = Vec::with_capacity(self.batch_size);

        for _ in 0..self.batch_size {
            match self.iter.next() {
                Some(event) => batch.push(event),
                None => break,
            }
        }

        if batch.is_empty() { None } else { Some(batch) }
    }
}

/// Advanced pipeline operations for complex transformations
type MessageBatchOperation = Box<dyn Fn(Vec<Message>) -> Vec<Message>>;

pub struct AdvancedPipeline {
    operations: Vec<MessageBatchOperation>,
}

impl Default for AdvancedPipeline {
    fn default() -> Self {
        Self::new()
    }
}

impl AdvancedPipeline {
    pub fn new() -> Self {
        Self {
            operations: Vec::new(),
        }
    }

    pub fn filter<F>(mut self, predicate: F) -> Self
    where
        F: Fn(&Message) -> bool + 'static,
    {
        self.operations.push(Box::new(move |messages| {
            messages.into_iter().filter(|m| predicate(m)).collect()
        }));
        self
    }

    pub fn map<F>(mut self, mapper: F) -> Self
    where
        F: Fn(Message) -> Message + 'static,
    {
        self.operations.push(Box::new(move |messages| {
            messages.into_iter().map(&mapper).collect()
        }));
        self
    }

    pub fn flat_map<F>(mut self, mapper: F) -> Self
    where
        F: Fn(Message) -> Vec<Message> + 'static,
    {
        self.operations.push(Box::new(move |messages| {
            messages.into_iter().flat_map(&mapper).collect()
        }));
        self
    }

    pub fn execute(self, messages: Vec<Message>) -> Vec<Message> {
        self.operations
            .into_iter()
            .fold(messages, |acc, op| op(acc))
    }
}

/// Functional composition helpers
pub fn compose<A, B, C, F, G>(f: F, g: G) -> impl Fn(A) -> C
where
    F: Fn(A) -> B,
    G: Fn(B) -> C,
{
    move |x| g(f(x))
}

pub fn pipe<A, B, C, F, G>(f: F, g: G) -> impl Fn(A) -> C
where
    F: Fn(A) -> B,
    G: Fn(B) -> C,
{
    compose(f, g)
}

/// Create a pipeline of message transformations
pub fn create_message_enrichment_pipeline() -> impl Fn(Message) -> Message {
    let add_timestamp = |mut msg: Message| {
        msg.value.extend_from_slice(b"|timestamp:");
        msg.value
            .extend_from_slice(current_timestamp().to_string().as_bytes());
        msg
    };

    let normalize_topic = |mut msg: Message| {
        msg.topic = msg.topic.to_lowercase();
        msg
    };

    let add_size_metadata = |mut msg: Message| {
        let size = msg.value.len();
        msg.key = Some(format!("size:{}", size));
        msg
    };

    pipe(add_timestamp, pipe(normalize_topic, add_size_metadata))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscription_processing() {
        let events = vec![
            SubscriptionEvent {
                user_id: 1,
                topic: "news".to_string(),
                timestamp: 100,
                event_type: EventType::Subscribe,
            },
            SubscriptionEvent {
                user_id: 2,
                topic: "news".to_string(),
                timestamp: 200,
                event_type: EventType::Subscribe,
            },
            SubscriptionEvent {
                user_id: 3,
                topic: "sports".to_string(),
                timestamp: 300,
                event_type: EventType::Subscribe,
            },
        ];

        let stats = process_subscription_events(&events);
        assert_eq!(stats.total_valid, 3);
        assert_eq!(stats.subscriptions_by_topic.get("news"), Some(&2));
        assert_eq!(stats.subscriptions_by_topic.get("sports"), Some(&1));
    }

    #[test]
    fn test_pipeline_composition() {
        let pipeline = create_message_enrichment_pipeline();

        let msg = Message::new("TEST.TOPIC", None, b"Hello");
        let enriched = pipeline(msg);

        assert_eq!(enriched.topic, "test.topic");
        assert!(enriched.value.starts_with(b"Hello|timestamp:"));
        assert!(enriched.key.is_some());
    }

    #[test]
    fn test_subscription_event_validity() {
        let valid = SubscriptionEvent {
            user_id: 1,
            topic: "news".to_string(),
            timestamp: 100,
            event_type: EventType::Subscribe,
        };
        assert!(valid.is_valid());
        assert!(valid.is_subscription());

        let invalid_user = SubscriptionEvent {
            user_id: 0,
            topic: "news".to_string(),
            timestamp: 100,
            event_type: EventType::Subscribe,
        };
        assert!(!invalid_user.is_valid());

        let empty_topic = SubscriptionEvent {
            user_id: 1,
            topic: "".to_string(),
            timestamp: 100,
            event_type: EventType::Subscribe,
        };
        assert!(!empty_topic.is_valid());

        let unsubscribe = SubscriptionEvent {
            user_id: 1,
            topic: "news".to_string(),
            timestamp: 100,
            event_type: EventType::Unsubscribe,
        };
        assert!(unsubscribe.is_valid());
        assert!(!unsubscribe.is_subscription());

        let invalid_type = SubscriptionEvent {
            user_id: 1,
            topic: "news".to_string(),
            timestamp: 100,
            event_type: EventType::Invalid,
        };
        assert!(!invalid_type.is_valid());
    }

    #[test]
    fn test_analyze_recent_subscriptions() {
        let events = vec![
            SubscriptionEvent {
                user_id: 1,
                topic: "news".to_string(),
                timestamp: 50,
                event_type: EventType::Subscribe,
            },
            SubscriptionEvent {
                user_id: 2,
                topic: "news".to_string(),
                timestamp: 150,
                event_type: EventType::Subscribe,
            },
            SubscriptionEvent {
                user_id: 3,
                topic: "sports".to_string(),
                timestamp: 200,
                event_type: EventType::Subscribe,
            },
            SubscriptionEvent {
                user_id: 4,
                topic: "sports".to_string(),
                timestamp: 250,
                event_type: EventType::Unsubscribe,
            },
        ];

        let result = analyze_recent_subscriptions(&events, 100);
        assert_eq!(result.get("news"), Some(&1));
        assert_eq!(result.get("sports"), Some(&1));
    }

    #[test]
    fn test_message_pipeline_filter() {
        let pipeline = MessagePipeline::new()
            .add_filter((|msg: &Message| !msg.topic.is_empty()) as fn(&Message) -> bool)
            .add_filter((|msg: &Message| msg.value.len() < 100) as fn(&Message) -> bool);

        let messages = vec![
            Message::new("topic1", None, b"short"),
            Message::new("", None, b"short"),
            Message::new("topic2", None, vec![0u8; 200]),
        ];

        let filtered = pipeline.process(messages);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].topic, "topic1");
    }

    #[test]
    fn test_batch_iterator() {
        let events: Vec<Event> = (0..7)
            .map(|i| Event {
                message: Message::new(&format!("topic{}", i), None, b"data"),
                offset: i,
            })
            .collect();

        let batches: Vec<_> = events.into_iter().batch(3).collect();
        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0].len(), 3);
        assert_eq!(batches[1].len(), 3);
        assert_eq!(batches[2].len(), 1);
    }

    #[test]
    fn test_compose_function() {
        let add_one = |x: i32| x + 1;
        let double = |x: i32| x * 2;
        let composed = compose(add_one, double);
        assert_eq!(composed(5), 12);
        assert_eq!(composed(0), 2);
    }

    #[test]
    fn test_advanced_pipeline() {
        let pipeline = AdvancedPipeline::new()
            .filter(|msg| !msg.topic.is_empty())
            .map(|mut msg| {
                msg.topic = msg.topic.to_uppercase();
                msg
            });

        let messages = vec![
            Message::new("hello", None, b"data1"),
            Message::new("", None, b"data2"),
            Message::new("world", None, b"data3"),
        ];

        let result = pipeline.execute(messages);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].topic, "HELLO");
        assert_eq!(result[1].topic, "WORLD");
    }

    #[test]
    fn test_empty_events() {
        let events: Vec<SubscriptionEvent> = vec![];
        let stats = process_subscription_events(&events);
        assert_eq!(stats.total_valid, 0);
        assert!(stats.subscriptions_by_topic.is_empty());
    }
}
