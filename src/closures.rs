//! Closure patterns for flexible and reusable code
//!
//! This module demonstrates various closure patterns including
//! configurable filters, strategy pattern, event systems, and pipelines.

use crate::message::{Message, current_timestamp};
use std::collections::HashMap;
use std::error::Error;
use std::hash::Hash;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

// ============== //
// Message Filter //
// ============== //

/// Configurable message filter using closures
pub struct MessageFilter<F> {
    predicate: F,
    name: String,
}

impl<F> MessageFilter<F>
where
    F: Fn(&Message) -> bool,
{
    pub fn new(name: String, predicate: F) -> Self {
        Self { predicate, name }
    }

    // central function for MessageFilter
    pub fn matches(&self, message: &Message) -> bool {
        (self.predicate)(message)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn and<G>(self, other: MessageFilter<G>) -> MessageFilter<impl Fn(&Message) -> bool>
    where
        G: Fn(&Message) -> bool,
    {
        let combined_name = format!("{} AND {}", self.name, other.name);
        MessageFilter::new(combined_name, move |msg| {
            self.matches(msg) && other.matches(msg)
        })
    }

    pub fn or<G>(self, other: MessageFilter<G>) -> MessageFilter<impl Fn(&Message) -> bool>
    where
        G: Fn(&Message) -> bool,
    {
        let combined_name = format!("{} OR {}", self.name, other.name);
        MessageFilter::new(combined_name, move |msg| {
            self.matches(msg) || other.matches(msg)
        })
    }
}

/// Create common message filters
///
/// Returns a tuple of three filters with complex impl Trait types.
/// The type complexity is intentional to demonstrate returning closures with different predicates.
#[allow(clippy::type_complexity)]
pub fn create_message_filters() -> (
    MessageFilter<impl Fn(&Message) -> bool>,
    MessageFilter<impl Fn(&Message) -> bool>,
    MessageFilter<impl Fn(&Message) -> bool>,
) {
    let large_filter = MessageFilter::new("Large Messages".to_string(), |msg: &Message| {
        msg.value.len() > 1000
    });

    let system_filter = MessageFilter::new("System Topics".to_string(), |msg: &Message| {
        msg.topic.starts_with("system.")
    });

    let recent_filter = MessageFilter::new("Recent Messages".to_string(), |msg: &Message| {
        // Message is recent if it was created within the last hour
        let now = current_timestamp();
        now.saturating_sub(msg.timestamp) <= 3600
    });

    (large_filter, system_filter, recent_filter)
}

// ============== //
// Message Router //
// ============== //

/// Strategy pattern using closures for routing
///
/// Function returns a vector of queue names
pub type RoutingStrategy = Box<dyn Fn(&Message) -> Vec<String> + Send + Sync>;

pub struct MessageRouter {
    strategies: Vec<RoutingStrategy>,
    default_targets: Vec<String>, // vec of queues/queue names
}

impl Default for MessageRouter {
    fn default() -> Self {
        Self::new()
    }
}

/// First-match-wins router with a default fallback
impl MessageRouter {
    pub fn new() -> Self {
        Self {
            strategies: Vec::new(),
            default_targets: vec!["default_queue".to_string()],
        }
    }

    pub fn add_strategy<F>(&mut self, strategy: F)
    where
        F: Fn(&Message) -> Vec<String> + Send + Sync + 'static,
    {
        self.strategies.push(Box::new(strategy));
    }

    pub fn route_message(&self, message: &Message) -> Vec<String> {
        for strategy in &self.strategies {
            let targets = strategy(message);
            if !targets.is_empty() {
                return targets;
            }
        }
        self.default_targets.clone()
    }
}

/// Create routing strategies using closures
pub fn create_routing_strategies() -> MessageRouter {
    let mut router = MessageRouter::new();

    // Large messages get special handling
    router.add_strategy(|msg| {
        if msg.value.len() > 10240 {
            vec!["large_message_queue".to_string()]
        } else {
            vec![]
        }
    });

    // System messages distributed by topic
    router.add_strategy(|msg| {
        if msg.topic.starts_with("system.") {
            let service = msg.topic.strip_prefix("system.").unwrap_or("unknown");
            vec![format!("system_{}_queue", service)]
        } else {
            vec![]
        }
    });

    // API messages load-balanced across workers
    router.add_strategy({
        let worker_count = 4; // in real code this would come from e.g. a configuration file
        move |msg| {
            if msg.topic.starts_with("api.") {
                // Use topic hash for consistent routing
                let hash = msg
                    .topic
                    .bytes()
                    .fold(0u64, |acc, b| acc.wrapping_add(b as u64));
                let worker_id = hash % worker_count;
                vec![format!("api_worker_{}_queue", worker_id)]
            } else {
                vec![]
            }
        }
    });

    router
}

// ========= //
// Event Bus //
// ========= //

/// System events for the event bus
#[derive(Debug, Clone)]
pub enum SystemEvent {
    MessageReceived { message_id: u64, topic: String },
    UserConnected { user_id: u64 },
    UserDisconnected { user_id: u64 },
    ErrorOccurred { error_code: u32, details: String },
}

/// Event handler type for a generic event type
///
/// EventHandler is parameterized on any type T for events.
/// In this code only `SystemEvent` is used for T.
pub type EventHandler<T> = Box<dyn Fn(&T) + Send + Sync>;

/// Simple event bus using closures
pub struct EventBus<T> {
    handlers: Arc<Mutex<HashMap<String, Vec<EventHandler<T>>>>>, // event_type -> vec of EventHandlers
}

impl<T> Default for EventBus<T>
where
    T: Clone + Send + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> EventBus<T>
where
    T: Clone + Send + 'static,
{
    pub fn new() -> Self {
        Self {
            handlers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn subscribe<F>(&self, event_type: &str, handler: F)
    where
        F: Fn(&T) + Send + Sync + 'static,
    {
        let mut handlers = self.handlers.lock().unwrap();
        handlers
            .entry(event_type.to_string())
            .or_default()
            .push(Box::new(handler));
    }

    pub fn publish(&self, event_type: &str, event: &T) {
        let handlers = self.handlers.lock().unwrap();
        if let Some(event_handlers) = handlers.get(event_type) {
            for handler in event_handlers {
                handler(event);
            }
        }
    }

    pub fn subscribe_with_filter<F, P>(&self, event_type: &str, predicate: P, handler: F)
    where
        F: Fn(&T) + Send + Sync + 'static,
        P: Fn(&T) -> bool + Send + Sync + 'static,
    {
        self.subscribe(event_type, move |event| {
            if predicate(event) {
                handler(event);
            }
        });
    }
}

// ======== //
// Pipeline //
// ======== //

/// Processing pipeline using closures.
/// Every function is a stage going from T to T.
type PipelineStage<T> = Box<dyn Fn(T) -> Result<T, Box<dyn Error + Send + Sync>> + Send + Sync>;

/// A configurable processing pipeline generic over any value type T
///
/// In this code only `Message` is used for T.
pub struct Pipeline<T> {
    stages: Vec<PipelineStage<T>>,
}

impl<T> Default for Pipeline<T>
where
    T: Send + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Pipeline<T>
where
    T: Send + 'static,
{
    pub fn new() -> Self {
        Self { stages: Vec::new() }
    }

    // can fail
    pub fn add_stage<F>(mut self, stage: F) -> Self
    where
        F: Fn(T) -> Result<T, Box<dyn Error + Send + Sync>> + Send + Sync + 'static,
    {
        self.stages.push(Box::new(stage));
        self
    }

    // always succeeds
    pub fn add_transformation<F>(self, transform: F) -> Self
    where
        F: Fn(T) -> T + Send + Sync + 'static,
    {
        self.add_stage(move |input| Ok(transform(input)))
    }

    pub fn add_validation<F>(self, validator: F, error_message: String) -> Self
    where
        F: Fn(&T) -> bool + Send + Sync + 'static,
    {
        self.add_stage(move |input| {
            if validator(&input) {
                Ok(input)
            } else {
                Err(error_message.clone().into())
            }
        })
    }

    // short-circuiting on first error
    pub fn execute(&self, input: T) -> Result<T, Box<dyn Error + Send + Sync>> {
        self.stages.iter().try_fold(input, |acc, stage| stage(acc))
    }
}

/// Create a message processing pipeline
pub fn create_message_pipeline() -> Pipeline<Message> {
    Pipeline::new()
        .add_validation(
            |msg: &Message| !msg.topic.is_empty(),
            "Topic cannot be empty".to_string(),
        )
        .add_transformation(|mut msg| {
            // Normalize topic names
            msg.topic = msg.topic.to_lowercase();
            msg
        })
        .add_transformation(|mut msg| {
            // Add processing timestamp
            msg.processed_at = Some(current_timestamp());
            msg
        })
        .add_stage(|msg| {
            // Content-specific processing
            if msg.value.len() > 50000 {
                Err("Message too large".to_string().into())
            } else {
                Ok(msg)
            }
        })
}

/// Higher-order function that creates validators
pub fn create_length_validator(max_length: usize) -> impl Fn(&Message) -> bool {
    move |msg| msg.value.len() <= max_length
}

/// Higher-order function that creates topic filters
pub fn create_topic_filter(prefix: String) -> impl Fn(&Message) -> bool {
    move |msg| msg.topic.starts_with(&prefix)
}

/// Compose functions using closures
pub fn compose<A, B, C, F, G>(f: F, g: G) -> impl Fn(A) -> C
where
    F: Fn(A) -> B,
    G: Fn(B) -> C,
{
    move |x| g(f(x))
}

/// Create a configurable message processor
pub fn create_configurable_processor<F>(transform: F) -> impl Fn(Message) -> Message
where
    F: Fn(&mut Message),
{
    move |mut msg| {
        transform(&mut msg);
        msg
    }
}

/// Closure-based caching
pub struct Cache<K, V, F> {
    cache: Arc<Mutex<HashMap<K, V>>>,
    loader: F,
}

impl<K, V, F> Cache<K, V, F>
where
    K: Hash + Eq + Clone,
    V: Clone,
    F: Fn(&K) -> V,
{
    pub fn new(loader: F) -> Self {
        Self {
            cache: Arc::new(Mutex::new(HashMap::new())),
            loader,
        }
    }

    pub fn get(&self, key: &K) -> V {
        let mut cache = self.cache.lock().unwrap();
        if let Some(value) = cache.get(key) {
            value.clone()
        } else {
            let value = (self.loader)(key);
            cache.insert(key.clone(), value.clone());
            value
        }
    }
}

/// Retry logic using closures
pub fn with_retry<F, T, E>(operation: F, max_attempts: u32) -> Result<T, E>
where
    F: Fn() -> Result<T, E>,
{
    let mut attempts = 0;
    loop {
        match operation() {
            Ok(result) => return Ok(result),
            Err(e) => {
                attempts += 1;
                if attempts >= max_attempts {
                    return Err(e);
                }
                // In a real implementation, add delay between retries
            }
        }
    }
}

// ============ //
// Rate Limiter //
// ============ //

/// Rate limiting using closures
pub struct RateLimiter<F> {
    operation: F,
    last_call: Arc<Mutex<Option<Instant>>>,
    interval: Duration,
}

impl<F> RateLimiter<F>
where
    F: Fn(),
{
    pub fn new(operation: F, interval: Duration) -> Self {
        Self {
            operation,
            last_call: Arc::new(Mutex::new(None)),
            interval,
        }
    }

    pub fn call(&self) {
        let mut last_call = self.last_call.lock().unwrap();
        let now = Instant::now();

        if let Some(last) = *last_call {
            if now.duration_since(last) < self.interval {
                return; // Rate limited (not doing anything; no operation)
            }
        }

        *last_call = Some(now);
        (self.operation)();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_filter_composition() {
        let large_filter =
            MessageFilter::new("Large".to_string(), |msg: &Message| msg.value.len() > 100);

        let system_filter = MessageFilter::new("System".to_string(), |msg: &Message| {
            msg.topic.starts_with("system.")
        });

        let composite = large_filter.and(system_filter);

        let msg = Message::new("system.alerts", None, vec![0u8; 200]);
        assert!(composite.matches(&msg));
    }

    #[test]
    fn test_routing_strategies() {
        let router = create_routing_strategies();

        let system_msg = Message::new("system.alerts", None, b"test");
        let routes = router.route_message(&system_msg);
        assert!(routes.contains(&"system_alerts_queue".to_string()));

        let large_msg = Message::new("test", None, vec![0u8; 20000]);
        let routes = router.route_message(&large_msg);
        assert!(routes.contains(&"large_message_queue".to_string()));
    }

    #[test]
    fn test_event_bus() {
        let bus = EventBus::new();

        bus.subscribe("test", |_event: &SystemEvent| {
            // In a real test, we'd use Arc<Mutex<bool>> to capture this
            println!("Event received!");
        });

        bus.publish(
            "test",
            &SystemEvent::MessageReceived {
                message_id: 123,
                topic: "test".to_string(),
            },
        );
    }

    #[test]
    fn test_pipeline() {
        let pipeline = create_message_pipeline();

        let msg = Message::new("TEST.TOPIC", None, b"Hello");
        let result = pipeline.execute(msg).unwrap();

        assert_eq!(result.topic, "test.topic");
    }

    #[test]
    fn test_cache() {
        let cache = Cache::new(|key: &String| format!("computed_{}", key));

        let result1 = cache.get(&"test".to_string());
        let result2 = cache.get(&"test".to_string());

        assert_eq!(result1, result2);
        assert_eq!(result1, "computed_test");
    }

    #[test]
    fn test_higher_order_functions() {
        let validator = create_length_validator(100);
        let topic_filter = create_topic_filter("api.".to_string());

        let short_api_msg = Message::new("api.users", None, b"test");
        assert!(validator(&short_api_msg));
        assert!(topic_filter(&short_api_msg));

        let composed = compose(|x: i32| x + 1, |x: i32| x * 2);
        assert_eq!(composed(5), 12); // (5 + 1) * 2 = 12
    }

    #[test]
    fn test_message_filter_or() {
        let filter_a =
            MessageFilter::new("A".to_string(), |msg: &Message| msg.topic.starts_with("a"));
        let filter_b =
            MessageFilter::new("B".to_string(), |msg: &Message| msg.topic.starts_with("b"));

        let combined = filter_a.or(filter_b);

        let msg_a = Message::new("alpha", None, b"data");
        let msg_b = Message::new("beta", None, b"data");
        let msg_c = Message::new("gamma", None, b"data");

        assert!(combined.matches(&msg_a));
        assert!(combined.matches(&msg_b));
        assert!(!combined.matches(&msg_c));
        assert_eq!(combined.name(), "A OR B");
    }

    #[test]
    fn test_with_retry_success() {
        let call_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count_clone = call_count.clone();
        let result = with_retry(
            move || {
                count_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if count_clone.load(std::sync::atomic::Ordering::SeqCst) < 3 {
                    Err("not yet")
                } else {
                    Ok("success")
                }
            },
            5,
        );
        assert_eq!(result, Ok("success"));
        assert_eq!(call_count.load(std::sync::atomic::Ordering::SeqCst), 3);
    }

    #[test]
    fn test_with_retry_exhaustion() {
        let result = with_retry(|| -> Result<(), &str> { Err("always fail") }, 3);
        assert_eq!(result, Err("always fail"));
    }

    #[test]
    fn test_rate_limiter() {
        use std::thread;
        use std::time::Duration;

        let call_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count_clone = call_count.clone();

        let limiter = RateLimiter::new(
            move || {
                count_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            },
            Duration::from_millis(50),
        );

        limiter.call();
        limiter.call(); // should be rate limited

        thread::sleep(Duration::from_millis(60));
        limiter.call();

        assert_eq!(call_count.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[test]
    fn test_configurable_processor() {
        let processor = create_configurable_processor(|msg: &mut Message| {
            msg.topic = msg.topic.to_uppercase();
        });

        let msg = Message::new("hello", None, b"data");
        let result = processor(msg);
        assert_eq!(result.topic, "HELLO");
    }

    #[test]
    fn test_pipeline_validation_failure() {
        let pipeline = create_message_pipeline();

        let empty_topic_msg = Message::new("", None, b"test");
        let result = pipeline.execute(empty_topic_msg);
        assert!(result.is_err());
    }

    #[test]
    fn test_create_message_filters() {
        let (large, system, recent) = create_message_filters();

        let large_msg = Message::new("test", None, vec![0u8; 2000]);
        assert!(large.matches(&large_msg));

        let system_msg = Message::new("system.alerts", None, b"data");
        assert!(system.matches(&system_msg));

        let mut recent_msg = Message::new("test", None, b"data");
        recent_msg.timestamp = current_timestamp();
        assert!(recent.matches(&recent_msg));

        let mut stale_msg = Message::new("test", None, b"data");
        stale_msg.timestamp = 0;
        assert!(!recent.matches(&stale_msg));

        let small_msg = Message::new("test", None, b"small");
        assert!(!large.matches(&small_msg));
        assert!(!system.matches(&small_msg));
    }
}
