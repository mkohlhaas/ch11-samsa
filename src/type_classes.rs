//! Generics as Type Classes pattern implementation
//!
//! This module demonstrates using Rust's generics system to model
//! type classes from functional programming, enhancing the TypeState
//! pattern with more flexibility.

use crate::error::{self, SamsaError};
use crate::message::current_timestamp;
use rand::random;
use std::marker::PhantomData;

// ============= //
// Subscriptions //
// ============= //

/// State type markers
pub mod state {
    #[derive(Debug)]
    pub struct Pending;

    #[derive(Debug)]
    pub struct Active;

    #[derive(Debug)]
    pub struct Suspended;

    #[derive(Debug)]
    pub struct Cancelled;
}

/// Type class for subscriptions that can be activated
pub trait ActivatableSubscription {
    type Output;
    fn activate(self) -> Result<Self::Output, SamsaError>;
}

/// Type class for subscriptions that can be suspended  
pub trait SuspendableSubscription {
    type Output;
    fn suspend(self, reason: String) -> Self::Output;
}

/// Type class for subscriptions that can be canceled
pub trait CancellableSubscription {
    type Output;
    fn cancel(self, reason: String) -> Self::Output;
}

/// Type class for subscriptions that can deliver messages (or not)
pub trait MessageDeliverableSubscription {
    fn deliver_message(&self, message: &str) -> Result<(), SamsaError>;
}

/// Generic subscription with phantom state
#[derive(Debug)]
pub struct Subscription<S> {
    pub id: u64,
    pub user_id: u64,
    pub topic: String,
    pub created_at: u64,
    state: PhantomData<S>,
}

impl Subscription<state::Pending> {
    pub fn new(id: u64, user_id: u64, topic: String) -> Self {
        Self {
            id,
            user_id,
            topic,
            created_at: current_timestamp(),
            state: PhantomData,
        }
    }
}

impl ActivatableSubscription for Subscription<state::Pending> {
    type Output = Subscription<state::Active>;

    fn activate(self) -> Result<Self::Output, SamsaError> {
        if self.user_id == 0 {
            return Err(SamsaError::activation("Invalid user"));
        }

        if self.topic.is_empty() {
            return Err(SamsaError::activation("Topic not found"));
        }

        Ok(Subscription {
            id: self.id,
            user_id: self.user_id,
            topic: self.topic,
            created_at: self.created_at,
            state: PhantomData,
        })
    }
}

impl SuspendableSubscription for Subscription<state::Active> {
    type Output = Subscription<state::Suspended>;

    fn suspend(self, _reason: String) -> Self::Output {
        Subscription {
            id: self.id,
            user_id: self.user_id,
            topic: self.topic,
            created_at: self.created_at,
            state: PhantomData,
        }
    }
}

impl CancellableSubscription for Subscription<state::Active> {
    type Output = Subscription<state::Cancelled>;

    fn cancel(self, _reason: String) -> Self::Output {
        Subscription {
            id: self.id,
            user_id: self.user_id,
            topic: self.topic,
            created_at: self.created_at,
            state: PhantomData,
        }
    }
}

impl CancellableSubscription for Subscription<state::Suspended> {
    type Output = Subscription<state::Cancelled>;

    fn cancel(self, _reason: String) -> Self::Output {
        Subscription {
            id: self.id,
            user_id: self.user_id,
            topic: self.topic,
            created_at: self.created_at,
            state: PhantomData,
        }
    }
}

impl MessageDeliverableSubscription for Subscription<state::Active> {
    // we pretend every message can be delivered without any errors
    fn deliver_message(&self, message: &str) -> Result<(), SamsaError> {
        println!(
            "Delivering message '{}' to subscription {}",
            message, self.id
        );
        Ok(())
    }
}

// ------------------------ //
// Generic Helper Functions //
// ------------------------ //

/// Generic function that works with any cancellable subscription
pub fn cancel_subscription_with_audit<S>(
    subscription: Subscription<S>,
    reason: String,
) -> Subscription<state::Cancelled>
where
    Subscription<S>: CancellableSubscription<Output = Subscription<state::Cancelled>>,
{
    println!(
        "Auditing cancellation of subscription {}: {}",
        subscription.id, reason
    );
    subscription.cancel(reason)
}

/// Generic function for message delivery with fallback
pub fn try_deliver_message<S>(subscription: &Subscription<S>, message: &str) -> bool
where
    Subscription<S>: MessageDeliverableSubscription,
{
    match subscription.deliver_message(message) {
        Ok(()) => {
            println!("Message delivered successfully");
            true
        }
        Err(e) => {
            println!("Message delivery failed: {:?}", e);
            false
        }
    }
}

// ==================== //
// Subscription Manager //
// ==================== //

/// Subscription manager using type classes
#[derive(Default)]
pub struct SubscriptionManager {
    active_subscriptions: Vec<Subscription<state::Active>>,
    suspended_subscriptions: Vec<Subscription<state::Suspended>>,
    cancelled_subscriptions: Vec<Subscription<state::Cancelled>>,
}

impl SubscriptionManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create_subscription(&mut self, user_id: u64, topic: String) -> error::Result<u64> {
        let id = random();
        let pending_subscription = Subscription::new(id, user_id, topic);
        let active_subscription = pending_subscription.activate()?;

        self.active_subscriptions.push(active_subscription);
        Ok(id)
    }

    pub fn suspend_subscription(&mut self, id: u64, reason: String) -> error::Result<()> {
        if let Some(pos) = self.active_subscriptions.iter().position(|s| s.id == id) {
            let active_subscription = self.active_subscriptions.remove(pos);
            let suspended_subscription = active_subscription.suspend(reason);
            self.suspended_subscriptions.push(suspended_subscription);
            Ok(())
        } else {
            Err(SamsaError::consumer("Subscription not found or not active"))
        }
    }

    pub fn cancel_subscription(&mut self, id: u64, reason: String) -> error::Result<()> {
        // Try to cancel from active subscriptions
        if let Some(pos) = self.active_subscriptions.iter().position(|s| s.id == id) {
            let active_subscription = self.active_subscriptions.remove(pos);
            let cancelled_subscription = active_subscription.cancel(reason);
            self.cancelled_subscriptions.push(cancelled_subscription);
            return Ok(());
        }

        // Try to cancel from suspended subscriptions
        if let Some(pos) = self.suspended_subscriptions.iter().position(|s| s.id == id) {
            let suspended_subscription = self.suspended_subscriptions.remove(pos);
            let cancelled_subscription = suspended_subscription.cancel(reason);
            self.cancelled_subscriptions.push(cancelled_subscription);
            return Ok(());
        }

        Err(SamsaError::consumer("Subscription not found"))
    }

    pub fn broadcast_message(&self, topic: &str, message: &str) {
        for subscription in &self.active_subscriptions {
            if subscription.topic == topic {
                try_deliver_message(subscription, message);
            }
        }
    }
}

// ============================================== //
// Higher-order type class for monadic operations //
// ============================================== //

/// This demonstrates the Monad pattern from functional programming.
///
/// The `Output<B>` associated type is a **Generic Associated Type (GAT)**:
/// an associated type that itself takes a type parameter. Whereas a plain
/// associated type (like `Item`) fixes one concrete type per impl, a GAT
/// behaves like a type-level function — "give me a `B` and I get an
/// `Output<B>`". This lets a trait abstract over type constructors such as
/// `Result<_, E>` or `Option<_>`, which in languages with full
/// higher-kinded types (HKTs) would be written directly with a type
/// constructor parameter.
///
/// ```haskell
/// class Monad m where
///   return :: a -> m a                     // pure
///   (>>=)  :: m a -> (a -> m b) -> m b     // bind
/// ```
///
/// Rust has no HKTs, so `m` (the type constructor) cannot appear as a
/// parameter; instead the GAT `type Output<B>` plays the role of `m b`.
///
/// Output<...> acts as a template/placeholder, could be Result, Option, etc...
///
/// Item                 = a
/// Monad<A>             = m a
/// Output<B> = Monad<B> = m b
pub trait Monad {
    type Item;
    /// Higher-kinded proxy: wraps `B` in the same type constructor as `Self`
    /// (e.g. `Result<B, E>` for `Result<T, E>`).
    type Output<B>;

    /// Lifts a value into the monad.
    /// Haskell/Purescript: a -> m a
    fn pure(item: Self::Item) -> Self;

    /// Sequences an effect: unwraps `Self`, applies `f` to the inner value,
    /// and returns the re-wrapped result. Because the return type is the GAT
    /// `Self::Output<B>` (not an arbitrary `B`), the result stays inside the
    /// monad, preserving short-circuiting of failure.
    ///
    /// Haskell/Purescript: m a -> (a -> m b) -> m b
    fn bind<F, B>(self, f: F) -> Self::Output<B>
    where
        F: FnOnce(Self::Item) -> Self::Output<B>; // a -> m b
}

/// Result monad implementation
///
/// `bind` composes with `and_then`, so `Err` values are propagated instead of panicking.
///
/// https://pursuit.purescript.org/packages/purescript-prelude/6.0.2/docs/Control.Monad#t:Monad
impl<A, E> Monad for Result<A, E> {
    type Item = A;
    type Output<B> = Result<B, E>;

    // T -> Result<B, E>
    fn pure(item: Self::Item) -> Self {
        Ok(item)
    }

    // Result<T, E> -> (T -> Result<B, E>) -> Result<B, E>
    fn bind<F, B>(self, f: F) -> Self::Output<B>
    where
        F: FnOnce(Self::Item) -> Self::Output<B>,
    {
        self.and_then(f)
    }
}

/// Option monad implementation
///
/// `bind` composes with `and_then`, so `None` short-circuits the chain.
impl<A> Monad for Option<A> {
    type Item = A;
    type Output<B> = Option<B>;

    // A -> Option<A>
    fn pure(item: Self::Item) -> Self {
        Some(item)
    }

    // Option<A> -> (A -> Option<B>) -> Option<B>
    fn bind<F, B>(self, f: F) -> Self::Output<B>
    where
        F: FnOnce(Self::Item) -> Self::Output<B>,
    {
        self.and_then(f)
    }
}

/// Type class for foldable collections
pub trait Foldable {
    type Item;

    fn fold_left<B, F>(self, init: B, f: F) -> B
    where
        F: Fn(B, Self::Item) -> B;

    fn fold_right<B, F>(self, init: B, f: F) -> B
    where
        F: Fn(Self::Item, B) -> B;
}

impl<T> Foldable for Vec<T> {
    type Item = T;

    fn fold_left<B, F>(self, init: B, f: F) -> B
    where
        F: Fn(B, Self::Item) -> B,
    {
        self.into_iter().fold(init, f)
    }

    fn fold_right<B, F>(self, init: B, f: F) -> B
    where
        F: Fn(Self::Item, B) -> B,
    {
        self.into_iter().rev().fold(init, |acc, item| f(item, acc))
    }
}

/// Type class for mappable functors
///
/// Item                   = a
/// Functor<A>             = f a
/// Output<B> = Functor<B> = f b
///
/// https://pursuit.purescript.org/packages/purescript-prelude/6.0.2/docs/Data.Functor
pub trait Functor {
    type Item;
    type Output<B>;

    // f a -> (a -> b) -> f b
    fn map<B, F>(self, f: F) -> Self::Output<B>
    where
        F: FnOnce(Self::Item) -> B; // a -> b
}

/// Option functor implementation
///
/// https://pursuit.purescript.org/packages/purescript-prelude/6.0.2/docs/Data.Functor#t:Functor
impl<A> Functor for Option<A> {
    type Item = A;
    type Output<B> = Option<B>;

    // in Haskell/Purescript: Option T -> Option B
    fn map<B, F>(self, f: F) -> Self::Output<B>
    where
        F: FnOnce(Self::Item) -> B, // basically: T -> B (in Haskell/Purescript)
    {
        self.map(f)
    }
}

/// Result functor implementation
impl<A, E> Functor for Result<A, E> {
    type Item = A;
    type Output<B> = Result<B, E>;

    // in Haskell/Purescript: Either E A -> Either E B
    // the error type E is preserved by the GAT
    fn map<B, F>(self, f: F) -> Self::Output<B>
    where
        F: FnOnce(Self::Item) -> B,
    {
        self.map(f)
    }
}

/// Type class for filtering operations
pub trait Filterable {
    type Item;

    fn filter<F>(self, predicate: F) -> Self
    where
        F: Fn(&Self::Item) -> bool;
}

impl<T> Filterable for Vec<T> {
    type Item = T;

    fn filter<F>(self, predicate: F) -> Self
    where
        F: Fn(&Self::Item) -> bool,
    {
        self.into_iter().filter(|item| predicate(item)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscription_lifecycle() {
        let pending = Subscription::new(1, 123, "test.topic".to_string());
        let active = pending.activate().unwrap();
        let suspended = active.suspend("Maintenance".to_string());
        let _cancelled = cancel_subscription_with_audit(suspended, "User request".to_string());
    }

    #[test]
    fn test_message_delivery() {
        let pending = Subscription::new(2, 456, "events".to_string());
        let active = pending.activate().unwrap();

        assert!(try_deliver_message(&active, "Test message"));
    }

    #[test]
    fn test_type_class_operations() {
        // Foldable
        let numbers = vec![1, 2, 3, 4, 5];
        let sum = numbers.fold_left(0, |acc, x| acc + x);
        assert_eq!(sum, 15);

        // Functor
        let opt = Some("hello".to_string());
        let mapped = opt.map(|s| s.len());
        assert_eq!(mapped, Some(5));

        // Functor for Result preserves the error side
        let ok_result: Result<i32, &str> = Ok(5);
        let mapped = ok_result.map(|x| x * 2);
        assert_eq!(mapped, Ok(10));

        let err_result: Result<i32, &str> = Err("boom");
        let mapped = err_result.map(|x| x * 2);
        assert_eq!(mapped, Err("boom"));

        // Filterable
        let items = vec![1, 2, 3, 4, 5];
        let evens = items.filter(|x| x % 2 == 0);
        assert_eq!(evens, vec![2, 4]);
    }

    #[test]
    fn test_activation_errors() {
        let invalid_user = Subscription::new(1, 0, "topic".to_string());
        assert!(matches!(
            invalid_user.activate(),
            Err(SamsaError::Activation(ref msg)) if msg == "Invalid user"
        ));

        let empty_topic = Subscription::new(1, 100, "".to_string());
        assert!(matches!(
            empty_topic.activate(),
            Err(SamsaError::Activation(ref msg)) if msg == "Topic not found"
        ));

        let valid = Subscription::new(1, 100, "topic".to_string());
        assert!(valid.activate().is_ok());
    }

    #[test]
    fn test_monad_operations() {
        let ok_val: Result<i32, &str> = Ok(5);
        let result = ok_val.bind(|x| Ok(x * 2));
        assert_eq!(result, Ok(10));

        // bind short-circuits on Err instead of panicking
        let err_val: Result<i32, &str> = Err("boom");
        let result = err_val.bind(|x| Ok(x * 2));
        assert_eq!(result, Err("boom"));

        let pure_val = <Result<i32, &str> as Monad>::pure(42);
        assert_eq!(pure_val.unwrap(), 42);

        // Option monad
        let some_val: Option<i32> = Some(5);
        let result = some_val.bind(|x| Some(x * 2));
        assert_eq!(result, Some(10));

        // bind short-circuits on None
        let none_val: Option<i32> = None;
        let result = none_val.bind(|x| Some(x * 2));
        assert_eq!(result, None);

        let pure_val = <Option<i32> as Monad>::pure(42);
        assert_eq!(pure_val, Some(42));
    }

    #[test]
    fn test_fold_right() {
        let numbers = vec![1, 2, 3, 4, 5];
        let result = numbers.fold_right(0, |acc, x| acc + x);
        assert_eq!(result, 15);
    }

    #[test]
    fn test_subscription_manager_operations() {
        let mut manager = SubscriptionManager::new();

        let id = manager.create_subscription(100, "test.topic".to_string());
        assert!(id.is_ok());
        let id = id.unwrap();

        assert!(
            manager
                .suspend_subscription(id, "Maintenance".to_string())
                .is_ok()
        );
        assert!(manager.cancel_subscription(id, "Done".to_string()).is_ok());
    }

    #[test]
    fn test_message_deliverable_trait() {
        let pending = Subscription::new(1, 100, "topic".to_string());
        let active = pending.activate().unwrap();

        assert!(active.deliver_message("test").is_ok());

        let _suspended = active.suspend("reason".to_string());
        // Suspended subscriptions do not implement MessageDeliverableSubscription,
        // so delivery is prevented at compile time:
        // suspended.deliver_message("test"); // no longer compiles
    }
}
