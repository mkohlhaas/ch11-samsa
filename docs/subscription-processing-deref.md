# `SubscriptionProcessing` and the `Deref` design note

Design notes for `SubscriptionProcessing` in `src/pipeline.rs`, covering why the
trait is bound on `Deref<Target = SubscriptionEvent>` instead of
`Iterator<Item = SubscriptionEvent>`, and what the code looks like without it.

## 1. Why do we need `Deref`?

`Deref` is what lets `&SubscriptionEvent` stand in for `SubscriptionEvent`
transparently.

The old trait required owned items: `Iterator<Item = SubscriptionEvent>`. A
borrowed slice yields `&SubscriptionEvent`, so we had to materialize owned
copies via `.cloned()`.

Changing the bound to `Self::Item: Deref<Target = SubscriptionEvent>` makes the
combinators work generically on borrowed items because:

- `&SubscriptionEvent` implements `Deref<Target = SubscriptionEvent>` (the
  standard `std` impl for references), so `slice::Iter` qualifies.
- Method/field access auto-derefs: in `valid_subscriptions`,
  `event.is_valid()` on a `&Self::Item` (`= &&SubscriptionEvent`) resolves by
  deref coercion to the `impl SubscriptionEvent` methods, and `event.timestamp`
  in `recent_events` auto-derefs too — no manual `*.` needed.
- Items stay borrowed end-to-end: the filter combinators pass `Self::Item`
  through unchanged, so no clones of the whole event occur.

### Alternatives considered

- `Borrow<SubscriptionEvent>` — implemented reflexively for `SubscriptionEvent`
  but **not** for `&SubscriptionEvent` in `std`, so it would not cover the
  borrowed case the same way.
- Keeping owned items — forces `.cloned()` again (see section 6).

Trade-off: `count_by_topic` now does `event.topic.clone()` per entry — only the
small `String`, not the whole event.

## 2. Is `Self::Item` from the `Iterator` trait?

Yes. `Item` is the associated type of the `Iterator` trait (`Iterator::Item`),
and `Self::Item` refers to it within the `SubscriptionProcessing` trait, which
is bound by `Self: Iterator`.

The `where Self::Item: Deref<Target = SubscriptionEvent>` clause means:
"whatever type this iterator yields must deref to `SubscriptionEvent`" — e.g.
`slice::Iter` has `Item = &SubscriptionEvent`, which satisfies it.

## 3. What does `pub trait SubscriptionProcessing: Iterator + Sized` mean?

It is a trait with a supertrait constraint:

- `pub trait SubscriptionProcessing` — defines the trait, public (exported).
- `: Iterator` — a *supertrait bound*: any type implementing
  `SubscriptionProcessing` must also implement `Iterator`. This lets the trait
  methods (like `recent_events`) call iterator combinator methods (`filter`,
  `fold`) on `self`.
- `+ Sized` — the implementing type must be `Sized` (have a known size at
  compile time). Common for traits like this because the methods use `self` by
  value (e.g. `fn recent_events(self, ...)`), which cannot be called on
  unsized types (e.g. `dyn Iterator` or `[T]`).

So: "only sized iterators can be given these subscription-processing
extensions."

## 4. Does that mean `SubscriptionProcessing` is an `Iterator`?

Not exactly — it is the reverse. Any type implementing `SubscriptionProcessing`
must *also be* an `Iterator`. `SubscriptionProcessing` itself is not necessarily
an `Iterator`; it builds *on top of* one.

Roughly: `SubscriptionProcessing: Iterator` ≈ "impl `SubscriptionProcessing`
implies impl `Iterator`" (each implementor is required to be an `Iterator`). It
is like inheritance: `SubscriptionProcessing` is a subinterface of `Iterator`,
not an `Iterator` itself.

`slice::Iter` is *both*:

- It implements the `Iterator` trait, yielding `&SubscriptionEvent`.
- Because of the blanket impl
  `impl<I> SubscriptionProcessing for I where I: Iterator + Sized, I::Item:
  Deref<Target = SubscriptionEvent>`, it is also a `SubscriptionProcessing`.

That is why `events.iter().recent_events(...)` compiles.

## 5. What is `&events` in `examples/functional_patterns.rs`?

`&events` is a shared reference (borrow) to the `Vec<SubscriptionEvent>`
declared at `examples/functional_patterns.rs:81`. Its type is
`&Vec<SubscriptionEvent>`, which auto-coerces to `&[SubscriptionEvent]` at the
call sites (lines 110, 120, 398) because the functions take a slice.

It is **not** an iterator. It is just the slice argument passed to the
functions:

- `process_subscription_events(&events)` — iterates internally with
  `events.iter()`.
- `analyze_recent_subscriptions(&events, cutoff)` — inside, `events.iter()`
  produces the `slice::Iter` (the thing that is both `Iterator` and
  `SubscriptionProcessing`).

So the chain is: `&events` (borrowed `Vec`) → `events.iter()`
(`slice::Iter<SubscriptionEvent>`, an `Iterator<Item = &SubscriptionEvent>`) →
implements `SubscriptionProcessing` via the blanket impl → combinators work, no
clones.

## 6. Without the `Deref` trick, how would the code look?

Without `Deref`, `SubscriptionProcessing` would need owned items again, so
`analyze_recent_subscriptions` would have to clone to feed it a `slice::Iter`:

```rust
pub trait SubscriptionProcessing: Iterator<Item = SubscriptionEvent> + Sized {
    // Iterators //
    fn valid_subscriptions(self) -> impl Iterator<Item = SubscriptionEvent> {
        self.filter(|event| event.is_valid())
            .filter(|event| event.is_subscription())
    }
    fn recent_events(self, cutoff_timestamp: u64) -> impl Iterator<Item = SubscriptionEvent> {
        self.filter(move |event| event.timestamp >= cutoff_timestamp)
    }

    // Result //
    fn count_by_topic(self) -> HashMap<String, usize> {
        self.fold(HashMap::new(), |mut acc, event| {
            *acc.entry(event.topic).or_insert(0) += 1;
            acc
        })
    }
}

pub fn analyze_recent_subscriptions(
    events: &[SubscriptionEvent],
    cutoff_timestamp: u64,
) -> HashMap<String, usize> {
    events
        .iter()
        .cloned() // clones each event to get owned SubscriptionEvent items
        .recent_events(cutoff_timestamp)
        .valid_subscriptions()
        .count_by_topic()
}
```

Plus the blanket impl reverts to:

```rust
impl<I> SubscriptionProcessing for I where I: Iterator<Item = SubscriptionEvent> {}
```

And the `use std::ops::Deref;` import goes away.

**Trade-off:** the trait is simpler (`Item` ops work directly, `event.topic`
moves, no `Deref`/`clone()` on the topic string), but the pipeline clones every
whole `SubscriptionEvent` upfront rather than only cloning the `String` topic
on insert like the `Deref` version does.