//! Iterator + Deref pattern demo
//!
//! A self-contained example walking through the trait design discussed in
//! `docs/subscription-processing-deref.md`, applied to a fresh domain
//! (`PageView`) so it reads as a reusable pattern rather than subscription
//! plumbing.
//!
//! Topics covered:
//!   1. Supertrait bound `: Iterator + Sized`
//!   2. `Self::Item` — the `Iterator` associated type
//!   3. `Deref<Target = PageView>` bound and why (no cloning of whole events)
//!   4. Blanket implementation and which `I`'s are valid
//!   5. `&views` vs `views.iter()` (borrowed slice argument, `slice::Iter` inside)
//!   6. Autoderef on the fly — we never materialize an owned `PageView`
//!   7. The non-`Deref` alternative using `.cloned()` and its trade-off

use std::collections::HashMap;
use std::ops::Deref;

/// A single page view record.
#[derive(Debug, Clone, PartialEq)]
struct PageView {
    user_id: u64,
    page: String,
    timestamp: u64,
}

impl PageView {
    fn is_valid(&self) -> bool {
        self.user_id > 0 && !self.page.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Topics 1-3: supertrait, associated type, and Deref bound.
//
// `: Iterator` is a supertrait bound: any type implementing
// `PageViewProcessing` must ALSO implement `Iterator`. `+ Sized` guarantees we
// can take `self` by value in the methods.
//
// `Self::Item` is `Iterator`'s associated type. We require it to deref to
// `PageView`, which lets the combinators work on BORROWED items
// (`&PageView`) with no `.cloned()` in the pipeline.
// ---------------------------------------------------------------------------

trait PageViewProcessing: Iterator + Sized
where
    Self::Item: Deref<Target = PageView>,
{
    // Keeps borrowed items: filters pass `Self::Item` through unchanged.
    fn recent_views(self, cutoff_timestamp: u64) -> impl Iterator<Item = Self::Item> {
        self.filter(move |view| view.timestamp >= cutoff_timestamp)
    }

    fn valid_views(self) -> impl Iterator<Item = Self::Item> {
        self.filter(|view| view.is_valid())
    }

    // Fold; `view.page.clone()` derefs the borrow on the fly and clones only
    // the small String key, never the whole PageView.
    fn count_by_page(self) -> HashMap<String, usize> {
        self.fold(HashMap::new(), |mut acc, view| {
            *acc.entry(view.page.clone()).or_insert(0) += 1;
            acc
        })
    }
}

// ---------------------------------------------------------------------------
// Topic 4: blanket implementation.
//
// ANY `I` that is a sized iterator whose items deref to `PageView` gets these
// methods automatically. Valid: `slice::Iter<'_, PageView>`
// (Item = &PageView) and `slice::IterMut<'_, PageView>`
// (Item = &mut PageView). NOT valid: owned `vec::IntoIter<PageView>`,
// because `PageView` itself does not implement `Deref<Target = PageView>`.
// ---------------------------------------------------------------------------

impl<I> PageViewProcessing for I
where
    I: Iterator + Sized,
    I::Item: Deref<Target = PageView>,
{
}

// ---------------------------------------------------------------------------
// Topic 5: `&views` vs `views.iter()`.
//
// `views` here is `&[PageView]` (deref-coerced from `&Vec<PageView>` at the
// call site). It is NOT an iterator. `views.iter()` produces the
// `slice::Iter` — the thing that is both `Iterator` and (via the blanket impl)
// a `PageViewProcessing`.
// ---------------------------------------------------------------------------

fn analyze_recent_views(views: &[PageView], cutoff_timestamp: u64) -> HashMap<String, usize> {
    views
        .iter() // slice::Iter<'_, PageView>, Item = &PageView
        .recent_views(cutoff_timestamp) // Filter: timestamp >= cutoff
        .valid_views() // Filter: is_valid()
        .count_by_page() // fold into HashMap<String, usize>
}

// ---------------------------------------------------------------------------
// Topic 7: the non-Deref alternative.
//
// Requires owned items (`Iterator<Item = PageView>`), so the pipeline must
// `.cloned()` the whole record up front. Same result, more copying.
// ---------------------------------------------------------------------------

trait PageViewProcessingOwned: Iterator<Item = PageView> + Sized {
    fn recent_views_owned(self, cutoff_timestamp: u64) -> impl Iterator<Item = PageView> {
        self.filter(move |view| view.timestamp >= cutoff_timestamp)
    }

    fn valid_views_owned(self) -> impl Iterator<Item = PageView> {
        self.filter(|view| view.is_valid())
    }

    fn count_by_page_owned(self) -> HashMap<String, usize> {
        self.fold(HashMap::new(), |mut acc, view| {
            *acc.entry(view.page).or_insert(0) += 1;
            acc
        })
    }
}

impl<I> PageViewProcessingOwned for I where I: Iterator<Item = PageView> {}

fn analyze_recent_views_owned(views: &[PageView], cutoff_timestamp: u64) -> HashMap<String, usize> {
    views
        .iter()
        .cloned() // clones each PageView up front
        .recent_views_owned(cutoff_timestamp)
        .valid_views_owned()
        .count_by_page_owned()
}

fn main() {
    println!("=== Iterator + Deref pattern demo ===\n");

    let mut views = vec![
        PageView {
            user_id: 1,
            page: "home".into(),
            timestamp: 300,
        },
        PageView {
            user_id: 2,
            page: "home".into(),
            timestamp: 200,
        },
        PageView {
            user_id: 3,
            page: "pricing".into(),
            timestamp: 100,
        },
        PageView {
            user_id: 0,
            page: "".into(),
            timestamp: 400,
        }, // invalid
        PageView {
            user_id: 4,
            page: "docs".into(),
            timestamp: 800,
        },
    ];

    let cutoff = 150u64;

    println!("1. Trait `PageViewProcessing: Iterator + Sized` is a supertrait:");
    println!("   any implementor must also be an Iterator.\n");

    println!("2. `Self::Item` is Iterator's associated type; the trait requires:");
    println!("   Self::Item: Deref<Target = PageView>. Borrowed items qualify.\n");

    println!("3. Blanket impl: any sized iterator whose items deref to PageView");
    println!("   gets the methods; e.g. slice::Iter and slice::IterMut.\n");

    println!("4. `&views` is a &[PageView] argument; `views.iter()` yields the");
    println!("   slice::Iter that is both Iterator and PageViewProcessing.\n");

    println!("5. Autoderef is on the fly: filters read fields through the borrow,");
    println!("   no owned PageView is ever materialized.\n");

    let deref_result = analyze_recent_views(&views, cutoff);
    println!(
        "Deref pipeline      (cutoff >= {}): {:?}",
        cutoff, deref_result
    );

    let owned_result = analyze_recent_views_owned(&views, cutoff);
    println!("Owned .cloned() pipeline      : {:?}", owned_result);
    println!(
        "Identical results? {}",
        if deref_result == owned_result {
            "yes"
        } else {
            "no"
        }
    );

    // iter_mut also satisfies the blanket impl
    let mutable_counts = views.iter_mut().valid_views().count_by_page();
    println!("\niter_mut via blanket impl  : {:?}", mutable_counts);
    println!(
        "Same as deref count (all views, no cutoff)? {}",
        if mutable_counts == views.iter().valid_views().count_by_page() {
            "yes"
        } else {
            "no"
        }
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_views() -> Vec<PageView> {
        vec![
            PageView {
                user_id: 1,
                page: "home".into(),
                timestamp: 300,
            },
            PageView {
                user_id: 2,
                page: "home".into(),
                timestamp: 200,
            },
            PageView {
                user_id: 3,
                page: "pricing".into(),
                timestamp: 100,
            },
            PageView {
                user_id: 0,
                page: "".into(),
                timestamp: 400,
            }, // invalid
            PageView {
                user_id: 4,
                page: "docs".into(),
                timestamp: 800,
            },
        ]
    }

    #[test]
    fn deref_pipeline_filters_and_counts() {
        let result = analyze_recent_views(&sample_views(), 150);
        assert_eq!(result.get("home"), Some(&2));
        assert_eq!(result.get("docs"), Some(&1));
        // pricing at 100 < 150 is excluded; invalid user/page excluded
        assert_eq!(result.get("pricing"), None);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn deref_and_cloned_versions_agree() {
        let views = sample_views();
        assert_eq!(
            analyze_recent_views(&views, 150),
            analyze_recent_views_owned(&views, 150)
        );
    }

    #[test]
    fn owned_intoiter_not_covered_but_cloned_makes_it_owned() {
        let views = sample_views();
        // `views.into_iter()` yields owned PageView; requires the Owned trait
        let result: HashMap<String, usize> = views
            .into_iter()
            .recent_views(150u64)
            .valid_views()
            .count_by_page();
        assert_eq!(result.get("home"), Some(&2));
    }

    #[test]
    fn iter_mut_satisfies_blanket_impl() {
        let mut views = sample_views();
        let result: HashMap<String, usize> = views.iter_mut().valid_views().count_by_page();
        assert_eq!(result.get("home"), Some(&2));
        assert_eq!(result.get("pricing"), Some(&1));
        assert_eq!(result.get("docs"), Some(&1));
    }
}

