//! A compact price-level order book.
//!
//! Prediction-market books are small (at most a few hundred levels) and we hold
//! several copies of every book (one per redundant connection plus the output
//! mirror) for hundreds of thousands of symbols, so a sorted `Vec` beats a
//! `BTreeMap` on both memory and cache behaviour.

use chrono::{DateTime, Utc};

use crate::{Side, exchange::Level};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OrderBook {
    /// Sorted by price, best (highest) first.
    bids: Vec<Level>,
    /// Sorted by price, best (lowest) first.
    asks: Vec<Level>,
    pub exchange_ts: Option<DateTime<Utc>>,
}

/// A change to a single level, produced when replacing or mutating a book.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LevelChange {
    pub side: Side,
    pub price: i64,
    pub quantity: i64,
}

impl OrderBook {
    pub fn from_levels(bids: Vec<Level>, asks: Vec<Level>, exchange_ts: DateTime<Utc>) -> Self {
        let mut book = Self {
            bids: Vec::new(),
            asks: Vec::new(),
            exchange_ts: Some(exchange_ts),
        };
        book.bids = normalise(bids, Side::Buy);
        book.asks = normalise(asks, Side::Sell);
        book
    }

    pub fn bids(&self) -> &[Level] {
        &self.bids
    }

    pub fn asks(&self) -> &[Level] {
        &self.asks
    }

    fn side_mut(&mut self, side: Side) -> &mut Vec<Level> {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
    }

    fn find(levels: &[Level], side: Side, price: i64) -> Result<usize, usize> {
        match side {
            Side::Buy => levels.binary_search_by(|l| price.cmp(&l.price)),
            Side::Sell => levels.binary_search_by(|l| l.price.cmp(&price)),
        }
    }

    pub fn quantity(&self, side: Side, price: i64) -> i64 {
        let levels = match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        };
        Self::find(levels, side, price).map_or(0, |i| levels[i].quantity)
    }

    /// Sets the absolute quantity at a level. Returns the change if the book
    /// actually changed.
    pub fn set(&mut self, side: Side, price: i64, quantity: i64) -> Option<LevelChange> {
        let quantity = quantity.max(0);
        let levels = self.side_mut(side);
        match Self::find(levels, side, price) {
            Ok(i) if quantity == 0 => {
                levels.remove(i);
            }
            Ok(i) if levels[i].quantity == quantity => return None,
            Ok(i) => levels[i].quantity = quantity,
            Err(_) if quantity == 0 => return None,
            Err(i) => levels.insert(i, Level { price, quantity }),
        }
        Some(LevelChange { side, price, quantity })
    }

    /// Adds `delta` to the quantity at a level.
    pub fn add(&mut self, side: Side, price: i64, delta: i64) -> Option<LevelChange> {
        let quantity = self.quantity(side, price).saturating_add(delta);
        self.set(side, price, quantity)
    }

    /// Replaces this book with `other`, appending the per-level changes needed
    /// to turn the old book into the new one.
    pub fn replace_diff(&mut self, other: OrderBook, changes: &mut Vec<LevelChange>) {
        diff_side(&self.bids, &other.bids, Side::Buy, changes);
        diff_side(&self.asks, &other.asks, Side::Sell, changes);
        *self = other;
    }
}

fn normalise(mut levels: Vec<Level>, side: Side) -> Vec<Level> {
    levels.retain(|l| l.quantity > 0);
    match side {
        Side::Buy => levels.sort_unstable_by_key(|l| std::cmp::Reverse(l.price)),
        Side::Sell => levels.sort_unstable_by_key(|l| l.price),
    }
    // Last write wins for duplicate prices.
    levels.dedup_by(|later, earlier| {
        if later.price == earlier.price {
            earlier.quantity = later.quantity;
            true
        } else {
            false
        }
    });
    levels
}

fn diff_side(old: &[Level], new: &[Level], side: Side, changes: &mut Vec<LevelChange>) {
    // Both sides are sorted in "best first" order, so a merge walk works if we
    // compare in that order.
    let better = |a: i64, b: i64| match side {
        Side::Buy => a > b,
        Side::Sell => a < b,
    };
    let (mut i, mut j) = (0, 0);
    while i < old.len() || j < new.len() {
        match (old.get(i), new.get(j)) {
            (Some(o), Some(n)) if o.price == n.price => {
                if o.quantity != n.quantity {
                    changes.push(LevelChange { side, price: n.price, quantity: n.quantity });
                }
                i += 1;
                j += 1;
            }
            (Some(o), Some(n)) if better(o.price, n.price) => {
                changes.push(LevelChange { side, price: o.price, quantity: 0 });
                i += 1;
            }
            (Some(_), Some(n)) | (None, Some(n)) => {
                changes.push(LevelChange { side, price: n.price, quantity: n.quantity });
                j += 1;
            }
            (Some(o), None) => {
                changes.push(LevelChange { side, price: o.price, quantity: 0 });
                i += 1;
            }
            (None, None) => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lv(price: i64, quantity: i64) -> Level {
        Level { price, quantity }
    }

    fn ts() -> DateTime<Utc> {
        DateTime::from_timestamp_millis(0).unwrap()
    }

    #[test]
    fn set_and_add() {
        let mut book = OrderBook::default();
        assert!(book.set(Side::Buy, 10, 5).is_some());
        assert!(book.set(Side::Buy, 12, 1).is_some());
        assert!(book.set(Side::Buy, 12, 1).is_none());
        assert_eq!(book.bids(), &[lv(12, 1), lv(10, 5)]);
        book.add(Side::Buy, 10, -5);
        assert_eq!(book.bids(), &[lv(12, 1)]);
        assert!(book.set(Side::Sell, 30, 0).is_none());
        book.set(Side::Sell, 30, 2);
        book.set(Side::Sell, 20, 2);
        assert_eq!(book.asks(), &[lv(20, 2), lv(30, 2)]);
    }

    #[test]
    fn normalises_snapshot() {
        let book = OrderBook::from_levels(vec![lv(1, 1), lv(3, 0), lv(2, 2)], vec![lv(9, 1), lv(5, 1)], ts());
        assert_eq!(book.bids(), &[lv(2, 2), lv(1, 1)]);
        assert_eq!(book.asks(), &[lv(5, 1), lv(9, 1)]);
    }

    #[test]
    fn diff_reproduces_target() {
        let mut a = OrderBook::from_levels(vec![lv(5, 1), lv(4, 2), lv(2, 3)], vec![lv(6, 1), lv(8, 1)], ts());
        let b = OrderBook::from_levels(vec![lv(5, 1), lv(3, 7), lv(2, 4)], vec![lv(7, 1), lv(8, 2), lv(9, 1)], ts());
        let mut replay = a.clone();
        let mut changes = Vec::new();
        a.replace_diff(b.clone(), &mut changes);
        assert_eq!(a, b);
        for c in &changes {
            replay.set(c.side, c.price, c.quantity);
        }
        assert_eq!(replay.bids(), b.bids());
        assert_eq!(replay.asks(), b.asks());
        assert_eq!(changes.len(), 7);
    }
}
