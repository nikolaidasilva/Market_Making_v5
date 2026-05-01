use std::time::Instant;

const MAX_PRICE: usize = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Yes,
    No,
}

#[derive(Debug, Clone)]
pub struct BookSide {
    pub quantities: [i64; MAX_PRICE],
    /// Bitmask: bit N is set if quantities[N] > 0. Prices 1-99 use bits 1-99.
    /// Finding best bid = 127 - mask.leading_zeros() which is O(1).
    pub occupied: u128,
    pub best_bid: i64,
    pub total_qty: i64,
}

impl BookSide {
    pub fn new() -> Self {
        Self {
            quantities: [0; MAX_PRICE],
            occupied: 0,
            best_bid: 0,
            total_qty: 0,
        }
    }

    #[inline(always)]
    fn recompute_best_bid(&mut self) {
        if self.occupied == 0 {
            self.best_bid = 0;
        } else {
            // Highest set bit position = 127 - leading_zeros
            self.best_bid = (127 - self.occupied.leading_zeros()) as i64;
        }
    }

    pub fn apply_snapshot(&mut self, levels: &[(i64, i64)]) {
        self.quantities = [0; MAX_PRICE];
        self.occupied = 0;
        self.total_qty = 0;
        for &(price, qty) in levels {
            if price >= 1 && price <= 99 {
                self.quantities[price as usize] = qty;
                self.total_qty += qty;
                if qty > 0 {
                    self.occupied |= 1u128 << price;
                }
            }
        }
        self.recompute_best_bid();
    }

    #[inline]
    pub fn apply_delta(&mut self, price: i64, delta: i64) {
        if price < 1 || price > 99 {
            return;
        }
        let idx = price as usize;
        let old_qty = self.quantities[idx];
        let new_qty = (old_qty + delta).max(0);
        self.quantities[idx] = new_qty;
        self.total_qty += new_qty - old_qty;

        if new_qty > 0 {
            self.occupied |= 1u128 << idx;
        } else {
            self.occupied &= !(1u128 << idx);
        }

        // Only recompute best_bid if this change could affect it
        if new_qty == 0 && price == self.best_bid {
            // Level depleted at best bid — O(1) recompute via leading_zeros
            self.recompute_best_bid();
        } else if new_qty > 0 && price > self.best_bid {
            self.best_bid = price;
        }
    }

    #[inline]
    pub fn best_bid_qty(&self) -> i64 {
        if self.best_bid >= 1 && self.best_bid <= 99 {
            self.quantities[self.best_bid as usize]
        } else {
            0
        }
    }

    /// Find the best bid strictly below `below_price` using bitmask.
    /// O(1) via leading_zeros on a masked subset.
    #[inline]
    pub fn best_bid_below(&self, below_price: i64) -> i64 {
        if below_price <= 1 {
            return 0;
        }
        // Mask off everything at or above `below_price`
        let mask = self.occupied & ((1u128 << below_price) - 1);
        if mask == 0 {
            0
        } else {
            (127 - mask.leading_zeros()) as i64
        }
    }

    pub fn top_n(&self, n: usize) -> Vec<(i64, i64)> {
        let mut result = Vec::with_capacity(n);
        let mut remaining = self.occupied;
        for _ in 0..n {
            if remaining == 0 {
                break;
            }
            let p = (127 - remaining.leading_zeros()) as usize;
            result.push((p as i64, self.quantities[p]));
            remaining &= !(1u128 << p);
        }
        result
    }
}

#[derive(Debug, Clone)]
pub struct OrderBook {
    pub yes_bids: BookSide,
    pub no_bids: BookSide,
    pub last_update: Instant,
    pub seq: u64,
    pub initialized: bool,
}

impl OrderBook {
    pub fn new() -> Self {
        Self {
            yes_bids: BookSide::new(),
            no_bids: BookSide::new(),
            last_update: Instant::now(),
            seq: 0,
            initialized: false,
        }
    }

    pub fn apply_snapshot(
        &mut self,
        yes_levels: &[(i64, i64)],
        no_levels: &[(i64, i64)],
        seq: u64,
    ) {
        self.yes_bids.apply_snapshot(yes_levels);
        self.no_bids.apply_snapshot(no_levels);
        self.seq = seq;
        self.last_update = Instant::now();
        self.initialized = true;
    }

    #[inline]
    pub fn apply_delta(&mut self, side: Side, price: i64, delta: i64, seq: u64) -> bool {
        if seq != self.seq + 1 {
            tracing::warn!(
                expected = self.seq + 1,
                got = seq,
                "Sequence gap detected — book state unreliable"
            );
            return false;
        }
        match side {
            Side::Yes => self.yes_bids.apply_delta(price, delta),
            Side::No => self.no_bids.apply_delta(price, delta),
        }
        self.seq = seq;
        self.last_update = Instant::now();
        true
    }

    #[inline]
    pub fn best_yes_bid(&self) -> i64 {
        self.yes_bids.best_bid
    }

    #[inline]
    pub fn best_yes_ask(&self) -> i64 {
        if self.no_bids.best_bid > 0 {
            100 - self.no_bids.best_bid
        } else {
            0
        }
    }

    pub fn yes_spread(&self) -> i64 {
        let ask = self.best_yes_ask();
        let bid = self.best_yes_bid();
        if ask > 0 && bid > 0 {
            ask - bid
        } else {
            i64::MAX
        }
    }

    pub fn midprice(&self) -> Option<f64> {
        let bid = self.best_yes_bid();
        let ask = self.best_yes_ask();
        if bid > 0 && ask > 0 && ask > bid {
            Some((bid as f64 + ask as f64) / 2.0)
        } else {
            None
        }
    }

    pub fn microprice(&self) -> Option<f64> {
        let bid = self.best_yes_bid();
        let ask = self.best_yes_ask();
        if bid <= 0 || ask <= 0 || ask <= bid {
            return None;
        }
        let bid_qty = self.yes_bids.best_bid_qty() as f64;
        let ask_qty = self.no_bids.best_bid_qty() as f64;
        if bid_qty + ask_qty == 0.0 {
            return self.midprice();
        }
        Some((bid as f64 * ask_qty + ask as f64 * bid_qty) / (bid_qty + ask_qty))
    }

    pub fn imbalance_ratio(&self) -> Option<f64> {
        let bid_qty = self.yes_bids.best_bid_qty() as f64;
        let ask_qty = self.no_bids.best_bid_qty() as f64;
        if ask_qty > 0.0 {
            Some(bid_qty / ask_qty)
        } else if bid_qty > 0.0 {
            Some(f64::MAX)
        } else {
            None
        }
    }

    /// Total quantity on both sides within `range` cents of BBO.
    /// Used by the volatility guard to detect book depth collapse.
    pub fn near_bbo_depth(&self, range: i64) -> i64 {
        let mut total = 0i64;

        // YES bids: from best_bid down to best_bid - range
        let yb = self.yes_bids.best_bid;
        if yb > 0 {
            let lo = (yb - range).max(1) as usize;
            let hi = yb as usize;
            for p in lo..=hi {
                total += self.yes_bids.quantities[p];
            }
        }

        // NO bids (= YES asks): from best_no_bid down to best_no_bid - range
        let nb = self.no_bids.best_bid;
        if nb > 0 {
            let lo = (nb - range).max(1) as usize;
            let hi = nb as usize;
            for p in lo..=hi {
                total += self.no_bids.quantities[p];
            }
        }

        total
    }
}

#[derive(Debug, Clone)]
pub enum BookEvent {
    Updated,
    SequenceGap { expected: u64, got: u64 },
    TopOfBookChanged {
        old_bid: i64,
        old_ask: i64,
        new_bid: i64,
        new_ask: i64,
    },
}

pub struct BookEventDetector {
    prev_yes_bid: i64,
    prev_yes_ask: i64,
    warmed_up: bool,
}

impl BookEventDetector {
    pub fn new() -> Self {
        Self {
            prev_yes_bid: 0,
            prev_yes_ask: 0,
            warmed_up: false,
        }
    }

    pub fn detect(&mut self, book: &OrderBook) -> Vec<BookEvent> {
        let new_bid = book.best_yes_bid();
        let new_ask = book.best_yes_ask();

        if !self.warmed_up {
            self.prev_yes_bid = new_bid;
            self.prev_yes_ask = new_ask;
            self.warmed_up = true;
            return Vec::new();
        }

        let mut events = Vec::new();

        if new_bid != self.prev_yes_bid || new_ask != self.prev_yes_ask {
            events.push(BookEvent::TopOfBookChanged {
                old_bid: self.prev_yes_bid,
                old_ask: self.prev_yes_ask,
                new_bid,
                new_ask,
            });
        }

        self.prev_yes_bid = new_bid;
        self.prev_yes_ask = new_ask;

        events
    }
}