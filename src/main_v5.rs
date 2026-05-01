mod auth;
mod config;
mod market_data;
mod market_finder;
mod ws;

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

use auth::Credentials;
use config::Config;
use market_data::OrderBook;
use ws::{WsManager, WsMessage};

// ═══════════════════════════════════════════════════════════════════════════
// RATE LIMITER
// ═══════════════════════════════════════════════════════════════════════════

fn spawn_rate_limiter(rate_per_sec: u32) -> Arc<tokio::sync::Semaphore> {
    let sem = Arc::new(tokio::sync::Semaphore::new(rate_per_sec as usize));
    let sem2 = sem.clone();
    let interval_us = 1_000_000u64 / rate_per_sec as u64;
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_micros(interval_us));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            if sem2.available_permits() < rate_per_sec as usize {
                sem2.add_permits(1);
            }
        }
    });
    sem
}

// ═══════════════════════════════════════════════════════════════════════════
// ORDER TRACKING — dead simple, keyed by kalshi order_id
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
struct Order {
    order_id: String,
    side: &'static str, // "yes" or "no"
    price: i64,         // in that side's native cents
    remaining: i64,
    placed_at: Instant,
}

struct Tracker {
    orders: HashMap<String, Order>,
}

impl Tracker {
    fn new() -> Self { Self { orders: HashMap::new() } }

    /// Count total pending contracts on a given side
    fn pending_on_side(&self, side: &str) -> i64 {
        self.orders.values()
            .filter(|o| o.side == side)
            .map(|o| o.remaining)
            .sum()
    }

    fn remove(&mut self, id: &str) { self.orders.remove(id); }

    fn remove_all(&mut self) { self.orders.clear(); }
}

// ═══════════════════════════════════════════════════════════════════════════
// PAIR TRACKING
// ═══════════════════════════════════════════════════════════════════════════

struct PairTracker {
    yes_legs: VecDeque<i64>, // YES fill prices (FIFO)
    no_legs: VecDeque<i64>,  // NO costs = 100 - yes_price (FIFO)
    pairs_completed: u64,
    realized_pnl: i64,
    inventory: i64, // positive = long YES, negative = long NO
}

impl PairTracker {
    fn new(starting_inventory: i64) -> Self {
        Self {
            yes_legs: VecDeque::new(),
            no_legs: VecDeque::new(),
            pairs_completed: 0,
            realized_pnl: 0,
            inventory: starting_inventory,
        }
    }

    fn record_fill(&mut self, side: &str, yes_price: i64, count: i64, post_position: i64) {
        // Update inventory from fill
        if side == "yes" { self.inventory += count; }
        else { self.inventory -= count; }

        // Sync to Kalshi ground truth
        if self.inventory != post_position {
            tracing::warn!(ours = self.inventory, kalshi = post_position, "Inventory drift — syncing");
            self.inventory = post_position;
        }

        // Pair off FIFO
        for _ in 0..count {
            if side == "yes" {
                if let Some(no_cost) = self.no_legs.pop_front() {
                    let profit = 100 - (yes_price + no_cost);
                    self.realized_pnl += profit;
                    self.pairs_completed += 1;
                } else {
                    self.yes_legs.push_back(yes_price);
                }
            } else {
                let no_cost = 100 - yes_price;
                if let Some(yes_cost) = self.yes_legs.pop_front() {
                    let profit = 100 - (yes_cost + no_cost);
                    self.realized_pnl += profit;
                    self.pairs_completed += 1;
                } else {
                    self.no_legs.push_back(no_cost);
                }
            }
        }
    }

    fn has_unpaired(&self) -> bool {
        !self.yes_legs.is_empty() || !self.no_legs.is_empty()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// REST CLIENT — minimal, inline
// ═══════════════════════════════════════════════════════════════════════════

use serde::{Deserialize, Serialize};

#[derive(Serialize)]
struct PlaceReq<'a> {
    ticker: &'a str,
    side: &'a str,
    action: &'a str,
    client_order_id: &'a str,
    count_fp: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    yes_price_dollars: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    no_price_dollars: Option<&'a str>,
    r#type: &'a str,
    time_in_force: &'a str,
    post_only: bool,
    cancel_order_on_pause: bool,
}

#[derive(Serialize)]
struct BatchCancelReq<'a> { orders: Vec<CancelItem<'a>> }
#[derive(Serialize)]
struct CancelItem<'a> { order_id: &'a str }

#[derive(Deserialize)]
struct OrderResp { order: OrderDetail }
#[derive(Deserialize)]
struct OrderDetail { order_id: String }

#[derive(Deserialize)]
struct PosResp { #[serde(default)] market_positions: Vec<Pos> }
#[derive(Deserialize)]
struct Pos { ticker: String, position: i64 }

struct Rest {
    http: reqwest::Client,
    base: String,
    creds: Arc<Credentials>,
    sem: Arc<tokio::sync::Semaphore>,
}

impl Rest {
    fn new(config: &Config, creds: Arc<Credentials>, sem: Arc<tokio::sync::Semaphore>) -> Self {
        let http = reqwest::Client::builder()
            .timeout(config.rest_timeout)
            .tcp_nodelay(true)
            .pool_max_idle_per_host(16)
            .build()
            .unwrap();
        Self { http, base: config.rest_base_url.clone(), creds, sem }
    }

    async fn acquire(&self) -> anyhow::Result<()> {
        match tokio::time::timeout(Duration::from_millis(500), self.sem.acquire()).await {
            Ok(Ok(p)) => { p.forget(); Ok(()) }
            _ => anyhow::bail!("rate limit"),
        }
    }

    fn auth(&self, method: &str, path: &str) -> [(&'static str, String); 3] {
        let (k, s, t) = self.creds.sign(method, path);
        [("KALSHI-ACCESS-KEY", k), ("KALSHI-ACCESS-SIGNATURE", s), ("KALSHI-ACCESS-TIMESTAMP", t)]
    }

    /// Place a maker order. Returns (order_id) on success.
    async fn place(&self, ticker: &str, side: &str, price: i64, count: i64) -> anyhow::Result<String> {
        self.acquire().await?;
        let path = "/trade-api/v2/portfolio/orders";
        let url = format!("{}/portfolio/orders", self.base);
        let hdrs = self.auth("POST", path);
        let cid = uuid::Uuid::new_v4().to_string();
        let pd = format!("{:.4}", price as f64 / 100.0);
        let cf = format!("{}.00", count);
        let body = PlaceReq {
            ticker, side, action: "buy", client_order_id: &cid, count_fp: &cf,
            yes_price_dollars: if side == "yes" { Some(&pd) } else { None },
            no_price_dollars: if side == "no" { Some(&pd) } else { None },
            r#type: "limit", time_in_force: "good_till_canceled",
            post_only: true, cancel_order_on_pause: true,
        };
        let mut req = self.http.post(&url).json(&body);
        for (k, v) in &hdrs { req = req.header(*k, v.as_str()); }
        let resp = req.send().await?;
        let st = resp.status();
        if !st.is_success() {
            let txt = resp.text().await.unwrap_or_default();
            anyhow::bail!("{} {}", st, txt);
        }
        let r: OrderResp = resp.json().await?;
        Ok(r.order.order_id)
    }

    /// Cancel all given order IDs in one batch. 1 rate limit permit.
    async fn cancel_batch(&self, ids: &[String]) -> anyhow::Result<()> {
        if ids.is_empty() { return Ok(()); }
        self.acquire().await?;
        let path = "/trade-api/v2/portfolio/orders/batched";
        let url = format!("{}/portfolio/orders/batched", self.base);
        let hdrs = self.auth("DELETE", path);
        let items: Vec<CancelItem> = ids.iter().map(|id| CancelItem { order_id: id }).collect();
        let body = BatchCancelReq { orders: items };
        let mut req = self.http.delete(&url).json(&body);
        for (k, v) in &hdrs { req = req.header(*k, v.as_str()); }
        let resp = req.send().await?;
        if !resp.status().is_success() {
            let txt = resp.text().await.unwrap_or_default();
            anyhow::bail!("batch cancel: {}", txt);
        }
        Ok(())
    }

    async fn get_position(&self, ticker: &str) -> anyhow::Result<i64> {
        let path = "/trade-api/v2/portfolio/positions";
        let url = format!("{}/portfolio/positions?ticker={}", self.base, ticker);
        let hdrs = self.auth("GET", path);
        let mut req = self.http.get(&url);
        for (k, v) in &hdrs { req = req.header(*k, v.as_str()); }
        let resp = req.send().await?;
        let body: PosResp = resp.json().await?;
        for p in body.market_positions { if p.ticker == ticker { return Ok(p.position); } }
        Ok(0)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// SWEEP DETECTOR — detects toxic flow from trade tape + price movement
//
// Tracks rolling taker volume per side. When taker volume on one side
// exceeds the threshold AND the BBO has moved 2+ cents in that direction,
// we know someone is sweeping the book → pull our quotes on that side.
// ═══════════════════════════════════════════════════════════════════════════

struct SweepDetector {
    // (timestamp, taker_side_is_yes, count)
    trades: Vec<(Instant, bool, i64)>,
    window: Duration,
    threshold: i64,
    prev_yes_bid: i64,
    prev_no_bid: i64,
}

impl SweepDetector {
    fn new(window_ms: u64, threshold: i64) -> Self {
        Self {
            trades: Vec::with_capacity(256),
            window: Duration::from_millis(window_ms),
            threshold,
            prev_yes_bid: 0,
            prev_no_bid: 0,
        }
    }

    fn record_trade(&mut self, taker_side_yes: bool, count: i64) {
        let now = Instant::now();
        self.trades.push((now, taker_side_yes, count));
        let cutoff = now - self.window;
        self.trades.retain(|(t, _, _)| *t >= cutoff);
    }

    fn record_bbo(&mut self, yes_bid: i64, no_bid: i64) {
        if self.prev_yes_bid == 0 { self.prev_yes_bid = yes_bid; }
        if self.prev_no_bid == 0 { self.prev_no_bid = no_bid; }
    }

    /// Check if a sweep is happening. Returns which side to pull quotes on, if any.
    /// "yes" = pull YES bids (someone is aggressively selling YES / buying NO)
    /// "no" = pull NO bids (someone is aggressively buying YES / selling NO)
    fn check(&mut self, yes_bid: i64, no_bid: i64) -> Option<&'static str> {
        // Sum recent taker volume per side
        let mut yes_taker_vol: i64 = 0; // contracts where taker bought YES
        let mut no_taker_vol: i64 = 0;  // contracts where taker bought NO
        for &(_, is_yes, count) in &self.trades {
            if is_yes { yes_taker_vol += count; }
            else { no_taker_vol += count; }
        }

        // How much has the BBO moved?
        let yes_bid_drop = self.prev_yes_bid - yes_bid; // positive = bid dropped
        let no_bid_drop = self.prev_no_bid - no_bid;    // positive = no bid dropped

        // Update prev for next check
        self.prev_yes_bid = yes_bid;
        self.prev_no_bid = no_bid;

        // Sweep detection:
        // Heavy NO taker volume + YES bid dropping = someone sweeping YES side
        // → pull our YES bids (we don't want to buy YES into a falling market)
        if no_taker_vol >= self.threshold && yes_bid_drop >= 3 {
            self.trades.clear(); // reset after triggering
            return Some("yes");
        }

        // Heavy YES taker volume + NO bid dropping = someone sweeping NO side
        // → pull our NO bids
        if yes_taker_vol >= self.threshold && no_bid_drop >= 3 {
            self.trades.clear();
            return Some("no");
        }

        None
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// DESIRED STATE
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, PartialEq)]
struct DesiredOrders {
    yes_prices: Vec<i64>, // YES bid prices (in yes cents)
    no_prices: Vec<i64>,  // NO bid prices (in no cents)
}

/// Compute what orders we WANT on the book right now.
fn compute_desired(
    book: &OrderBook,
    pairs: &PairTracker,
    tracker: &Tracker,
    config: &Config,
) -> DesiredOrders {
    let empty = DesiredOrders { yes_prices: vec![], no_prices: vec![] };

    let yb = book.best_yes_bid();
    let nb = book.no_bids.best_bid;
    if yb <= 0 || nb <= 0 { return empty; }

    let ya = book.best_yes_ask(); // = 100 - nb
    if ya <= yb { return empty; }

    // ── INVENTORY CAPS ──
    let inv = pairs.inventory;
    let pending_yes = tracker.pending_on_side("yes");
    let pending_no = tracker.pending_on_side("no");
    let effective_long = inv + pending_yes;
    let effective_short = -inv + pending_no;
    let max = config.max_inventory;
    let sz = config.order_size;

    // ── BUILD GRID ──
    let mut yes_prices = Vec::new();
    let mut no_prices = Vec::new();

    {
        let mut remaining_capacity = max - effective_long;
        for level in 0..config.grid_levels {
            if remaining_capacity < sz { break; }
            let price = yb - level;
            if price < 1 { break; }
            if price >= ya { continue; }
            yes_prices.push(price);
            remaining_capacity -= sz;
        }
    }

    {
        let mut remaining_capacity = max - effective_short;
        for level in 0..config.grid_levels {
            if remaining_capacity < sz { break; }
            let price = nb - level;
            if price < 1 { break; }
            let implied_yes_ask = 100 - price;
            if implied_yes_ask <= yb { continue; }
            no_prices.push(price);
            remaining_capacity -= sz;
        }
    }

    DesiredOrders { yes_prices, no_prices }
}

// ═══════════════════════════════════════════════════════════════════════════
// RECONCILE — diff desired vs actual, produce cancel/place actions
// ═══════════════════════════════════════════════════════════════════════════

enum Action {
    Cancel(String),           // order_id to cancel
    Place(&'static str, i64), // (side, price) to place
}

fn reconcile(desired: &DesiredOrders, tracker: &Tracker) -> Vec<Action> {
    let mut actions = Vec::new();

    // Current orders by side
    let mut cur_yes: Vec<(&str, i64)> = Vec::new();
    let mut cur_no: Vec<(&str, i64)> = Vec::new();
    for o in tracker.orders.values() {
        if o.side == "yes" { cur_yes.push((&o.order_id, o.price)); }
        else { cur_no.push((&o.order_id, o.price)); }
    }

    // Reconcile YES side
    reconcile_side(&desired.yes_prices, &cur_yes, "yes", &mut actions);
    // Reconcile NO side
    reconcile_side(&desired.no_prices, &cur_no, "no", &mut actions);

    actions
}

fn reconcile_side(
    desired: &[i64],
    current: &[(&str, i64)],
    side: &'static str,
    actions: &mut Vec<Action>,
) {
    let mut unmatched_desired: Vec<i64> = desired.to_vec();

    // Match current orders to desired prices
    for &(oid, price) in current {
        if let Some(idx) = unmatched_desired.iter().position(|&dp| dp == price) {
            unmatched_desired.remove(idx); // exact match, keep it
        } else {
            actions.push(Action::Cancel(oid.to_string())); // no match, cancel
        }
    }

    // Place remaining desired
    for price in unmatched_desired {
        actions.push(Action::Place(side, price));
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "kalshi_hft=info".into()),
        )
        .init();

    let config = load_config();
    let creds = Arc::new(Credentials::load(config.api_key_id.clone(), &config.private_key_pem));

    tracing::info!(
        series = %config.series_ticker,
        max_inv = config.max_inventory,
        grid = config.grid_levels,
        sz = config.order_size,
        sweep_vol = config.sweep_volume_threshold,
        sweep_ms = config.sweep_window_ms,
        "Bot starting"
    );

    loop {
        let market = match market_finder::find_current_market(
            &creds,
            &reqwest::Client::builder().timeout(Duration::from_secs(5)).tcp_nodelay(true).build()?,
            &config.series_ticker,
        ).await {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, "No market, retrying...");
                tokio::time::sleep(Duration::from_secs(10)).await;
                continue;
            }
        };

        let ticker = market.ticker.clone();
        let close_time = market.close_time.as_ref()
            .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
            .map(|dt| dt.with_timezone(&chrono::Utc));

        run_session(&config, &creds, &ticker, close_time).await;
    }
}

async fn run_session(
    config: &Config,
    creds: &Arc<Credentials>,
    ticker: &str,
    close_time: Option<chrono::DateTime<chrono::Utc>>,
) {
    let mut book = OrderBook::new();
    let sem = spawn_rate_limiter(config.rate_limit_writes_per_sec);
    let rest = Rest::new(config, creds.clone(), sem);
    let mut tracker = Tracker::new();

    // Load starting position
    let start_pos = rest.get_position(ticker).await.unwrap_or(0);
    let mut pairs = PairTracker::new(start_pos);
    tracing::info!(inventory = start_pos, ticker, "Session start");

    // Start WebSocket
    let (ws_tx, mut ws_rx) = mpsc::unbounded_channel::<WsMessage>();
    {
        let creds2 = creds.clone();
        let url = config.ws_url.clone();
        let t = ticker.to_string();
        tokio::spawn(async move {
            let ws = WsManager::new(url, creds2, t);
            let _ = ws.run(ws_tx).await;
        });
    }

    let mut ws_connected = false;
    let mut last_desired = DesiredOrders { yes_prices: vec![], no_prices: vec![] };
    let mut sweep = SweepDetector::new(config.sweep_window_ms, config.sweep_volume_threshold);

    loop {
        // ── Time check ──
        if let Some(close) = close_time {
            let secs_left = (close - chrono::Utc::now()).num_seconds().max(0);
            if secs_left <= config.market_rotate_before_close_secs {
                cancel_all(&rest, &mut tracker).await;
                tracing::info!(pairs = pairs.pairs_completed, pnl = pairs.realized_pnl, "Session end — rotating");
                return;
            }
        }

        // ── Drain WS ──
        let mut messages = Vec::new();
        match tokio::time::timeout(Duration::from_millis(5), ws_rx.recv()).await {
            Ok(Some(msg)) => {
                messages.push(msg);
                while let Ok(msg) = ws_rx.try_recv() { messages.push(msg); }
            }
            Ok(None) => {
                tracing::error!("WS channel closed");
                cancel_all(&rest, &mut tracker).await;
                return;
            }
            Err(_) => {} // timeout, continue
        }

        let mut book_changed = false;
        let mut got_fill = false;

        for msg in messages {
            if !ws_connected {
                ws_connected = true;
            }
            match msg {
                WsMessage::OrderbookSnapshot { market_ticker, yes_levels, no_levels, seq } => {
                    if market_ticker == ticker {
                        book.apply_snapshot(&yes_levels, &no_levels, seq);
                        book_changed = true;
                    }
                }
                WsMessage::OrderbookDelta { market_ticker, side, price, delta, seq } => {
                    if market_ticker == ticker {
                        if !book.apply_delta(side, price, delta, seq) {
                            tracing::error!("Sequence gap — canceling all and rotating");
                            cancel_all(&rest, &mut tracker).await;
                            return;
                        }
                        book_changed = true;

                        // Update sweep detector's BBO tracking
                        sweep.record_bbo(book.best_yes_bid(), book.no_bids.best_bid);
                    }
                }
                WsMessage::Trade { market_ticker, count, taker_side, .. } => {
                    if market_ticker != ticker { continue; }

                    // Record trade in sweep detector
                    let is_yes_taker = taker_side == "yes";
                    sweep.record_trade(is_yes_taker, count);

                    // Check for sweep — uses accumulated trades + BBO movement
                    if let Some(pull_side) = sweep.check(book.best_yes_bid(), book.no_bids.best_bid) {
                        let to_cancel: Vec<String> = tracker.orders.values()
                            .filter(|o| o.side == pull_side)
                            .map(|o| o.order_id.clone())
                            .collect();
                        if !to_cancel.is_empty() {
                            tracing::info!(
                                pull = pull_side,
                                yes_bid = book.best_yes_bid(),
                                no_bid = book.no_bids.best_bid,
                                "SWEEP detected — pulling quotes"
                            );
                            if let Ok(_) = rest.cancel_batch(&to_cancel).await {
                                for id in &to_cancel { tracker.remove(id); }
                            }
                        }
                    }
                }
                WsMessage::Fill { market_ticker, side, yes_price, count, order_id, post_position, .. } => {
                    if market_ticker != ticker { continue; }

                    pairs.record_fill(&side, yes_price, count, post_position);

                    // Update tracker
                    if let Some(order) = tracker.orders.get_mut(&order_id) {
                        order.remaining -= count;
                        if order.remaining <= 0 {
                            tracker.orders.remove(&order_id);
                        }
                    }

                    got_fill = true;
                    tracing::info!(
                        side = %side, yes_price, count, post_position,
                        inv = pairs.inventory, pairs = pairs.pairs_completed,
                        pnl = pairs.realized_pnl,
                        "FILL"
                    );

                    // Hard inventory cap check
                    if pairs.inventory.abs() > config.max_inventory {
                        tracing::error!(inv = pairs.inventory, "HARD CAP — canceling all");
                        cancel_all(&rest, &mut tracker).await;
                    }
                }
                WsMessage::UserOrder { order_id, status, .. } => {
                    // If Kalshi says an order is canceled or executed, remove from tracker
                    match status.as_str() {
                        "canceled" | "executed" => { tracker.remove(&order_id); }
                        _ => {}
                    }
                }
                WsMessage::Error { message, .. } => {
                    if message.starts_with("WS disconnect") {
                        ws_connected = false;
                        book = OrderBook::new();
                        cancel_all(&rest, &mut tracker).await;
                    }
                }
                _ => {}
            }
        }

        if !ws_connected || !book.initialized { continue; }

        // ── Halted? ──
        if pairs.realized_pnl <= -config.daily_loss_cap_cents {
            cancel_all(&rest, &mut tracker).await;
            tracing::error!(pnl = pairs.realized_pnl, "Daily loss cap hit — stopping");
            tokio::time::sleep(Duration::from_secs(60)).await;
            continue;
        }

        // ── Compute desired state ──
        let desired = compute_desired(&book, &pairs, &tracker, config);

        // Skip if nothing changed
        if desired == last_desired && !got_fill && !book_changed {
            continue;
        }

        // ── Reconcile and execute ──
        let actions = reconcile(&desired, &tracker);
        last_desired = desired;

        if actions.is_empty() { continue; }

        // Cancels first, then placements. All awaited inline.
        let cancel_ids: Vec<String> = actions.iter()
            .filter_map(|a| if let Action::Cancel(id) = a { Some(id.clone()) } else { None })
            .collect();

        if !cancel_ids.is_empty() {
            match rest.cancel_batch(&cancel_ids).await {
                Ok(_) => {
                    for id in &cancel_ids { tracker.remove(id); }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Batch cancel failed");
                }
            }
        }

        for action in &actions {
            if let Action::Place(side, price) = action {
                match rest.place(ticker, side, *price, config.order_size).await {
                    Ok(oid) => {
                        tracker.orders.insert(oid.clone(), Order {
                            order_id: oid,
                            side,
                            price: *price,
                            remaining: config.order_size,
                            placed_at: Instant::now(),
                        });
                    }
                    Err(e) => {
                        let err = e.to_string();
                        if !err.contains("post only cross") {
                            tracing::warn!(error = %err, side, price, "Place failed");
                        }
                    }
                }
            }
        }

        // Clean stale orders (>10s with no WS confirmation)
        let stale: Vec<String> = tracker.orders.iter()
            .filter(|(_, o)| o.placed_at.elapsed() > Duration::from_secs(10))
            .map(|(id, _)| id.clone())
            .collect();
        for id in stale {
            tracing::warn!(order_id = %id, "Removing stale order (>10s)");
            tracker.remove(&id);
        }
    }
}

async fn cancel_all(rest: &Rest, tracker: &mut Tracker) {
    let ids: Vec<String> = tracker.orders.keys().cloned().collect();
    if ids.is_empty() { return; }
    tracing::info!(count = ids.len(), "Canceling all orders");
    for attempt in 0..3 {
        match rest.cancel_batch(&ids).await {
            Ok(_) => { tracker.remove_all(); return; }
            Err(e) => {
                tracing::warn!(error = %e, attempt, "Cancel all failed, retrying");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    // Give up, clear tracker anyway
    tracker.remove_all();
}

fn load_config() -> Config {
    if let Ok(contents) = std::fs::read_to_string(".env") {
        for line in contents.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') { continue; }
            if let Some((key, val)) = line.split_once('=') {
                let key = key.trim();
                let val = val.trim().trim_matches('"');
                if std::env::var(key).is_err() {
                    unsafe { std::env::set_var(key, val) };
                }
            }
        }
    }
    let mut config = Config::from_env();
    config.api_key_id = std::env::var("KALSHI_KEY_ID").expect("KALSHI_KEY_ID required");
    let key_path = std::env::var("KALSHI_PRIVATE_KEY_PATH").expect("KALSHI_PRIVATE_KEY_PATH required");
    config.private_key_pem = std::fs::read_to_string(&key_path).expect("Failed to read private key");
    config
}