use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Config {
    pub api_key_id: String,
    pub private_key_pem: String,
    pub rest_base_url: String,
    pub ws_url: String,
    pub series_ticker: String,

    /// Contracts per order level
    pub order_size: i64,
    /// How many levels deep on each side (e.g. 2 = bid at BBO and BBO-1)
    pub grid_levels: i64,
    /// Max YES position (positive = long YES, negative = long NO). Hard wall.
    pub max_inventory: i64,

    /// Taker volume in the last sweep_window_ms that triggers quote pull.
    /// If someone market-buys > sweep_volume_threshold contracts in the window,
    /// AND the BBO moved 2+ cents, that's a sweep → pull opposite side quotes.
    pub sweep_volume_threshold: i64,
    /// Time window for sweep detection in milliseconds.
    pub sweep_window_ms: u64,

    pub rate_limit_writes_per_sec: u32,
    pub daily_loss_cap_cents: i64,
    pub rest_timeout: Duration,
    pub market_rotate_before_close_secs: i64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            api_key_id: String::new(),
            private_key_pem: String::new(),
            rest_base_url: "https://api.elections.kalshi.com/trade-api/v2".into(),
            ws_url: "wss://api.elections.kalshi.com/trade-api/ws/v2".into(),
            series_ticker: "KXBTC15M".into(),

            order_size: 1,
            grid_levels: 2,
            max_inventory: 3,
            sweep_volume_threshold: 100, // 10+ contracts in window = sweep
            sweep_window_ms: 100,      // 500ms rolling window
            rate_limit_writes_per_sec: 28,
            daily_loss_cap_cents: 500,
            rest_timeout: Duration::from_millis(200),
            market_rotate_before_close_secs: 180,
        }
    }
}

impl Config {
    pub fn from_env() -> Self {
        let mut c = Self::default();
        if let Ok(v) = std::env::var("SERIES_TICKER") { c.series_ticker = v; }
        if let Ok(v) = std::env::var("ORDER_SIZE") { if let Ok(n) = v.parse() { c.order_size = n; } }
        if let Ok(v) = std::env::var("GRID_LEVELS") { if let Ok(n) = v.parse() { c.grid_levels = n; } }
        if let Ok(v) = std::env::var("MAX_INVENTORY") { if let Ok(n) = v.parse() { c.max_inventory = n; } }
        if let Ok(v) = std::env::var("SWEEP_VOLUME") { if let Ok(n) = v.parse() { c.sweep_volume_threshold = n; } }
        if let Ok(v) = std::env::var("SWEEP_WINDOW_MS") { if let Ok(n) = v.parse() { c.sweep_window_ms = n; } }
        if let Ok(v) = std::env::var("RATE_LIMIT_WPS") { if let Ok(n) = v.parse() { c.rate_limit_writes_per_sec = n; } }
        if let Ok(v) = std::env::var("DAILY_LOSS_CAP") { if let Ok(n) = v.parse() { c.daily_loss_cap_cents = n; } }
        if let Ok(v) = std::env::var("REST_TIMEOUT_MS") { if let Ok(n) = v.parse::<u64>() { c.rest_timeout = Duration::from_millis(n); } }
        c
    }
}