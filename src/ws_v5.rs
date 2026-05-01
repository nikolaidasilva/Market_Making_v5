use std::sync::Arc;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite;
use serde::Deserialize;

use crate::auth::Credentials;
use crate::market_data::Side;

// ═══════════════════════════════════════════════════════════════════════════
// Outbound Message Enum (Sent to your main event loop)
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub enum WsMessage {
    OrderbookSnapshot {
        market_ticker: String,
        yes_levels: Vec<(i64, i64)>,
        no_levels: Vec<(i64, i64)>,
        seq: u64,
    },
    OrderbookDelta {
        market_ticker: String,
        side: Side,
        price: i64,
        delta: i64,
        seq: u64,
    },
    Fill {
        trade_id: String,
        order_id: String,
        market_ticker: String,
        side: String,
        yes_price: i64,
        count: i64,
        action: String,
        is_taker: bool,
        post_position: i64,
    },
    UserOrder {
        order_id: String,
        client_order_id: Option<String>,
        ticker: String,
        status: String,
        side: String,
        remaining_count_fp: String,
        fill_count_fp: String,
    },
    Ticker {
        market_ticker: String,
        yes_bid: i64,
        yes_ask: i64,
        price: i64,
        volume: i64,
    },
    Trade {
        market_ticker: String,
        yes_price: i64,
        count: i64,
        taker_side: String,
    },
    Subscribed {
        channel: String,
        sid: u64,
    },
    Error {
        code: u64,
        message: String,
    },
    Unknown(String),
}

// ═══════════════════════════════════════════════════════════════════════════
// Inbound Models — FULLY PERMISSIVE
//
// Since March 5, 2026 Kalshi stopped returning legacy integer fields.
// ALL numeric fields are now Optional with _fp string fallbacks.
// serde(default) on every struct so unknown/missing fields don't break parsing.
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Deserialize, Debug)]
#[serde(tag = "type")]
pub enum RawWsMessage<'a> {
    #[serde(rename = "orderbook_snapshot")]
    OrderbookSnapshot {
        #[serde(default)]
        seq: u64,
        #[serde(borrow)]
        msg: SnapshotMsg<'a>,
    },
    #[serde(rename = "orderbook_delta")]
    OrderbookDelta {
        #[serde(default)]
        seq: u64,
        #[serde(borrow)]
        msg: DeltaMsg<'a>,
    },
    #[serde(rename = "fill")]
    Fill {
        #[serde(borrow)]
        msg: FillMsg<'a>,
    },
    #[serde(rename = "user_order")]
    UserOrder {
        #[serde(borrow)]
        msg: UserOrderMsg<'a>,
    },
    #[serde(rename = "ticker")]
    Ticker {
        #[serde(borrow)]
        msg: TickerMsg<'a>,
    },
    #[serde(rename = "trade")]
    Trade {
        #[serde(borrow)]
        msg: TradeMsg<'a>,
    },
    #[serde(rename = "subscribed")]
    Subscribed {
        #[serde(borrow)]
        msg: SubscribedMsg<'a>,
    },
    #[serde(rename = "error")]
    Error {
        #[serde(default)]
        code: u64,
        #[serde(default)]
        msg: &'a str,
    },
}

#[derive(Deserialize, Debug, Default)]
#[serde(default)]
pub struct SnapshotMsg<'a> {
    pub market_ticker: &'a str,
    pub yes_dollars_fp: Option<Vec<(&'a str, &'a str)>>,
    pub no_dollars_fp: Option<Vec<(&'a str, &'a str)>>,
    pub yes: Option<Vec<(i64, i64)>>,
    pub no: Option<Vec<(i64, i64)>>,
}

#[derive(Deserialize, Debug, Default)]
#[serde(default)]
pub struct DeltaMsg<'a> {
    pub market_ticker: &'a str,
    pub side: &'a str,
    pub price_dollars: Option<&'a str>,
    pub price: Option<i64>,
    pub delta_fp: Option<&'a str>,
    pub delta: Option<i64>,
}

/// Fill message — all fields optional except identifiers.
/// Since March 5 2026, integer fields may no longer be present.
#[derive(Deserialize, Debug, Default)]
#[serde(default)]
pub struct FillMsg<'a> {
    pub trade_id: &'a str,
    pub order_id: &'a str,
    pub market_ticker: &'a str,
    pub side: &'a str,
    pub action: &'a str,
    // Price: prefer _dollars, fall back to integer
    pub yes_price_dollars: Option<&'a str>,
    pub yes_price: Option<i64>,
    // Count: prefer _fp, fall back to integer
    pub count_fp: Option<&'a str>,
    pub count: Option<i64>,
    // These may or may not be present after migration
    pub is_taker: Option<bool>,
    pub post_position: Option<i64>,
    pub post_position_fp: Option<&'a str>,
}

#[derive(Deserialize, Debug, Default)]
#[serde(default)]
pub struct UserOrderMsg<'a> {
    pub order_id: &'a str,
    pub ticker: &'a str,
    pub status: &'a str,
    pub side: &'a str,
    pub remaining_count_fp: Option<&'a str>,
    pub fill_count_fp: Option<&'a str>,
    pub client_order_id: Option<&'a str>,
    pub initial_count_fp: Option<&'a str>,
}

#[derive(Deserialize, Debug, Default)]
#[serde(default)]
pub struct TickerMsg<'a> {
    pub market_ticker: &'a str,
    pub yes_bid_dollars: Option<&'a str>,
    pub yes_bid: Option<i64>,
    pub yes_ask_dollars: Option<&'a str>,
    pub yes_ask: Option<i64>,
    pub price_dollars: Option<&'a str>,
    pub price: Option<i64>,
    pub volume_fp: Option<&'a str>,
    pub volume: Option<i64>,
}

#[derive(Deserialize, Debug, Default)]
#[serde(default)]
pub struct TradeMsg<'a> {
    pub market_ticker: &'a str,
    pub yes_price_dollars: Option<&'a str>,
    pub yes_price: Option<i64>,
    pub count_fp: Option<&'a str>,
    pub count: Option<i64>,
    pub taker_side: &'a str,
}

#[derive(Deserialize, Debug, Default)]
#[serde(default)]
pub struct SubscribedMsg<'a> {
    pub channel: &'a str,
    pub sid: u64,
}

/// Parse _fp string to cents (e.g. "0.750" -> 75)
#[inline]
fn fp_to_cents(s: &str) -> i64 {
    (s.parse::<f64>().unwrap_or(0.0) * 100.0).round() as i64
}

/// Parse _fp string to count (e.g. "10.00" -> 10)
#[inline]
fn fp_to_count(s: &str) -> i64 {
    s.parse::<f64>().unwrap_or(0.0).round() as i64
}

/// Parse _fp string to integer position (e.g. "500.00" -> 500)
#[inline]
fn fp_to_position(s: &str) -> i64 {
    s.parse::<f64>().unwrap_or(0.0).round() as i64
}

macro_rules! resolve_cents {
    ($dollars:expr, $int:expr) => {
        if let Some(s) = $dollars {
            fp_to_cents(s)
        } else {
            $int.unwrap_or(0)
        }
    };
}

macro_rules! resolve_count {
    ($fp:expr, $int:expr) => {
        if let Some(s) = $fp {
            fp_to_count(s)
        } else {
            $int.unwrap_or(0)
        }
    };
}

pub fn parse_ws_message(text: &str) -> WsMessage {
    let raw: RawWsMessage = match serde_json::from_str(text) {
        Ok(msg) => msg,
        Err(e) => {
            // Only log if it's not a heartbeat or other known noise
            if text.contains("\"type\"") && !text.contains("\"heartbeat\"") {
                tracing::warn!(error = %e, msg = &text[..text.len().min(200)], "WS parse failed");
            }
            return WsMessage::Unknown(text.to_string());
        }
    };

    match raw {
        RawWsMessage::OrderbookSnapshot { seq, msg } => {
            let parse_levels = |fp: Option<Vec<(&str, &str)>>, legacy: Option<Vec<(i64, i64)>>| -> Vec<(i64, i64)> {
                if let Some(arr) = fp {
                    arr.into_iter()
                        .map(|(p, q)| (fp_to_cents(p), fp_to_count(q)))
                        .collect()
                } else {
                    legacy.unwrap_or_default()
                }
            };

            WsMessage::OrderbookSnapshot {
                market_ticker: msg.market_ticker.to_string(),
                yes_levels: parse_levels(msg.yes_dollars_fp, msg.yes),
                no_levels: parse_levels(msg.no_dollars_fp, msg.no),
                seq,
            }
        }
        RawWsMessage::OrderbookDelta { seq, msg } => WsMessage::OrderbookDelta {
            market_ticker: msg.market_ticker.to_string(),
            side: if msg.side == "yes" { Side::Yes } else { Side::No },
            price: resolve_cents!(msg.price_dollars, msg.price),
            delta: resolve_count!(msg.delta_fp, msg.delta),
            seq,
        },
        RawWsMessage::Fill { msg } => {
            // Resolve post_position: try _fp first, then integer, default 0
            let post_pos = if let Some(fp) = msg.post_position_fp {
                fp_to_position(fp)
            } else {
                msg.post_position.unwrap_or(0)
            };

            WsMessage::Fill {
                trade_id: msg.trade_id.to_string(),
                order_id: msg.order_id.to_string(),
                market_ticker: msg.market_ticker.to_string(),
                side: msg.side.to_string(),
                yes_price: resolve_cents!(msg.yes_price_dollars, msg.yes_price),
                count: resolve_count!(msg.count_fp, msg.count),
                action: msg.action.to_string(),
                is_taker: msg.is_taker.unwrap_or(false),
                post_position: post_pos,
            }
        }
        RawWsMessage::UserOrder { msg } => WsMessage::UserOrder {
            order_id: msg.order_id.to_string(),
            client_order_id: msg.client_order_id.map(|s| s.to_string()),
            ticker: msg.ticker.to_string(),
            status: msg.status.to_string(),
            side: msg.side.to_string(),
            remaining_count_fp: msg.remaining_count_fp.unwrap_or("0.00").to_string(),
            fill_count_fp: msg.fill_count_fp.unwrap_or("0.00").to_string(),
        },
        RawWsMessage::Ticker { msg } => WsMessage::Ticker {
            market_ticker: msg.market_ticker.to_string(),
            yes_bid: resolve_cents!(msg.yes_bid_dollars, msg.yes_bid),
            yes_ask: resolve_cents!(msg.yes_ask_dollars, msg.yes_ask),
            price: resolve_cents!(msg.price_dollars, msg.price),
            volume: resolve_count!(msg.volume_fp, msg.volume),
        },
        RawWsMessage::Trade { msg } => WsMessage::Trade {
            market_ticker: msg.market_ticker.to_string(),
            yes_price: resolve_cents!(msg.yes_price_dollars, msg.yes_price),
            count: resolve_count!(msg.count_fp, msg.count),
            taker_side: msg.taker_side.to_string(),
        },
        RawWsMessage::Subscribed { msg } => WsMessage::Subscribed {
            channel: msg.channel.to_string(),
            sid: msg.sid,
        },
        RawWsMessage::Error { code, msg } => WsMessage::Error {
            code,
            message: msg.to_string(),
        },
    }
}

pub fn subscribe_cmd(id: u64, channels: &[&str], market_ticker: &str) -> String {
    serde_json::json!({
        "id": id,
        "cmd": "subscribe",
        "params": {
            "channels": channels,
            "market_ticker": market_ticker
        }
    })
    .to_string()
}

pub fn unsubscribe_cmd(id: u64, sids: &[u64]) -> String {
    serde_json::json!({
        "id": id,
        "cmd": "unsubscribe",
        "params": { "sids": sids }
    })
    .to_string()
}

pub struct WsManager {
    pub ws_url: String,
    pub creds: Arc<Credentials>,
    pub ticker: String,
}

impl WsManager {
    pub fn new(ws_url: String, creds: Arc<Credentials>, ticker: String) -> Self {
        Self { ws_url, creds, ticker }
    }

    pub async fn run(&self, tx: mpsc::UnboundedSender<WsMessage>) -> anyhow::Result<()> {
        loop {
            match self.connect_and_run(&tx).await {
                Ok(()) => {
                    tracing::info!("WebSocket closed cleanly, reconnecting...");
                }
                Err(e) => {
                    tracing::error!(error = %e, "WebSocket error, reconnecting in 1s...");
                    let _ = tx.send(WsMessage::Error {
                        code: 0,
                        message: format!("WS disconnect: {}", e),
                    });
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
    }

    async fn connect_and_run(
        &self,
        tx: &mpsc::UnboundedSender<WsMessage>,
    ) -> anyhow::Result<()> {
        let (key, sig, ts) = self.creds.sign("GET", "/trade-api/ws/v2");
        let url = url::Url::parse(&self.ws_url)?;

        let request = tungstenite::http::Request::builder()
            .uri(self.ws_url.as_str())
            .header("KALSHI-ACCESS-KEY", &key)
            .header("KALSHI-ACCESS-SIGNATURE", &sig)
            .header("KALSHI-ACCESS-TIMESTAMP", &ts)
            .header("Host", url.host_str().unwrap_or(""))
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Version", "13")
            .header(
                "Sec-WebSocket-Key",
                tungstenite::handshake::client::generate_key(),
            )
            .body(())?;

        let (ws_stream, _) = tokio_tungstenite::connect_async(request).await?;
        tracing::info!(ticker = %self.ticker, "WebSocket connected");

        let (mut write, mut read) = ws_stream.split();

        for (_id, cmd) in [
            (1, subscribe_cmd(1, &["orderbook_delta"], &self.ticker)),
            (2, subscribe_cmd(2, &["fill"], &self.ticker)),
            (3, subscribe_cmd(3, &["user_orders"], &self.ticker)),
            (4, subscribe_cmd(4, &["trade"], &self.ticker)),
            (5, subscribe_cmd(5, &["ticker"], &self.ticker)),
        ] {
            write
                .send(tungstenite::Message::Text(cmd.into()))
                .await?;
        }

        while let Some(msg) = read.next().await {
            match msg {
                Ok(tungstenite::Message::Text(text)) => {
                    let parsed = parse_ws_message(&text);
                    if tx.send(parsed).is_err() {
                        tracing::info!("Receiver dropped, shutting down WS");
                        return Ok(());
                    }
                }
                Ok(tungstenite::Message::Ping(_)) => {}
                Ok(tungstenite::Message::Close(_)) => {
                    tracing::info!("WS received close frame");
                    return Ok(());
                }
                Err(e) => return Err(e.into()),
                _ => {}
            }
        }

        Ok(())
    }
}