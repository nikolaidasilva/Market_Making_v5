# Kalshi Market Maker (v5) - Sweep Detection & API Resilience

market-making bot built in Rust for the Kalshi prediction market. Version 5 focuses on surviving toxic flow via real-time tape reading and introduces a highly resilient parser to handle Kalshi's recent API data structure migrations.

## Core Upgrades & Strategy
*   **Sweep Detection (Toxic Flow Protection):** The bot monitors real-time taker volume using a configurable rolling time window (`sweep_window_ms`). If taker volume spikes past the `sweep_volume_threshold` while the Best Bid/Offer (BBO) drops by 3+ cents, the bot detects a "sweep" and instantly pulls resting quotes on the dangerous side to avoid adverse selection.
*   **March 2026 API Resilience:** Built to handle Kalshi's deprecation of legacy integer fields. The WebSocket engine utilizes a "fully permissive" `serde` parsing strategy with custom fallback macros, dynamically resolving `_fp` string floats to integers to ensure the bot never crashes on missing legacy data.
*   **FIFO Pair Tracking:** Replaces generalized inventory PnL with a strict First-In-First-Out (FIFO) queue for YES and NO legs. This guarantees exact realized profit calculations for every completed pair.
*   **Grid Quoting:** Simplifies the orderbook strategy into a straightforward grid system, placing orders up to `grid_levels` deep on both sides, bound by strict `max_inventory` limits.

## Safety & Risk Management
*   **Hard Limits:** Instant halting if the hard inventory cap or daily loss limits (`daily_loss_cap_cents`) are breached.
*   **O(1) Bitmask Orderbook:** Retains the highly optimized `u128` bitmask orderbook for microsecond-level BBO lookups.

## Prerequisites
*   Rust and Cargo installed.
*   Kalshi API access (Key ID and Private Key).
