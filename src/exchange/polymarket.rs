//! Polymarket CLOB market channel. Public, no API key required.
//!
//! * Websocket: `wss://ws-subscriptions-clob.polymarket.com/ws/market`
//! * Symbols are CLOB token ids (one per outcome; a binary market has two).
//! * `book` messages are full snapshots (sent on subscribe and after trades),
//!   `price_change` messages carry absolute sizes per level.
//! * Discovery uses the Gamma REST API (`/events/keyset` for full scans,
//!   `/markets?order=id` for cheap "what's new" polling).

use std::{borrow::Cow, sync::Mutex, time::Duration};

use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::handshake::client::Request;

use super::{Codec, Exchange, Level, MarketEvent, Symbol, millis_to_datetime, plain_request};
use crate::{Side, fixed::parse_fixed};

const WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const GAMMA_URL: &str = "https://gamma-api.polymarket.com";
/// Subscribing too many ids in a single frame gets the frame rejected, so we
/// split subscription messages.
const IDS_PER_MESSAGE: usize = 100;
/// Gamma caps page sizes at 100 regardless of `limit`.
const NEW_MARKET_PAGE: usize = 100;
const MAX_NEW_MARKET_PAGES: usize = 20;

pub struct Polymarket {
    http: reqwest::Client,
    max_symbols_per_connection: usize,
    /// Highest Gamma market id seen so far, used by [`Exchange::discover_new`].
    newest_market_id: Mutex<u64>,
}

impl Polymarket {
    pub fn new(max_symbols_per_connection: usize) -> anyhow::Result<Self> {
        Ok(Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .user_agent("voluntas-collector/0.1")
                .build()?,
            max_symbols_per_connection,
            newest_market_id: Mutex::new(0),
        })
    }

    async fn get_json<T: serde::de::DeserializeOwned>(&self, url: &str) -> anyhow::Result<T> {
        let mut delay = Duration::from_millis(500);
        for attempt in 0.. {
            match self.http.get(url).send().await.and_then(|r| r.error_for_status()) {
                Ok(resp) => return Ok(resp.json().await?),
                Err(err) if attempt < 5 => {
                    tracing::warn!(%err, url, "gamma request failed, retrying");
                    tokio::time::sleep(delay).await;
                    delay *= 2;
                }
                Err(err) => return Err(err.into()),
            }
        }
        unreachable!()
    }

    fn record_market_id(&self, id: &str) {
        if let Ok(id) = id.parse::<u64>() {
            let mut newest = self.newest_market_id.lock().unwrap();
            *newest = (*newest).max(id);
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GammaMarket {
    id: String,
    #[serde(default)]
    enable_order_book: bool,
    #[serde(default)]
    accepting_orders: bool,
    #[serde(default)]
    active: bool,
    #[serde(default)]
    closed: bool,
    /// JSON-encoded array of token ids, e.g. `"[\"123\", \"456\"]"`.
    clob_token_ids: Option<String>,
}

impl GammaMarket {
    fn live_tokens(&self) -> Vec<Symbol> {
        if !(self.enable_order_book && self.accepting_orders && self.active && !self.closed) {
            return Vec::new();
        }
        self.clob_token_ids
            .as_deref()
            .and_then(|ids| serde_json::from_str::<Vec<String>>(ids).ok())
            .unwrap_or_default()
            .into_iter()
            .map(Symbol::from)
            .collect()
    }
}

#[derive(Deserialize)]
struct GammaEvent {
    #[serde(default)]
    markets: Vec<GammaMarket>,
}

#[derive(Deserialize)]
struct GammaEventsPage {
    events: Vec<GammaEvent>,
    next_cursor: Option<String>,
}

impl Exchange for Polymarket {
    type Codec = PolymarketCodec;

    fn name(&self) -> &'static str {
        "polymarket"
    }

    fn ws_request(&self) -> anyhow::Result<Request> {
        plain_request(WS_URL)
    }

    fn new_codec(&self) -> PolymarketCodec {
        PolymarketCodec
    }

    async fn discover_all(&self, tx: mpsc::UnboundedSender<Vec<Symbol>>) -> anyhow::Result<()> {
        let mut cursor: Option<String> = None;
        let (mut pages, mut markets, mut tokens) = (0usize, 0usize, 0usize);
        loop {
            let mut url = format!("{GAMMA_URL}/events/keyset?closed=false&limit=500");
            if let Some(c) = &cursor {
                url.push_str("&after_cursor=");
                url.push_str(c);
            }
            let page: GammaEventsPage = self.get_json(&url).await?;
            pages += 1;
            let mut symbols = Vec::new();
            for market in page.events.iter().flat_map(|e| &e.markets) {
                self.record_market_id(&market.id);
                markets += 1;
                symbols.extend(market.live_tokens());
            }
            tokens += symbols.len();
            if tx.send(symbols).is_err() {
                return Ok(());
            }
            match page.next_cursor {
                Some(c) if !c.is_empty() && !page.events.is_empty() => cursor = Some(c),
                _ => {
                    tracing::info!(pages, markets, tokens, "gamma scan finished");
                    return Ok(());
                }
            }
        }
    }

    async fn discover_new(&self) -> anyhow::Result<Vec<Symbol>> {
        let since = *self.newest_market_id.lock().unwrap();
        if since == 0 {
            // No full scan has completed a page yet; nothing to diff against.
            return Ok(Vec::new());
        }
        let mut symbols = Vec::new();
        for page in 0..MAX_NEW_MARKET_PAGES {
            let url = format!(
                "{GAMMA_URL}/markets?closed=false&order=id&ascending=false&limit={NEW_MARKET_PAGE}&offset={}",
                page * NEW_MARKET_PAGE
            );
            let markets: Vec<GammaMarket> = self.get_json(&url).await?;
            let mut reached_known = markets.len() < NEW_MARKET_PAGE;
            for market in &markets {
                if market.id.parse::<u64>().is_ok_and(|id| id <= since) {
                    reached_known = true;
                    continue;
                }
                self.record_market_id(&market.id);
                symbols.extend(market.live_tokens());
            }
            if reached_known {
                break;
            }
        }
        Ok(symbols)
    }

    fn max_symbols_per_connection(&self) -> usize {
        self.max_symbols_per_connection
    }

    fn ping_interval(&self) -> Duration {
        Duration::from_secs(10)
    }
}

#[derive(Default)]
pub struct PolymarketCodec;

fn subscription_messages(symbols: &[Symbol], operation: Option<&str>) -> Vec<String> {
    symbols
        .chunks(IDS_PER_MESSAGE)
        .map(|chunk| {
            let mut msg = serde_json::json!({ "assets_ids": chunk });
            match operation {
                Some(op) => msg["operation"] = op.into(),
                None => {
                    msg["type"] = "market".into();
                    msg["custom_feature_enabled"] = true.into();
                }
            }
            msg.to_string()
        })
        .collect()
}

#[derive(Deserialize)]
struct WirePriceSize<'a> {
    #[serde(borrow)]
    price: Cow<'a, str>,
    #[serde(borrow)]
    size: Cow<'a, str>,
}

#[derive(Deserialize)]
struct WirePriceChange<'a> {
    #[serde(borrow)]
    asset_id: Cow<'a, str>,
    #[serde(borrow)]
    price: Cow<'a, str>,
    #[serde(borrow)]
    size: Cow<'a, str>,
    #[serde(borrow)]
    side: Cow<'a, str>,
}

#[derive(Deserialize)]
struct WireMessage<'a> {
    #[serde(borrow, default)]
    event_type: Option<Cow<'a, str>>,
    #[serde(borrow, default)]
    asset_id: Option<Cow<'a, str>>,
    #[serde(borrow, default)]
    timestamp: Option<Cow<'a, str>>,
    #[serde(borrow, default, alias = "buys")]
    bids: Vec<WirePriceSize<'a>>,
    #[serde(borrow, default, alias = "sells")]
    asks: Vec<WirePriceSize<'a>>,
    #[serde(borrow, default)]
    price_changes: Vec<WirePriceChange<'a>>,
    #[serde(borrow, default)]
    assets_ids: Vec<Cow<'a, str>>,
}

fn parse_levels(levels: &[WirePriceSize<'_>]) -> anyhow::Result<Vec<Level>> {
    levels
        .iter()
        .map(|l| {
            Ok(Level {
                price: parse_fixed(&l.price)?,
                quantity: parse_fixed(&l.size)?,
            })
        })
        .collect()
}

fn parse_side(side: &str) -> anyhow::Result<Side> {
    match side {
        "BUY" | "buy" => Ok(Side::Buy),
        "SELL" | "sell" => Ok(Side::Sell),
        other => anyhow::bail!("unknown side {other:?}"),
    }
}

impl PolymarketCodec {
    fn decode_one(&mut self, msg: WireMessage<'_>, out: &mut Vec<MarketEvent>) -> anyhow::Result<()> {
        let event_type = msg.event_type.as_deref().unwrap_or_else(|| {
            // The initial subscription response is an array of books which,
            // historically, did not always carry `event_type`.
            if msg.bids.is_empty() && msg.asks.is_empty() && msg.asset_id.is_none() {
                ""
            } else {
                "book"
            }
        });
        let exchange_ts = || -> anyhow::Result<_> {
            let ts = msg.timestamp.as_deref().ok_or_else(|| anyhow::anyhow!("missing timestamp"))?;
            millis_to_datetime(ts.parse()?)
        };
        match event_type {
            "book" => {
                let symbol = msg.asset_id.as_deref().ok_or_else(|| anyhow::anyhow!("book without asset_id"))?;
                out.push(MarketEvent::Book {
                    symbol: Symbol::from(symbol),
                    exchange_ts: exchange_ts()?,
                    bids: parse_levels(&msg.bids)?,
                    asks: parse_levels(&msg.asks)?,
                });
            }
            "price_change" => {
                let exchange_ts = exchange_ts()?;
                for change in &msg.price_changes {
                    out.push(MarketEvent::SetLevel {
                        symbol: Symbol::from(change.asset_id.as_ref()),
                        exchange_ts,
                        side: parse_side(&change.side)?,
                        price: parse_fixed(&change.price)?,
                        quantity: parse_fixed(&change.size)?,
                    });
                }
            }
            "last_trade_price" | "tick_size_change" | "best_bid_ask" => {
                if let Some(symbol) = msg.asset_id.as_deref() {
                    out.push(MarketEvent::Activity { symbol: Symbol::from(symbol) });
                }
            }
            "market_resolved" => {
                out.extend(
                    msg.assets_ids
                        .iter()
                        .map(|id| MarketEvent::Closed { symbol: Symbol::from(id.as_ref()) }),
                );
            }
            _ => {}
        }
        Ok(())
    }
}

impl Codec for PolymarketCodec {
    fn on_connect(&mut self, symbols: &[Symbol]) -> Vec<String> {
        if symbols.is_empty() {
            // The server expects an initial `type: market` frame.
            return subscription_messages(&[], None);
        }
        let (first, rest) = symbols.split_at(symbols.len().min(IDS_PER_MESSAGE));
        let mut msgs = subscription_messages(first, None);
        msgs.extend(subscription_messages(rest, Some("subscribe")));
        msgs
    }

    fn subscribe(&mut self, symbols: &[Symbol]) -> Vec<String> {
        subscription_messages(symbols, Some("subscribe"))
    }

    fn unsubscribe(&mut self, symbols: &[Symbol]) -> Vec<String> {
        subscription_messages(symbols, Some("unsubscribe"))
    }

    fn ping(&self) -> Option<String> {
        Some("PING".to_owned())
    }

    fn decode(&mut self, text: &str, out: &mut Vec<MarketEvent>) -> anyhow::Result<()> {
        let text = text.trim_start();
        if text.starts_with('[') {
            let msgs: Vec<WireMessage<'_>> = serde_json::from_str(text)?;
            for msg in msgs {
                self.decode_one(msg, out)?;
            }
        } else if text.starts_with('{') {
            self.decode_one(serde_json::from_str(text)?, out)?;
        }
        // "PONG" and other non-JSON control text produce no events.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(text: &str) -> Vec<MarketEvent> {
        let mut out = Vec::new();
        PolymarketCodec.decode(text, &mut out).unwrap();
        out
    }

    #[test]
    fn decodes_book_array() {
        let events = decode(
            r#"[{"market":"0x27","asset_id":"791","timestamp":"1793147882577","hash":"e1",
                "bids":[{"price":"0.001","size":"358.34"}],"asks":[{"price":"0.999","size":"81.24"}],
                "event_type":"book"}]"#,
        );
        assert_eq!(
            events,
            vec![MarketEvent::Book {
                symbol: "791".into(),
                exchange_ts: millis_to_datetime(1793147882577).unwrap(),
                bids: vec![Level { price: 1_000, quantity: 358_340_000 }],
                asks: vec![Level { price: 999_000, quantity: 81_240_000 }],
            }]
        );
    }

    #[test]
    fn decodes_price_change() {
        let events = decode(
            r#"{"market":"0x27", "price_changes":[
                {"asset_id":"215", "price":"0.467", "size":"0", "side":"BUY", "hash":"31", "best_bid":"0.865", "best_ask":"0.995"},
                {"asset_id":"791", "price":"0.533", "size":"10", "side":"SELL", "hash":"58", "best_bid":"0.005", "best_ask":"0.135"}],
                "timestamp":"1793147912612", "event_type":"price_change"}"#,
        );
        let ts = millis_to_datetime(1793147912612).unwrap();
        assert_eq!(
            events,
            vec![
                MarketEvent::SetLevel { symbol: "215".into(), exchange_ts: ts, side: Side::Buy, price: 467_000, quantity: 0 },
                MarketEvent::SetLevel { symbol: "791".into(), exchange_ts: ts, side: Side::Sell, price: 533_000, quantity: 10_000_000 },
            ]
        );
    }

    #[test]
    fn ignores_control_frames() {
        assert!(decode("PONG").is_empty());
        assert!(decode(r#"{"event_type":"something_new"}"#).is_empty());
    }

    #[test]
    fn decodes_resolution() {
        let events = decode(r#"{"event_type":"market_resolved","assets_ids":["1","2"],"timestamp":"1"}"#);
        assert_eq!(
            events,
            vec![MarketEvent::Closed { symbol: "1".into() }, MarketEvent::Closed { symbol: "2".into() }]
        );
    }

    #[test]
    fn splits_subscriptions() {
        let symbols: Vec<Symbol> = (0..250).map(|i| Symbol::from(i.to_string())).collect();
        let msgs = PolymarketCodec.on_connect(&symbols);
        assert_eq!(msgs.len(), 3);
        assert!(msgs[0].contains(r#""type":"market""#));
        assert!(msgs[1].contains(r#""operation":"subscribe""#));
    }
}
