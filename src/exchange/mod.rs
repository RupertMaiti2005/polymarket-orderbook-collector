//! The exchange abstraction (extension 3).
//!
//! Everything that differs between venues lives behind two traits:
//!
//! * [`Exchange`]: how to discover tradeable symbols, where the websocket
//!   lives (including any auth headers), and connection limits.
//! * [`Codec`]: how to encode subscribe/unsubscribe/ping messages and how to
//!   decode the venue's wire messages into normalised [`MarketEvent`]s. One
//!   codec instance is created per websocket connection, so it may hold
//!   per-connection state (sequence numbers, subscription ids, ...).
//!
//! The collector (connection management, redundancy, subscription monitoring,
//! order books, output) is written once against these traits.

use std::{future::Future, sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, handshake::client::Request};

use crate::Side;

pub mod kalshi;
pub mod polymarket;

/// Exchange-native instrument identifier (Polymarket token id, Kalshi ticker).
pub type Symbol = Arc<str>;

/// A single price level in fixed point (see [`crate::fixed::SCALE`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Level {
    pub price: i64,
    pub quantity: i64,
}

/// A normalised, venue-independent market data event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarketEvent {
    /// Replace the whole book for `symbol`.
    Book {
        symbol: Symbol,
        exchange_ts: DateTime<Utc>,
        bids: Vec<Level>,
        asks: Vec<Level>,
    },
    /// Set the absolute quantity resting at one price level (0 = remove).
    SetLevel {
        symbol: Symbol,
        exchange_ts: DateTime<Utc>,
        side: Side,
        price: i64,
        quantity: i64,
    },
    /// Add `delta` (may be negative) to the quantity resting at one level.
    AddLevel {
        symbol: Symbol,
        exchange_ts: DateTime<Utc>,
        side: Side,
        price: i64,
        delta: i64,
    },
    /// The book for `symbol` on this connection can no longer be trusted
    /// (for example a sequence gap). The collector drops it and resubscribes.
    Invalidate { symbol: Symbol, reason: &'static str },
    /// The venue reports that `symbol` stopped trading.
    Closed { symbol: Symbol },
    /// Proof of life for `symbol` that does not change the book (trades,
    /// tick size changes...). Counts towards subscription activity.
    Activity { symbol: Symbol },
}

impl MarketEvent {
    pub fn symbol(&self) -> &Symbol {
        match self {
            Self::Book { symbol, .. }
            | Self::SetLevel { symbol, .. }
            | Self::AddLevel { symbol, .. }
            | Self::Invalidate { symbol, .. }
            | Self::Closed { symbol }
            | Self::Activity { symbol } => symbol,
        }
    }
}

/// Venue-specific wire encoding/decoding. One instance per websocket.
pub trait Codec: Send + 'static {
    /// Messages to send right after the websocket opens, subscribing to the
    /// initial `symbols` (may be empty).
    fn on_connect(&mut self, symbols: &[Symbol]) -> Vec<String>;

    /// Messages that add `symbols` to the live connection.
    fn subscribe(&mut self, symbols: &[Symbol]) -> Vec<String>;

    /// Messages that remove `symbols` from the live connection.
    fn unsubscribe(&mut self, symbols: &[Symbol]) -> Vec<String>;

    /// Application-level keepalive text, if the venue needs one.
    fn ping(&self) -> Option<String>;

    /// Decodes one text frame, appending the resulting events to `out`.
    /// Control/ack messages simply produce no events.
    fn decode(&mut self, text: &str, out: &mut Vec<MarketEvent>) -> anyhow::Result<()>;
}

/// Venue-level configuration and REST helpers.
pub trait Exchange: Send + Sync + 'static {
    type Codec: Codec;

    fn name(&self) -> &'static str;

    /// Builds the websocket handshake request. Called for every (re)connect
    /// so that signed auth headers are always fresh.
    fn ws_request(&self) -> anyhow::Result<Request>;

    fn new_codec(&self) -> Self::Codec;

    /// Full scan of every symbol that is currently live. Results are streamed
    /// page by page through `tx` so the collector can start subscribing before
    /// a (possibly multi-minute) scan finishes. Symbols absent from a completed
    /// scan are considered no longer trading.
    fn discover_all(
        &self,
        tx: mpsc::UnboundedSender<Vec<Symbol>>,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;

    /// Cheap incremental poll for symbols listed since the last call. Venues
    /// without such an endpoint can rely on [`Self::discover_all`] alone.
    fn discover_new(&self) -> impl Future<Output = anyhow::Result<Vec<Symbol>>> + Send {
        async { Ok(Vec::new()) }
    }

    /// Upper bound on subscriptions per websocket.
    fn max_symbols_per_connection(&self) -> usize;

    /// Interval at which [`Codec::ping`] should be sent.
    fn ping_interval(&self) -> Duration {
        Duration::from_secs(10)
    }
}

pub(crate) fn plain_request(url: &str) -> anyhow::Result<Request> {
    Ok(url.into_client_request()?)
}

pub(crate) fn millis_to_datetime(ms: i64) -> anyhow::Result<DateTime<Utc>> {
    DateTime::from_timestamp_millis(ms).ok_or_else(|| anyhow::anyhow!("timestamp {ms} out of range"))
}
