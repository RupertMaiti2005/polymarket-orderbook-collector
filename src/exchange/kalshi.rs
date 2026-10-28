//! Kalshi `orderbook_delta` channel.
//!
//! Requires an API key: set `KALSHI_API_KEY_ID` and `KALSHI_PRIVATE_KEY_PATH`
//! (RSA private key, PEM). The websocket handshake is signed with RSA-PSS
//! over `timestamp_ms + "GET" + "/trade-api/ws/v2"`.
//!
//! Kalshi books are expressed as two bid ladders, `yes` and `no`. A NO bid at
//! price `p` is a YES ask at `1 - p`, so we normalise everything to the YES
//! instrument: YES bids -> [`Side::Buy`], NO bids -> [`Side::Sell`] at the
//! complementary price.
//!
//! Deltas are *relative* and sequenced per subscription id (`sid`), so this
//! codec subscribes each ticker separately (one `sid` per ticker), tracks
//! `seq`, and emits [`MarketEvent::Invalidate`] on a gap so the collector
//! resubscribes.

use std::{collections::HashMap, time::Duration};

use base64::Engine;
use chrono::{DateTime, Utc};
use rsa::{
    RsaPrivateKey,
    pkcs1::DecodeRsaPrivateKey,
    pkcs8::DecodePrivateKey,
    pss::BlindedSigningKey,
    signature::{RandomizedSigner, SignatureEncoding},
};
use serde::Deserialize;
use serde_json::Value;
use sha2::Sha256;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, handshake::client::Request, http::HeaderValue};

use super::{Codec, Exchange, Level, MarketEvent, Symbol, millis_to_datetime};
use crate::{
    Side,
    fixed::{SCALE, parse_fixed},
};

const WS_URL: &str = "wss://api.elections.kalshi.com/trade-api/ws/v2";
const WS_PATH: &str = "/trade-api/ws/v2";
const REST_URL: &str = "https://api.elections.kalshi.com/trade-api/v2";

pub struct Kalshi {
    http: reqwest::Client,
    key_id: String,
    key: BlindedSigningKey<Sha256>,
    max_symbols_per_connection: usize,
}

impl Kalshi {
    pub fn from_env(max_symbols_per_connection: usize) -> anyhow::Result<Self> {
        let key_id = std::env::var("KALSHI_API_KEY_ID")
            .map_err(|_| anyhow::anyhow!("KALSHI_API_KEY_ID is not set"))?;
        let path = std::env::var("KALSHI_PRIVATE_KEY_PATH")
            .map_err(|_| anyhow::anyhow!("KALSHI_PRIVATE_KEY_PATH is not set"))?;
        let pem = std::fs::read_to_string(&path)?;
        let key = RsaPrivateKey::from_pkcs8_pem(&pem).or_else(|_| RsaPrivateKey::from_pkcs1_pem(&pem))?;
        Ok(Self {
            http: reqwest::Client::builder().timeout(Duration::from_secs(30)).build()?,
            key_id,
            key: BlindedSigningKey::<Sha256>::new(key),
            max_symbols_per_connection,
        })
    }
}

#[derive(Deserialize)]
struct MarketsPage {
    markets: Vec<KalshiMarket>,
    #[serde(default)]
    cursor: String,
}

#[derive(Deserialize)]
struct KalshiMarket {
    ticker: String,
    #[serde(default)]
    status: String,
}

impl Exchange for Kalshi {
    type Codec = KalshiCodec;

    fn name(&self) -> &'static str {
        "kalshi"
    }

    fn ws_request(&self) -> anyhow::Result<Request> {
        let ts = Utc::now().timestamp_millis().to_string();
        let payload = format!("{ts}GET{WS_PATH}");
        let signature = self.key.sign_with_rng(&mut rand::thread_rng(), payload.as_bytes());
        let signature = base64::engine::general_purpose::STANDARD.encode(signature.to_bytes());
        let mut request = WS_URL.into_client_request()?;
        let headers = request.headers_mut();
        headers.insert("KALSHI-ACCESS-KEY", HeaderValue::from_str(&self.key_id)?);
        headers.insert("KALSHI-ACCESS-TIMESTAMP", HeaderValue::from_str(&ts)?);
        headers.insert("KALSHI-ACCESS-SIGNATURE", HeaderValue::from_str(&signature)?);
        Ok(request)
    }

    fn new_codec(&self) -> KalshiCodec {
        KalshiCodec::default()
    }

    async fn discover_all(&self, tx: mpsc::UnboundedSender<Vec<Symbol>>) -> anyhow::Result<()> {
        let mut cursor = String::new();
        loop {
            // `mve_filter=exclude` drops multivariate events (out of scope).
            let url = format!("{REST_URL}/markets?status=open&limit=1000&mve_filter=exclude&cursor={cursor}");
            let page: MarketsPage = self.http.get(&url).send().await?.error_for_status()?.json().await?;
            let symbols = page
                .markets
                .iter()
                .filter(|m| (m.status == "open" || m.status == "active") && !m.ticker.starts_with("KXMVE"))
                .map(|m| Symbol::from(m.ticker.as_str()))
                .collect();
            if tx.send(symbols).is_err() || page.cursor.is_empty() {
                return Ok(());
            }
            cursor = page.cursor;
        }
    }

    fn max_symbols_per_connection(&self) -> usize {
        self.max_symbols_per_connection
    }
}

#[derive(Default)]
pub struct KalshiCodec {
    next_id: u64,
    /// Outstanding subscribe command id -> ticker.
    pending: HashMap<u64, Symbol>,
    /// Ticker -> subscription id.
    sids: HashMap<Symbol, u64>,
    /// Subscription id -> (ticker, last seq).
    seqs: HashMap<u64, (Symbol, Option<u64>)>,
}

impl KalshiCodec {
    fn command(&mut self, cmd: &str, params: Value) -> (u64, String) {
        self.next_id += 1;
        let id = self.next_id;
        (id, serde_json::json!({ "id": id, "cmd": cmd, "params": params }).to_string())
    }
}

#[derive(Deserialize)]
struct Envelope {
    #[serde(rename = "type")]
    kind: String,
    id: Option<u64>,
    sid: Option<u64>,
    seq: Option<u64>,
    #[serde(default)]
    msg: Value,
}

/// Fixed-point price from either `<name>_dollars` ("0.4200") or `<name>`
/// (integer cents).
fn price(v: &Value, name: &str) -> anyhow::Result<i64> {
    if let Some(s) = v.get(format!("{name}_dollars")).and_then(Value::as_str) {
        return parse_fixed(s);
    }
    let cents = v.get(name).and_then(Value::as_i64).ok_or_else(|| anyhow::anyhow!("missing {name}"))?;
    Ok(cents * (SCALE / 100))
}

/// Fixed-point contract count from either `<name>_fp` ("54.00") or `<name>`
/// (integer, may be negative for deltas).
fn quantity(v: &Value, name: &str) -> anyhow::Result<i64> {
    if let Some(s) = v.get(format!("{name}_fp")).and_then(Value::as_str) {
        return match s.strip_prefix('-') {
            Some(abs) => Ok(-parse_fixed(abs)?),
            None => parse_fixed(s),
        };
    }
    let n = v.get(name).and_then(Value::as_i64).ok_or_else(|| anyhow::anyhow!("missing {name}"))?;
    Ok(n * SCALE)
}

/// Parses `[[price, qty], ...]` (cents/contracts) or `[["0.42", "10.00"], ...]` ladders.
fn ladder(v: Option<&Value>) -> anyhow::Result<Vec<Level>> {
    let Some(Value::Array(rows)) = v else { return Ok(Vec::new()) };
    rows.iter()
        .map(|row| {
            let (p, q) = (&row[0], &row[1]);
            let price = match p {
                Value::String(s) => parse_fixed(s)?,
                _ => p.as_i64().ok_or_else(|| anyhow::anyhow!("bad price"))? * (SCALE / 100),
            };
            let quantity = match q {
                Value::String(s) => parse_fixed(s)?,
                _ => q.as_i64().ok_or_else(|| anyhow::anyhow!("bad quantity"))? * SCALE,
            };
            Ok(Level { price, quantity })
        })
        .collect()
}

fn timestamp(msg: &Value) -> Option<DateTime<Utc>> {
    if let Some(ms) = msg.get("ts_ms").and_then(Value::as_i64) {
        return millis_to_datetime(ms).ok();
    }
    match msg.get("ts")? {
        Value::String(s) => DateTime::parse_from_rfc3339(s).ok().map(|d| d.with_timezone(&Utc)),
        Value::Number(n) => DateTime::from_timestamp(n.as_i64()?, 0),
        _ => None,
    }
}

impl Codec for KalshiCodec {
    fn on_connect(&mut self, symbols: &[Symbol]) -> Vec<String> {
        *self = Self { next_id: self.next_id, ..Self::default() };
        self.subscribe(symbols)
    }

    fn subscribe(&mut self, symbols: &[Symbol]) -> Vec<String> {
        symbols
            .iter()
            .map(|s| {
                let (id, msg) = self.command(
                    "subscribe",
                    serde_json::json!({ "channels": ["orderbook_delta"], "market_ticker": s.as_ref() }),
                );
                self.pending.insert(id, s.clone());
                msg
            })
            .collect()
    }

    fn unsubscribe(&mut self, symbols: &[Symbol]) -> Vec<String> {
        let sids: Vec<u64> = symbols
            .iter()
            .filter_map(|s| self.sids.remove(s))
            .inspect(|sid| {
                self.seqs.remove(sid);
            })
            .collect();
        if sids.is_empty() {
            return Vec::new();
        }
        vec![self.command("unsubscribe", serde_json::json!({ "sids": sids })).1]
    }

    fn ping(&self) -> Option<String> {
        // Kalshi uses websocket-level pings; the connection sends those.
        None
    }

    fn decode(&mut self, text: &str, out: &mut Vec<MarketEvent>) -> anyhow::Result<()> {
        let env: Envelope = serde_json::from_str(text)?;
        match env.kind.as_str() {
            "subscribed" => {
                let sid = env.msg.get("sid").and_then(Value::as_u64);
                if let (Some(id), Some(sid)) = (env.id, sid)
                    && let Some(symbol) = self.pending.remove(&id)
                {
                    self.sids.insert(symbol.clone(), sid);
                    self.seqs.insert(sid, (symbol, None));
                }
            }
            "orderbook_snapshot" | "orderbook_delta" => {
                let (Some(sid), Some(seq)) = (env.sid, env.seq) else {
                    anyhow::bail!("{} without sid/seq", env.kind)
                };
                let ticker = env.msg.get("market_ticker").and_then(Value::as_str).unwrap_or_default();
                let entry = self.seqs.entry(sid).or_insert_with(|| (Symbol::from(ticker), None));
                let symbol = entry.0.clone();
                let expected = entry.1.map(|s| s + 1);
                entry.1 = Some(seq);
                if env.kind == "orderbook_snapshot" {
                    let yes = ladder(env.msg.get("yes_dollars").or(env.msg.get("yes")))?;
                    let no = ladder(env.msg.get("no_dollars").or(env.msg.get("no")))?;
                    out.push(MarketEvent::Book {
                        symbol,
                        exchange_ts: timestamp(&env.msg).unwrap_or_else(Utc::now),
                        bids: yes,
                        asks: no.into_iter().map(|l| Level { price: SCALE - l.price, quantity: l.quantity }).collect(),
                    });
                    return Ok(());
                }
                if expected.is_some_and(|e| e != seq) {
                    out.push(MarketEvent::Invalidate { symbol, reason: "sequence gap" });
                    return Ok(());
                }
                let p = price(&env.msg, "price")?;
                let delta = quantity(&env.msg, "delta")?;
                let (side, price) = match env.msg.get("side").and_then(Value::as_str) {
                    Some("yes") => (Side::Buy, p),
                    Some("no") => (Side::Sell, SCALE - p),
                    other => anyhow::bail!("unknown side {other:?}"),
                };
                out.push(MarketEvent::AddLevel {
                    symbol,
                    exchange_ts: timestamp(&env.msg).unwrap_or_else(Utc::now),
                    side,
                    price,
                    delta,
                });
            }
            "market_lifecycle_v2" | "market_lifecycle" => {
                let status = env.msg.get("event_type").and_then(Value::as_str).unwrap_or_default();
                if let Some(ticker) = env.msg.get("market_ticker").and_then(Value::as_str)
                    && matches!(status, "closed" | "settled" | "determined" | "deactivated")
                {
                    out.push(MarketEvent::Closed { symbol: Symbol::from(ticker) });
                }
            }
            "error" => tracing::warn!(msg = %env.msg, "kalshi error"),
            _ => {}
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(codec: &mut KalshiCodec, text: &str) -> Vec<MarketEvent> {
        let mut out = Vec::new();
        codec.decode(text, &mut out).unwrap();
        out
    }

    #[test]
    fn snapshot_delta_and_gap() {
        let mut codec = KalshiCodec::default();
        let msgs = codec.on_connect(&["KXTEST-1".into()]);
        assert_eq!(msgs.len(), 1);
        decode(&mut codec, r#"{"id":1,"type":"subscribed","msg":{"channel":"orderbook_delta","sid":7}}"#);

        let book = decode(
            &mut codec,
            r#"{"type":"orderbook_snapshot","sid":7,"seq":1,"msg":{"market_ticker":"KXTEST-1","yes":[[40,100]],"no":[[55,20]]}}"#,
        );
        let [MarketEvent::Book { bids, asks, .. }] = &book[..] else { panic!("{book:?}") };
        assert_eq!(bids, &[Level { price: 400_000, quantity: 100_000_000 }]);
        assert_eq!(asks, &[Level { price: 450_000, quantity: 20_000_000 }]);

        let delta = decode(
            &mut codec,
            r#"{"type":"orderbook_delta","sid":7,"seq":2,"msg":{"market_ticker":"KXTEST-1","price":55,"delta":-5,"side":"no","ts":"2022-11-22T20:44:01Z"}}"#,
        );
        assert!(matches!(
            delta[..],
            [MarketEvent::AddLevel { side: Side::Sell, price: 450_000, delta: -5_000_000, .. }]
        ));

        let gap = decode(
            &mut codec,
            r#"{"type":"orderbook_delta","sid":7,"seq":4,"msg":{"market_ticker":"KXTEST-1","price":40,"delta":1,"side":"yes"}}"#,
        );
        assert!(matches!(gap[..], [MarketEvent::Invalidate { .. }]));

        let unsub = codec.unsubscribe(&["KXTEST-1".into()]);
        assert!(unsub[0].contains(r#""sids":[7]"#));
    }

    #[test]
    fn dollar_fields() {
        let mut codec = KalshiCodec::default();
        let delta = decode(
            &mut codec,
            r#"{"type":"orderbook_delta","sid":1,"seq":1,"msg":{"market_ticker":"T","price_dollars":"0.4200","delta_fp":"-1.50","side":"yes","ts_ms":1700000000000}}"#,
        );
        assert!(matches!(
            delta[..],
            [MarketEvent::AddLevel { side: Side::Buy, price: 420_000, delta: -1_500_000, .. }]
        ));
    }
}
