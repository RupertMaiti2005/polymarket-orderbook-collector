//! A shard owns a fixed set of symbols and `redundancy` websocket connections
//! that are all subscribed to the same symbols (extension 1).
//!
//! Every connection maintains its own copy of every book (a "feed"). For each
//! symbol exactly one feed is the *primary*: only its changes are written to
//! the output. When the primary becomes unusable (disconnect, sequence gap,
//! stalled subscription, lagging behind a sibling, consistently slower) the
//! shard switches to another feed and emits a fresh snapshot from that feed,
//! because the two in-memory books may legitimately differ.
//!
//! The shard also does subscription monitoring (extension 2):
//!
//! * a feed that falls behind its siblings is resubscribed,
//! * a symbol that is quiet on *every* connection for much longer than its
//!   usual inter-message gap is resubscribed (one connection at a time, with
//!   exponential backoff so truly idle markets are not hammered),
//! * every connection is periodically recycled (closed and reopened), one at
//!   a time and only while its siblings are healthy.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use crossbeam::channel::Sender;
use rand::Rng;
use tokio::sync::{mpsc, watch};

use super::{
    book::{LevelChange, OrderBook},
    connection::{ConnCmd, ConnEvent, Connection},
    output::OutputMsg,
};
use crate::exchange::{Exchange, MarketEvent, Symbol};

#[derive(Debug, Clone)]
pub struct ShardConfig {
    /// Number of identical websocket connections per shard.
    pub redundancy: usize,
    /// Switch away from the primary feed once it has been behind a sibling
    /// (missing a delta the sibling already has) for this long.
    pub max_source_lag: Duration,
    /// Resubscribe a feed that has been behind its siblings (or has had no
    /// snapshot) for this long.
    pub stale_after: Duration,
    /// Bounds on how long a symbol may be silent on every connection before
    /// it is resubscribed. The actual threshold adapts to the symbol's
    /// observed message rate.
    pub min_quiet: Duration,
    pub max_quiet: Duration,
    /// Periodically recycle each connection after roughly this long.
    pub recycle_every: Option<Duration>,
    /// Prefer a connection whose average latency is better by this margin.
    pub latency_switch: Duration,
    /// Minimum time between latency-motivated primary switches for a symbol.
    pub min_hold: Duration,
}

impl Default for ShardConfig {
    fn default() -> Self {
        Self {
            redundancy: 2,
            max_source_lag: Duration::from_secs(2),
            stale_after: Duration::from_secs(10),
            min_quiet: Duration::from_secs(30 * 60),
            max_quiet: Duration::from_secs(6 * 3600),
            recycle_every: Some(Duration::from_secs(30 * 60)),
            latency_switch: Duration::from_millis(50),
            min_hold: Duration::from_secs(300),
        }
    }
}

/// Counters shared by all shards, logged by the collector.
#[derive(Default)]
pub struct Stats {
    pub frames_events: AtomicU64,
    pub updates_out: AtomicU64,
    pub snapshots_out: AtomicU64,
    pub source_switches: AtomicU64,
    /// Subsets of `source_switches` by cause; the rest are failovers.
    pub lag_switches: AtomicU64,
    pub latency_switches: AtomicU64,
    pub stale_resubscribes: AtomicU64,
    pub quiet_resubscribes: AtomicU64,
    pub invalidations: AtomicU64,
    pub disconnects: AtomicU64,
    pub recycles: AtomicU64,
    pub connected: AtomicU64,
}

#[derive(Debug)]
pub enum ShardCmd {
    Add(Vec<Symbol>),
    Remove(Vec<Symbol>),
}

const RESUB_BATCH: usize = 100;
/// A connection this much slower (EWMA) than its best sibling, for longer
/// than the grace period, is recycled early.
const SLOW_CONN_GAP: Duration = Duration::from_millis(75);
const SLOW_CONN_GRACE: Duration = Duration::from_secs(180);
const MAX_FEED_BACKOFF: Duration = Duration::from_secs(3600);

struct Feed {
    book: Option<OrderBook>,
    /// Newest exchange timestamp this feed's book is known to reflect: the
    /// max of its snapshot time and the deltas applied since.
    watermark: Option<DateTime<Utc>>,
    /// Newest exchange timestamp of a *delta* seen on this feed. Snapshot
    /// times are excluded: a resubscription snapshot is newer than anything a
    /// sibling has, without the sibling having missed anything.
    last_delta_ts: Option<DateTime<Utc>>,
    subscribed_at: Instant,
    behind_since: Option<Instant>,
    resubs: u32,
    next_resub_allowed: Instant,
}

impl Feed {
    fn new(now: Instant) -> Self {
        Self {
            book: None,
            watermark: None,
            last_delta_ts: None,
            subscribed_at: now,
            behind_since: None,
            resubs: 0,
            next_resub_allowed: now,
        }
    }
}

struct SymbolState {
    feeds: Vec<Feed>,
    primary: Option<usize>,
    primary_since: Instant,
    /// Last organic (non-snapshot) message on any connection.
    last_activity: Instant,
    /// EWMA of the gap between organic messages, in seconds.
    gap_ewma: f64,
    quiet_resubs: u32,
}

struct ConnState {
    cmd_tx: mpsc::UnboundedSender<ConnCmd>,
    connected: bool,
    connected_at: Instant,
    recycle_at: Instant,
    /// EWMA of (receive time - exchange time), milliseconds. Includes clock
    /// offset, which is identical across connections so comparisons are fair.
    latency_ewma_ms: Option<f64>,
}

pub struct Shard<E: Exchange> {
    pub id: usize,
    pub exchange: Arc<E>,
    pub config: ShardConfig,
    pub cmd_rx: mpsc::UnboundedReceiver<ShardCmd>,
    pub output: Sender<OutputMsg>,
    /// Symbols the exchange told us are closed, reported to the collector.
    pub closed_tx: mpsc::UnboundedSender<Symbol>,
    pub shutdown: watch::Receiver<bool>,
    pub stats: Arc<Stats>,
    pub connect_delay: Duration,
}

impl<E: Exchange> Shard<E> {
    pub async fn run(self) {
        let Shard { id, exchange, config, mut cmd_rx, output, closed_tx, mut shutdown, stats, connect_delay } = self;
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let now = Instant::now();
        let mut conns = Vec::new();
        let mut handles = Vec::new();
        for c in 0..config.redundancy {
            let (cmd_tx, conn_rx) = mpsc::unbounded_channel();
            let connection = Connection {
                exchange: exchange.clone(),
                id: c,
                label: format!("{}-{id}-{c}", exchange.name()),
                cmd_rx: conn_rx,
                events_tx: events_tx.clone(),
                shutdown: shutdown.clone(),
                // Redundant connections of a shard start a little apart so
                // they are unlikely to share a transient failure.
                initial_delay: connect_delay + Duration::from_millis(250 * c as u64),
            };
            handles.push(tokio::spawn(connection.run()));
            conns.push(ConnState {
                cmd_tx,
                connected: false,
                connected_at: now,
                recycle_at: now + jittered(config.recycle_every.unwrap_or(Duration::MAX / 4)),
                latency_ewma_ms: None,
            });
        }
        drop(events_tx);

        let mut state = ShardState {
            config,
            symbols: HashMap::new(),
            conns,
            output,
            stats,
            recycling: None,
            changes: Vec::new(),
        };
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = shutdown.changed() => break,
                cmd = cmd_rx.recv() => match cmd {
                    Some(ShardCmd::Add(symbols)) => state.add(symbols),
                    Some(ShardCmd::Remove(symbols)) => state.remove(symbols),
                    None => break,
                },
                ev = events_rx.recv() => match ev {
                    Some(ev) => {
                        for symbol in state.on_conn_event(ev) {
                            state.remove(vec![symbol.clone()]);
                            let _ = closed_tx.send(symbol);
                        }
                    }
                    None => break,
                },
                _ = tick.tick() => state.on_tick(Instant::now()),
            }
        }
        // Connections observe the same shutdown signal and close themselves.
        for handle in handles {
            let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
        }
        tracing::debug!(shard = id, "shard stopped");
    }
}

fn latency_gap(conns: &[ConnState], slow: usize, fast: usize) -> Duration {
    match (conns[slow].latency_ewma_ms, conns[fast].latency_ewma_ms) {
        (Some(s), Some(f)) if s > f => Duration::from_secs_f64((s - f) / 1000.0),
        _ => Duration::ZERO,
    }
}

fn jittered(d: Duration) -> Duration {
    d.mul_f64(rand::thread_rng().gen_range(0.8..1.2))
}

struct ShardState {
    config: ShardConfig,
    symbols: HashMap<Symbol, SymbolState>,
    conns: Vec<ConnState>,
    output: Sender<OutputMsg>,
    stats: Arc<Stats>,
    recycling: Option<usize>,
    changes: Vec<LevelChange>,
}

impl ShardState {
    fn emit(&self, msg: OutputMsg) {
        // The writer only goes away during shutdown.
        let _ = self.output.send(msg);
    }

    fn add(&mut self, symbols: Vec<Symbol>) {
        let now = Instant::now();
        let new: Vec<Symbol> = symbols.into_iter().filter(|s| !self.symbols.contains_key(s)).collect();
        for symbol in &new {
            self.symbols.insert(
                symbol.clone(),
                SymbolState {
                    feeds: (0..self.conns.len()).map(|_| Feed::new(now)).collect(),
                    primary: None,
                    primary_since: now,
                    last_activity: now,
                    gap_ewma: self.config.min_quiet.as_secs_f64() / 20.0,
                    quiet_resubs: 0,
                },
            );
        }
        for conn in &self.conns {
            let _ = conn.cmd_tx.send(ConnCmd::Subscribe(new.clone()));
        }
    }

    fn remove(&mut self, symbols: Vec<Symbol>) {
        let gone: Vec<Symbol> = symbols.into_iter().filter(|s| self.symbols.remove(s).is_some()).collect();
        for conn in &self.conns {
            let _ = conn.cmd_tx.send(ConnCmd::Unsubscribe(gone.clone()));
        }
        for symbol in gone {
            self.emit(OutputMsg::Remove { symbol });
        }
    }

    /// Returns symbols the exchange reported as closed.
    fn on_conn_event(&mut self, ev: ConnEvent) -> Vec<Symbol> {
        let mut closed = Vec::new();
        match ev {
            ConnEvent::Connected { conn } => {
                let state = &mut self.conns[conn];
                state.connected = true;
                state.connected_at = Instant::now();
                state.latency_ewma_ms = None;
                if let Some(every) = self.config.recycle_every {
                    state.recycle_at = Instant::now() + jittered(every);
                }
                let now = Instant::now();
                for sym in self.symbols.values_mut() {
                    let feed = &mut sym.feeds[conn];
                    feed.subscribed_at = now;
                    feed.behind_since = None;
                }
                self.stats.connected.fetch_add(1, Relaxed);
            }
            ConnEvent::Disconnected { conn } => {
                if self.conns[conn].connected {
                    self.stats.connected.fetch_sub(1, Relaxed);
                }
                self.conns[conn].connected = false;
                if self.recycling == Some(conn) {
                    self.recycling = None;
                } else {
                    self.stats.disconnects.fetch_add(1, Relaxed);
                }
                let symbols: Vec<Symbol> = self.symbols.keys().cloned().collect();
                for symbol in symbols {
                    self.drop_feed(&symbol, conn);
                }
            }
            ConnEvent::Data { conn, received_at, events } => {
                self.stats.frames_events.fetch_add(events.len() as u64, Relaxed);
                for event in events {
                    if let MarketEvent::Closed { symbol } = &event {
                        if self.symbols.contains_key(symbol) {
                            closed.push(symbol.clone());
                        }
                        continue;
                    }
                    self.on_market_event(conn, received_at, event);
                }
            }
        }
        closed
    }

    fn on_market_event(&mut self, conn: usize, received_at: DateTime<Utc>, event: MarketEvent) {
        let now = Instant::now();
        let Some(sym) = self.symbols.get_mut(event.symbol()) else {
            // e.g. Polymarket sends the complementary token's changes too.
            return;
        };
        let is_primary = sym.primary == Some(conn);
        let feed = &mut sym.feeds[conn];
        self.changes.clear();
        let mut new_book = false;

        let exchange_ts = match &event {
            MarketEvent::Book { exchange_ts, .. }
            | MarketEvent::SetLevel { exchange_ts, .. }
            | MarketEvent::AddLevel { exchange_ts, .. } => Some(*exchange_ts),
            _ => None,
        };
        if !matches!(event, MarketEvent::Book { .. }) {
            // Organic activity: drives the adaptive quiet threshold.
            let gap = now.duration_since(sym.last_activity).as_secs_f64();
            sym.gap_ewma = 0.9 * sym.gap_ewma + 0.1 * gap;
            sym.last_activity = now;
            sym.quiet_resubs = 0;
        }
        if let Some(ts) = exchange_ts {
            let is_delta = matches!(event, MarketEvent::SetLevel { .. } | MarketEvent::AddLevel { .. });
            if feed.book.is_some() || !is_delta {
                feed.watermark = Some(feed.watermark.map_or(ts, |t| t.max(ts)));
            }
            if is_delta && feed.book.is_some() {
                feed.last_delta_ts = Some(feed.last_delta_ts.map_or(ts, |t| t.max(ts)));
                let latency = (received_at - ts).num_microseconds().unwrap_or(0) as f64 / 1000.0;
                let ewma = &mut self.conns[conn].latency_ewma_ms;
                *ewma = Some(ewma.map_or(latency, |e| 0.99 * e + 0.01 * latency));
            }
        }

        match event {
            MarketEvent::Book { symbol, exchange_ts, bids, asks } => {
                let book = OrderBook::from_levels(bids, asks, exchange_ts);
                match &mut feed.book {
                    Some(existing) => existing.replace_diff(book, &mut self.changes),
                    None => {
                        feed.book = Some(book);
                        new_book = true;
                    }
                }
                feed.resubs = 0;
                feed.behind_since = None;
                if is_primary {
                    self.emit_changes(&symbol, received_at, exchange_ts);
                }
                if new_book {
                    self.elect_on_new_data(&symbol, conn);
                }
            }
            MarketEvent::SetLevel { symbol, exchange_ts, side, price, quantity } => {
                let Some(book) = &mut feed.book else { return };
                book.exchange_ts = Some(exchange_ts);
                if let Some(change) = book.set(side, price, quantity) {
                    self.changes.push(change);
                }
                if is_primary {
                    self.emit_changes(&symbol, received_at, exchange_ts);
                }
                self.elect_on_new_data(&symbol, conn);
            }
            MarketEvent::AddLevel { symbol, exchange_ts, side, price, delta } => {
                let Some(book) = &mut feed.book else { return };
                book.exchange_ts = Some(exchange_ts);
                if let Some(change) = book.add(side, price, delta) {
                    self.changes.push(change);
                }
                if is_primary {
                    self.emit_changes(&symbol, received_at, exchange_ts);
                }
                self.elect_on_new_data(&symbol, conn);
            }
            MarketEvent::Invalidate { symbol, reason } => {
                tracing::warn!(%symbol, conn, reason, "feed invalidated");
                self.stats.invalidations.fetch_add(1, Relaxed);
                self.drop_feed(&symbol, conn);
                let _ = self.conns[conn].cmd_tx.send(ConnCmd::Resubscribe(vec![symbol]));
            }
            MarketEvent::Activity { .. } | MarketEvent::Closed { .. } => {}
        }
    }

    fn emit_changes(&mut self, symbol: &Symbol, received_at: DateTime<Utc>, exchange_ts: DateTime<Utc>) {
        for change in self.changes.drain(..) {
            let _ = self.output.send(OutputMsg::Update {
                symbol: symbol.clone(),
                received_at,
                exchange_ts,
                side: change.side,
                price: change.price,
                quantity: change.quantity,
            });
            self.stats.updates_out.fetch_add(1, Relaxed);
        }
    }

    /// Called after `conn`'s feed for `symbol` changed. Promotes it if there is
    /// no usable primary. Lag-based switching happens in [`Self::on_tick`],
    /// because "behind" is only meaningful after a grace period.
    fn elect_on_new_data(&mut self, symbol: &Symbol, conn: usize) {
        let Some(sym) = self.symbols.get(symbol) else { return };
        if sym.primary.is_none() && sym.feeds[conn].book.is_some() {
            self.switch_primary(symbol, conn);
        }
    }

    fn switch_primary(&mut self, symbol: &Symbol, conn: usize) {
        let Some(sym) = self.symbols.get_mut(symbol) else { return };
        let Some(book) = &sym.feeds[conn].book else { return };
        if sym.primary.is_some() {
            self.stats.source_switches.fetch_add(1, Relaxed);
        }
        sym.primary = Some(conn);
        sym.primary_since = Instant::now();
        let now = Utc::now();
        let msg = OutputMsg::Snapshot {
            symbol: symbol.clone(),
            received_at: now,
            exchange_ts: book.exchange_ts.unwrap_or(now),
            bids: book.bids().to_vec(),
            asks: book.asks().to_vec(),
        };
        self.stats.snapshots_out.fetch_add(1, Relaxed);
        self.emit(msg);
    }

    /// Forgets `conn`'s book for `symbol`, failing over if it was the primary.
    fn drop_feed(&mut self, symbol: &Symbol, conn: usize) {
        let Some(sym) = self.symbols.get_mut(symbol) else { return };
        let feed = &mut sym.feeds[conn];
        feed.book = None;
        feed.watermark = None;
        feed.last_delta_ts = None;
        feed.behind_since = None;
        feed.subscribed_at = Instant::now();
        if sym.primary != Some(conn) {
            return;
        }
        let replacement = sym
            .feeds
            .iter()
            .enumerate()
            .filter(|(_, f)| f.book.is_some())
            .max_by_key(|(_, f)| f.watermark)
            .map(|(c, _)| c);
        match replacement {
            Some(c) => self.switch_primary(symbol, c),
            None => {
                sym.primary = None;
                self.emit(OutputMsg::Invalidate { symbol: symbol.clone() });
            }
        }
    }

    fn on_tick(&mut self, now: Instant) {
        let mut resub: Vec<Vec<Symbol>> = vec![Vec::new(); self.conns.len()];
        let mut switches: Vec<(Symbol, usize)> = Vec::new();
        let preferred = self.preferred_conn();

        for (symbol, sym) in &mut self.symbols {
            let mut pending_switch_from = None;
            let newest_delta = sym.feeds.iter().filter_map(|f| f.last_delta_ts).max();

            // Stale feeds: missing a delta a sibling already has, or
            // subscribed without ever receiving a snapshot.
            for (c, feed) in sym.feeds.iter_mut().enumerate() {
                if !self.conns[c].connected {
                    continue;
                }
                let behind = match (&feed.book, newest_delta) {
                    (None, _) => now.duration_since(feed.subscribed_at) > self.config.stale_after,
                    (Some(_), Some(newest)) => feed.watermark.is_none_or(|w| w < newest),
                    (Some(_), None) => false,
                };
                if !behind {
                    feed.behind_since = None;
                    continue;
                }
                let since = *feed.behind_since.get_or_insert(now);
                let behind_for = now.duration_since(since);
                if sym.primary == Some(c) && feed.book.is_some() && behind_for >= self.config.max_source_lag {
                    pending_switch_from = Some(c);
                }
                if behind_for >= self.config.stale_after
                    && now >= feed.next_resub_allowed
                    && resub[c].len() < RESUB_BATCH
                {
                    resub[c].push(symbol.clone());
                    feed.resubs += 1;
                    let backoff = self.config.stale_after * 2u32.saturating_pow(feed.resubs.min(16));
                    feed.next_resub_allowed = now + backoff.min(MAX_FEED_BACKOFF);
                    feed.behind_since = None;
                    self.stats.stale_resubscribes.fetch_add(1, Relaxed);
                }
            }

            if let Some(from) = pending_switch_from {
                let to = sym
                    .feeds
                    .iter()
                    .enumerate()
                    .filter(|(i, f)| *i != from && f.book.is_some() && f.behind_since.is_none())
                    .max_by_key(|(_, f)| f.watermark)
                    .map(|(i, _)| i);
                if let Some(to) = to {
                    tracing::debug!(%symbol, from, to, "primary feed lagging; switching");
                    self.stats.lag_switches.fetch_add(1, Relaxed);
                    switches.push((symbol.clone(), to));
                }
            }

            // Quiet everywhere for far longer than usual: resubscribe on one
            // connection (rotating) so the others keep serving data.
            let threshold = Duration::from_secs_f64(sym.gap_ewma * 20.0)
                .clamp(self.config.min_quiet, self.config.max_quiet)
                .saturating_mul(2u32.saturating_pow(sym.quiet_resubs.min(16)))
                .min(self.config.max_quiet);
            if now.duration_since(sym.last_activity) > threshold {
                let c = sym.quiet_resubs as usize % self.conns.len();
                if self.conns[c].connected && resub[c].len() < RESUB_BATCH {
                    resub[c].push(symbol.clone());
                    sym.quiet_resubs += 1;
                    sym.last_activity = now;
                    self.stats.quiet_resubscribes.fetch_add(1, Relaxed);
                }
            }

            // Latency preference (extension 4).
            if let (Some(best), Some(p)) = (preferred, sym.primary)
                && best != p
                && sym.feeds[best].book.is_some()
                && sym.feeds[best].behind_since.is_none()
                && pending_switch_from.is_none()
                && now.duration_since(sym.primary_since) > self.config.min_hold
                && latency_gap(&self.conns, p, best) > self.config.latency_switch
            {
                switches.push((symbol.clone(), best));
                self.stats.latency_switches.fetch_add(1, Relaxed);
            }
        }

        for (symbol, c) in switches {
            self.switch_primary(&symbol, c);
        }
        for (c, symbols) in resub.into_iter().enumerate() {
            if symbols.is_empty() {
                continue;
            }
            for symbol in &symbols {
                self.drop_feed(symbol, c);
            }
            let _ = self.conns[c].cmd_tx.send(ConnCmd::Resubscribe(symbols));
        }
        self.maybe_recycle(now);
    }

    fn preferred_conn(&self) -> Option<usize> {
        self.conns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.connected && c.connected_at.elapsed() > Duration::from_secs(60))
            .filter_map(|(i, c)| c.latency_ewma_ms.map(|l| (i, l)))
            .min_by(|a, b| a.1.total_cmp(&b.1))
            .map(|(i, _)| i)
    }

    fn maybe_recycle(&mut self, now: Instant) {
        if self.config.recycle_every.is_none() || self.recycling.is_some() {
            return;
        }
        // Due for periodic recycling, or persistently slower than the best
        // sibling: measurements (LATENCY.md) show some connections land on a
        // backend that is ~100ms+ behind; a fresh connection re-rolls that.
        let best = self.preferred_conn();
        let due = |c: usize| {
            let conn = &self.conns[c];
            conn.connected
                && (now >= conn.recycle_at
                    || (now.duration_since(conn.connected_at) > SLOW_CONN_GRACE
                        && best.is_some_and(|b| latency_gap(&self.conns, c, b) > SLOW_CONN_GAP)))
        };
        let Some(c) = (0..self.conns.len()).find(|&c| due(c)) else {
            return;
        };
        // Only recycle while every sibling is up and warmed up, so a recycle
        // never leaves a symbol without a source.
        let siblings_healthy = self.conns.iter().enumerate().all(|(i, s)| {
            i == c || (s.connected && now.duration_since(s.connected_at) > Duration::from_secs(60))
        });
        if !siblings_healthy && self.conns.len() > 1 {
            return;
        }
        // Move primaries off this connection first so the output sees a clean
        // switch rather than a gap.
        let moving: Vec<(Symbol, usize)> = self
            .symbols
            .iter()
            .filter(|(_, s)| s.primary == Some(c))
            .filter_map(|(sym, s)| {
                s.feeds
                    .iter()
                    .enumerate()
                    .filter(|(i, f)| *i != c && f.book.is_some())
                    .max_by_key(|(_, f)| f.watermark)
                    .map(|(i, _)| (sym.clone(), i))
            })
            .collect();
        for (symbol, to) in moving {
            self.switch_primary(&symbol, to);
        }
        self.recycling = Some(c);
        self.stats.recycles.fetch_add(1, Relaxed);
        let _ = self.conns[c].cmd_tx.send(ConnCmd::Reconnect);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Side, exchange::Level};
    use crossbeam::channel::Receiver;

    fn state(redundancy: usize) -> (ShardState, Receiver<OutputMsg>, Vec<mpsc::UnboundedReceiver<ConnCmd>>) {
        let (out_tx, out_rx) = crossbeam::channel::unbounded();
        let mut rxs = Vec::new();
        let conns = (0..redundancy)
            .map(|_| {
                let (tx, rx) = mpsc::unbounded_channel();
                rxs.push(rx);
                ConnState {
                    cmd_tx: tx,
                    connected: true,
                    connected_at: Instant::now(),
                    recycle_at: Instant::now() + Duration::from_secs(3600),
                    latency_ewma_ms: None,
                }
            })
            .collect();
        let state = ShardState {
            config: ShardConfig { redundancy, ..Default::default() },
            symbols: HashMap::new(),
            conns,
            output: out_tx,
            stats: Arc::default(),
            recycling: None,
            changes: Vec::new(),
        };
        (state, out_rx, rxs)
    }

    fn ts(ms: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(1_700_000_000_000 + ms).unwrap()
    }

    fn book(conn: usize, ms: i64, bid_qty: i64) -> ConnEvent {
        ConnEvent::Data {
            conn,
            received_at: ts(ms),
            events: vec![MarketEvent::Book {
                symbol: "S".into(),
                exchange_ts: ts(ms),
                bids: vec![Level { price: 500_000, quantity: bid_qty }],
                asks: vec![],
            }],
        }
    }

    fn set(conn: usize, ms: i64, qty: i64) -> ConnEvent {
        ConnEvent::Data {
            conn,
            received_at: ts(ms),
            events: vec![MarketEvent::SetLevel {
                symbol: "S".into(),
                exchange_ts: ts(ms),
                side: Side::Buy,
                price: 500_000,
                quantity: qty,
            }],
        }
    }

    fn drain(rx: &Receiver<OutputMsg>) -> Vec<OutputMsg> {
        rx.try_iter().collect()
    }

    #[test]
    fn first_book_becomes_primary_and_only_primary_emits() {
        let (mut st, out, _rx) = state(2);
        st.add(vec!["S".into()]);
        st.on_conn_event(book(1, 0, 10));
        assert!(matches!(drain(&out)[..], [OutputMsg::Snapshot { .. }]));
        st.on_conn_event(book(0, 1, 10));
        assert!(drain(&out).is_empty(), "secondary book must not emit");
        st.on_conn_event(set(0, 2, 11));
        assert!(drain(&out).is_empty(), "secondary delta must not emit");
        st.on_conn_event(set(1, 3, 11));
        assert!(matches!(drain(&out)[..], [OutputMsg::Update { quantity: 11_i64, .. }]));
    }

    #[test]
    fn disconnect_fails_over_with_snapshot() {
        let (mut st, out, _rx) = state(2);
        st.add(vec!["S".into()]);
        st.on_conn_event(book(0, 0, 10));
        st.on_conn_event(book(1, 0, 10));
        st.on_conn_event(set(1, 5, 12));
        drain(&out);
        st.on_conn_event(ConnEvent::Disconnected { conn: 0 });
        let msgs = drain(&out);
        let [OutputMsg::Snapshot { bids, .. }] = &msgs[..] else { panic!("{msgs:?}") };
        assert_eq!(bids[0].quantity, 12);
        assert_eq!(st.symbols["S"].primary, Some(1));
    }

    #[test]
    fn total_loss_invalidates_then_recovers() {
        let (mut st, out, _rx) = state(1);
        st.add(vec!["S".into()]);
        st.on_conn_event(book(0, 0, 10));
        st.on_conn_event(ConnEvent::Disconnected { conn: 0 });
        let msgs = drain(&out);
        assert!(matches!(msgs[..], [OutputMsg::Snapshot { .. }, OutputMsg::Invalidate { .. }]));
        // Stray delta after reconnect but before the snapshot is ignored.
        st.on_conn_event(set(0, 1, 3));
        assert!(drain(&out).is_empty());
        st.on_conn_event(book(0, 2, 4));
        assert!(matches!(drain(&out)[..], [OutputMsg::Snapshot { .. }]));
    }

    #[test]
    fn lagging_primary_is_replaced() {
        let (mut st, out, _rx) = state(2);
        st.add(vec!["S".into()]);
        st.on_conn_event(book(0, 0, 10));
        st.on_conn_event(book(1, 0, 10));
        drain(&out);
        // Connection 1 sees a delta that connection 0 never gets.
        st.on_conn_event(set(1, 5_000, 20));
        assert!(drain(&out).is_empty(), "no switch before the grace period");
        let now = Instant::now();
        st.on_tick(now);
        assert!(drain(&out).is_empty());
        st.on_tick(now + Duration::from_secs(3));
        let msgs = drain(&out);
        let [OutputMsg::Snapshot { bids, .. }] = &msgs[..] else { panic!("{msgs:?}") };
        assert_eq!(bids[0].quantity, 20);
        assert_eq!(st.symbols["S"].primary, Some(1));
    }

    #[test]
    fn primary_book_refresh_emits_diff() {
        let (mut st, out, _rx) = state(1);
        st.add(vec!["S".into()]);
        st.on_conn_event(book(0, 0, 10));
        drain(&out);
        st.on_conn_event(book(0, 1, 15));
        assert!(matches!(drain(&out)[..], [OutputMsg::Update { quantity: 15_i64, .. }]));
    }

    #[test]
    fn invalidate_resubscribes() {
        let (mut st, out, mut rx) = state(1);
        st.add(vec!["S".into()]);
        let _ = rx[0].try_recv(); // initial Subscribe
        st.on_conn_event(book(0, 0, 10));
        drain(&out);
        st.on_conn_event(ConnEvent::Data {
            conn: 0,
            received_at: ts(1),
            events: vec![MarketEvent::Invalidate { symbol: "S".into(), reason: "gap" }],
        });
        assert!(matches!(drain(&out)[..], [OutputMsg::Invalidate { .. }]));
        assert!(matches!(rx[0].try_recv(), Ok(ConnCmd::Resubscribe(_))));
    }
}
