//! Exchange-agnostic collector: symbol discovery, load balancing symbols over
//! shards of redundant websockets, and the output writer.

pub mod book;
pub mod connection;
pub mod output;
pub mod shard;

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, atomic::Ordering::Relaxed},
    time::{Duration, Instant},
};

use tokio::sync::{mpsc, watch};

use self::{
    output::{Destination, OutputMsg, OutputWriter},
    shard::{Shard, ShardCmd, ShardConfig, Stats},
};
use crate::exchange::{Exchange, Symbol};

#[derive(Debug, Clone)]
pub struct CollectorConfig {
    pub shard: ShardConfig,
    /// How often to run a full symbol scan (which also detects removals).
    pub full_scan_every: Duration,
    /// How often to poll for newly listed symbols between full scans.
    pub new_symbol_poll_every: Duration,
    pub snapshot_every: Duration,
    /// Cap on collected symbols (useful for testing). `None` = everything.
    pub max_symbols: Option<usize>,
    /// Delay between opening consecutive shards, to stay under connection
    /// rate limits.
    pub shard_open_interval: Duration,
}

/// Run the collector until `shutdown` flips to true.
pub async fn run<E: Exchange>(
    exchange: Arc<E>,
    config: CollectorConfig,
    destination: Destination,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let (out_tx, out_rx) = crossbeam::channel::unbounded::<OutputMsg>();
    let writer = OutputWriter::new(destination, config.snapshot_every);
    let writer_thread = std::thread::Builder::new()
        .name("output-writer".into())
        .spawn(move || writer.run(out_rx))?;

    let stats = Arc::new(Stats::default());
    let (closed_tx, mut closed_rx) = mpsc::unbounded_channel();
    let mut manager = Manager {
        exchange: exchange.clone(),
        config: config.clone(),
        shards: Vec::new(),
        assignment: HashMap::new(),
        closed: HashSet::new(),
        out_tx,
        closed_tx,
        shutdown: shutdown.clone(),
        stats: stats.clone(),
        handles: Vec::new(),
        next_open_at: Instant::now(),
        first_free: 0,
    };

    // Full scans stream pages of symbols back to us.
    let (scan_tx, mut scan_rx) = mpsc::unbounded_channel::<ScanMsg>();
    let scanner = {
        let exchange = exchange.clone();
        let every = config.full_scan_every;
        let poll_every = config.new_symbol_poll_every;
        let mut shutdown = shutdown.clone();
        tokio::spawn(async move {
            let mut next_full = Instant::now();
            loop {
                if Instant::now() >= next_full {
                    next_full = Instant::now() + every;
                    let (page_tx, mut page_rx) = mpsc::unbounded_channel();
                    let started = Instant::now();
                    let scan = exchange.discover_all(page_tx);
                    tokio::pin!(scan);
                    let mut done = false;
                    let mut result = Ok(());
                    while !done {
                        tokio::select! {
                            r = &mut scan, if !done => { result = r; done = true; }
                            Some(page) = page_rx.recv() => { let _ = scan_tx.send(ScanMsg::Page(page)); }
                            _ = shutdown.changed() => return,
                        }
                    }
                    while let Ok(page) = page_rx.try_recv() {
                        let _ = scan_tx.send(ScanMsg::Page(page));
                    }
                    match result {
                        Ok(()) => {
                            tracing::info!(secs = started.elapsed().as_secs(), "full symbol scan complete");
                            let _ = scan_tx.send(ScanMsg::FullScanDone { started });
                        }
                        Err(err) => tracing::warn!(%err, "full symbol scan failed"),
                    }
                }
                tokio::select! {
                    _ = tokio::time::sleep(poll_every) => {}
                    _ = shutdown.changed() => return,
                }
                match exchange.discover_new().await {
                    Ok(new) if !new.is_empty() => {
                        tracing::info!(count = new.len(), "discovered new symbols");
                        let _ = scan_tx.send(ScanMsg::New(new));
                    }
                    Ok(_) => {}
                    Err(err) => tracing::warn!(%err, "new symbol poll failed"),
                }
            }
        })
    };

    // Symbols seen during the current full scan, with the time they were added.
    let mut seen_this_scan: HashSet<Symbol> = HashSet::new();
    let mut added_at: HashMap<Symbol, Instant> = HashMap::new();
    let mut last_scan_size = 0usize;
    let mut stats_tick = tokio::time::interval(Duration::from_secs(30));

    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            msg = scan_rx.recv() => match msg {
                Some(ScanMsg::Page(symbols)) | Some(ScanMsg::New(symbols)) => {
                    let now = Instant::now();
                    for s in &symbols {
                        seen_this_scan.insert(s.clone());
                        added_at.entry(s.clone()).or_insert(now);
                    }
                    manager.add(symbols);
                }
                Some(ScanMsg::FullScanDone { started }) => {
                    tracing::info!(symbols = seen_this_scan.len(), "full scan result");
                    // A scan that came back far smaller than the last one is
                    // much more likely a truncated API response than half the
                    // venue delisting at once; don't mass-unsubscribe on it.
                    if seen_this_scan.len() < last_scan_size / 2 {
                        tracing::warn!(
                            seen = seen_this_scan.len(),
                            previous = last_scan_size,
                            "full scan suspiciously small; skipping removals"
                        );
                        seen_this_scan.clear();
                        continue;
                    }
                    last_scan_size = seen_this_scan.len();
                    // Anything assigned before this scan started but not seen
                    // in it has stopped trading.
                    let stale: Vec<Symbol> = manager
                        .assignment
                        .keys()
                        .filter(|s| !seen_this_scan.contains(*s) && added_at.get(*s).is_none_or(|t| *t < started))
                        .cloned()
                        .collect();
                    if !stale.is_empty() {
                        tracing::info!(count = stale.len(), "unsubscribing symbols that stopped trading");
                    }
                    for s in &stale {
                        added_at.remove(s);
                    }
                    manager.remove(stale);
                    seen_this_scan.clear();
                    manager.closed.clear();
                }
                None => break,
            },
            Some(symbol) = closed_rx.recv() => {
                tracing::info!(%symbol, "exchange reported symbol closed");
                manager.forget(&symbol);
                added_at.remove(&symbol);
            }
            _ = stats_tick.tick() => manager.log_stats(),
        }
    }

    tracing::info!("shutting down: closing websockets");
    scanner.abort();
    for handle in manager.handles.drain(..) {
        let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;
    }
    // Dropping every output sender lets the writer flush and close the file.
    drop(manager);
    match tokio::task::spawn_blocking(move || writer_thread.join()).await? {
        Ok(result) => result,
        Err(_) => anyhow::bail!("output writer panicked"),
    }
}

enum ScanMsg {
    Page(Vec<Symbol>),
    New(Vec<Symbol>),
    FullScanDone { started: Instant },
}

struct ShardHandle {
    tx: mpsc::UnboundedSender<ShardCmd>,
    count: usize,
}

struct Manager<E: Exchange> {
    exchange: Arc<E>,
    config: CollectorConfig,
    shards: Vec<ShardHandle>,
    assignment: HashMap<Symbol, usize>,
    /// Symbols the exchange reported closed during the current scan cycle;
    /// not re-added until the next full scan confirms their status.
    closed: HashSet<Symbol>,
    out_tx: crossbeam::channel::Sender<OutputMsg>,
    closed_tx: mpsc::UnboundedSender<Symbol>,
    shutdown: watch::Receiver<bool>,
    stats: Arc<Stats>,
    handles: Vec<tokio::task::JoinHandle<()>>,
    next_open_at: Instant,
    /// No shard below this index has spare capacity.
    first_free: usize,
}

impl<E: Exchange> Manager<E> {
    fn add(&mut self, symbols: Vec<Symbol>) {
        let cap = self.exchange.max_symbols_per_connection();
        let mut pending: HashMap<usize, Vec<Symbol>> = HashMap::new();
        for symbol in symbols {
            if self.assignment.contains_key(&symbol) || self.closed.contains(&symbol) {
                continue;
            }
            if self.config.max_symbols.is_some_and(|max| self.assignment.len() >= max) {
                break;
            }
            // Fill the first shard with spare capacity (removals free slots
            // in older shards), otherwise open a new one.
            let free = self.shards[self.first_free..].iter().position(|s| s.count < cap);
            let shard = match free.map(|i| i + self.first_free) {
                Some(i) => i,
                None => self.open_shard(),
            };
            self.shards[shard].count += 1;
            self.first_free = shard;
            self.assignment.insert(symbol.clone(), shard);
            pending.entry(shard).or_default().push(symbol);
        }
        for (shard, symbols) in pending {
            let _ = self.shards[shard].tx.send(ShardCmd::Add(symbols));
        }
    }

    fn open_shard(&mut self) -> usize {
        let id = self.shards.len();
        let (tx, rx) = mpsc::unbounded_channel();
        // Spread connection attempts out in time instead of sleeping here,
        // which would block discovery.
        let now = Instant::now();
        self.next_open_at = self.next_open_at.max(now) + self.config.shard_open_interval;
        let shard = Shard {
            id,
            exchange: self.exchange.clone(),
            config: self.config.shard.clone(),
            cmd_rx: rx,
            output: self.out_tx.clone(),
            closed_tx: self.closed_tx.clone(),
            shutdown: self.shutdown.clone(),
            stats: self.stats.clone(),
            connect_delay: self.next_open_at - now,
        };
        self.handles.push(tokio::spawn(shard.run()));
        self.shards.push(ShardHandle { tx, count: 0 });
        tracing::debug!(shard = id, "opened shard");
        id
    }

    fn remove(&mut self, symbols: Vec<Symbol>) {
        let mut pending: HashMap<usize, Vec<Symbol>> = HashMap::new();
        for symbol in symbols {
            if let Some(shard) = self.assignment.remove(&symbol) {
                self.shards[shard].count -= 1;
                self.first_free = self.first_free.min(shard);
                pending.entry(shard).or_default().push(symbol);
            }
        }
        for (shard, symbols) in pending {
            let _ = self.shards[shard].tx.send(ShardCmd::Remove(symbols));
        }
    }

    /// The shard already dropped `symbol`; just free its slot.
    fn forget(&mut self, symbol: &Symbol) {
        if let Some(shard) = self.assignment.remove(symbol) {
            self.shards[shard].count -= 1;
            self.first_free = self.first_free.min(shard);
        }
        self.closed.insert(symbol.clone());
    }

    fn log_stats(&self) {
        let s = &self.stats;
        tracing::info!(
            symbols = self.assignment.len(),
            shards = self.shards.len(),
            connected = s.connected.load(Relaxed),
            events_in = s.frames_events.load(Relaxed),
            updates_out = s.updates_out.load(Relaxed),
            snapshots_out = s.snapshots_out.load(Relaxed),
            source_switches = s.source_switches.load(Relaxed),
            lag_switches = s.lag_switches.load(Relaxed),
            latency_switches = s.latency_switches.load(Relaxed),
            stale_resubs = s.stale_resubscribes.load(Relaxed),
            quiet_resubs = s.quiet_resubscribes.load(Relaxed),
            invalidations = s.invalidations.load(Relaxed),
            disconnects = s.disconnects.load(Relaxed),
            recycles = s.recycles.load(Relaxed),
            "stats"
        );
    }
}
