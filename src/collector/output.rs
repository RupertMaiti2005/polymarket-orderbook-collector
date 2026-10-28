//! The single output writer.
//!
//! All shards send [`OutputMsg`]s to one dedicated OS thread that owns the
//! [`AlignedSbeWriter`]. The writer keeps a mirror of every book *as emitted*
//! (i.e. built only from the snapshots and deltas it has written), which gives
//! us two things for free:
//!
//! * periodic full snapshots that are, by construction, exactly consistent with
//!   the delta stream a consumer has replayed, and
//! * a full set of snapshots at the top of every rotated file, so each file can
//!   be replayed on its own.

use std::{
    collections::HashMap,
    path::PathBuf,
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use crossbeam::channel::{Receiver, RecvTimeoutError};

use super::book::OrderBook;
use crate::{
    Side,
    exchange::{Level, Symbol},
    util::{
        AlignedSbeWriter,
        dtos::challenge_orderbook::{
            PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
        },
    },
};

/// Must match the reader's chunk size so output files can be read back with
/// [`crate::util::AlignedSbeReader::open`].
pub const CHUNK_SIZE: usize = 64 << 20;

#[derive(Debug)]
pub enum OutputMsg {
    /// A new source book for `symbol` (first book, or source switch).
    Snapshot {
        symbol: Symbol,
        received_at: DateTime<Utc>,
        exchange_ts: DateTime<Utc>,
        bids: Vec<Level>,
        asks: Vec<Level>,
    },
    Update {
        symbol: Symbol,
        received_at: DateTime<Utc>,
        exchange_ts: DateTime<Utc>,
        side: Side,
        price: i64,
        quantity: i64,
    },
    /// No connection currently has a trustworthy book for `symbol`. Periodic
    /// snapshots are suppressed until the next [`OutputMsg::Snapshot`].
    Invalidate { symbol: Symbol },
    /// `symbol` stopped trading; forget it.
    Remove { symbol: Symbol },
}

pub enum Destination {
    /// A single file (or `/dev/stdout`), never rotated.
    Path(PathBuf),
    /// Rotating files `<dir>/<prefix>_<UTC start time>.sbe`.
    Rotating {
        dir: PathBuf,
        prefix: String,
        every: Duration,
    },
}

struct MirrorBook {
    book: OrderBook,
    valid: bool,
}

pub struct OutputWriter {
    destination: Destination,
    snapshot_every: Duration,
    writer: Option<AlignedSbeWriter>,
    file_opened_at: Instant,
    books: HashMap<Symbol, MirrorBook>,
    pub messages_written: u64,
}

impl OutputWriter {
    pub fn new(destination: Destination, snapshot_every: Duration) -> Self {
        Self {
            destination,
            snapshot_every,
            writer: None,
            file_opened_at: Instant::now(),
            books: HashMap::new(),
            messages_written: 0,
        }
    }

    /// Runs until every sender of `rx` has been dropped, then flushes and
    /// closes the current file.
    pub fn run(mut self, rx: Receiver<OutputMsg>) -> anyhow::Result<()> {
        self.open()?;
        let mut next_snapshot = next_boundary(self.snapshot_every);
        loop {
            let timeout = next_snapshot.saturating_duration_since(Instant::now());
            match rx.recv_timeout(timeout) {
                Ok(msg) => self.handle(msg)?,
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }
            if Instant::now() >= next_snapshot {
                self.maybe_rotate()?;
                self.write_all_snapshots()?;
                next_snapshot = next_boundary(self.snapshot_every);
            }
        }
        self.close()
    }

    fn open(&mut self) -> anyhow::Result<()> {
        let path = match &self.destination {
            Destination::Path(p) => p.clone(),
            Destination::Rotating { dir, prefix, .. } => {
                std::fs::create_dir_all(dir)?;
                dir.join(format!("{prefix}_{}.sbe", Utc::now().format("%Y%m%dT%H%M%SZ")))
            }
        };
        tracing::info!(path = %path.display(), "opening output file");
        self.writer = Some(AlignedSbeWriter::create(&path, CHUNK_SIZE)?);
        self.file_opened_at = Instant::now();
        Ok(())
    }

    fn close(&mut self) -> anyhow::Result<()> {
        if let Some(mut writer) = self.writer.take() {
            writer.flush()?;
            // Dropping the writer closes the file.
            tracing::info!(messages = self.messages_written, "output file flushed and closed");
        }
        Ok(())
    }

    fn maybe_rotate(&mut self) -> anyhow::Result<()> {
        if let Destination::Rotating { every, .. } = self.destination
            && self.file_opened_at.elapsed() >= every
        {
            self.close()?;
            self.open()?;
        }
        Ok(())
    }

    fn writer(&mut self) -> &mut AlignedSbeWriter {
        self.writer.as_mut().expect("output file is open")
    }

    fn handle(&mut self, msg: OutputMsg) -> anyhow::Result<()> {
        match msg {
            OutputMsg::Snapshot { symbol, received_at, exchange_ts, bids, asks } => {
                let book = OrderBook::from_levels(bids, asks, exchange_ts);
                write_snapshot(self.writer.as_mut().expect("open"), &symbol, &book, received_at)?;
                self.messages_written += 1;
                self.books.insert(symbol, MirrorBook { book, valid: true });
            }
            OutputMsg::Update { symbol, received_at, exchange_ts, side, price, quantity } => {
                let Some(mirror) = self.books.get_mut(&symbol).filter(|m| m.valid) else {
                    // Shards never emit deltas without a snapshot first.
                    tracing::error!(%symbol, "delta for a symbol without a snapshot; dropping");
                    return Ok(());
                };
                mirror.book.set(side, price, quantity);
                mirror.book.exchange_ts = Some(exchange_ts);
                let update = PriceLevelUpdateDomain {
                    backtest_timestamp: received_at,
                    exchange_timestamp: exchange_ts,
                    price_level: price,
                    quantity,
                    side,
                    symbol: symbol.to_string(),
                };
                self.writer().write_msg(&update)?;
                self.messages_written += 1;
            }
            OutputMsg::Invalidate { symbol } => {
                if let Some(mirror) = self.books.get_mut(&symbol) {
                    mirror.valid = false;
                }
            }
            OutputMsg::Remove { symbol } => {
                self.books.remove(&symbol);
            }
        }
        Ok(())
    }

    fn write_all_snapshots(&mut self) -> anyhow::Result<()> {
        let now = Utc::now();
        let started = Instant::now();
        let writer = self.writer.as_mut().expect("open");
        let mut written = 0u64;
        for (symbol, mirror) in &self.books {
            if mirror.valid {
                write_snapshot(writer, symbol, &mirror.book, now)?;
                written += 1;
            }
        }
        self.messages_written += written;
        tracing::info!(
            snapshots = written,
            total_symbols = self.books.len(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "wrote periodic snapshots"
        );
        Ok(())
    }
}

fn write_snapshot(
    writer: &mut AlignedSbeWriter,
    symbol: &Symbol,
    book: &OrderBook,
    received_at: DateTime<Utc>,
) -> anyhow::Result<()> {
    let snapshot = SnapshotDomain {
        received_at,
        exchange_timestamp: book.exchange_ts.unwrap_or(received_at),
        bids: book
            .bids()
            .iter()
            .map(|l| SnapshotBidsEntryDomain { price_level: l.price, quantity: l.quantity })
            .collect(),
        asks: book
            .asks()
            .iter()
            .map(|l| SnapshotAsksEntryDomain { price_level: l.price, quantity: l.quantity })
            .collect(),
        symbol: symbol.to_string(),
    };
    writer.write_msg(&snapshot)
}

/// The next wall-clock multiple of `every` (e.g. the top of the next minute),
/// expressed as an [`Instant`].
fn next_boundary(every: Duration) -> Instant {
    let every_ms = every.as_millis().max(1) as i64;
    let now_ms = Utc::now().timestamp_millis();
    let wait_ms = every_ms - now_ms.rem_euclid(every_ms);
    Instant::now() + Duration::from_millis(wait_ms as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::{AlignedSbeReader, OrderbookSchema, dtos::challenge_orderbook::AnyMessage};

    #[test]
    fn writes_readable_file() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let (tx, rx) = crossbeam::channel::unbounded();
        let ts = DateTime::from_timestamp_millis(1_700_000_000_000).unwrap();
        tx.send(OutputMsg::Snapshot {
            symbol: "A".into(),
            received_at: ts,
            exchange_ts: ts,
            bids: vec![Level { price: 400_000, quantity: 1 }],
            asks: vec![Level { price: 600_000, quantity: 2 }],
        })
        .unwrap();
        tx.send(OutputMsg::Update {
            symbol: "A".into(),
            received_at: ts,
            exchange_ts: ts,
            side: Side::Sell,
            price: 600_000,
            quantity: 0,
        })
        .unwrap();
        // Dropped: no snapshot for B.
        tx.send(OutputMsg::Update {
            symbol: "B".into(),
            received_at: ts,
            exchange_ts: ts,
            side: Side::Buy,
            price: 1,
            quantity: 1,
        })
        .unwrap();
        drop(tx);
        OutputWriter::new(Destination::Path(file.path().to_owned()), Duration::from_secs(3600))
            .run(rx)
            .unwrap();

        let mut reader = AlignedSbeReader::<OrderbookSchema>::open(file.path()).unwrap();
        let Some(AnyMessage::Snapshot(s)) = reader.read_msg().unwrap() else { panic!() };
        let s = SnapshotDomain::try_from_decoder(s).unwrap();
        assert_eq!(s.symbol, "A");
        assert_eq!(s.asks.len(), 1);
        let Some(AnyMessage::PriceLevelUpdate(u)) = reader.read_msg().unwrap() else { panic!() };
        let u = PriceLevelUpdateDomain::try_from_decoder(u).unwrap();
        assert_eq!((u.side, u.quantity), (Side::Sell, 0));
        assert!(reader.read_msg().unwrap().is_none());
    }
}
