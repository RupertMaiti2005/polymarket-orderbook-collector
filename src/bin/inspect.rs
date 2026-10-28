//! Reads an SBE file written by the collector and prints a summary, or every
//! message with `--all`. Also flags deltas for symbols that were never
//! snapshotted, which would make the stream unreplayable.

use std::collections::HashMap;

use engineering_challenge::util::{
    AlignedSbeReader, OrderbookSchema,
    dtos::challenge_orderbook::{AnyMessage, PriceLevelUpdateDomain, SnapshotDomain},
};

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let path = args.next().ok_or_else(|| anyhow::anyhow!("usage: inspect <file> [--all]"))?;
    let print_all = args.next().as_deref() == Some("--all");

    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&path)?;
    let (mut snapshots, mut updates, mut orphan_updates) = (0u64, 0u64, 0u64);
    // symbol -> (snapshots, updates)
    let mut symbols: HashMap<String, (u64, u64)> = HashMap::new();
    let mut first_ts = None;
    let mut last_ts = None;
    let mut latencies_ms = Vec::new();

    while let Some(msg) = reader.read_msg()? {
        match msg {
            AnyMessage::Snapshot(decoder) => {
                let s = SnapshotDomain::try_from_decoder(decoder)?;
                snapshots += 1;
                symbols.entry(s.symbol.clone()).or_default().0 += 1;
                first_ts.get_or_insert(s.received_at);
                if print_all {
                    println!(
                        "SNAP {} recv={} exch={} bids={} asks={} best_bid={:?} best_ask={:?}",
                        s.symbol,
                        s.received_at,
                        s.exchange_timestamp,
                        s.bids.len(),
                        s.asks.len(),
                        s.bids.first().map(|l| (l.price_level, l.quantity)),
                        s.asks.first().map(|l| (l.price_level, l.quantity)),
                    );
                }
            }
            AnyMessage::PriceLevelUpdate(decoder) => {
                let u = PriceLevelUpdateDomain::try_from_decoder(decoder)?;
                updates += 1;
                let entry = symbols.entry(u.symbol.clone()).or_default();
                if entry.0 == 0 {
                    orphan_updates += 1;
                }
                entry.1 += 1;
                first_ts.get_or_insert(u.backtest_timestamp);
                last_ts = Some(u.backtest_timestamp);
                let latency = (u.backtest_timestamp - u.exchange_timestamp).num_milliseconds();
                latencies_ms.push(latency);
                if print_all {
                    println!(
                        "UPD  {} recv={} exch={} {} {} -> {} (recv-exch {latency}ms)",
                        u.symbol, u.backtest_timestamp, u.exchange_timestamp, u.side, u.price_level, u.quantity,
                    );
                }
            }
            _ => anyhow::bail!("unknown message"),
        }
    }
    println!("snapshots:            {snapshots}");
    println!("price level updates:  {updates}");
    println!("symbols:              {}", symbols.len());
    println!("symbols with updates: {}", symbols.values().filter(|(_, u)| *u > 0).count());
    println!("updates w/o snapshot: {orphan_updates}");
    if !latencies_ms.is_empty() {
        latencies_ms.sort_unstable();
        let pct = |p: f64| latencies_ms[((latencies_ms.len() - 1) as f64 * p) as usize];
        println!(
            "recv - exchange ts (ms): p50={} p90={} p99={} max={}",
            pct(0.5),
            pct(0.9),
            pct(0.99),
            latencies_ms[latencies_ms.len() - 1]
        );
    }
    if let (Some(a), Some(b)) = (first_ts, last_ts) {
        println!("time span:            {a} .. {b}");
    }
    Ok(())
}
