//! Order book collector. Binary SBE output goes to stdout (or rotating files);
//! logs go to stderr.

use std::{path::PathBuf, sync::Arc, time::Duration};

use clap::{Parser, ValueEnum};
use engineering_challenge::{
    collector::{self, CollectorConfig, output::Destination, shard::ShardConfig},
    exchange::{Exchange, kalshi::Kalshi, polymarket::Polymarket},
};
use tokio::sync::watch;

#[derive(Clone, Copy, ValueEnum)]
enum Venue {
    Polymarket,
    Kalshi,
}

#[derive(Parser)]
#[command(about = "Collects order book snapshots and deltas into SBE")]
struct Args {
    #[arg(long, value_enum, default_value = "polymarket")]
    exchange: Venue,

    /// Output file. Defaults to stdout. Ignored when --output-dir is set.
    #[arg(long, default_value = "/dev/stdout")]
    output: PathBuf,

    /// Write rotating files into this directory instead of a single output.
    #[arg(long)]
    output_dir: Option<PathBuf>,

    /// Rotate output files this often (seconds), with --output-dir.
    #[arg(long, default_value_t = 3600)]
    rotate_secs: u64,

    /// Full snapshot of every book this often (seconds).
    #[arg(long, default_value_t = 60)]
    snapshot_secs: u64,

    /// Symbols per websocket connection.
    #[arg(long, default_value_t = 500)]
    symbols_per_connection: usize,

    /// Redundant websockets per group of symbols.
    #[arg(long, default_value_t = 2)]
    redundancy: usize,

    /// Recycle every connection roughly this often (seconds); 0 disables.
    #[arg(long, default_value_t = 1800)]
    recycle_secs: u64,

    /// Only collect the first N discovered symbols (testing).
    #[arg(long)]
    max_symbols: Option<usize>,

    /// Full symbol rescan interval (seconds).
    #[arg(long, default_value_t = 1800)]
    full_scan_secs: u64,

    /// Poll for newly listed symbols this often (seconds).
    #[arg(long, default_value_t = 30)]
    new_symbol_secs: u64,

    /// Milliseconds between opening consecutive groups of connections.
    #[arg(long, default_value_t = 100)]
    open_interval_ms: u64,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();
    tokio::runtime::Builder::new_multi_thread().enable_all().build()?.block_on(async_main(args))
}

async fn async_main(args: Args) -> anyhow::Result<()> {
    let config = CollectorConfig {
        shard: ShardConfig {
            redundancy: args.redundancy.max(1),
            recycle_every: (args.recycle_secs > 0).then(|| Duration::from_secs(args.recycle_secs)),
            ..Default::default()
        },
        full_scan_every: Duration::from_secs(args.full_scan_secs),
        new_symbol_poll_every: Duration::from_secs(args.new_symbol_secs),
        snapshot_every: Duration::from_secs(args.snapshot_secs),
        max_symbols: args.max_symbols,
        shard_open_interval: Duration::from_millis(args.open_interval_ms),
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        wait_for_signal().await;
        tracing::info!("signal received; shutting down");
        let _ = shutdown_tx.send(true);
        // A second signal forces exit.
        wait_for_signal().await;
        std::process::exit(130);
    });

    match args.exchange {
        Venue::Polymarket => {
            let exchange = Polymarket::new(args.symbols_per_connection)?;
            start(exchange, config, destination(&args), shutdown_rx).await
        }
        Venue::Kalshi => {
            let exchange = Kalshi::from_env(args.symbols_per_connection)?;
            start(exchange, config, destination(&args), shutdown_rx).await
        }
    }
}

async fn start<E: Exchange>(
    exchange: E,
    config: CollectorConfig,
    destination: Destination,
    shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    tracing::info!(exchange = exchange.name(), "starting collector");
    collector::run(Arc::new(exchange), config, destination, shutdown).await
}

fn destination(args: &Args) -> Destination {
    match &args.output_dir {
        Some(dir) => Destination::Rotating {
            dir: dir.clone(),
            prefix: match args.exchange {
                Venue::Polymarket => "polymarket".into(),
                Venue::Kalshi => "kalshi".into(),
            },
            every: Duration::from_secs(args.rotate_secs),
        },
        None => Destination::Path(args.output.clone()),
    }
}

async fn wait_for_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}
