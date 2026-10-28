//! Extension 4 experiment harness: opens N Polymarket market-channel
//! connections subscribed to the *same* tokens and measures, for every
//! message, how far behind the first-arriving connection each one was.
//!
//! Connections can be pinned to specific resolved IPs and/or local source
//! ports so we can test whether those choices matter.
//!
//! ```text
//! latency_probe --connections 8 --tokens 200 --secs 120 [--pin-ips] [--source-port-base 40000]
//! ```

use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::{Duration, Instant},
};

use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::{net::TcpSocket, sync::mpsc};
use tokio_tungstenite::tungstenite::Message;

const HOST: &str = "ws-subscriptions-clob.polymarket.com";
const URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const WARMUP: Duration = Duration::from_secs(5);

#[derive(Parser)]
struct Args {
    #[arg(long, default_value_t = 8)]
    connections: usize,
    /// Number of most active tokens (by 24h volume) to subscribe to.
    #[arg(long, default_value_t = 200)]
    tokens: usize,
    #[arg(long, default_value_t = 120)]
    secs: u64,
    /// Round-robin connections across every resolved IP instead of letting
    /// the OS pick the first.
    #[arg(long)]
    pin_ips: bool,
    /// Bind connection i to local port base+i.
    #[arg(long)]
    source_port_base: Option<u16>,
    /// Disable TCP_NODELAY (to measure its effect).
    #[arg(long)]
    no_nodelay: bool,
}

#[derive(Deserialize)]
struct GammaMarket {
    #[serde(rename = "clobTokenIds")]
    clob_token_ids: Option<String>,
}

struct Arrival {
    conn: usize,
    key: String,
    at: Instant,
    /// Local receive time minus exchange timestamp, ms.
    one_way_ms: i64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let tokens = top_tokens(args.tokens).await?;
    // IPv4 only: IPv6 is often unroutable (it is from WSL).
    let ips: Vec<IpAddr> = tokio::net::lookup_host((HOST, 443)).await?.map(|a| a.ip()).filter(IpAddr::is_ipv4).collect();
    println!("resolved {HOST} -> {ips:?}");
    println!("subscribing {} tokens on {} connections for {}s", tokens.len(), args.connections, args.secs);

    let (tx, mut rx) = mpsc::unbounded_channel::<Arrival>();
    let tokens = Arc::new(tokens);
    let mut info = Vec::new();
    let mut alive = vec![false; args.connections];
    for i in 0..args.connections {
        let ip = if args.pin_ips { ips[i % ips.len()] } else { ips[0] };
        let port = args.source_port_base.map(|b| b + i as u16);
        let started = Instant::now();
        match connect(ip, port, !args.no_nodelay).await {
            Ok(ws) => {
                let handshake = started.elapsed();
                println!("conn {i}: ip={ip} local_port={port:?} handshake={handshake:?}");
                info.push((ip, port, handshake));
                alive[i] = true;
                tokio::spawn(pump(i, ws, tokens.clone(), tx.clone()));
            }
            Err(err) => {
                println!("conn {i}: ip={ip} failed: {err}");
                info.push((ip, port, Duration::ZERO));
            }
        }
    }
    drop(tx);

    // Connections open one after another; give the last one time to get its
    // snapshots before comparing, otherwise later connections look lossy.
    let measure_from = Instant::now() + WARMUP;
    let deadline = tokio::time::sleep(WARMUP + Duration::from_secs(args.secs));
    tokio::pin!(deadline);
    // key -> arrival time per connection
    let mut seen: HashMap<String, Vec<Option<Instant>>> = HashMap::new();
    let mut one_way: Vec<Vec<i64>> = vec![Vec::new(); args.connections];
    loop {
        tokio::select! {
            _ = &mut deadline => break,
            a = rx.recv() => {
                let Some(a) = a else { break };
                if a.at < measure_from {
                    continue;
                }
                let slot = seen.entry(a.key).or_insert_with(|| vec![None; args.connections]);
                slot[a.conn].get_or_insert(a.at);
                one_way[a.conn].push(a.one_way_ms);
            }
        }
    }

    // Per-connection delay behind the first arrival, over messages that every
    // connection received.
    let mut behind_us: Vec<Vec<u64>> = vec![Vec::new(); args.connections];
    let mut wins = vec![0usize; args.connections];
    let mut complete = 0usize;
    let mut missing = vec![0usize; args.connections];
    for arrivals in seen.values() {
        for (c, a) in arrivals.iter().enumerate() {
            if alive[c] && a.is_none() {
                missing[c] += 1;
            }
        }
        if arrivals.iter().zip(&alive).any(|(a, up)| *up && a.is_none()) {
            continue;
        }
        complete += 1;
        let first = arrivals.iter().flatten().min().copied().unwrap();
        for (c, a) in arrivals.iter().enumerate() {
            let Some(a) = a else { continue };
            let d = a.duration_since(first).as_micros() as u64;
            if d == 0 {
                wins[c] += 1;
            }
            behind_us[c].push(d);
        }
    }

    println!("\nmessages seen: {} (received by all connections: {complete})", seen.len());
    println!(
        "conn | ip              | port  | handshake | first% | behind p50 | p90     | p99     | max     | >250ms | recv-exch p50 | missing"
    );
    for c in 0..args.connections {
        if !alive[c] {
            println!("{c:4} | {:15} | connect failed", info[c].0);
            continue;
        }
        let b = &mut behind_us[c];
        let stalled = b.iter().filter(|&&d| d > 250_000).count();
        b.sort_unstable();
        let o = &mut one_way[c];
        o.sort_unstable();
        let pct = |v: &Vec<u64>, p: f64| if v.is_empty() { 0 } else { v[((v.len() - 1) as f64 * p) as usize] };
        let (ip, port, hs) = info[c];
        println!(
            "{c:4} | {ip:15} | {:5} | {:7.1}ms | {:5.1}% | {:8.2}ms | {:5.2}ms | {:5.2}ms | {:5.1}ms | {:5.1}% | {:11}ms | {}",
            port.map_or("-".into(), |p| p.to_string()),
            hs.as_secs_f64() * 1000.0,
            100.0 * wins[c] as f64 / complete.max(1) as f64,
            pct(b, 0.5) as f64 / 1000.0,
            pct(b, 0.9) as f64 / 1000.0,
            pct(b, 0.99) as f64 / 1000.0,
            b.last().copied().unwrap_or(0) as f64 / 1000.0,
            100.0 * stalled as f64 / b.len().max(1) as f64,
            if o.is_empty() { 0 } else { o[o.len() / 2] },
            missing[c],
        );
    }
    Ok(())
}

async fn top_tokens(n: usize) -> anyhow::Result<Vec<String>> {
    let client = reqwest::Client::new();
    let mut tokens = Vec::new();
    let mut offset = 0;
    while tokens.len() < n {
        let url = format!(
            "https://gamma-api.polymarket.com/markets?closed=false&active=true&order=volume24hr&ascending=false&limit=100&offset={offset}"
        );
        let markets: Vec<GammaMarket> = client.get(url).send().await?.json().await?;
        if markets.is_empty() {
            break;
        }
        for m in markets {
            if let Some(ids) = m.clob_token_ids.and_then(|s| serde_json::from_str::<Vec<String>>(&s).ok()) {
                tokens.extend(ids);
            }
        }
        offset += 100;
    }
    tokens.truncate(n);
    Ok(tokens)
}

type Ws = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect(ip: IpAddr, local_port: Option<u16>, nodelay: bool) -> anyhow::Result<Ws> {
    let socket = match ip {
        IpAddr::V4(_) => TcpSocket::new_v4()?,
        IpAddr::V6(_) => TcpSocket::new_v6()?,
    };
    if let Some(port) = local_port {
        socket.set_reuseaddr(true)?;
        let any: IpAddr = match ip {
            IpAddr::V4(_) => "0.0.0.0".parse()?,
            IpAddr::V6(_) => "::".parse()?,
        };
        socket.bind(SocketAddr::new(any, port))?;
    }
    let stream = socket.connect(SocketAddr::new(ip, 443)).await?;
    stream.set_nodelay(nodelay)?;
    let (ws, _) = tokio_tungstenite::client_async_tls(URL, stream).await?;
    Ok(ws)
}

#[derive(Deserialize)]
struct Wire {
    event_type: Option<String>,
    timestamp: Option<String>,
    #[serde(default)]
    price_changes: Vec<WireChange>,
}

#[derive(Deserialize)]
struct WireChange {
    hash: Option<String>,
}

async fn pump(conn: usize, ws: Ws, tokens: Arc<Vec<String>>, tx: mpsc::UnboundedSender<Arrival>) {
    let (mut sink, mut stream) = ws.split();
    let first = serde_json::json!({ "assets_ids": tokens.as_slice(), "type": "market" });
    if sink.send(Message::text(first.to_string())).await.is_err() {
        return;
    }
    let mut ping = tokio::time::interval(Duration::from_secs(10));
    loop {
        tokio::select! {
            _ = ping.tick() => { let _ = sink.send(Message::text("PING")).await; }
            msg = stream.next() => {
                let at = Instant::now();
                let now_ms = chrono::Utc::now().timestamp_millis();
                let Some(Ok(Message::Text(text))) = msg else {
                    if msg.is_none() { return; }
                    continue;
                };
                let Ok(wire) = serde_json::from_str::<Wire>(&text) else { continue };
                if wire.event_type.as_deref() != Some("price_change") {
                    continue;
                }
                // Each price_change frame is identified by its timestamp plus
                // the order-book hashes it carries.
                let ts = wire.timestamp.unwrap_or_default();
                let hashes: Vec<_> = wire.price_changes.iter().filter_map(|c| c.hash.as_deref()).collect();
                let key = format!("{ts}:{}", hashes.join(","));
                let one_way_ms = now_ms - ts.parse::<i64>().unwrap_or(now_ms);
                if tx.send(Arrival { conn, key, at, one_way_ms }).is_err() {
                    return;
                }
            }
        }
    }
}
