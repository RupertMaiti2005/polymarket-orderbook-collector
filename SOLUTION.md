# Polymarket order book collector

Connects to Polymarket's CLOB market channel, subscribes to every live order
book token, keeps a book per token, and writes SBE output with the provided
helpers (`AlignedSbeWriter`, `SnapshotDomain`, `PriceLevelUpdateDomain`):

* a `PriceLevelUpdate` for every level change, as it happens
* a `Snapshot` for every book at the top of every minute
* a `Snapshot` whenever a book's source changes (first book, failover, resubscribe)

Polymarket was used instead of Kalshi because its market data websocket needs no
account or API key. A Kalshi codec is included to show the exchange abstraction
(extension 3), but it has only been unit-tested against documented message
shapes. It has not been run live.

## Running

The template's reader uses `std::os::unix::fs::FileExt`, so build on Linux, macOS
or WSL:

```sh
cargo build --release
# binary SBE to stdout, logs to stderr
./target/release/engineering-challenge > out.sbe
# or rotating hourly files
./target/release/engineering-challenge --output-dir data/
# small test run
./target/release/engineering-challenge --max-symbols 2000 > out.sbe
# decode / sanity-check a file
./target/release/inspect out.sbe [--all]
```

`Ctrl-C` (SIGINT) or SIGTERM triggers a clean shutdown: every websocket sends a
close frame and waits briefly for the server's close, then the writer flushes its
last chunk and closes the file. A second signal forces an exit. From Windows,
`scripts/wsl-cargo.sh` runs cargo inside WSL.

Useful flags (`--help` lists them all): `--redundancy` (default 2),
`--symbols-per-connection` (500), `--recycle-secs` (1800, 0 disables),
`--snapshot-secs` (60), `--full-scan-secs` (1800), `--new-symbol-secs` (30).

## Architecture

```
            discovery (REST)                 exchange-specific
     full scan (streamed) + "new" poll      ┌──────────────────┐
                    │                       │ Exchange + Codec │  polymarket.rs / kalshi.rs
                    ▼                       └────────▲─────────┘
               Manager ── assigns symbols ──► Shard 0 ──┬── Connection 0 ─ ws ─┐
                                          ─► Shard 1    └── Connection 1 ─ ws ─┤ same symbols
                                          ─► ...                               │
                                                │  OutputMsg (crossbeam)       ▼
                                                └──────────► Output writer thread ─► SBE file/stdout
```

* `exchange/`: the `Exchange` trait (discovery, handshake request, limits) and
  the `Codec` trait (subscribe/unsubscribe/ping encoding, wire decoding into
  normalised `MarketEvent`s). Only these two files change per venue.
* `collector/connection.rs`: one websocket. It reconnects with jittered
  exponential backoff, sends app-level pings, treats 30s of silence as a dead
  socket, and keeps its subscription set across reconnects. The receive
  timestamp is taken as soon as a frame comes off the socket, before parsing.
* `collector/shard.rs`: owns up to 500 symbols and `redundancy` connections.
  It holds a book per (symbol, connection) and decides which one is the
  output source. See below.
* `collector/output.rs`: a dedicated OS thread that owns the
  `AlignedSbeWriter`. It keeps a mirror of every book **as emitted**, so the
  per-minute snapshots match exactly what a consumer gets by replaying the
  deltas. Each rotated file also starts with snapshots, so every file can be
  replayed on its own.
* `collector/book.rs`: a sorted-`Vec` price ladder. There are about 3 copies ×
  500k books, so it is kept compact. Book replacement produces level diffs.
* Prices and sizes are parsed from decimal strings straight into fixed point
  (`fixed.rs`), never through `f64`.

### Discovery and load balancing

Polymarket has about 262k open order-book markets (about 524k tokens). A full
Gamma scan takes 20s–7min depending on caching, so pages are streamed to the
manager and subscribed as they arrive. Between full scans, `/markets?order=id`
is polled every 30s for new listings. Symbols missing from a completed full scan,
or reported `market_resolved` on the websocket, are unsubscribed. If a scan comes
back less than half the size of the previous one, removals are skipped, because a
truncated API response is far more likely than half the venue delisting.

New symbols fill the first shard with spare capacity, so slots freed by
delistings get reused. New shards open on a staggered schedule
(`--open-interval-ms`) so a cold start doesn't flood the venue with handshakes.

## Extension 1: redundant websockets

Every shard runs N connections subscribed to identical symbols, each with its own
book per symbol (a "feed"). For each symbol, one feed is the **primary**, and only
the primary's changes reach the output. The primary is replaced, and a snapshot
from the new feed is emitted, when:

* its connection drops (all of that connection's feeds fail over at once),
* the codec reports the feed invalid (for example a Kalshi sequence gap),
* it has been **behind** a sibling for longer than `max_source_lag` (2s),
  meaning a sibling has applied a delta with a newer exchange timestamp than
  anything this feed has.

A feed's "behind" check compares its *watermark* (max of snapshot time and
applied deltas) with siblings' newest *delta* timestamp. Snapshot times are
excluded on purpose: the snapshot that follows a resubscribe is newer than
anything a sibling has, without the sibling having missed anything. Without
this rule, resubscribes made connections bounce the primary back and forth.

If no feed has a book, the writer gets `Invalidate` and stops writing periodic
snapshots for that symbol. A stale book is never presented as current. The next
book from any connection is written as a fresh snapshot.

## Extension 2: subscription monitoring

Each shard runs a 1-second health tick:

* **Stalled feed:** behind a sibling, or subscribed but no snapshot, for 10s →
  unsubscribe and resubscribe on that connection only. Backoff is exponential
  per feed, capped at 1h.
* **Quiet symbol:** silent on *every* connection for longer than
  `clamp(20 × its usual message gap, 30min, 6h)` → resubscribe on one connection
  at a time, rotating, with exponential backoff. Truly dead markets (most of
  Polymarket) aren't hammered, while a busy market that stops is caught quickly.
* **Periodic recycling:** each connection is closed and reopened about every
  30 min (±20% jitter). Only one connection per shard recycles at a time, and
  only while all its siblings have been up for over 60s. Primaries are moved
  off the connection *before* it closes, so the output sees a clean source switch
  with no gap.

## Extension 3: generic encode/decode

The collector only depends on the `Exchange` and `Codec` traits. `Codec` handles
both absolute updates (`SetLevel`, Polymarket) and relative updates
(`AddLevel`, Kalshi), plus `Invalidate`, `Closed` and `Activity` events, and
holds per-connection state (Kalshi `sid`/`seq` tracking). `--exchange kalshi`
runs the same collector with the Kalshi codec, given `KALSHI_API_KEY_ID` and
`KALSHI_PRIVATE_KEY_PATH`; the handshake is RSA-PSS signed.

## Extension 4: latency

See [LATENCY.md](LATENCY.md). The shard also tracks a per-connection average of
receive time minus exchange time. It moves primaries to a connection that is
consistently at least 50ms faster, at most once per 5 minutes per symbol. (With a 25ms margin and a 60s hold, a 7-minute 10k-symbol run made about 12.6k latency switches as shard-wide primaries flapped; the wider hysteresis stops that.)

## Testing

`cargo test` covers decimal parsing, book diffs, both codecs, the writer
round-trip through `AlignedSbeReader`, and the shard's failover state machine:
first book, primary-only emission, disconnect failover, total loss then
recovery, lag-based switching, and invalidate → resubscribe.

Live run (2,000 symbols, 4 shards × 2 connections, recycling every 60s, 2.5 min,
SIGINT):

```
snapshots:            9192
price level updates:  83636
symbols:              2000
updates w/o snapshot: 0
```
Shutdown took 0.36s: all 8 sockets closed, then the file was flushed and closed.

## Known limitations

* `AlignedSbeReader::open` reads with a fixed 64 MiB chunk, so the writer must
  use the same chunk size. That means data reaches disk in 64 MiB chunks, plus
  a final padded chunk on shutdown or rotation; a hard crash can lose up to one
  chunk. In production I'd make the reader's chunk size configurable and flush
  smaller chunks every second or so.
* Output from different shards is interleaved in arrival order. Receive
  timestamps can therefore go slightly backwards between *different* symbols,
  but never within one symbol.
* Collecting all ~524k tokens needs about 1,050 shards (about 2,100 sockets at
  redundancy 2). The largest run tested here was 50k symbols (100 shards,
  200 sockets, 3 min). All 200 sockets stayed connected, with no rate limiting
  seen. One server-initiated close ("server shutting down") failed over and
  reconnected cleanly. Output was 1.28M updates and 220k snapshots with 0
  updates missing a snapshot, and writing periodic snapshots of 50k books took
  under 200ms. For the first ~60s, while 50k subscriptions were being served,
  25–48% of updates arrived more than 1s late; after that, 0%. The full
  ~2,100-socket configuration has not been tested from one IP.
