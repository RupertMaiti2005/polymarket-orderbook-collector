# Extension 4: latency experiments on Polymarket

## Setup

`src/bin/latency_probe.rs` opens N connections to
`wss://ws-subscriptions-clob.polymarket.com/ws/market`, all subscribed to the
same 300 most active tokens (by 24h volume). Every `price_change` frame is
identified by `timestamp + order hashes`. For each frame received by every
connection, the probe records how far each connection was **behind the first
connection to deliver it**. Connections can be pinned to a resolved IP
(`--pin-ips`), bound to a fixed local source port (`--source-port-base`), or run
without `TCP_NODELAY`. Measurement starts 5s after the last connection opens, so
staggered startup doesn't show up as loss.

`scripts/latency-experiments.sh` runs every experiment: 90s each, from a home
connection in Toronto through WSL2, on 2026-10-28 around 01:15 UTC.

Columns: `first%` is the share of frames this connection delivered first.
`behind` is the delay behind the first arrival. `>250ms` is the share of frames
delivered more than 250ms late.

## Findings

### 1. Exchange timestamps can't measure absolute latency

On every connection and run, the median `receive time − exchange timestamp`
was **negative, between −300 and −560 ms**. The WSL clock is NTP-synced to well
under a second, so Polymarket's `timestamp` field is not the time the message was
published. It is either stamped ahead of publication or comes from a skewed
clock. Consequences:

* Absolute one-way latency can't be measured against Polymarket timestamps.
  All comparisons below are relative, between connections receiving the same
  frame.
* The collector writes the exchange timestamp as-is and uses the local receive
  time as `backtest_timestamp`. Backtests should key off the receive time.
* Within one run, however, the offset is stable per connection, so the
  difference between connections is meaningful. The collector relies on that
  difference.

### 2. Which backend a connection lands on matters most

Every connection resolves to the same two Cloudflare anycast IPs, but they
clearly end up on different origin servers with very different behaviour. With
16 connections (run E):

| group | connections | behind p50 | behind p90 | >250ms | recv−exch p50 |
|---|---|---|---|---|---|
| fastest | #13 | 0.00ms (first 76% of the time) | 7ms | 0.0% | −430ms |
| normal | 8 others | 3–8ms | 8–20ms | 0.1–0.9% | −413 to −428ms |
| **slow** | #3, 8, 9, 11, 12, 14, 15 | 10–14ms | **580–870ms** | **16–22%** | **−301 to −336ms** |

Almost half the connections were on a "slow" path, regularly delivering frames
0.5–2s late. The other runs show the same pattern at smaller scale: run A
connection 7 had 9.2% of frames over 250ms late; run B connection 2 had 14.2%.
In almost every run a single connection won 45–76% of races.

The slow group also shows up as a roughly 100ms worse median
`recv − exchange`, which the collector can measure without any cross-connection
matching.

### 3. Subscriptions do go silent

In run D, connection 1 delivered only about 1,000 of 85,000 frames. The socket
stayed open, no error was reported, and data simply stopped flowing. This is
the silent failure mode from extension 2, observed on the first afternoon of
testing.

### 4. Things that made no measurable difference

* **Source port** (run C): binding fixed local ports gave the same spread as
  ephemeral ports. Any effect it has comes from landing on a different backend,
  the same as opening a fresh connection.
* **Pinning the second anycast IP** (run B): both IPs produced fast and slow
  connections. Neither IP was consistently better.
* **`TCP_NODELAY`** (run D): no effect, as expected. Nagle only delays *our*
  sends, and the hot path is receive-only. The collector sets it anyway, because
  it costs nothing and helps pings and resubscribes.
* **Handshake time** (~400–530ms from here) doesn't predict later speed. The
  winner in run E (#13, 405ms) and a slow one (#11, 410ms) had nearly identical
  handshakes. So "connect many times and keep the quickest handshake" doesn't
  work; you have to measure actual data delivery.

## What the collector does with this

1. **Redundancy plus a primary per symbol** (extension 1) absorbs the slow and
   silent connections. A primary that misses deltas a sibling has for more than
   2s is replaced with a snapshot from the sibling.
2. **Latency-preferring primary:** each shard keeps a per-connection EWMA of
   `recv − exchange` (the offset is shared, so the difference is real). After a
   5 min hold, it moves primaries to a connection that is consistently ≥50ms
   faster.
3. **Re-rolling slow connections:** a connection that stays ≥75ms (EWMA) slower
   than its best sibling for 3 minutes is recycled early, the same way as the
   periodic recycle (one at a time, siblings healthy, primaries moved off
   first). Given finding 2, that's roughly a coin flip each time to land on a
   good backend.

## Ideas not implemented

* **Connection spraying:** open, say, 4 candidates per shard, measure for a
  minute, keep the best 2, and keep periodically trialling new ones. The data
  says this would pay off, since the best of 16 was clearly better than the
  median. It costs proportionally more connections, and with about 524k tokens
  we already need about 2,100 sockets.
* **First-arrival merging:** emit each delta from whichever connection
  delivers it first instead of from one primary. Polymarket updates carry
  absolute sizes, but frames from different connections can interleave
  (A: size 5 then 7, B delivers its 5 late), so you need a per-symbol order that
  frames can be checked against. Millisecond timestamps with many frames per ms
  aren't enough. It's doable with per-frame hashes and an ordering window, but
  it risks correctness, which is the higher priority.
* **Colocation:** the endpoint sits behind Cloudflare, so we only see the
  anycast edge. Finding the origin region (for example by comparing receive
  lag from probes in several cloud regions) and running the collector there
  would probably beat any socket tuning, since a home connection adds tens of
  milliseconds before anything else.
* **Post-maintenance races:** I didn't get to observe a Polymarket maintenance
  window. The harness can be pointed at one by running it in a loop through the
  window.

## Reproducing

```sh
cargo build --release
SECS=90 bash scripts/latency-experiments.sh
./target/release/latency_probe --connections 16 --tokens 300 --secs 120
```
