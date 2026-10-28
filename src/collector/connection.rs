//! One websocket connection. Owns the socket and the exchange codec, keeps its
//! desired subscription set across reconnects, and forwards decoded events to
//! its shard. It makes no decisions about data quality: the shard does that.

use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use rand::Rng;
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::{Message, protocol::CloseFrame, protocol::frame::coding::CloseCode};

use crate::exchange::{Codec, Exchange, MarketEvent, Symbol};

/// If nothing at all (data, pong, ping) arrives for this long the socket is
/// considered dead and reconnected.
const SILENCE_TIMEOUT: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

#[derive(Debug)]
pub enum ConnCmd {
    Subscribe(Vec<Symbol>),
    Unsubscribe(Vec<Symbol>),
    /// Unsubscribe then subscribe again, forcing a fresh snapshot.
    Resubscribe(Vec<Symbol>),
    /// Drop the socket and connect again (periodic recycling).
    Reconnect,
}

#[derive(Debug)]
pub enum ConnEvent {
    Connected { conn: usize },
    Disconnected { conn: usize },
    Data {
        conn: usize,
        /// Taken as soon as the frame is read off the socket, before parsing.
        received_at: DateTime<Utc>,
        events: Vec<MarketEvent>,
    },
}

pub struct Connection<E: Exchange> {
    pub exchange: Arc<E>,
    pub id: usize,
    pub label: String,
    pub cmd_rx: mpsc::UnboundedReceiver<ConnCmd>,
    pub events_tx: mpsc::UnboundedSender<ConnEvent>,
    pub shutdown: watch::Receiver<bool>,
    /// Delay before the very first connect, to stagger startup.
    pub initial_delay: Duration,
}

enum Exit {
    Shutdown,
    Reconnect,
}

impl<E: Exchange> Connection<E> {
    pub async fn run(mut self) {
        let mut subscribed: HashSet<Symbol> = HashSet::new();
        let mut backoff = Duration::from_millis(500);
        if !self.sleep_or_shutdown(self.initial_delay, &mut subscribed).await {
            return;
        }
        loop {
            let started = Instant::now();
            match self.session(&mut subscribed).await {
                Ok(Exit::Shutdown) => return,
                Ok(Exit::Reconnect) => {
                    tracing::info!(conn = %self.label, "recycling connection");
                    backoff = Duration::from_millis(500);
                    continue;
                }
                Err(err) => tracing::warn!(conn = %self.label, %err, "connection failed"),
            }
            if started.elapsed() > Duration::from_secs(60) {
                backoff = Duration::from_millis(500);
            }
            let jitter = rand::thread_rng().gen_range(0.5..1.5);
            if !self.sleep_or_shutdown(backoff.mul_f64(jitter), &mut subscribed).await {
                return;
            }
            backoff = (backoff * 2).min(MAX_BACKOFF);
        }
    }

    /// Sleeps while still applying subscription commands. Returns false on shutdown.
    async fn sleep_or_shutdown(&mut self, dur: Duration, subscribed: &mut HashSet<Symbol>) -> bool {
        let deadline = tokio::time::sleep(dur);
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                _ = &mut deadline => return true,
                _ = self.shutdown.changed() => return false,
                cmd = self.cmd_rx.recv() => match cmd {
                    Some(ConnCmd::Subscribe(s)) => subscribed.extend(s),
                    Some(ConnCmd::Unsubscribe(s)) => s.iter().for_each(|s| { subscribed.remove(s); }),
                    Some(ConnCmd::Resubscribe(_) | ConnCmd::Reconnect) => {}
                    None => return false,
                },
            }
        }
    }

    async fn session(&mut self, subscribed: &mut HashSet<Symbol>) -> anyhow::Result<Exit> {
        let request = self.exchange.ws_request()?;
        let (ws, _) = tokio::time::timeout(CONNECT_TIMEOUT, tokio_tungstenite::connect_async(request))
            .await
            .map_err(|_| anyhow::anyhow!("connect timed out"))??;
        if let tokio_tungstenite::MaybeTlsStream::Rustls(tls) = ws.get_ref() {
            tls.get_ref().0.set_nodelay(true).ok();
        }
        let (mut sink, mut stream) = ws.split();
        let mut codec = self.exchange.new_codec();

        let initial: Vec<Symbol> = subscribed.iter().cloned().collect();
        for msg in codec.on_connect(&initial) {
            sink.send(Message::text(msg)).await?;
        }
        tracing::info!(conn = %self.label, symbols = initial.len(), "connected");
        let _ = self.events_tx.send(ConnEvent::Connected { conn: self.id });

        let result = self.pump(&mut codec, subscribed, &mut sink, &mut stream).await;
        let _ = self.events_tx.send(ConnEvent::Disconnected { conn: self.id });
        if matches!(result, Ok(Exit::Shutdown)) {
            let close = Message::Close(Some(CloseFrame { code: CloseCode::Normal, reason: "shutdown".into() }));
            let _ = tokio::time::timeout(Duration::from_secs(2), async {
                sink.send(close).await?;
                // Wait for the server's close acknowledgement.
                while let Some(msg) = stream.next().await {
                    if matches!(msg?, Message::Close(_)) {
                        break;
                    }
                }
                anyhow::Ok(())
            })
            .await;
            tracing::debug!(conn = %self.label, "websocket closed");
        }
        result
    }

    async fn pump<S, R>(
        &mut self,
        codec: &mut E::Codec,
        subscribed: &mut HashSet<Symbol>,
        sink: &mut S,
        stream: &mut R,
    ) -> anyhow::Result<Exit>
    where
        S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
        R: futures_util::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
    {
        let mut ping = tokio::time::interval(self.exchange.ping_interval());
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last_frame = Instant::now();
        let mut silence_check = tokio::time::interval(Duration::from_secs(1));

        loop {
            tokio::select! {
                biased;
                _ = self.shutdown.changed() => return Ok(Exit::Shutdown),
                frame = stream.next() => {
                    let received_at = Utc::now();
                    last_frame = Instant::now();
                    let text = match frame {
                        None => anyhow::bail!("stream ended"),
                        Some(Err(e)) => return Err(e.into()),
                        Some(Ok(Message::Text(t))) => t,
                        Some(Ok(Message::Binary(b))) => match String::from_utf8(b.to_vec()) {
                            Ok(t) => t,
                            Err(_) => continue,
                        },
                        Some(Ok(Message::Close(frame))) => anyhow::bail!("server closed: {frame:?}"),
                        Some(Ok(_)) => continue, // ping/pong handled by tungstenite
                    };
                    let mut events = Vec::new();
                    if let Err(err) = codec.decode(&text, &mut events) {
                        let preview: String = text.chars().take(200).collect();
                        tracing::warn!(conn = %self.label, %err, preview, "decode failed");
                    }
                    if !events.is_empty() {
                        let _ = self.events_tx.send(ConnEvent::Data { conn: self.id, received_at, events });
                    }
                }
                cmd = self.cmd_rx.recv() => {
                    let Some(cmd) = cmd else { return Ok(Exit::Shutdown) };
                    let msgs = match cmd {
                        ConnCmd::Subscribe(symbols) => {
                            let new: Vec<_> = symbols.into_iter().filter(|s| subscribed.insert(s.clone())).collect();
                            codec.subscribe(&new)
                        }
                        ConnCmd::Unsubscribe(symbols) => {
                            let gone: Vec<_> = symbols.into_iter().filter(|s| subscribed.remove(s)).collect();
                            codec.unsubscribe(&gone)
                        }
                        ConnCmd::Resubscribe(symbols) => {
                            let live: Vec<_> = symbols.into_iter().filter(|s| subscribed.contains(s)).collect();
                            let mut msgs = codec.unsubscribe(&live);
                            msgs.extend(codec.subscribe(&live));
                            msgs
                        }
                        ConnCmd::Reconnect => return Ok(Exit::Reconnect),
                    };
                    for msg in msgs {
                        sink.send(Message::text(msg)).await?;
                    }
                }
                _ = ping.tick() => {
                    if let Some(text) = codec.ping() {
                        sink.send(Message::text(text)).await?;
                    } else {
                        sink.send(Message::Ping(Vec::new())).await?;
                    }
                }
                _ = silence_check.tick() => {
                    if last_frame.elapsed() > SILENCE_TIMEOUT {
                        anyhow::bail!("no frames for {SILENCE_TIMEOUT:?}");
                    }
                }
            }
        }
    }
}
