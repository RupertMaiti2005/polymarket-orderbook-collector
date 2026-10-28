//! Prints raw messages from the Polymarket market channel for a few busy tokens.
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let tokens: Vec<String> = std::env::args().skip(1).collect();
    let (mut ws, _) =
        tokio_tungstenite::connect_async("wss://ws-subscriptions-clob.polymarket.com/ws/market")
            .await?;
    let sub = serde_json::json!({"assets_ids": tokens, "type": "market", "custom_feature_enabled": true});
    ws.send(Message::text(sub.to_string())).await?;
    let start = std::time::Instant::now();
    let mut n = 0;
    while let Some(msg) = ws.next().await {
        let msg = msg?;
        if let Message::Text(t) = msg {
            n += 1;
            let s = t.as_str();
            println!("[{:?}] {}", start.elapsed(), &s[..s.len().min(700)]);
        } else {
            println!("[{:?}] {:?}", start.elapsed(), msg);
        }
        if n > 12 || start.elapsed().as_secs() > 25 {
            break;
        }
    }
    Ok(())
}
