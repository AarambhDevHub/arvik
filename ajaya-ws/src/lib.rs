//! # ajaya-ws
//!
//! WebSocket support for the Ajaya web framework.
//!
//! ## Quick start
//!
//! ```rust,ignore
//! use ajaya::ws::{WebSocket, WebSocketUpgrade, Message};
//! use ajaya::{Router, get};
//!
//! async fn ws_echo(ws: WebSocketUpgrade) -> impl ajaya::IntoResponse {
//!     ws.on_upgrade(|mut socket| async move {
//!         while let Some(Ok(msg)) = socket.recv().await {
//!             match msg {
//!                 Message::Text(_) | Message::Binary(_) => {
//!                     socket.send(msg).await.ok();
//!                 }
//!                 _ => break,
//!             }
//!         }
//!     })
//! }
//!
//! let app = Router::new().route("/ws", get(ws_echo));
//! ```
//!
//! ## Concurrent send + receive
//!
//! ```rust,ignore
//! use ajaya::ws::{WebSocket, Message};
//!
//! async fn handle(socket: WebSocket) {
//!     let (mut sender, mut receiver) = socket.split();
//!
//!     let s = tokio::spawn(async move {
//!         sender.send(Message::Text("hello".into())).await.ok();
//!     });
//!
//!     while let Some(Ok(msg)) = receiver.next().await {
//!         println!("got: {msg:?}");
//!     }
//!     s.await.ok();
//! }
//! ```
//!
//! ## Configuration
//!
//! ```rust,ignore
//! ws.protocols(["chat", "json"])        // subprotocol negotiation
//!   .max_message_size(64 * 1024)        // reject oversized messages
//!   .max_frame_size(16 * 1024)
//!   .on_upgrade(handler)
//! ```

pub mod message;
pub mod rejection;
pub mod socket;
pub mod split;
pub mod upgrade;

// ── Public API re-exports ─────────────────────────────────────────────────────

pub use message::{CloseCode, CloseFrame, Message};
pub use rejection::WebSocketUpgradeRejection;
pub use socket::WebSocket;
pub use split::{Receiver, Sender, WsError};
pub use upgrade::{WebSocketConfig, WebSocketUpgrade};
