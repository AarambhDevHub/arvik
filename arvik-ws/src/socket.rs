//! The connected WebSocket handle.
//!
//! [`WebSocket`] wraps the underlying `tokio-tungstenite` stream and
//! provides a clean ergonomic API for sending, receiving, and splitting
//! the connection.
//!
//! # Auto ping/pong
//!
//! [`WebSocket::recv`] automatically responds to `Ping` frames with a
//! matching `Pong` and does **not** surface them to the caller. This keeps
//! connection-keepalive transparent to application code — no boilerplate needed.
//!
//! # Examples
//!
//! ```rust,ignore
//! use arvik::ws::{WebSocket, Message};
//!
//! async fn echo(mut socket: WebSocket) {
//!     while let Some(Ok(msg)) = socket.recv().await {
//!         match msg {
//!             Message::Text(_) | Message::Binary(_) => {
//!                 socket.send(msg).await.ok();
//!             }
//!             Message::Close(_) => break,
//!             _ => {}
//!         }
//!     }
//! }
//! ```

use futures_util::{SinkExt, StreamExt};
use hyper_util::rt::TokioIo;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite;

use crate::message::{CloseFrame, Message};
use crate::split::{Receiver, Sender, WsError};

/// The IO type used by the WebSocket stream.
pub(crate) type Io = TokioIo<hyper::upgrade::Upgraded>;

/// A connected WebSocket session.
///
/// Created by [`WebSocketUpgrade::on_upgrade`](crate::WebSocketUpgrade::on_upgrade)
/// and passed to your callback.
///
/// Use [`send`](Self::send) / [`recv`](Self::recv) for simple interactions, or
/// [`split`](Self::split) for concurrent bidirectional communication.
pub struct WebSocket {
    pub(crate) inner: WebSocketStream<Io>,
    /// The negotiated subprotocol, if any.
    pub(crate) protocol: Option<String>,
}

impl WebSocket {
    /// The negotiated WebSocket subprotocol (from `Sec-WebSocket-Protocol`), if any.
    pub fn protocol(&self) -> Option<&str> {
        self.protocol.as_deref()
    }

    /// Send a message to the remote peer.
    ///
    /// Flushes immediately after sending.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying connection is closed or if the
    /// message cannot be sent.
    #[inline]
    pub async fn send(&mut self, msg: impl Into<Message>) -> Result<(), WsError> {
        self.inner.send(msg.into().into()).await
    }

    /// Receive the next message from the remote peer.
    ///
    /// Returns `None` when the connection is closed gracefully.
    ///
    /// **Auto-pong**: `Ping` frames are handled transparently — this method
    /// sends a `Pong` response automatically and loops to the next message.
    /// Application code never sees raw `Ping` frames.
    ///
    /// **Close**: When the peer sends a close frame, this method returns
    /// `Some(Ok(Message::Close(_)))` so you can perform cleanup, then returns
    /// `None` on the next call.
    pub async fn recv(&mut self) -> Option<Result<Message, WsError>> {
        loop {
            match self.inner.next().await? {
                Ok(tungstenite::Message::Ping(data)) => {
                    // Auto-pong: reply immediately, continue loop
                    if let Err(e) = self.inner.send(tungstenite::Message::Pong(data)).await {
                        return Some(Err(e));
                    }
                }
                Ok(tungstenite::Message::Frame(_)) => {
                    // Raw frames are internal to tungstenite; skip
                    continue;
                }
                Ok(msg) => return Some(Ok(Message::from(msg))),
                Err(e) => return Some(Err(e)),
            }
        }
    }

    /// Close the WebSocket connection gracefully.
    ///
    /// Sends a close frame with the given frame (or a default normal close if
    /// `None`), then flushes.
    pub async fn close(mut self, frame: Option<CloseFrame>) -> Result<(), WsError> {
        self.inner.send(Message::Close(frame).into()).await?;
        SinkExt::flush(&mut self.inner).await
    }

    /// Split the socket into independent `Sender` and `Receiver` halves.
    ///
    /// Useful for concurrent bidirectional communication — spawn one task
    /// for sending and one for receiving.
    ///
    /// **Note**: The split `Receiver` does **not** auto-pong. Handle `Ping`
    /// frames manually via the `Sender` when using split mode.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let (mut sender, mut receiver) = socket.split();
    /// let send_task = tokio::spawn(async move {
    ///     sender.send(Message::Text("hi".into())).await.ok();
    /// });
    /// while let Some(Ok(msg)) = receiver.next().await {
    ///     println!("{msg:?}");
    /// }
    /// ```
    pub fn split(self) -> (Sender<Io>, Receiver<Io>) {
        let (sink, stream) = self.inner.split();
        (Sender { inner: sink }, Receiver { inner: stream })
    }

    /// Send a text message.
    ///
    /// Convenience wrapper around [`send`](Self::send).
    #[inline]
    pub async fn send_text(&mut self, text: impl Into<String>) -> Result<(), WsError> {
        self.send(Message::Text(text.into())).await
    }

    /// Send a binary message.
    ///
    /// Convenience wrapper around [`send`](Self::send).
    #[inline]
    pub async fn send_binary(&mut self, data: impl Into<Vec<u8>>) -> Result<(), WsError> {
        self.send(Message::Binary(data.into())).await
    }

    /// Send multiple messages, flushing once at the end.
    ///
    /// More efficient than calling `send` in a loop.
    pub async fn send_batch(
        &mut self,
        msgs: impl IntoIterator<Item = Message>,
    ) -> Result<(), WsError> {
        for msg in msgs {
            SinkExt::feed(&mut self.inner, msg.into()).await?;
        }
        SinkExt::flush(&mut self.inner).await
    }
}

impl std::fmt::Debug for WebSocket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebSocket")
            .field("protocol", &self.protocol)
            .finish_non_exhaustive()
    }
}
