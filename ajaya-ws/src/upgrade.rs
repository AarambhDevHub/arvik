//! WebSocket upgrade extractor.
//!
//! [`WebSocketUpgrade`] is a [`FromRequest`] extractor that validates the
//! WebSocket handshake, then returns a [`Response`] that initiates the
//! protocol switch. The actual WebSocket session runs in a spawned Tokio task.
//!
//! # Handshake
//!
//! The extractor validates:
//! - Method is `GET`
//! - `Connection: upgrade`
//! - `Upgrade: websocket`
//! - `Sec-WebSocket-Version: 13`
//! - `Sec-WebSocket-Key` present
//!
//! On success it computes `Sec-WebSocket-Accept` (SHA-1 + base64) and
//! returns a `101 Switching Protocols` response. Hyper then hands the raw
//! TCP stream to the WebSocket layer via its upgrade machinery.
//!
//! # Example
//!
//! ```rust,ignore
//! use ajaya::ws::{WebSocket, WebSocketUpgrade, Message};
//! use ajaya::{Router, get, State};
//!
//! async fn ws_handler(
//!     ws: WebSocketUpgrade,
//!     State(state): State<AppState>,
//! ) -> impl IntoResponse {
//!     ws.protocols(["chat", "json"])
//!       .max_message_size(64 * 1024)
//!       .on_upgrade(|socket| handle(socket, state))
//! }
//!
//! async fn handle(mut socket: WebSocket, state: AppState) {
//!     while let Some(Ok(msg)) = socket.recv().await {
//!         socket.send(msg).await.ok();
//!     }
//! }
//! ```

use std::future::Future;

use ajaya_core::Body;
use ajaya_core::extract::FromRequest;
use ajaya_core::request::Request;
use ajaya_core::response::Response;
use base64::Engine;
use http::{
    HeaderValue, StatusCode,
    header::{
        CONNECTION, SEC_WEBSOCKET_ACCEPT, SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_PROTOCOL,
        SEC_WEBSOCKET_VERSION, UPGRADE,
    },
};
use hyper_util::rt::TokioIo;
use sha1::{Digest, Sha1};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::protocol::Role;

use crate::rejection::WebSocketUpgradeRejection;
use crate::socket::WebSocket;

// ── Constants ────────────────────────────────────────────────────────────────

const GUID: &[u8] = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

// ── WebSocketConfig ───────────────────────────────────────────────────────────

/// Configuration for the WebSocket connection.
///
/// Apply via builder methods on [`WebSocketUpgrade`]:
///
/// ```rust,ignore
/// ws.max_message_size(64 * 1024)
///   .max_frame_size(16 * 1024)
///   .protocols(["chat"])
///   .on_upgrade(handler)
/// ```
#[derive(Debug, Clone)]
pub struct WebSocketConfig {
    /// Maximum message payload size in bytes.
    /// Default: 64 MiB (matches tungstenite default).
    pub max_message_size: Option<usize>,
    /// Maximum single frame size in bytes.
    /// Default: 16 MiB (matches tungstenite default).
    pub max_frame_size: Option<usize>,
    /// Accept frames from a client without masking (violates RFC, but useful
    /// for performance-sensitive internal services).
    /// Default: `false`.
    pub accept_unmasked_frames: bool,
    /// Accepted subprotocols in preference order.
    /// The first client-offered protocol that appears in this list is selected.
    pub protocols: Vec<String>,
    /// Size of the write buffer before flushing.
    /// Default: 128 KiB.
    pub write_buffer_size: usize,
    /// Maximum size of the write buffer (soft limit).
    /// Default: 512 KiB.
    pub max_write_buffer_size: usize,
}

impl Default for WebSocketConfig {
    fn default() -> Self {
        Self {
            max_message_size: Some(64 * 1024 * 1024),
            max_frame_size: Some(16 * 1024 * 1024),
            accept_unmasked_frames: false,
            protocols: Vec::new(),
            write_buffer_size: 128 * 1024,
            max_write_buffer_size: 512 * 1024,
        }
    }
}

impl WebSocketConfig {
    fn to_tungstenite(&self) -> tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
        tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
            max_message_size: self.max_message_size,
            max_frame_size: self.max_frame_size,
            accept_unmasked_frames: self.accept_unmasked_frames,
            write_buffer_size: self.write_buffer_size,
            max_write_buffer_size: self.max_write_buffer_size,
            ..Default::default()
        }
    }
}

// ── WebSocketUpgrade ──────────────────────────────────────────────────────────

/// WebSocket upgrade extractor.
///
/// Place as a handler parameter to accept WebSocket connections.
/// Must be used as the **last** parameter (it is a body extractor via
/// `FromRequest`).
///
/// Call [`on_upgrade`](Self::on_upgrade) to complete the handshake and spawn
/// the WebSocket handler task. The returned [`Response`] (101) must be
/// returned from the Ajaya handler.
pub struct WebSocketUpgrade {
    /// The hyper upgrade future — resolved after the 101 response is sent.
    on_upgrade: hyper::upgrade::OnUpgrade,
    /// The computed Sec-WebSocket-Accept value.
    accept_key: String,
    /// The negotiated subprotocol (picked from client's offer vs. our list).
    selected_protocol: Option<String>,
    /// All raw protocol options offered by the client.
    client_protocols: Vec<String>,
    /// Connection configuration.
    config: WebSocketConfig,
}

impl WebSocketUpgrade {
    // ── Configuration builder methods ─────────────────────────────────────────

    /// Set the maximum allowed message size in bytes.
    ///
    /// Connections sending larger messages are rejected with a close frame.
    /// Default: 64 MiB.
    #[must_use]
    pub fn max_message_size(mut self, size: usize) -> Self {
        self.config.max_message_size = Some(size);
        self
    }

    /// Set the maximum frame size in bytes. Default: 16 MiB.
    #[must_use]
    pub fn max_frame_size(mut self, size: usize) -> Self {
        self.config.max_frame_size = Some(size);
        self
    }

    /// Accept frames from unmasked clients (non-RFC; for internal services only).
    #[must_use]
    pub fn accept_unmasked_frames(mut self, accept: bool) -> Self {
        self.config.accept_unmasked_frames = accept;
        self
    }

    /// Set the accepted subprotocols in preference order.
    ///
    /// The first protocol from the client's `Sec-WebSocket-Protocol` header
    /// that matches an entry in `protocols` is selected and echoed back.
    ///
    /// ```rust,ignore
    /// ws.protocols(["chat", "json"])
    /// ```
    #[must_use]
    pub fn protocols<I, S>(mut self, protocols: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let our: Vec<String> = protocols.into_iter().map(|s| s.into()).collect();

        // Select the first matching protocol that the client also offered
        self.selected_protocol = self
            .client_protocols
            .iter()
            .find(|cp| our.iter().any(|op| op == *cp))
            .cloned();

        self.config.protocols = our;
        self
    }

    /// Return the negotiated subprotocol after [`protocols`](Self::protocols) is called.
    ///
    /// `None` if no subprotocol was negotiated.
    pub fn selected_protocol(&self) -> Option<&str> {
        self.selected_protocol.as_deref()
    }

    // ── Upgrade ───────────────────────────────────────────────────────────────

    /// Complete the WebSocket handshake and spawn `callback` in a Tokio task.
    ///
    /// Returns the `101 Switching Protocols` response that **must** be returned
    /// from your Ajaya handler.
    ///
    /// The `callback` receives a [`WebSocket`] once the underlying TCP
    /// connection has been upgraded. It runs in a detached `tokio::spawn` task.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// async fn ws_handler(ws: WebSocketUpgrade) -> impl IntoResponse {
    ///     ws.on_upgrade(|mut socket| async move {
    ///         while let Some(Ok(msg)) = socket.recv().await {
    ///             socket.send(msg).await.ok();
    ///         }
    ///     })
    /// }
    /// ```
    pub fn on_upgrade<F, Fut>(self, callback: F) -> Response
    where
        F: FnOnce(WebSocket) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let on_upgrade = self.on_upgrade;
        let config = self.config;
        let selected_protocol = self.selected_protocol.clone();

        // Spawn the WebSocket handler as a detached task.
        // The task waits for hyper to finish sending the 101 response, then
        // hands off the raw TCP stream to tungstenite.
        tokio::spawn(async move {
            let upgraded = match on_upgrade.await {
                Ok(u) => u,
                Err(e) => {
                    tracing::error!("WebSocket upgrade failed: {e}");
                    return;
                }
            };

            let io = TokioIo::new(upgraded);
            let tungstenite_config = config.to_tungstenite();

            let stream =
                WebSocketStream::from_raw_socket(io, Role::Server, Some(tungstenite_config)).await;

            let ws = WebSocket {
                inner: stream,
                protocol: selected_protocol,
            };

            tracing::debug!("WebSocket connection established");
            callback(ws).await;
            tracing::debug!("WebSocket connection closed");
        });

        // Build and return the 101 response immediately.
        // hyper processes this, sends it to the client, then resolves the
        // OnUpgrade future (which unblocks the spawned task above).
        let mut builder = http::Response::builder()
            .status(StatusCode::SWITCHING_PROTOCOLS)
            .header(CONNECTION, "upgrade")
            .header(UPGRADE, "websocket")
            .header(SEC_WEBSOCKET_ACCEPT, self.accept_key);

        if let Some(protocol) = &self.selected_protocol {
            builder = builder.header(
                SEC_WEBSOCKET_PROTOCOL,
                HeaderValue::from_str(protocol).unwrap_or_else(|_| HeaderValue::from_static("")),
            );
        }

        builder.body(Body::empty()).unwrap()
    }
}

impl std::fmt::Debug for WebSocketUpgrade {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebSocketUpgrade")
            .field("selected_protocol", &self.selected_protocol)
            .finish_non_exhaustive()
    }
}

// ── FromRequest impl ──────────────────────────────────────────────────────────

impl<S: Send + Sync> FromRequest<S> for WebSocketUpgrade {
    type Rejection = WebSocketUpgradeRejection;

    async fn from_request(req: Request, _state: &S) -> Result<Self, Self::Rejection> {
        // ── 1. Validate method ────────────────────────────────────────────────
        if req.method() != http::Method::GET {
            return Err(WebSocketUpgradeRejection::MethodNotGet);
        }

        // ── 2. Validate Connection: upgrade ───────────────────────────────────
        let connection_ok = req
            .headers()
            .get(CONNECTION)
            .and_then(|v| v.to_str().ok())
            .map(|v| {
                v.split(',')
                    .any(|t| t.trim().eq_ignore_ascii_case("upgrade"))
            })
            .unwrap_or(false);

        if !connection_ok {
            return Err(WebSocketUpgradeRejection::ConnectionNotUpgrade);
        }

        // ── 3. Validate Upgrade: websocket ────────────────────────────────────
        let upgrade_ok = req
            .headers()
            .get(UPGRADE)
            .and_then(|v| v.to_str().ok())
            .map(|v| v.trim().eq_ignore_ascii_case("websocket"))
            .unwrap_or(false);

        if !upgrade_ok {
            return Err(WebSocketUpgradeRejection::UpgradeNotWebSocket);
        }

        // ── 4. Validate Sec-WebSocket-Version: 13 ─────────────────────────────
        let version_ok = req
            .headers()
            .get(SEC_WEBSOCKET_VERSION)
            .and_then(|v| v.to_str().ok())
            .map(|v| v.trim() == "13")
            .unwrap_or(false);

        if !version_ok {
            return Err(WebSocketUpgradeRejection::InvalidWebSocketVersionHeader);
        }

        // ── 5. Extract Sec-WebSocket-Key ──────────────────────────────────────
        let sec_key = req
            .headers()
            .get(SEC_WEBSOCKET_KEY)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.trim().to_owned())
            .ok_or(WebSocketUpgradeRejection::MissingSecWebSocketKey)?;

        // Compute the accept key: SHA-1(key + GUID) → base64
        let accept_key = compute_accept_key(&sec_key);

        // ── 6. Parse Sec-WebSocket-Protocol (client offers) ───────────────────
        let client_protocols: Vec<String> = req
            .headers()
            .get(SEC_WEBSOCKET_PROTOCOL)
            .and_then(|v| v.to_str().ok())
            .map(|v| {
                v.split(',')
                    .map(|p| p.trim().to_owned())
                    .filter(|p| !p.is_empty())
                    .collect()
            })
            .unwrap_or_default();

        // ── 7. Extract OnUpgrade from inner hyper request ─────────────────────
        // We consume the Ajaya Request to get the inner http::Request<Body>.
        // hyper's server injects the `Pending` upgrade handle into the
        // http-level extensions — preserved through our from_hyper() conversion.
        let mut inner = req.into_inner();
        let on_upgrade = hyper::upgrade::on(&mut inner);

        // Verify the upgrade is actually available (hyper sets this up; plain
        // HTTP/2 requests or non-hyper servers won't have it).
        // OnUpgrade::none() resolves to Err immediately — we'll catch it later
        // in the spawned task rather than reject here, because we can't test it
        // cheaply without consuming it.

        Ok(WebSocketUpgrade {
            on_upgrade,
            accept_key,
            selected_protocol: None,
            client_protocols,
            config: WebSocketConfig::default(),
        })
    }
}

// ── Handshake helpers ─────────────────────────────────────────────────────────

/// Compute the `Sec-WebSocket-Accept` header value.
///
/// Algorithm: base64(SHA-1(client_key || GUID))
#[inline]
fn compute_accept_key(client_key: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(client_key.as_bytes());
    hasher.update(GUID);
    base64::engine::general_purpose::STANDARD.encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accept_key_rfc_example() {
        // RFC 6455 Section 1.2 test vector
        let key = "dGhlIHNhbXBsZSBub25jZQ==";
        let expected = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";
        assert_eq!(compute_accept_key(key), expected);
    }
}
