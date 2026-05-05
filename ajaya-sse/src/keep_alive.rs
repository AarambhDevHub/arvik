//! Keep-alive configuration for SSE connections.
//!
//! Proxies and load balancers (nginx, AWS ALB, Cloudflare Workers, etc.)
//! often close idle connections after 30–60 seconds.  [`KeepAlive`] solves
//! this by injecting a comment frame (`: ping\n\n`) at a configurable
//! interval whenever the event stream is quiet.
//!
//! # Example
//!
//! ```rust,ignore
//! use ajaya::sse::{Sse, KeepAlive};
//! use std::time::Duration;
//!
//! async fn handler() -> Sse<impl Stream<...>> {
//!     Sse::new(my_stream)
//!         .keep_alive(
//!             KeepAlive::new()
//!                 .interval(Duration::from_secs(15))
//!                 .text("ping"),
//!         )
//! }
//! ```

use std::time::Duration;

use bytes::Bytes;

use crate::event::Event;

/// Periodic keep-alive comment sender for SSE connections.
///
/// See the [module-level docs](self) for background and examples.
#[must_use]
#[derive(Debug, Clone)]
pub struct KeepAlive {
    /// How often to send the comment frame when the stream is idle.
    pub(crate) interval: Duration,
    /// Pre-serialized comment bytes sent on each tick.
    pub(crate) comment_bytes: Bytes,
}

impl Default for KeepAlive {
    /// 15-second interval, empty comment (`: \n\n`).
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(15),
            comment_bytes: Bytes::from_static(b": \n\n"),
        }
    }
}

impl KeepAlive {
    /// Create a `KeepAlive` with a 15-second interval and no comment text.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the idle interval between keep-alive frames.
    ///
    /// Choose a value shorter than your proxy's idle-connection timeout
    /// (typically 30–60 s). 15 s is a safe default.
    pub fn interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Set the comment text sent in each keep-alive frame.
    ///
    /// The text is serialised as `: <text>\n\n`.
    /// An empty string (the default) produces `: \n\n`.
    pub fn text(mut self, text: impl AsRef<str>) -> Self {
        // Pre-serialise once so the hot path never allocates.
        self.comment_bytes = Event::default().comment(text.as_ref()).serialize();
        self
    }
}
