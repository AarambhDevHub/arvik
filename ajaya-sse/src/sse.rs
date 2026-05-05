//! [`Sse`] response type and the internal streaming body.
//!
//! `Sse<S>` converts any `Stream<Item = Result<Event, E>>` into a proper
//! HTTP response with all mandatory SSE headers set, optionally wiring in a
//! [`KeepAlive`] timer.

use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use ajaya_core::Body;
use ajaya_core::body::BoxError;
use ajaya_core::into_response::IntoResponse;
use ajaya_core::response::{Response, ResponseBuilder};
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use http_body::Frame;
use tokio::time::Sleep;

use crate::event::Event;
use crate::keep_alive::KeepAlive;

// ── Public type ──────────────────────────────────────────────────────────────

/// Server-Sent Events response.
///
/// Wraps any `Stream<Item = Result<Event, E>>` and implements
/// [`IntoResponse`], automatically setting:
///
/// | Header | Value |
/// |---|---|
/// | `Content-Type` | `text/event-stream` |
/// | `Cache-Control` | `no-cache` |
/// | `X-Accel-Buffering` | `no` |
///
/// # Examples
///
/// **Infinite counter**:
///
/// ```rust,ignore
/// use ajaya::sse::{Event, KeepAlive, Sse};
/// use std::convert::Infallible;
/// use std::time::Duration;
/// use tokio_stream::StreamExt as _;
///
/// async fn counter() -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
///     let stream = tokio_stream::wrappers::IntervalStream::new(
///         tokio::time::interval(Duration::from_secs(1)),
///     )
///     .enumerate()
///     .map(|(i, _)| Ok(Event::default().id(i.to_string()).data(i.to_string())));
///     Sse::new(stream).keep_alive(KeepAlive::new())
/// }
/// ```
///
/// **JSON payloads via `.json_data()`**:
///
/// ```rust,ignore
/// use ajaya::sse::{Event, Sse};
/// use serde::Serialize;
/// use futures_util::stream;
///
/// #[derive(Serialize)]
/// struct Tick { count: u64 }
///
/// async fn handler() -> Sse<impl Stream<Item = Result<Event, serde_json::Error>>> {
///     let stream = stream::iter((0u64..).take(5))
///         .map(|i| Event::default().id(i.to_string()).json_data(&Tick { count: i }));
///     Sse::new(stream)
/// }
/// ```
#[must_use]
pub struct Sse<S> {
    stream: S,
    keep_alive: Option<KeepAlive>,
}

impl<S> Sse<S> {
    /// Create a new `Sse` response from a stream of events.
    pub fn new(stream: S) -> Self {
        Self {
            stream,
            keep_alive: None,
        }
    }

    /// Attach a keep-alive configuration.
    ///
    /// When the event stream is idle, the keep-alive timer fires and a
    /// comment frame is sent to prevent proxy / load-balancer timeouts.
    ///
    /// ```rust,ignore
    /// Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(10)))
    /// ```
    pub fn keep_alive(mut self, keep_alive: KeepAlive) -> Self {
        self.keep_alive = Some(keep_alive);
        self
    }
}

impl<S, E> IntoResponse for Sse<S>
where
    S: Stream<Item = Result<Event, E>> + Send + 'static,
    E: Into<BoxError>,
{
    fn into_response(self) -> Response {
        // Eagerly map Event → Bytes (serialises the SSE wire format).
        // Box the stream so SseBody is Unpin → compatible with Body::new().
        let byte_stream: Pin<Box<dyn Stream<Item = Result<Bytes, BoxError>> + Send + 'static>> =
            Box::pin(
                self.stream
                    .map(|r| r.map(|e| e.serialize()).map_err(Into::into)),
            );

        let body = SseBody {
            stream: byte_stream,
            keep_alive: self.keep_alive.map(|ka| KeepAliveState {
                sleep: Box::pin(tokio::time::sleep(ka.interval)),
                interval: ka.interval,
                comment_bytes: ka.comment_bytes,
            }),
        };

        ResponseBuilder::new()
            .header(http::header::CONTENT_TYPE, "text/event-stream")
            .header(http::header::CACHE_CONTROL, "no-cache")
            .header("x-accel-buffering", "no") // nginx: disable response buffering
            .body(Body::new(body))
    }
}

// ── Internal: keep-alive timer state ─────────────────────────────────────────

/// Holds the Tokio sleep future and the pre-serialised comment bytes.
/// All fields are `Unpin`:
///   - `Pin<Box<Sleep>>` is `Unpin` because `Box<T>` is always `Unpin`.
///   - `Duration` is `Unpin`.
///   - `Bytes` is `Unpin`.
struct KeepAliveState {
    sleep: Pin<Box<Sleep>>,
    interval: Duration,
    comment_bytes: Bytes,
}

// ── Internal: http_body::Body implementation ──────────────────────────────────

/// The streaming HTTP body produced by [`Sse::into_response`].
///
/// # Unpin guarantee
///
/// `SseBody` is `Unpin` because every field is `Unpin`:
/// - `Pin<Box<dyn Stream>>` — `Box<T>` is always `Unpin`.
/// - `Option<KeepAliveState>` — see `KeepAliveState` above.
///
/// This is required by [`Body::new`] which has an `Unpin` bound.
struct SseBody {
    /// Bytes-stream produced by mapping `Event::serialize()` over the user stream.
    stream: Pin<Box<dyn Stream<Item = Result<Bytes, BoxError>> + Send + 'static>>,
    /// Optional keep-alive state, `None` if the user didn't configure one.
    keep_alive: Option<KeepAliveState>,
}

impl http_body::Body for SseBody {
    type Data = Bytes;
    type Error = BoxError;

    /// Poll strategy:
    ///
    /// 1. Poll the event stream — if it yields, reset the keep-alive timer
    ///    and return the bytes immediately.
    /// 2. If the event stream is pending AND a keep-alive is configured,
    ///    poll the sleep future.  If it fires, reset it for the next tick
    ///    and return the pre-serialised comment bytes.
    /// 3. Otherwise return `Poll::Pending`.
    ///
    /// The sleep is reset on *every* real event so the keep-alive interval
    /// always measures time since the last actual event, not a fixed clock.
    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        // SAFETY: SseBody is Unpin — no projection needed.
        let this = self.get_mut();

        // ── Step 1: poll the event stream ─────────────────────────────────
        match this.stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(bytes))) => {
                // Reset the keep-alive sleep so it counts from now.
                if let Some(ka) = &mut this.keep_alive {
                    ka.sleep
                        .as_mut()
                        .reset(tokio::time::Instant::now() + ka.interval);
                }
                return Poll::Ready(Some(Ok(Frame::data(bytes))));
            }
            Poll::Ready(Some(Err(e))) => {
                return Poll::Ready(Some(Err(e)));
            }
            Poll::Ready(None) => {
                return Poll::Ready(None); // stream exhausted → close body
            }
            Poll::Pending => {
                // ── Step 2: poll the keep-alive timer ─────────────────────
                if let Some(ka) = &mut this.keep_alive {
                    if ka.sleep.as_mut().poll(cx).is_ready() {
                        // Arm the next tick before returning so we stay woken.
                        ka.sleep
                            .as_mut()
                            .reset(tokio::time::Instant::now() + ka.interval);
                        return Poll::Ready(Some(Ok(Frame::data(
                            ka.comment_bytes.clone(), // Bytes clone = ref-count bump only
                        ))));
                    }
                }
            }
        }

        // ── Step 3: nothing ready ──────────────────────────────────────────
        Poll::Pending
    }

    #[inline]
    fn is_end_stream(&self) -> bool {
        false // unknown until the stream says so
    }
}
