//! # arvik-sse
//!
//! Server-Sent Events (SSE) for the Arvik web framework.
//!
//! ## Quick start
//!
//! ```rust,ignore
//! use arvik::sse::{Event, KeepAlive, Sse};
//! use futures_util::stream;
//! use std::{convert::Infallible, time::Duration};
//!
//! // Return a finite stream of events
//! async fn events() -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
//!     let s = stream::iter(vec![
//!         Ok(Event::default().data("hello").id("1")),
//!         Ok(Event::default().data("world").id("2")),
//!     ]);
//!     Sse::new(s).keep_alive(KeepAlive::new())
//! }
//! ```
//!
//! ## Infinite clock stream
//!
//! ```rust,ignore
//! use arvik::sse::{Event, KeepAlive, Sse};
//! use std::{convert::Infallible, time::Duration};
//! use tokio_stream::StreamExt as _;
//!
//! async fn clock() -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
//!     let stream = tokio_stream::wrappers::IntervalStream::new(
//!         tokio::time::interval(Duration::from_secs(1)),
//!     )
//!     .map(|_| {
//!         Ok(Event::default()
//!             .event("tick")
//!             .data(chrono::Utc::now().to_rfc3339()))
//!     });
//!
//!     Sse::new(stream)
//!         .keep_alive(KeepAlive::new().interval(Duration::from_secs(10)))
//! }
//! ```

pub mod event;
pub mod keep_alive;
pub mod sse;

pub use event::Event;
pub use keep_alive::KeepAlive;
pub use sse::Sse;
