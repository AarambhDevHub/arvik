# ajaya-sse

[![Crates.io](https://img.shields.io/crates/v/ajaya-sse.svg)](https://crates.io/crates/ajaya-sse)
[![Docs.rs](https://docs.rs/ajaya-sse/badge.svg)](https://docs.rs/ajaya-sse)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](../LICENSE-MIT)

**Server-Sent Events (SSE) for the Ajaya web framework.**

This crate provides zero-allocation Server-Sent Events (SSE) streaming for Ajaya.

## Features (v0.5.1)

- `Sse<S>` response type wrapping any `Stream` of `Result<Event, E>`
- `Event` builder: `.data()`, `.id()`, `.event()`, `.retry()`, `.comment()`
- `.json_data<T: Serialize>()` helper to stream structured JSON payloads
- `KeepAlive` configuration to prevent proxy and load-balancer timeouts
- Full SSE spec compliance (newline normalization, null-byte stripping)
- Pre-allocated string rendering (zero allocations on the hot path)

## Quick Start

```rust,ignore
use ajaya_sse::{Event, KeepAlive, Sse};
use std::time::Duration;
use tokio_stream::StreamExt as _;

async fn counter() -> Sse<impl futures_util::Stream<Item = Result<Event, std::convert::Infallible>>> {
    let stream = tokio_stream::wrappers::IntervalStream::new(
        tokio::time::interval(Duration::from_secs(1))
    )
    .enumerate()
    .map(|(i, _)| {
        Ok(Event::default()
            .event("tick")
            .id(i.to_string())
            .data(i.to_string()))
    });

    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}
```

## Status

**v0.5.1** — Fully implemented.

## License

Licensed under either of [Apache License, Version 2.0](../LICENSE-APACHE) or [MIT License](../LICENSE-MIT) at your option.
