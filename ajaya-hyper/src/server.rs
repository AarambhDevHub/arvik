//! Server implementation using Hyper 1.x, Tokio, and socket2.
//!
//! ## Performance Architecture
//!
//! The standard approach of binding one `TcpListener` and accepting in a
//! loop creates a single kernel accept queue that all Tokio worker threads
//! compete over via a shared lock. On multi-core machines this becomes the
//! bottleneck before the application code ever runs.
//!
//! `serve_app_multi` (the high-performance entry point) eliminates this by:
//!
//! 1. **`SO_REUSEPORT`** — binds N independent sockets all on the same
//!    `(addr, port)`. The kernel distributes incoming connections across
//!    them using a hash of the client's source IP+port, so no single socket
//!    becomes a bottleneck.
//!
//! 2. **One accept-loop per CPU** — each Tokio task owns one of the N
//!    sockets. There is no shared mutex; each task runs accept → handle
//!    completely independently.
//!
//! 3. **`TCP_NODELAY`** — disables Nagle's algorithm. Individual small
//!    HTTP responses are flushed immediately instead of being buffered for
//!    40 ms, halving round-trip latency for short payloads.
//!
//! 4. **Large backlog** — the kernel SYN queue is set to 4096 so burst
//!    traffic does not shed connections before your handler even runs.
//!
//! 5. **`SO_REUSEADDR`** — lets the process restart without waiting for
//!    `TIME_WAIT` sockets to expire.
//!
//! Together these changes mirror the socket strategy used by Actix-web and
//! nginx, enabling linear throughput scaling across all available cores.

use std::net::SocketAddr;
use std::sync::Arc;

use ajaya_core::Body;
use ajaya_core::handler::Handler;
use ajaya_router::layer::BoxCloneService;
use ajaya_router::{MethodRouter, Router};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::TcpListener;
use tower_service::Service as _;

// ── Server (single listener) ──────────────────────────────────────────────────

/// The Ajaya HTTP server (single listener).
///
/// For maximum performance use [`serve_app_multi`] which activates
/// `SO_REUSEPORT` and spawns one accept loop per CPU core.
///
/// `Server` is kept for development, tests, and single-core use cases.
pub struct Server {
    listener: TcpListener,
    addr: SocketAddr,
}

impl Server {
    /// Bind to `addr` using a plain `TcpListener`.
    ///
    /// For production workloads prefer [`serve_app_multi`] which binds N
    /// sockets with `SO_REUSEPORT` instead of this single listener.
    pub async fn bind(addr: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind(addr).await?;
        listener.set_ttl(64)?;
        let addr = listener.local_addr()?;
        tracing::info!("🔱 Ajaya listening on http://{}", addr);
        Ok(Self { listener, addr })
    }

    /// Returns the local address the server is bound to.
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Serve any pre-built Tower [`BoxCloneService`] (single listener).
    pub async fn serve_service(
        self,
        service: BoxCloneService,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        accept_loop(self.listener, service).await;
        Ok(())
    }

    /// Serve a [`Router`] with all configured layers applied (single listener).
    pub async fn serve_app(
        self,
        router: Router,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let svc = router.into_service();
        self.serve_service(svc).await
    }

    /// Serve a bare async handler — no routing, no layers (single listener).
    pub async fn serve<H, T>(
        self,
        handler: H,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        H: Handler<T> + Clone + Send + Sync + 'static,
        T: 'static,
    {
        let listener = self.listener;
        loop {
            let (stream, peer_addr) = listener.accept().await?;
            let io = TokioIo::new(stream);
            let handler = handler.clone();

            tracing::debug!("Accepted connection from {}", peer_addr);

            tokio::task::spawn(async move {
                let hyper_svc = service_fn(move |req: hyper::Request<Incoming>| {
                    let handler = handler.clone();
                    async move {
                        let ajaya_req = ajaya_core::Request::from_hyper(req);
                        let response = handler.call(ajaya_req, ()).await;
                        Ok::<http::Response<Body>, hyper::Error>(response)
                    }
                });

                if let Err(err) = Builder::new(TokioExecutor::new())
                    .serve_connection_with_upgrades(io, hyper_svc)
                    .await
                {
                    if is_normal_close(&err.to_string()) {
                        tracing::debug!("Connection closed normally ({})", peer_addr);
                    } else {
                        tracing::warn!("Connection error ({}): {}", peer_addr, err);
                    }
                }
            });
        }
    }

    /// Serve a [`MethodRouter`] directly (single listener).
    pub async fn serve_method_router(
        self,
        router: MethodRouter,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let router = Arc::new(router);
        let listener = self.listener;

        loop {
            let (stream, peer_addr) = listener.accept().await?;
            let io = TokioIo::new(stream);
            let router = Arc::clone(&router);

            tracing::debug!("Accepted connection from {}", peer_addr);

            tokio::task::spawn(async move {
                let router = Arc::clone(&router);
                let hyper_svc = service_fn(move |req: hyper::Request<Incoming>| {
                    let router = Arc::clone(&router);
                    async move {
                        let ajaya_req = ajaya_core::Request::from_hyper(req);
                        let response = router.call(ajaya_req, ()).await;
                        Ok::<http::Response<Body>, hyper::Error>(response)
                    }
                });

                if let Err(err) = Builder::new(TokioExecutor::new())
                    .serve_connection_with_upgrades(io, hyper_svc)
                    .await
                {
                    if is_normal_close(&err.to_string()) {
                        tracing::debug!("Connection closed normally ({})", peer_addr);
                    } else {
                        tracing::warn!("Connection error ({}): {}", peer_addr, err);
                    }
                }
            });
        }
    }
}

// ── High-performance multi-socket server ─────────────────────────────────────

/// Bind `workers` independent sockets all on `addr`.
///
/// On Linux/macOS: uses `SO_REUSEPORT` so the kernel load-balances
/// connections across all sockets with zero userspace locking.
///
/// On Windows: `SO_REUSEPORT` is unavailable. A warning is logged and
/// only ONE socket is created regardless of `workers`.
fn create_reuseport_listeners(
    addr: SocketAddr,
    workers: usize,
) -> Result<Vec<TcpListener>, Box<dyn std::error::Error + Send + Sync>> {
    #[cfg(not(unix))]
    {
        tracing::warn!(
            "SO_REUSEPORT is not available on this platform. \
             Running with a single accept loop. \
             Use Linux or macOS for full multi-core performance."
        );
        // On Windows fall back to a single standard listener
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        return Ok(vec![listener]);
    }

    let mut listeners = Vec::with_capacity(workers);

    for i in 0..workers {
        let domain = Domain::for_address(addr);
        let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;

        // Allow multiple sockets to bind the same port.
        // The kernel distributes connections at the SYN level — no userspace lock.
        #[cfg(unix)]
        socket.set_reuse_port(true)?;

        // Instant restart without waiting for TIME_WAIT to expire.
        socket.set_reuse_address(true)?;

        // Disable Nagle's algorithm — flush writes immediately.
        // Halves RTT for short request/response payloads.
        socket.set_nodelay(true)?;

        // Warn on SO_RCVBUF failure instead of propagating the error.
        // The kernel may cap recv buffer size; this is non-fatal.
        if let Err(e) = socket.set_recv_buffer_size(256 * 1024) {
            tracing::warn!(
                "Could not set SO_RCVBUF to 256 KiB on worker {}: {} (continuing with kernel default)",
                i,
                e
            );
        }

        // SYN backlog — absorbs traffic bursts without rejecting connections.
        socket.bind(&addr.into())?;
        socket.listen(4096)?;
        socket.set_nonblocking(true)?;

        let std_listener: std::net::TcpListener = socket.into();
        let tokio_listener = TcpListener::from_std(std_listener)?;

        tracing::debug!("Worker {} bound to http://{} (SO_REUSEPORT)", i, addr);

        listeners.push(tokio_listener);
    }

    Ok(listeners)
}

/// High-performance multi-core server entry point.
///
/// Binds one `SO_REUSEPORT` socket per logical CPU, then runs one
/// independent accept loop per socket — all within the current Tokio runtime.
/// No userspace lock is held during accept; throughput scales linearly
/// with the number of CPU cores.
///
/// This is `pub(crate)` — the public API is [`crate::serve::serve_app_multi`].
///
/// # Example
///
/// ```rust,ignore
/// serve_app_multi("0.0.0.0:8080", app).await.unwrap();
/// ```
pub(crate) async fn serve_app_multi(
    addr: &str,
    router: Router,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: SocketAddr = addr.parse()?;
    let workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);

    tracing::info!(
        "🔱 Ajaya starting {} worker accept loops on http://{}",
        workers,
        addr
    );

    let svc = router.into_service();
    let listeners = create_reuseport_listeners(addr, workers)?;

    // Each task owns its socket exclusively — zero contention on accept.
    let mut handles = Vec::with_capacity(listeners.len());
    for listener in listeners {
        let svc = svc.clone();
        handles.push(tokio::spawn(async move {
            accept_loop(listener, svc).await;
        }));
    }

    for handle in handles {
        let _ = handle.await;
    }

    Ok(())
}

/// High-performance multi-core server for a pre-built Tower service.
///
/// This is `pub(crate)` — the public API is [`crate::serve::serve_service_multi`].
pub(crate) async fn serve_service_multi(
    addr: &str,
    service: BoxCloneService,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: SocketAddr = addr.parse()?;
    let workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);

    tracing::info!(
        "🔱 Ajaya (multi) starting {} workers on http://{}",
        workers,
        addr
    );

    let listeners = create_reuseport_listeners(addr, workers)?;

    let mut handles = Vec::with_capacity(listeners.len());
    for listener in listeners {
        let svc = service.clone();
        handles.push(tokio::spawn(async move {
            accept_loop(listener, svc).await;
        }));
    }

    for handle in handles {
        let _ = handle.await;
    }

    Ok(())
}

// ── Shared accept loop ────────────────────────────────────────────────────────

/// Tight accept loop — one Tokio task per connection, zero shared state.
///
/// Runs forever (until process exit). Each spawned task owns its connection
/// exclusively so there is no contention on the hot path.
pub(crate) async fn accept_loop(listener: TcpListener, service: BoxCloneService) {
    loop {
        match listener.accept().await {
            Ok((stream, peer_addr)) => {
                // Belt-and-braces TCP_NODELAY — the socket already has it set
                // from create_reuseport_listeners, but some kernels don't
                // inherit socket options on accept().
                if let Err(e) = stream.set_nodelay(true) {
                    tracing::warn!("set_nodelay failed for {}: {}", peer_addr, e);
                }

                let io = TokioIo::new(stream);
                let svc = service.clone();

                tokio::task::spawn(async move {
                    let hyper_svc = service_fn(move |req: hyper::Request<Incoming>| {
                        let mut s = svc.clone();
                        async move {
                            let ajaya_req = ajaya_core::Request::from_hyper(req);
                            // Our services always return Poll::Ready(Ok(()))
                            let _ = std::future::poll_fn(|cx| s.poll_ready(cx)).await;
                            let response = s
                                .call(ajaya_req)
                                .await
                                .unwrap_or_else(|infallible| match infallible {});
                            Ok::<http::Response<Body>, hyper::Error>(response)
                        }
                    });

                    if let Err(err) = Builder::new(TokioExecutor::new())
                        .serve_connection_with_upgrades(io, hyper_svc)
                        .await
                    {
                        // "connection reset by peer" and "broken pipe"
                        // are normal for HTTP keep-alive clients — debug only.
                        // Real errors (TLS failures, protocol errors) log at warn.
                        let msg = err.to_string();
                        if is_normal_close(&msg) {
                            tracing::debug!("Connection closed normally ({})", peer_addr);
                        } else {
                            tracing::warn!("Connection error ({}): {}", peer_addr, err);
                        }
                    }
                });
            }

            Err(e) => {
                // EMFILE/ENFILE: out of file descriptors.
                // Sleep briefly so existing connections can close rather than
                // spinning at 100% CPU in an error loop.
                tracing::error!("Accept error: {} — backing off 10 ms", e);
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Returns `true` for connection-close events that are normal operation,
/// not bugs: client disconnect, keep-alive timeout, connection reset.
#[inline]
fn is_normal_close(msg: &str) -> bool {
    msg.contains("connection reset by peer")
        || msg.contains("broken pipe")
        || msg.contains("connection closed")
        || msg.contains("unexpected end of file")
        || msg.contains("os error 104") // ECONNRESET on Linux
        || msg.contains("os error 32") // EPIPE on Linux
}
