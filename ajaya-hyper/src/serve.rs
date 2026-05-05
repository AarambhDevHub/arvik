//! Convenience serve functions.
//!
//! # Quick reference
//!
//! | Function | Sockets | Use when |
//! |---|---|---|
//! | [`serve_app`] | 1 | Development, tests, single-core |
//! | [`serve_app_multi`] | 1 per CPU | **Production** — maximum throughput |
//! | [`serve_router`] | 1 | Single-path method dispatch |
//! | [`serve_service`] | 1 | Manual tower service composition |
//! | [`serve_service_multi`] | 1 per CPU | Manual tower + multi-core |

use ajaya_core::handler::Handler;
use ajaya_router::layer::BoxCloneService;
use ajaya_router::{MethodRouter, Router};

use crate::Server;
use crate::server::{
    serve_app_multi as _serve_app_multi, serve_service_multi as _serve_service_multi,
};

/// Start the server with a bare handler (no routing, single listener).
pub async fn serve<H, T>(
    addr: &str,
    handler: H,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    H: Handler<T> + Clone + Send + Sync + 'static,
    T: 'static,
{
    Server::bind(addr).await?.serve(handler).await
}

/// Start the server with a [`MethodRouter`] (single listener).
pub async fn serve_router(
    addr: &str,
    router: MethodRouter,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    Server::bind(addr).await?.serve_method_router(router).await
}

/// Start the server with a [`Router`] using a **single** listener.
///
/// For maximum throughput on multi-core machines use [`serve_app_multi`].
pub async fn serve_app(
    addr: &str,
    router: Router,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    Server::bind(addr).await?.serve_app(router).await
}

/// Start the server with a pre-built Tower [`BoxCloneService`] (single listener).
pub async fn serve_service(
    addr: &str,
    service: BoxCloneService,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    Server::bind(addr).await?.serve_service(service).await
}

/// **High-performance entry point.** Start the server with one
/// `SO_REUSEPORT` socket per logical CPU, each running its own
/// independent accept loop.
///
/// This is the recommended way to start Ajaya in production.  The kernel
/// load-balances incoming connections across all sockets; there is no
/// shared accept-queue mutex, so throughput scales linearly with core count.
///
/// # Requirements
///
/// - The Tokio runtime must be `multi_thread` (default for `#[tokio::main]`).
/// - `SO_REUSEPORT` requires Linux ≥ 3.9 or macOS. On Windows the function
///   gracefully falls back to a single listener.
///
/// # Example
///
/// ```rust,ignore
/// use ajaya::{Router, get, serve_app_multi};
///
/// #[tokio::main]
/// async fn main() {
///     let app = Router::new().route("/", get(|| async { "Hello" }));
///     serve_app_multi("0.0.0.0:8080", app).await.unwrap();
/// }
/// ```
pub async fn serve_app_multi(
    addr: &str,
    router: Router,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    _serve_app_multi(addr, router).await
}

/// **High-performance entry point** for a manually composed Tower service.
///
/// Like [`serve_app_multi`] but takes a [`BoxCloneService`] directly.
pub async fn serve_service_multi(
    addr: &str,
    service: BoxCloneService,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    _serve_service_multi(addr, service).await
}
