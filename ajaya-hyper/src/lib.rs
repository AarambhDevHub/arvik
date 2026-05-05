//! # ajaya-hyper
//!
//! Hyper 1.x server integration for the Ajaya web framework.
//!
//! ## Serve entry points
//!
//! | Function | Description |
//! |---|---|
//! | [`serve_app_multi`] | **Recommended for production** — one `SO_REUSEPORT` socket per CPU |
//! | [`serve_app`] | Single listener (development / tests) |
//! | [`serve_service_multi`] | Multi-socket for a manually built Tower service |
//! | [`serve_service`] | Single listener for a Tower service |
//! | [`serve_router`] | Single listener for a `MethodRouter` |
//! | [`serve`] | Single listener for a bare handler |

pub mod serve;
pub mod server;

pub use serve::{
    serve, serve_app, serve_app_multi, serve_router, serve_service, serve_service_multi,
};
pub use server::Server;
