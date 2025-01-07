#![deny(missing_docs)]
//! A simple Redis clone written in Rust.
mod parse;

pub(crate) mod connection;
pub(crate) use connection::Connection;
pub(crate) use connection::ConnectionStream;

pub(crate) mod db;
pub(crate) use db::Db;

pub(crate) mod error;
pub(crate) use error::Error;

pub(crate) mod frame;
pub(crate) use frame::Frame;

pub(crate) mod shutdown;
pub(crate) use shutdown::Shutdown;

pub mod clients;
pub use clients::Client;

pub mod cmd;
pub use cmd::CommandVariant;

pub mod server;

/// The default port for the server to bind to.
pub const DEFAULT_PORT: u16 = 6379;
/// The default host/interface for the server to bind to.
pub const DEFAULT_HOST: &str = "0.0.0.0";

/// A type alias for the result of a function that may return a [`Error`].
pub type Result<T> = std::result::Result<T, crate::error::Error>;
