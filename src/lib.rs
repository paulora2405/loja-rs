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

pub const DEFAULT_PORT: u16 = 6379;
pub const DEFAULT_HOST: &str = "0.0.0.0";

pub type LResult<T> = std::result::Result<T, crate::error::Error>;
