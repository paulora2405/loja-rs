pub mod connection;
pub mod error;
pub mod frame;
pub use connection::Connection;
pub use error::Error;
pub use frame::Frame;

pub const DEFAULT_PORT: u16 = 6379;

type Result<T> = std::result::Result<T, crate::error::Error>;
