#![allow(dead_code, unused)] // TODO: remove this
use bytes::Bytes;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

mod parse;

pub mod cmd;
pub mod connection;
pub mod db;
pub mod error;
pub mod frame;
pub mod shutdown;

pub use cmd::CommandVariant;
pub use connection::Connection;
pub use error::Error;
pub use frame::Frame;
pub use shutdown::Shutdown;

pub const DEFAULT_PORT: u16 = 6379;
pub const DEFAULT_HOST: &str = "0.0.0.0";

type NVResult<T> = std::result::Result<T, crate::error::Error>;

pub type Db = Arc<RwLock<HashMap<String, Bytes>>>;
