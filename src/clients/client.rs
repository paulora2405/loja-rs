use std::time::Duration;

use bytes::Bytes;
use tokio::net::{TcpStream, ToSocketAddrs};
use tracing::debug;

use crate::{
    cmd::{Command, GetCmd, PingCmd, SetCmd},
    Connection, Error, Frame, LResult,
};

/// Established connection with a Redis server.
///
/// Backed by a single `TcpStream`, `Client` provides basic network client
/// functionality (no pooling, retrying, ...).
/// Requests are issued using the various methods of `Client`.
#[derive(Debug)]
pub struct Client {
    /// The TCP connection decorated with the RESP encoder / decoder
    /// implemented using a buffered `TcpStream`.
    ///
    /// When `Listener` receives an inbound connection, the `TcpStream` is
    /// passed to `Connection::new`, which initializes the associated buffers.
    /// `Connection` allows the handler to operate at the "frame" level and keep
    /// the byte level protocol parsing details encapsulated in `Connection`.
    connection: Connection,
}

impl Client {
    /// Establish a connection with the Redis server located at `addr`.
    ///
    /// `addr` may be any type that can be asynchronously converted to a
    /// `SocketAddr`. This includes `SocketAddr` and strings. The `ToSocketAddrs`
    /// trait is the Tokio version and not the `std` version.
    pub async fn connect(addr: impl ToSocketAddrs) -> LResult<Self> {
        // The `addr` argument is passed directly to `TcpStream::connect`. This
        // performs any asynchronous DNS lookup and attempts to establish the TCP
        // connection. An error at either step returns an error, which is then
        // bubbled up to the caller of `Client` connect.
        let socket = TcpStream::connect(addr).await?;
        // Initialize a new `Connection` with the `TcpStream`.
        // This allocates read/write buffers to perform RESP frame parsing.
        let connection = Connection::new(socket);
        Ok(Client { connection })
    }

    /// Ping to the server.
    ///
    /// Returns PONG if no argument is provided, otherwise
    /// return a copy of the argument as a bulk.
    ///
    /// This command is often used to test if a connection
    /// is still alive, or to measure latency.
    #[tracing::instrument(skip(self))]
    pub async fn ping(&mut self, msg: Option<Bytes>) -> LResult<Bytes> {
        let frame = PingCmd::new(msg).into_frame()?;
        debug!(request = ?frame);
        self.connection.write_frame(&frame).await?;
        match self.read_response().await? {
            Frame::SimpleString(val) => Ok(val.into()),
            Frame::BulkString(val) => Ok(val),
            frame => Err(Error::Response(format!("unexpected frame: {frame}"))),
        }
    }

    /// Get the value of key.
    ///
    /// If the key does not exist `None` is returned.
    #[tracing::instrument(skip(self))]
    pub async fn get(&mut self, key: &str) -> LResult<Option<Bytes>> {
        let frame = GetCmd::new(key).into_frame()?;
        debug!(request = ?frame);
        // Write the full frame to the socket, waiting if necessary.
        self.connection.write_frame(&frame).await?;
        // Wait for the response frame from the server.
        // Both `SimpleString` and `BulkString` are valid responses.
        // `Null` represents the key not being present.
        match self.read_response().await? {
            Frame::SimpleString(val) => Ok(Some(val.into())),
            Frame::BulkString(val) => Ok(Some(val)),
            Frame::Null => Ok(None),
            frame => Err(Error::Response(format!("unexpected frame: {frame}"))),
        }
    }

    /// Set `key` to hold the given `value`.
    ///
    /// The `value` is associated with `key` until it is overwritten by the next
    /// call to `set` or it is removed.
    ///
    /// If key already holds a value, it is overwritten. Any previous time to
    /// live associated with the key is discarded on successful SET operation.
    pub async fn set(&mut self, key: &str, val: Bytes) -> LResult<()> {
        self.set_cmd(SetCmd::new(key, val, None)).await
    }

    /// Set `key` to hold the given `value`. The value expires after `expiration`
    ///
    /// The `value` is associated with `key` until one of the following:
    /// - it expires.
    /// - it is overwritten by the next call to `set`.
    /// - it is removed.
    ///
    /// If key already holds a value, it is overwritten. Any previous time to
    /// live associated with the key is discarded on a successful SET operation.
    pub async fn set_expires(&mut self, key: &str, val: Bytes, expire: Duration) -> LResult<()> {
        self.set_cmd(SetCmd::new(key, val, Some(expire))).await
    }

    /// The core `SET` logic, used by both `set` and `set_expires.
    async fn set_cmd(&mut self, cmd: SetCmd) -> LResult<()> {
        let frame = cmd.into_frame()?;
        debug!(request = ?frame);
        // Write the full frame to the socket, waiting if necessary.
        self.connection.write_frame(&frame).await?;
        // Wait for the response frame from the server.
        // `SimpleString` with a value of `OK` is the only valid response.
        match self.read_response().await? {
            Frame::SimpleString(val) if val == "OK" => Ok(()),
            frame => Err(Error::Response(format!("unexpected frame: {frame}"))),
        }
    }

    async fn read_response(&mut self) -> LResult<Frame> {
        let response = self.connection.read_frame().await?;
        debug!(?response);
        match response {
            Some(Frame::SimpleError(msg)) => Err(Error::Response(msg)),
            Some(frame) => Ok(frame),
            None => {
                // Receiving `None` indicates the connection has been closed by the server
                // without sending a frame. This is unexpected and treated as an `Error::Io`.
                Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "connection reset by server",
                )
                .into())
            }
        }
    }
}
