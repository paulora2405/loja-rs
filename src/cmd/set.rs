//! Implement the `SET` command.
use super::Command;
use crate::{ConnectionStream, Error, Frame, Result};
use bytes::Bytes;
use std::time::Duration;
use tracing::debug;

/// Set `key` to hold the string `value`.
///
/// If `key` already holds a value, it is overwritten, regardless of its type.
/// Any previous time to live associated with the key is discarded on successful
/// SET operation.
///
/// # Options
///
/// Currently, the following options are supported:
///
/// * EX `seconds` -- Set the specified expire time, in seconds.
/// * PX `milliseconds` -- Set the specified expire time, in milliseconds.
#[derive(Debug, PartialEq, Eq)]
pub struct SetCmd {
    /// The lookup key.
    key: String,
    /// The value to be stored.
    value: Bytes,
    /// When to expire the key.
    expire: Option<Duration>,
}

impl SetCmd {
    /// Create a new `Set` command which sets `key` to `value`.
    ///
    /// If `expire` is `Some`, the value should expire after the specified
    /// duration.
    pub fn new(key: impl ToString, value: Bytes, expire: Option<Duration>) -> Self {
        Self {
            key: key.to_string(),
            value,
            expire,
        }
    }

    /// Get the key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Get the value.
    pub fn value(&self) -> &Bytes {
        &self.value
    }

    /// Get the expire duration.
    pub fn expire(&self) -> Option<Duration> {
        self.expire
    }
}

impl Command for SetCmd {
    /// Parse a `Set` instance from a received frame.
    ///
    /// The `Parse` argument provides a cursor-like API to read fields from the
    /// `Frame`. At this point, the entire frame has already been received from
    /// the socket.
    ///
    /// The `SET` string has already been consumed.
    ///
    /// # Returns
    ///
    /// Returns the `Set` value on success. If the frame is malformed, `Err` is
    /// returned.
    ///
    /// # Format
    ///
    /// Expects an array frame containing at least 3 entries.
    ///
    /// ```text
    /// SET key value [EX seconds|PX milliseconds]
    /// ```
    fn parse_frames(parse: &mut crate::parse::Parse) -> Result<Self>
    where
        Self: Sized,
    {
        let key = parse.next_string()?;
        let value = parse.next_bytes()?;
        // The expiration is optional. If nothing else follows,
        // then it is `None`.
        let mut expire = None;

        // Attempt to parse another string.
        match parse.next_string() {
            Ok(s) if s.to_uppercase() == "EX" => {
                // The expiration is specified in seconds.
                // The next value must be an integer.
                let secs = parse.next_int_unsigned()?;
                expire = Some(Duration::from_secs(secs));
            }
            Ok(s) if s.to_uppercase() == "PX" => {
                // The expiration is specified in milliseconds.
                // The next value must be an integer.
                let ms = parse.next_int_unsigned()?;
                expire = Some(Duration::from_millis(ms));
            }
            // Currently, we don't support any of the other SET
            // options. An error here results in the connection being
            // terminated. Other connections will continue to operate normally.
            Ok(_) => {
                return Err(Error::Protocol(
                    "currently, `SET` only supports the expiration option".into(),
                ))
            }
            // The `Error::EndOfStream` error indicates there is no further data to
            // parse. In this case, it is a normal run time situation and
            // indicates there are no specified `SET` options.
            Err(Error::EndOfStream) => {}
            // All other errors are bubbled up, resulting in the connection
            // being terminated.
            Err(err) => return Err(err),
        }

        Ok(Self { key, value, expire })
    }

    /// Apply the `SetCmd` command to the specified `Db` instance.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    #[tracing::instrument(skip_all)]
    async fn apply<S: ConnectionStream>(
        self,
        db: &crate::Db,
        dst: &mut crate::Connection<S>,
    ) -> Result<()> {
        db.set(self.key, self.value, self.expire);
        let response = Frame::SimpleString("OK".to_string());
        debug!(?response);
        dst.write_frame(&response).await?;
        Ok(())
    }

    /// Converts the command into an equivalent `Frame`.
    ///
    /// This is called by the client when encoding a `Set` command to send to
    /// the server.
    fn into_frame(self) -> Result<crate::Frame> {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("set"))?;
        frame.push_bulk(Bytes::from(self.key))?;
        frame.push_bulk(self.value)?;
        if let Some(ms) = self.expire {
            // Expirations in RESP can be specified in two ways
            // `SET key value EX` seconds
            // `SET key value PX` milliseconds
            // if we only got `ms.as_millis()`,
            // technically it could overflow if the user inputed
            // a value close to i64::MAX as seconds,
            // and it got converted to miliseconds
            if ms.subsec_millis() == 0 {
                frame.push_bulk(Bytes::from("ex"))?;
                frame.push_int(ms.as_secs() as i64)?;
            } else {
                frame.push_bulk(Bytes::from("px"))?;
                frame.push_int(ms.as_millis() as i64)?;
            }
        }
        Ok(frame)
    }
}
