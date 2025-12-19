//! Implement the `PUBLISH` command.
use super::Command;
use crate::Frame;
use bytes::Bytes;

/// Publishes a message to the given channel.
#[derive(Debug, PartialEq, Eq)]
pub struct PublishCmd {
    /// Name of the channel on which the message should be published.
    channel: String,
    /// The message to publish.
    message: Bytes,
}

impl PublishCmd {
    /// Create a new [`PublishCmd`].
    pub(crate) fn new(channel: String, message: Bytes) -> Self {
        Self { channel, message }
    }

    /// Get the channel name.
    pub(crate) fn channel(&self) -> &str {
        &self.channel
    }

    /// Get the message.
    pub(crate) fn message(&self) -> &Bytes {
        &self.message
    }
}

impl Command for PublishCmd {
    /// Parse a [`PublishCmd`] instance from a received frame.
    ///
    /// The `Parse` argument provides a cursor-like API to read fields from the
    /// `Frame`. At this point, the entire frame has already been received from
    /// the socket.
    ///
    /// The `PUBLISH` string has already been consumed.
    ///
    /// # Returns
    ///
    /// On success, the [`PublishCmd`] value is returned. If the frame is malformed,
    /// [`Err`] is returned.
    ///
    /// # Format
    ///
    /// Expects an array frame containing three entries.
    ///
    /// ```text
    /// PUBLISH channel message
    /// ```
    fn parse_frames(parse: &mut crate::parse::Parse) -> crate::Result<Self>
    where
        Self: Sized,
    {
        // The `PUBLISH` string has already been consumed.
        // Extract the `channel` and `message` values from the frame.
        // The channel must be a valid string.
        let channel = parse.next_string()?;
        // The message is just arbitrary bytes.
        let message = parse.next_bytes()?;
        Ok(Self { channel, message })
    }

    /// Apply the command to the specified [`Db`] instance.
    ///
    /// The response is written to the `dst` [`Connection`].
    /// This is called by the server in order to execute a received command.
    async fn apply<S: crate::ConnectionStream>(
        self,
        db: &crate::Db,
        dst: &mut crate::Connection<S>,
    ) -> crate::Result<()> {
        let num_subscribers = db.publish(&self.channel, self.message);
        let response = Frame::Integer(num_subscribers as i64);
        dst.write_frame(&response).await?;
        Ok(())
    }

    fn into_frame(self) -> crate::Result<crate::Frame> {
        todo!()
    }
}
