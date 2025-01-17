//! Implementation of the `GET` command.
use super::Command;
use crate::{parse::Parse, ConnectionStream, Frame, Result};
use bytes::Bytes;
use tracing::debug;

/// Get a `value` for a given `key`.
///
/// If the key does not exist, a `Null` RESP type is returned.
#[derive(Debug)]
pub struct GetCmd {
    key: String,
}

impl GetCmd {
    /// Creates a new [`GetCmd`] command.
    pub fn new(key: impl ToString) -> Self {
        Self {
            key: key.to_string(),
        }
    }

    /// Returns a reference to the key.
    pub fn key(&self) -> &str {
        &self.key
    }
}

impl Command for GetCmd {
    fn parse_frames(parse: &mut Parse) -> Result<Self>
    where
        Self: Sized,
    {
        let key = parse.next_string()?;
        Ok(Self { key })
    }

    #[tracing::instrument(skip_all)]
    async fn apply<S: ConnectionStream>(
        self,
        db: &crate::Db,
        dst: &mut crate::Connection<S>,
    ) -> Result<()> {
        let response = if let Some(value) = db.get(&self.key) {
            Frame::BulkString(value.clone())
        } else {
            Frame::NullBulkString
        };

        debug!(?response);

        dst.write_frame(&response).await?;

        Ok(())
    }

    fn into_frame(self) -> Result<crate::Frame> {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("get"))?;
        frame.push_bulk(Bytes::from(self.key))?;
        Ok(frame)
    }
}
