use bytes::Bytes;
use tracing::debug;

use super::Command;
use crate::{Frame, NVResult};

#[derive(Debug)]
pub struct SetCmd {
    key: String,
    value: Bytes,
    // TODO: Add the expire field
}

impl SetCmd {
    pub fn new(key: impl ToString, value: Bytes) -> SetCmd {
        SetCmd {
            key: key.to_string(),
            value,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> &Bytes {
        &self.value
    }
}

impl Command for SetCmd {
    fn parse_frames(parse: &mut crate::parse::Parse) -> NVResult<Self>
    where
        Self: Sized,
    {
        let key = parse.next_string()?;
        let value = parse.next_bytes()?;
        // TODO: Parse the optional EX or PX arguments
        Ok(Self { key, value })
    }

    #[tracing::instrument(skip_all)]
    async fn apply(self, db: &crate::Db, dst: &mut crate::Connection) -> NVResult<()> {
        {
            db.set(self.key, self.value, None);
        }
        let response = Frame::SimpleString("OK".to_string());
        debug!(?response);
        dst.write_frame(&response).await?;
        Ok(())
    }

    fn into_frame(self) -> NVResult<crate::Frame> {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("set"))?;
        frame.push_bulk(Bytes::from(self.key))?;
        frame.push_bulk(self.value)?;
        Ok(frame)
    }
}
