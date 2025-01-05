use super::Command;
use crate::{parse::Parse, Frame, NVResult};
use bytes::Bytes;
use tracing::debug;

#[derive(Debug)]
pub struct GetCmd {
    key: String,
}

impl GetCmd {
    pub fn new(key: impl ToString) -> GetCmd {
        GetCmd {
            key: key.to_string(),
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }
}

impl Command for GetCmd {
    fn parse_frames(parse: &mut Parse) -> NVResult<Self>
    where
        Self: Sized,
    {
        let key = parse.next_string()?;
        Ok(Self { key })
    }

    #[tracing::instrument(skip_all)]
    async fn apply(self, db: &crate::Db, dst: &mut crate::Connection) -> NVResult<()> {
        let response = if let Some(value) = db.get(&self.key) {
            Frame::BulkString(value.clone())
        } else {
            Frame::Null
        };

        debug!(?response);

        dst.write_frame(&response).await?;

        Ok(())
    }

    fn into_frame(self) -> NVResult<crate::Frame> {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("get"))?;
        frame.push_bulk(Bytes::from(self.key))?;
        Ok(frame)
    }
}
