use crate::{Error, Frame, NVResult};
use bytes::Bytes;

#[derive(Debug)]
pub struct Parse {
    parts: std::vec::IntoIter<Frame>,
}

impl Parse {
    pub(crate) fn new(frame: Frame) -> NVResult<Parse> {
        let array = match frame {
            Frame::Array(array) => array,
            frame => return Err(Error::Protocol(format!("expected array, got {frame:?}"))),
        };

        Ok(Parse {
            parts: array.into_iter(),
        })
    }

    fn next(&mut self) -> NVResult<Frame> {
        self.parts.next().ok_or(Error::EndOfStream)
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn next_string(&mut self) -> NVResult<String> {
        match self.next()? {
            Frame::SimpleString(s) => Ok(s),
            Frame::BulkString(data) => std::str::from_utf8(&data[..])
                .map(|s| s.to_string())
                .map_err(|e| Error::Protocol(e.to_string())),
            frame => Err(Error::Protocol(format!(
                "expected simple frame or bulk frame, got {frame:?}"
            ))),
        }
    }

    pub(crate) fn next_bytes(&mut self) -> NVResult<Bytes> {
        match self.next()? {
            Frame::SimpleString(s) => Ok(Bytes::from(s.into_bytes())),
            Frame::BulkString(data) => Ok(data),
            frame => Err(Error::Protocol(format!(
                "expected simple frame or bulk frame, got {frame:?}"
            ))),
        }
    }

    pub(crate) fn next_int(&mut self) -> NVResult<u64> {
        use atoi::atoi;
        let invalid_number_err: Error = Error::Protocol("invalid number".to_string());

        match self.next()? {
            Frame::Integer(v) => Ok(v),
            Frame::SimpleString(data) => atoi::<u64>(data.as_bytes()).ok_or(invalid_number_err),
            Frame::BulkString(data) => atoi::<u64>(&data).ok_or(invalid_number_err),
            frame => Err(Error::Protocol(format!(
                "expected int frame, got {frame:?}"
            ))),
        }
    }

    pub(crate) fn finish(&mut self) -> NVResult<()> {
        if self.parts.next().is_none() {
            Ok(())
        } else {
            Err(Error::Protocol("expected end of frame".to_string()))
        }
    }
}
