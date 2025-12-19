//! Implementation of the `PING` command.
use super::Command;
use crate::{ConnectionStream, Frame};
use bytes::Bytes;
use tracing::debug;

/// Pings the server, which responds with either `PONG` or a provided custom message.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct PingCmd {
    msg: Option<Bytes>,
}

impl PingCmd {
    /// Creates a new [`PingCmd`] command.
    pub fn new(msg: Option<Bytes>) -> Self {
        Self { msg }
    }

    pub(crate) fn msg(&self) -> Option<&Bytes> {
        self.msg.as_ref()
    }
}

impl Command for PingCmd {
    fn parse_frames(parse: &mut super::Parse) -> crate::Result<Self>
    where
        Self: Sized,
    {
        match parse.next_bytes() {
            Ok(msg) => Ok(Self::new(Some(msg))),
            Err(crate::Error::EndOfStream) => Ok(Self::default()),
            Err(e) => Err(e),
        }
    }

    #[tracing::instrument(skip_all)]
    async fn apply<S: ConnectionStream>(
        self,
        _db: &crate::Db,
        dst: &mut crate::Connection<S>,
    ) -> crate::Result<()> {
        let response = match self.msg {
            None => Frame::SimpleString("PONG".to_string()),
            Some(msg) => Frame::BulkString(msg),
        };

        debug!(?response);

        dst.write_frame(&response).await?;

        Ok(())
    }

    fn into_frame(self) -> crate::Result<Frame> {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("ping"))?;
        if let Some(msg) = self.msg {
            frame.push_bulk(msg)?;
        }
        Ok(frame)
    }
}

#[cfg(test)]
mod tests {
    use core::panic;
    use std::io::Cursor;

    use crate::CommandVariant;

    use super::*;

    #[test]
    fn test_ping_cmd() {
        let src = b"*2\r\n+ping\r\n$4\r\nMONG\r\n";
        let mut src = Cursor::new(&src[..]);
        let frame = Frame::parse(&mut src).expect("correct frame");
        let CommandVariant::Ping(ping_cmd) =
            CommandVariant::from_frame(frame).expect("correct frame")
        else {
            panic!("unexpected command");
        };
        let expected = b"*2\r\n$4\r\nping\r\n$4\r\nMONG\r\n";
        let mut expected = Cursor::new(&expected[..]);
        let expected_frame = Frame::Array(vec![
            Frame::BulkString(Bytes::from("ping")),
            Frame::BulkString(Bytes::from("MONG")),
        ]);
        assert_eq!(expected_frame, Frame::parse(&mut expected).unwrap());
        assert_eq!(
            ping_cmd.into_frame().expect("correct frame"),
            expected_frame
        );
    }
}
