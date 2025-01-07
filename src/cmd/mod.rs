//! Commands module.
use crate::{parse::Parse, Connection, ConnectionStream, Db, Error, Frame, LResult, Shutdown};
use std::fmt::Display;

pub mod get;
pub use get::GetCmd;

pub mod ping;
pub use ping::PingCmd;

pub mod set;
pub use set::SetCmd;

pub(crate) trait Command {
    fn parse_frames(parse: &mut Parse) -> LResult<Self>
    where
        Self: Sized;

    fn apply<S: ConnectionStream>(
        self,
        db: &Db,
        dst: &mut Connection<S>,
    ) -> impl std::future::Future<Output = LResult<()>> + Send;

    fn into_frame(self) -> LResult<Frame>;
}

/// All possible command variants.
#[derive(Debug)]
pub enum CommandVariant {
    /// `GET` command.
    Get(GetCmd),
    /// `SET` command.
    Set(SetCmd),
    /// `PING` command.
    Ping(PingCmd),
}

impl CommandVariant {
    /// Parse a frame into a command variant.
    #[tracing::instrument(ret, skip_all, level = "debug")]
    pub fn from_frame(frame: Frame) -> LResult<Self> {
        let mut parse = Parse::new(frame)?;

        let command_name = parse.next_string()?.to_uppercase();

        let command = match &command_name[..] {
            "GET" => CommandVariant::Get(GetCmd::parse_frames(&mut parse)?),
            "SET" => CommandVariant::Set(SetCmd::parse_frames(&mut parse)?),
            "PING" => CommandVariant::Ping(PingCmd::parse_frames(&mut parse)?),
            _ => return Err(Error::UnknownCommand(command_name)),
        };

        parse.finish()?;

        Ok(command)
    }

    pub(crate) async fn apply<S: ConnectionStream>(
        self,
        db: &Db,
        dst: &mut Connection<S>,
        _shutdown: &mut Shutdown,
    ) -> LResult<()> {
        use CommandVariant as C;

        match self {
            C::Get(cmd) => cmd.apply(db, dst).await,
            C::Set(cmd) => cmd.apply(db, dst).await,
            C::Ping(cmd) => cmd.apply(db, dst).await,
        }
    }
}

impl Display for CommandVariant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use CommandVariant as C;

        match self {
            C::Get(cmd) => write!(f, "GET {}", cmd.key()),
            C::Set(cmd) => {
                if let Some(exp) = cmd.expire() {
                    write!(
                        f,
                        "SET {} {:?} EX {}",
                        cmd.key(),
                        cmd.value(),
                        exp.as_millis()
                    )
                } else {
                    write!(f, "SET {} {:?}", cmd.key(), cmd.value())
                }
            }
            C::Ping(cmd) => {
                if let Some(msg) = cmd.msg() {
                    write!(f, "PING {:?}", msg)
                } else {
                    write!(f, "PING")
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_cmd_variant_display() {
        let cmd = CommandVariant::Get(GetCmd::new("foo"));
        assert_eq!(cmd.to_string(), "GET foo");

        let cmd = CommandVariant::Set(SetCmd::new("foo", Bytes::from("bar"), None));
        assert_eq!(cmd.to_string(), "SET foo b\"bar\"");

        let cmd = CommandVariant::Set(SetCmd::new(
            "foo",
            Bytes::from("bar"),
            Some(Duration::from_secs(10)),
        ));
        assert_eq!(cmd.to_string(), "SET foo b\"bar\" EX 10000");

        let cmd = CommandVariant::Ping(PingCmd::new(None));
        assert_eq!(cmd.to_string(), "PING");

        let cmd = CommandVariant::Ping(PingCmd::new(Some(Bytes::from("hello"))));
        assert_eq!(cmd.to_string(), "PING b\"hello\"");
    }
}
