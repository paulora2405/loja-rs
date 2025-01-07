//! Commands module.
use crate::{parse::Parse, Connection, ConnectionStream, Db, Error, Frame, LResult, Shutdown};

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

        let command_name = parse.next_string()?.to_lowercase();

        let command = match &command_name[..] {
            "get" => CommandVariant::Get(GetCmd::parse_frames(&mut parse)?),
            "set" => CommandVariant::Set(SetCmd::parse_frames(&mut parse)?),
            "ping" => CommandVariant::Ping(PingCmd::parse_frames(&mut parse)?),
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

    pub fn get_name(&self) -> &str {
        use CommandVariant as C;
        match self {
            C::Get(_) => "get",
            C::Set(_) => "set",
            C::Ping(_) => "ping",
        }
    }
}
