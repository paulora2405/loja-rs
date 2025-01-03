use crate::{parse::Parse, Connection, Db, Error, Frame, NVResult};

pub mod get;
pub mod ping;
pub mod set;

pub use get::GetCmd;
pub use ping::PingCmd;
pub use set::SetCmd;

pub trait Command {
    fn parse_frames(parse: &mut Parse) -> NVResult<Self>
    where
        Self: Sized;

    fn apply(
        self,
        db: &Db,
        dst: &mut Connection,
    ) -> impl std::future::Future<Output = NVResult<()>> + Send;

    fn into_frame(self) -> NVResult<Frame>;
}

#[derive(Debug)]
pub enum CommandVariant {
    Get(GetCmd),
    Set(SetCmd),
    Ping(PingCmd),
}

impl CommandVariant {
    #[tracing::instrument(ret, skip_all)]
    pub fn from_frame(frame: Frame) -> NVResult<Self> {
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

    pub async fn apply(
        self,
        db: &Db,
        dst: &mut Connection,
        _shutdown: &mut crate::Shutdown,
    ) -> NVResult<()> {
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
