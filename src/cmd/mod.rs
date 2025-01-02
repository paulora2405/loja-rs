use crate::{parse::Parse, Connection, Db, Error, Frame, Result};

pub mod ping;

pub use ping::PingCmd;

pub trait Command {
    fn parse_frames(parse: &mut Parse) -> Result<Self>
    where
        Self: Sized;

    async fn apply(self, db: &Db, dst: &mut Connection) -> Result<()>;

    fn into_frame(self) -> Result<Frame>;
}

#[derive(Debug)]
pub enum CommandVariant {
    Get,
    Set,
    Ping(PingCmd),
}

impl CommandVariant {
    pub fn from_frame(frame: Frame) -> Result<Self> {
        let mut parse = Parse::new(frame)?;

        let command_name = parse.next_string()?.to_lowercase();

        let command = match &command_name[..] {
            "get" => CommandVariant::Get,
            "set" => CommandVariant::Set,
            "ping" => CommandVariant::Ping(PingCmd::parse_frames(&mut parse)?),
            _ => return Err(Error::UnknownCommand(command_name)),
        };

        parse.finish()?;

        Ok(command)
    }

    pub(crate) async fn apply(
        self,
        db: &Db,
        dst: &mut Connection,
        _shutdown: &mut crate::shutdown::Shutdown,
    ) -> Result<()> {
        use CommandVariant as C;

        match self {
            C::Get => todo!(),
            C::Set => todo!(),
            C::Ping(cmd) => cmd.apply(db, dst).await,
        }
    }

    pub(crate) fn get_name(&self) -> &str {
        use CommandVariant as C;
        match self {
            C::Get => "get",
            C::Set => "set",
            C::Ping(_) => "ping",
        }
    }
}
