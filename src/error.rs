use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("incomplete frame")]
    IncompleteFrame,
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error(transparent)]
    Conversion(#[from] std::num::TryFromIntError),
    #[error(transparent)]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("end of stream error")]
    /// Attempting to extract a value failed due to the frame being fully consumed
    EndOfStream,
    #[error("unknown command `{0}`")]
    UnknownCommand(String),
    #[error("wrong frame type: {0}")]
    WrongFrameType(String),
}
