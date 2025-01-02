use std::io::Cursor;

use bytes::{Buf, Bytes};

use crate::{Error, Result};

/// A frame in Redis Serialization Protocol (RESP).
///
/// See: https://redis.io/docs/latest/develop/reference/protocol-spec/
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    SimpleString(String),
    SimpleError(String),
    /// TODO: Use `i64` instead of `u64` to represent signed integers. And update the codec accordingly.
    Integer(u64),
    BulkString(Bytes),
    Array(Vec<Frame>),
    Null,
}

impl Frame {
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<()> {
        match get_u8(src)? {
            b'+' | b'-' => {
                get_line(src)?;
                Ok(())
            }
            b':' => {
                let _ = get_decimal(src)?;
                Ok(())
            }
            b'$' => {
                if b'-' == peek_u8(src)? {
                    // skip '-1\r\n'
                    skip(src, 4)
                } else {
                    // read the bulk string
                    let len: usize = get_decimal(src)?.try_into()?;
                    // skip that number of bytes + 2 for '\r\n'
                    skip(src, len + 2)
                }
            }
            b'*' => {
                let len = get_decimal(src)?;
                for _ in 0..len {
                    Frame::check(src)?;
                }
                Ok(())
            }
            actual => Err(Error::Protocol(format!("invalid frame byte `{actual}`"))),
        }
    }

    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame> {
        // The first byte of the frame indicates the data type.
        match get_u8(src)? {
            b'+' => {
                let line = get_line(src)?.to_vec();
                let string = String::from_utf8(line)?;
                Ok(Frame::SimpleString(string))
            }
            b'-' => {
                let line = get_line(src)?.to_vec();
                let string = String::from_utf8(line)?;
                Ok(Frame::SimpleError(string))
            }
            b':' => Ok(Frame::Integer(get_decimal(src)?)),
            b'$' => {
                if b'-' == peek_u8(src)? {
                    let line = get_line(src)?;
                    if line != b"-1" {
                        return Err(Error::Protocol("invalid frame format".into()));
                    }
                    Ok(Frame::Null)
                } else {
                    let len = get_decimal(src)?.try_into()?;
                    let n = len + 2;
                    if src.remaining() < n {
                        return Err(Error::IncompleteFrame);
                    }
                    let data = Bytes::copy_from_slice(&src.chunk()[..len]);
                    // skip that number of bytes + 2 for '\r\n'
                    skip(src, n)?;
                    Ok(Frame::BulkString(data))
                }
            }
            b'*' => {
                let len: usize = get_decimal(src)?.try_into()?;
                let mut out = Vec::with_capacity(len);
                for _ in 0..len {
                    out.push(Frame::parse(src)?);
                }
                Ok(Frame::Array(out))
            }
            _ => unimplemented!(),
        }
    }

    pub(crate) fn array() -> Self {
        Frame::Array(vec![])
    }

    pub(crate) fn push_bulk(&mut self, bytes: Bytes) -> Result<()> {
        match self {
            Frame::Array(vec) => {
                vec.push(Frame::BulkString(bytes));
                Ok(())
            }
            ty => Err(Error::WrongFrameType(format!(
                "cannot push to non-array frame type, type was {:?}",
                ty
            ))),
        }
    }

    pub(crate) fn push_int(&mut self, value: u64) -> Result<()> {
        match self {
            Frame::Array(vec) => {
                vec.push(Frame::Integer(value));
                Ok(())
            }
            ty => Err(Error::WrongFrameType(format!(
                "cannot push to non-array frame type, type was {:?}",
                ty
            ))),
        }
    }
}

fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8> {
    if !src.has_remaining() {
        return Err(Error::IncompleteFrame);
    }
    Ok(src.get_u8())
}

fn peek_u8(src: &mut Cursor<&[u8]>) -> Result<u8> {
    if !src.has_remaining() {
        return Err(Error::IncompleteFrame);
    }
    Ok(src.chunk()[0])
}

fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<()> {
    if src.remaining() < n {
        return Err(Error::IncompleteFrame);
    }
    src.advance(n);
    Ok(())
}

fn get_decimal(src: &mut Cursor<&[u8]>) -> Result<u64> {
    use atoi::atoi;

    let line = get_line(src)?;

    atoi(line).ok_or(Error::Protocol("invalid frame format".into()))
}

fn get_line<'a>(src: &'a mut Cursor<&[u8]>) -> Result<&'a [u8]> {
    let start = src.position() as usize;
    let end = src.get_ref().len() - 1;

    for i in start..end {
        if src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n' {
            src.set_position((i + 2) as u64);
            return Ok(&src.get_ref()[start..i]);
        }
    }
    Err(Error::IncompleteFrame)
}
