use crate::{Error, Result};
use bytes::{Buf, Bytes};
use std::io::Cursor;

/// A frame in Redis Serialization Protocol (RESP).
///
/// See: <https://redis.io/docs/latest/develop/reference/protocol-spec/>
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    // RESP 2
    SimpleString(String),
    SimpleError(String),
    Integer(i64),
    BulkString(Bytes),
    Array(Vec<Frame>),
    NullBulkString,
    NullArray,
    // RESP 3
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
                let _ = get_decimal_signed(src)?;
                Ok(())
            }
            b'$' => {
                if b'-' == peek_u8(src)? {
                    // skip '-1\r\n'
                    skip(src, 4)
                } else {
                    // read the bulk string
                    let len: usize = get_decimal_signed(src)?.try_into()?;
                    // skip that number of bytes + 2 for '\r\n'
                    skip(src, len + 2)
                }
            }
            b'*' => {
                let len = get_decimal_signed(src)?;
                for _ in 0..len {
                    Frame::check(src)?;
                }
                Ok(())
            }
            b'_' => {
                let line = get_line(src)?;
                if line != b"" {
                    Err(Error::Protocol(format!(
                        "invalid `null` data type frame format, frame contained bytes `{line:?}`"
                    )))
                } else {
                    Ok(())
                }
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
            b':' => Ok(Frame::Integer(get_decimal_signed(src)?)),
            b'$' => {
                if b'-' == peek_u8(src)? {
                    let line = get_line(src)?;
                    if line != b"-1" {
                        return Err(Error::Protocol(format!(
                            "invalid frame format, only valid negative length is -1, got `{line:?}`"
                        )));
                    }
                    Ok(Frame::NullBulkString)
                } else {
                    // Technically, the spec does not say that a '+' is allowed
                    // but we do in order to accomodate to weird clients
                    let len = get_decimal_unsigned(src)?.try_into()?;
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
                let decimal = get_decimal_signed(src)?;
                if decimal == -1 {
                    return Ok(Frame::NullArray);
                }
                let len: usize = decimal.try_into()?;
                let mut out = Vec::with_capacity(len);
                for _ in 0..len {
                    out.push(Frame::parse(src)?);
                }
                Ok(Frame::Array(out))
            }
            b'_' => {
                let line = get_line(src)?;
                if line != b"" {
                    Err(Error::Protocol(format!(
                        "invalid `null` data type frame format, frame contained bytes `{line:?}`"
                    )))
                } else {
                    Ok(Frame::Null)
                }
            }
            first_byte => Err(Error::Protocol(format!(
                "first byte was not a valid RESP data type `{first_byte}`"
            ))),
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

    pub(crate) fn push_int(&mut self, value: i64) -> Result<()> {
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

fn get_decimal_signed(src: &mut Cursor<&[u8]>) -> Result<i64> {
    use atoi::atoi;

    let line = get_line(src)?;

    atoi(line).ok_or(Error::Protocol("invalid frame format".into()))
}

fn get_decimal_unsigned(src: &mut Cursor<&[u8]>) -> Result<u64> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all_data_types() {
        let frames: &[&[u8]] = &[
            b"+OK\r\n",
            b"-ERR unknown command 'foobar'\r\n",
            b":1000\r\n",
            b"$6\r\nfoobar\r\n",
            b"$0\r\n\r\n",
            b"$-1\r\n",
            b"*2\r\n+OK\r\n$6\r\nfoobar\r\n",
            b"*1\r\n+OK\r\n",
            b"*-1\r\n",
            b"*0\r\n",
            b"_\r\n",
        ];
        for frame in frames {
            match_frame(frame);
        }
    }

    /// This function is used to ensure that parse contains every variant of [`Frame`].
    fn match_frame(src: &[u8]) {
        let mut buf = Cursor::new(src);
        let frame = Frame::parse(&mut buf).unwrap();
        // A match statement without a catch-all arm will fail to compile if a variant is missing.
        match frame {
            Frame::SimpleString(_) => (),
            Frame::SimpleError(_) => (),
            Frame::Integer(_) => (),
            Frame::BulkString(_) => (),
            Frame::Array(_) => (),
            Frame::NullBulkString => (),
            Frame::NullArray => (),
            Frame::Null => (),
        }
    }

    #[test]
    fn test_simple_string() {
        let mut buf = Cursor::new(b"+OK\r\n".as_slice());
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(frame, Frame::SimpleString("OK".to_string()));
    }

    #[test]
    fn test_long_simple_string() {
        let mut buf = Cursor::new(b"+this is a long string\r\n".as_slice());
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(
            frame,
            Frame::SimpleString("this is a long string".to_string())
        );
    }

    #[test]
    fn test_simple_error() {
        let mut buf = Cursor::new(b"-ERR unknown command 'foobar'\r\n".as_slice());
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(
            frame,
            Frame::SimpleError("ERR unknown command 'foobar'".to_string())
        );
    }

    #[test]
    fn test_integer() {
        let mut buf = Cursor::new(b":1000\r\n".as_slice());
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(frame, Frame::Integer(1000));

        let mut buf = Cursor::new(b":000001\r\n".as_slice());
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(frame, Frame::Integer(1));

        let mut buf = Cursor::new(b":00000000\r\n".as_slice());
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(frame, Frame::Integer(0));

        let mut buf = Cursor::new(b":-0\r\n".as_slice());
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(frame, Frame::Integer(0));

        let mut buf = Cursor::new(b":+0\r\n".as_slice());
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(frame, Frame::Integer(0));

        let mut buf = Cursor::new(b":-1\r\n".as_slice());
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(frame, Frame::Integer(-1));

        let mut buf = Cursor::new(b":+1\r\n".as_slice());
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(frame, Frame::Integer(1));

        let mut buf = Cursor::new(b":+9223372036854775807\r\n".as_slice());
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(frame, Frame::Integer(i64::MAX));

        let mut buf = Cursor::new(b":-9223372036854775808\r\n".as_slice());
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(frame, Frame::Integer(i64::MIN));
    }

    #[test]
    fn test_bulk_string() {
        let mut buf = Cursor::new(b"$6\r\nfoobar\r\n".as_slice());
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(frame, Frame::BulkString(Bytes::from("foobar")));

        let mut buf = Cursor::new(b"$0\r\n\r\n".as_slice());
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(frame, Frame::BulkString(Bytes::from("")));

        let mut buf = Cursor::new(b"$+2\r\nOK\r\n".as_slice());
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(frame, Frame::BulkString(Bytes::from("OK")));
    }

    #[test]
    fn test_null_bulk_string() {
        let mut buf = Cursor::new(b"$-1\r\n".as_slice());
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(frame, Frame::NullBulkString);
    }

    #[test]
    fn test_array() {
        let mut buf = Cursor::new(b"*2\r\n+OK\r\n$6\r\nfoobar\r\n".as_slice());
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(
            frame,
            Frame::Array(vec![
                Frame::SimpleString("OK".to_string()),
                Frame::BulkString(Bytes::from("foobar")),
            ])
        );

        let mut buf = Cursor::new(b"*1\r\n+OK\r\n".as_slice());
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(
            frame,
            Frame::Array(vec![Frame::SimpleString("OK".to_string())])
        );

        let mut buf = Cursor::new(b"*3\r\n+OK\r\n".as_slice());
        let frame = Frame::parse(&mut buf);
        assert!(frame.is_err());
    }

    #[test]
    fn test_null_array() {
        let mut buf = Cursor::new(b"*-1\r\n".as_slice());
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(frame, Frame::NullArray);
    }

    #[test]
    fn test_empty_array() {
        let mut buf = Cursor::new(b"*0\r\n".as_slice());
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(frame, Frame::array());
    }

    #[test]
    fn test_recursive_array() {
        let mut buf = Cursor::new(b"*2\r\n*2\r\n+OK\r\n$6\r\nfoobar\r\n$3\r\nbaz\r\n".as_slice());
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(
            frame,
            Frame::Array(vec![
                Frame::Array(vec![
                    Frame::SimpleString("OK".to_string()),
                    Frame::BulkString(Bytes::from("foobar")),
                ]),
                Frame::BulkString(Bytes::from("baz")),
            ])
        );
    }

    #[test]
    fn test_null() {
        let mut buf = Cursor::new(b"_\r\n".as_slice());
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(frame, Frame::Null);

        let mut buf = Cursor::new(b"_text\r\n".as_slice());
        let frame = Frame::parse(&mut buf);
        assert!(frame.is_err());
    }

    #[test]
    fn test_invalid_frame() {
        let mut buf = Cursor::new(b"invalid frame\r\n".as_slice());
        let frame = Frame::parse(&mut buf);
        assert!(frame.is_err());
    }
}
