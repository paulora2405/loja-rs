//! A module for handling the connection to a stream, usually a remote peer via a [`TcpStream`].
use crate::frame::Frame;
use crate::{Error, Result};
use bytes::{Buf, BytesMut};
use std::io::Cursor;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter};
use tracing::{debug, error};

const DEFAULT_BUFFER_SIZE: usize = 16 * 1024;

/// Send and receive `Frame` values from a remote peer.
///
/// To read frames, the `Connection` uses an internal buffer, which is filled
/// up until there are enough bytes to create a full frame. Once this happens,
/// the `Connection` creates the frame and returns it to the caller.
///
/// When sending frames, the frame is first encoded into the write buffer.
/// The contents of the write buffer are then written to the socket.
#[derive(Debug)]
pub(crate) struct Connection<S> {
    /// Stream wrapped with a `BufWriter` for buffering writes.
    stream: BufWriter<S>,
    /// Buffer used for reading frames.
    // TODO: Look into `tokio_util::codec` and implementing my own codec for decoding and enco
    buffer: BytesMut,
}

/// A trait for types that can be used as a connection stream.
pub(crate) trait ConnectionStream: AsyncRead + AsyncWrite + Unpin + Send {}

// Blanket implementation for all types that implement `AsyncRead + AsyncWrite + Unpin + Send`.
impl<T: AsyncRead + AsyncWrite + Unpin + Send> ConnectionStream for T {}

impl<S: ConnectionStream> Connection<S> {
    /// Create a new `Connection` from a `TcpStream` socket.
    ///
    /// The connection is internally buffered, with a default buffer size of 16KB.
    pub fn new(socket: S) -> Self {
        Self {
            stream: BufWriter::new(socket),
            buffer: BytesMut::with_capacity(DEFAULT_BUFFER_SIZE),
        }
    }

    /// Read a single `Frame` from the connection.
    ///
    /// The function waits until it has retrieved enough data to parse a frame.
    /// Any data remaining in the read buffer after the frame has been parsed is
    /// kept there for the next call to `read_frame`.
    ///
    /// # Returns
    ///
    /// On success, the received frame is returned. If the stream
    /// is closed in a way that doesn't break a frame in half, it returns
    /// `None`. Otherwise, an error is returned.
    #[tracing::instrument(skip_all)]
    pub async fn read_frame(&mut self) -> Result<Option<Frame>> {
        loop {
            // Attempt to parse a frame from the buffered data. If enough data
            // has been buffered, the frame is returned.
            if let Some(frame) = self.parse_frame()? {
                debug!(?frame, "frame received");
                return Ok(Some(frame));
            }

            // There is not enough buffered data to read a frame. Attempt to
            // read more data from the socket.
            //
            // On success, the number of bytes is returned. `0` indicates "end
            // of stream".
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                // The remote closed the connection. For this to be a clean
                // shutdown, there should be no data in the read buffer. If
                // there is, this means that the peer closed the socket while
                // sending a frame.
                if self.buffer.is_empty() {
                    debug!("no more frames to read from the buffer");
                    return Ok(None);
                } else {
                    error!("connection was closed mid frame");
                    return Err(Error::Io(std::io::Error::new(
                        std::io::ErrorKind::ConnectionReset,
                        "connection was closed mid frame",
                    )));
                }
            }
        }
    }

    /// Tries to parse a frame from the buffered data, if enough data has been buffered.
    ///
    /// If there isn't enough data, i.e. `Error::IncompleteFrame` occurs,
    /// `Ok(None)` is returned.
    ///
    /// Any other errors are returned as is.
    fn parse_frame(&mut self) -> Result<Option<Frame>> {
        let mut buf = Cursor::new(&self.buffer[..]);

        match Frame::check(&mut buf) {
            Ok(_) => {
                // get the byte length of the frame
                let len = buf.position() as usize;
                // reset the cursor in order to call `parse`
                buf.set_position(0);
                let frame = Frame::parse(&mut buf)?;
                // discard the frame from the buffer
                self.buffer.advance(len);

                Ok(Some(frame))
            }
            // not enough data has been buffered
            Err(Error::IncompleteFrame) => Ok(None),
            // an actual error has occurred
            Err(e) => Err(e),
        }
    }

    /// Write a frame to the connection's underlying stream.
    ///
    /// The `Frame` value is written to the socket using the various `write_*`
    /// functions provided by `AsyncWrite`. Calling these functions directly on
    /// a [`TcpStream`], for example, is **not** advised,
    /// as this will result in a large number of syscalls.
    /// However, it is fine to call these functions on a *buffered*
    /// write stream. The data will be written to the buffer. Once the buffer is
    /// full, it is flushed to the underlying socket.
    pub async fn write_frame(&mut self, frame: &Frame) -> Result<()> {
        self.write_value(frame).await?;
        self.stream.flush().await.map_err(Error::from)
    }

    #[tracing::instrument(skip(self))]
    #[async_recursion::async_recursion]
    async fn write_value(&mut self, frame: &Frame) -> std::io::Result<()> {
        debug!(?frame);
        match frame {
            Frame::SimpleString(val) => {
                self.stream.write_u8(b'+').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::SimpleError(val) => {
                self.stream.write_u8(b'-').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Integer(val) => {
                self.stream.write_u8(b':').await?;
                self.write_decimal(*val).await?;
            }
            Frame::BulkString(val) => {
                let len = val.len();

                self.stream.write_u8(b'$').await?;
                self.write_decimal(len as u64).await?;
                self.stream.write_all(val).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Null => {
                self.stream.write_all(b"$-1\r\n").await?;
            }
            Frame::Array(frames) => {
                self.stream.write_u8(b'*').await?;
                self.write_decimal(frames.len() as u64).await?;
                for frame in frames {
                    self.write_value(frame).await?;
                }
            }
        };

        Ok(())
    }

    async fn write_decimal(&mut self, val: u64) -> std::io::Result<()> {
        use std::io::Write;

        let mut buf = [0u8; 12];

        let mut buf = Cursor::new(&mut buf[..]);
        write!(&mut buf, "{}", val)?;

        let pos = buf.position() as usize;
        self.stream.write_all(&buf.get_ref()[..pos]).await?;
        self.stream.write_all(b"\r\n").await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_read_write_frame() {
        let bytes_frames: &[(&[u8], Frame)] = &[
            // simple string
            (b"+OK\r\n", Frame::SimpleString("OK".to_string())),
            // simple error
            (
                b"-ERR unknown command 'foobar'\r\n",
                Frame::SimpleError("ERR unknown command 'foobar'".to_string()),
            ),
            // integer
            (b":1234\r\n", Frame::Integer(1234)),
            // null bulk strig
            (b"$-1\r\n", Frame::Null),
            // bulk string
            (b"$4\r\nping\r\n", Frame::BulkString(Bytes::from("ping"))),
            (
                // simple array
                b"*2\r\n+OK\r\n$6\r\nfoobar\r\n",
                Frame::Array(vec![
                    Frame::SimpleString("OK".to_string()),
                    Frame::BulkString(Bytes::from("foobar")),
                ]),
            ),
            (
                // nested array
                b"*2\r\n*2\r\n+OK\r\n$6\r\nfoobar\r\n$3\r\nbaz\r\n",
                Frame::Array(vec![
                    Frame::Array(vec![
                        Frame::SimpleString("OK".to_string()),
                        Frame::BulkString(Bytes::from("foobar")),
                    ]),
                    Frame::BulkString(Bytes::from("baz")),
                ]),
            ),
        ];

        // create a mock stream that expects the bytes in the test to be both read and written
        let stream = bytes_frames
            .iter()
            .fold(tokio_test::io::Builder::new(), |mut acc, (bytes, _f)| {
                acc.read(bytes);
                acc.write(bytes);
                acc
            })
            .build();
        // create a new connection with the mocket stream
        let mut conn = Connection::new(stream);

        for (_b, frame) in bytes_frames {
            // read a frame from the connection, which the mock stream is expecting
            let received = conn.read_frame().await.unwrap().unwrap();
            // assert the frame read is the equivalent to the binary representation
            assert_eq!(received, *frame);
            // write the same frame, which the mock stream is expecting, and will panic
            // if the frame is not written as it should be
            conn.write_frame(frame).await.unwrap();
        }
    }
}
