use crate::frame::Frame;
use anyhow::Result;
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub struct Connection {
    pub stream: TcpStream,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Connection {
            stream,
            buffer: BytesMut::with_capacity(512),
        }
    }

    pub async fn read_value(&mut self) -> Result<Option<Frame>> {
        let bytes_read = self.stream.read_buf(&mut self.buffer).await?;

        if bytes_read == 0 {
            return Ok(None);
        }

        let (v, _) = Frame::parse_message(self.buffer.split())?;
        Ok(Some(v))
    }

    pub async fn write_value(&mut self, value: Frame) -> Result<()> {
        self.stream.write_all(value.serialize().as_bytes()).await?;
        self.stream.flush().await?;

        Ok(())
    }

    pub async fn write(&mut self, contents: &[u8]) -> Result<()> {
        self.stream.write(contents).await?;
        self.stream.flush().await?;

        Ok(())
    }
}
