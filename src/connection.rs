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

    pub async fn read_frames(&mut self) -> Result<Option<Vec<Frame>>> {
        let bytes_read = self.stream.read_buf(&mut self.buffer).await?;

        if bytes_read == 0 {
            return Ok(None);
        }

        let mut frames: Vec<Frame> = vec![];
        let mut consumed_bytes = 0;

        while consumed_bytes != bytes_read {
            let (frame, bytes) =
                Frame::parse_message(BytesMut::from(&self.buffer[consumed_bytes..]))?;

            frames.push(frame.clone());
            consumed_bytes += bytes;
        }

        self.buffer.clear();
        Ok(Some(frames))
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> Result<()> {
        self.stream.write_all(frame.serialize().as_bytes()).await?;
        self.stream.flush().await?;

        Ok(())
    }

    pub async fn write(&mut self, contents: &[u8]) -> Result<()> {
        self.stream.write(contents).await?;
        self.stream.flush().await?;

        Ok(())
    }
}
