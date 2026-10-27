use bytes::BytesMut;
use chorus_proto::{
    ClientCommand, MAX_FRAME_LEN, PROTOCOL_VERSION, Reader, ServerCommand, encode_frame,
    split_frame,
};
use std::io;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        TcpStream, ToSocketAddrs,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
};

pub use chorus_proto as proto;

pub struct Connection {
    reader: FrameReader,
    writer: FrameWriter,
}

impl Connection {
    pub async fn connect(addr: impl ToSocketAddrs) -> io::Result<Self> {
        Self::handshake(TcpStream::connect(addr).await?, PROTOCOL_VERSION).await
    }

    pub async fn handshake(stream: TcpStream, version: u8) -> io::Result<Self> {
        stream.set_nodelay(true)?;
        let (rd, mut wr) = stream.into_split();
        wr.write_all(&[version]).await?;
        Ok(Self {
            reader: FrameReader::new(rd),
            writer: FrameWriter { wr },
        })
    }

    pub async fn send(&mut self, cmd: &ClientCommand) -> io::Result<()> {
        self.writer.send(cmd).await
    }

    pub async fn recv(&mut self) -> io::Result<ServerCommand> {
        self.reader.recv().await
    }

    pub fn into_split(self) -> (FrameReader, FrameWriter) {
        (self.reader, self.writer)
    }
}

pub struct FrameReader {
    rd: OwnedReadHalf,
    buf: BytesMut,
}

impl FrameReader {
    fn new(rd: OwnedReadHalf) -> Self {
        Self {
            rd,
            buf: BytesMut::with_capacity(4096),
        }
    }

    /// Cancel-safe
    pub async fn recv(&mut self) -> io::Result<ServerCommand> {
        loop {
            match split_frame(&mut self.buf, MAX_FRAME_LEN) {
                Ok(Some(payload)) => {
                    return Reader::new(&payload)
                        .read()
                        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err));
                }
                Ok(None) => {}
                Err(err) => return Err(io::Error::new(io::ErrorKind::InvalidData, err)),
            }
            if self.buf.capacity() - self.buf.len() < 1024 {
                self.buf.reserve(8192);
            }
            if self.rd.read_buf(&mut self.buf).await? == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
        }
    }
}

pub struct FrameWriter {
    wr: OwnedWriteHalf,
}

impl FrameWriter {
    pub async fn send(&mut self, cmd: &ClientCommand) -> io::Result<()> {
        self.wr.write_all(&encode_frame(cmd)).await
    }

    pub async fn send_raw(&mut self, frame: &[u8]) -> io::Result<()> {
        self.wr.write_all(frame).await
    }

    pub async fn shutdown(&mut self) -> io::Result<()> {
        self.wr.shutdown().await
    }
}
