use super::{Transport, TransportListener, CloneableTransport};
use anyhow::Result;
use async_trait::async_trait;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct TcpTransport {
    reader: Arc<Mutex<OwnedReadHalf>>,
    writer: Arc<Mutex<OwnedWriteHalf>>,
}

impl TcpTransport {
    pub fn new(stream: TcpStream) -> Self {
        let (reader, writer) = stream.into_split();
        Self {
            reader: Arc::new(Mutex::new(reader)),
            writer: Arc::new(Mutex::new(writer)),
        }
    }

    pub async fn connect(addr: SocketAddr) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self::new(stream))
    }
}

#[async_trait]
impl Transport for TcpTransport {
    async fn send(&mut self, data: &[u8]) -> Result<()> {
        let len = data.len() as u32;
        let mut writer = self.writer.lock().await;
        writer.write_u32(len).await?;
        writer.write_all(data).await?;
        Ok(())
    }

    async fn receive(&mut self) -> Result<Vec<u8>> {
        let mut reader = self.reader.lock().await;
        let len = reader.read_u32().await?;
        let mut buf = vec![0u8; len as usize];
        reader.read_exact(&mut buf).await?;
        Ok(buf)
    }
}

// Removed explicit implementation of CloneableTransport for TcpTransport
// because the blanket implementation in mod.rs covers it since TcpTransport implements Clone.

pub struct TcpTransportListener {
    listener: TcpListener,
}

impl TcpTransportListener {
    pub async fn bind(addr: SocketAddr) -> Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self { listener })
    }
}

#[async_trait]
impl TransportListener for TcpTransportListener {
    async fn accept(&mut self) -> Result<Box<dyn CloneableTransport>> {
        let (stream, _) = self.listener.accept().await?;
        Ok(Box::new(TcpTransport::new(stream)))
    }
}
