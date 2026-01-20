use super::{Transport, TransportListener, CloneableTransport};
use anyhow::Result;
use async_trait::async_trait;
use std::path::PathBuf;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[cfg(unix)]
use tokio::net::{UnixListener, UnixStream};
use std::sync::Arc;
use tokio::sync::Mutex;

#[cfg(unix)]
#[derive(Clone)]
pub struct UnixTransport {
    reader: Arc<Mutex<tokio::net::unix::OwnedReadHalf>>,
    writer: Arc<Mutex<tokio::net::unix::OwnedWriteHalf>>,
}

#[cfg(unix)]
impl UnixTransport {
    pub fn new(stream: UnixStream) -> Self {
        let (reader, writer) = stream.into_split();
        Self {
            reader: Arc::new(Mutex::new(reader)),
            writer: Arc::new(Mutex::new(writer)),
        }
    }

    pub async fn connect(path: PathBuf) -> Result<Self> {
        let stream = UnixStream::connect(path).await?;
        Ok(Self::new(stream))
    }
}

#[cfg(unix)]
#[async_trait]
impl Transport for UnixTransport {
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

// Removed explicit implementation of CloneableTransport for UnixTransport
// because the blanket implementation in mod.rs covers it since UnixTransport implements Clone.

#[cfg(unix)]
pub struct UnixTransportListener {
    listener: UnixListener,
}

#[cfg(unix)]
impl UnixTransportListener {
    pub fn bind(path: PathBuf) -> Result<Self> {
        // Ensure socket doesn't exist
        if path.exists() {
            std::fs::remove_file(&path)?;
        }
        let listener = UnixListener::bind(path)?;
        Ok(Self { listener })
    }
}

#[cfg(unix)]
#[async_trait]
impl TransportListener for UnixTransportListener {
    async fn accept(&mut self) -> Result<Box<dyn CloneableTransport>> {
        let (stream, _) = self.listener.accept().await?;
        Ok(Box::new(UnixTransport::new(stream)))
    }
}
