use super::Transport;
use anyhow::Result;
use async_trait::async_trait;
use rustls::ClientConfig;
use rustls::pki_types::ServerName;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

pub struct TlsTransport {
    stream: tokio_rustls::client::TlsStream<TcpStream>,
}

impl TlsTransport {
    pub async fn connect(
        addr: SocketAddr,
        domain: String,
        config: Arc<ClientConfig>,
    ) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        let connector = TlsConnector::from(config);
        let domain =
            ServerName::try_from(domain).map_err(|_| anyhow::anyhow!("Invalid domain name"))?;
        let stream = connector.connect(domain, stream).await?;
        Ok(Self { stream })
    }
}

#[async_trait]
impl Transport for TlsTransport {
    async fn send(&mut self, data: &[u8]) -> Result<()> {
        let len = data.len() as u32;
        self.stream.write_u32(len).await?;
        self.stream.write_all(data).await?;
        Ok(())
    }

    async fn receive(&mut self) -> Result<Vec<u8>> {
        let len = self.stream.read_u32().await?;
        let mut buf = vec![0u8; len as usize];
        self.stream.read_exact(&mut buf).await?;
        Ok(buf)
    }
}
