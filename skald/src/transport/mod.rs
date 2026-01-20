use anyhow::Result;
use async_trait::async_trait;

pub mod tcp;
pub mod tls;
#[cfg(unix)]
pub mod unix;

pub use tcp::{TcpTransport, TcpTransportListener};
pub use tls::TlsTransport;
#[cfg(unix)]
pub use unix::{UnixTransport, UnixTransportListener};

#[async_trait]
pub trait Transport: Send + Sync {
    async fn send(&mut self, data: &[u8]) -> Result<()>;
    async fn receive(&mut self) -> Result<Vec<u8>>;
}

// Clone trait for Transport to allow sharing in Sagas
pub trait CloneableTransport: Transport + Send + Sync {
    fn clone_box(&self) -> Box<dyn CloneableTransport>;
}

impl<T> CloneableTransport for T
where
    T: Transport + Clone + Send + Sync + 'static,
{
    fn clone_box(&self) -> Box<dyn CloneableTransport> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn CloneableTransport> {
    fn clone(&self) -> Box<dyn CloneableTransport> {
        self.clone_box()
    }
}

#[async_trait]
pub trait TransportListener: Send + Sync {
    // Updated to return CloneableTransport to support server-side cloning
    async fn accept(&mut self) -> Result<Box<dyn CloneableTransport>>;
}
