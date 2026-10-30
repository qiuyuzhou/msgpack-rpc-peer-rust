use super::Failure;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpStream},
};

pub(super) trait Stream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Stream for T {}
pub(super) type Socket = Box<dyn Stream>;

pub(super) enum Address {
    Tcp(String),
    #[cfg(unix)]
    Unix(std::path::PathBuf),
}
impl Address {
    pub(super) fn parse(text: &str) -> Result<Self, Failure> {
        if let Some(address) = text.strip_prefix("tcp://") {
            if let Some((host, port)) = address.rsplit_once(':') {
                if !host.is_empty() && !host.contains('/') && port.parse::<u16>().is_ok() {
                    return Ok(Self::Tcp(address.into()));
                }
            }
        }
        #[cfg(unix)]
        if let Some(path) = text.strip_prefix("unix://") {
            if std::path::Path::new(path).is_absolute() {
                return Ok(Self::Unix(path.into()));
            }
        }
        Err("address must be tcp://host:port or unix:///absolute/path".into())
    }
    pub(super) async fn connect(&self) -> Result<Socket, Failure> {
        match self {
            Self::Tcp(address) => Ok(Box::new(TcpStream::connect(address).await?)),
            #[cfg(unix)]
            Self::Unix(path) => Ok(Box::new(tokio::net::UnixStream::connect(path).await?)),
        }
    }
}
pub(super) enum Listener {
    Tcp(TcpListener),
    #[cfg(unix)]
    Unix(tokio::net::UnixListener),
}
impl Listener {
    pub(super) async fn bind(address: Address) -> Result<Self, Failure> {
        match address {
            Address::Tcp(address) => Ok(Self::Tcp(TcpListener::bind(address).await?)),
            #[cfg(unix)]
            Address::Unix(path) => Ok(Self::Unix(tokio::net::UnixListener::bind(path)?)),
        }
    }
    pub(super) async fn accept(&self) -> Result<Socket, Failure> {
        match self {
            Self::Tcp(listener) => Ok(Box::new(listener.accept().await?.0)),
            #[cfg(unix)]
            Self::Unix(listener) => Ok(Box::new(listener.accept().await?.0)),
        }
    }
}
