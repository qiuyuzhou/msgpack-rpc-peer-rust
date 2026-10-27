use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Pair independently owned asynchronous read and write handles.
///
/// [`PeerBuilder::start`](crate::PeerBuilder::start) accepts any owned
/// `AsyncRead + AsyncWrite + Unpin + Send + 'static` stream; Unix sockets use
/// the same API on supported platforms. For subprocess RPC, pass the child's
/// stdout as `reader` and stdin as `writer`. This adapter owns only those
/// handles: the application retains process ownership. Reserve subprocess
/// stdout for RPC bytes and send diagnostics to stderr.
#[derive(Debug)]
pub struct SplitIo<R, W> {
    reader: R,
    writer: W,
}

impl<R, W> SplitIo<R, W> {
    /// Combine handles without adding buffering or transport policy.
    pub fn new(reader: R, writer: W) -> Self {
        Self { reader, writer }
    }
}

impl<R: AsyncRead + Unpin, W: Unpin> AsyncRead for SplitIo<R, W> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().reader).poll_read(cx, buf)
    }
}

impl<R: Unpin, W: AsyncWrite + Unpin> AsyncWrite for SplitIo<R, W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().writer).poll_write(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().writer).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().writer).poll_shutdown(cx)
    }
}
