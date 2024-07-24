use nix::NixPath;
pub use socket2::TcpKeepalive;

use crate::network::adapter::{
    Resource, Remote, Local, Adapter, SendStatus, AcceptedType, ReadStatus, ConnectionInfo,
    ListeningInfo, PendingStatus,
};
use crate::network::{RemoteAddr, Readiness, TransportConnect, TransportListen};
use crate::util::encoding::{self, Decoder, MAX_ENCODED_SIZE};

use mio::net::{TcpListener, TcpStream};
use mio::event::Source;

use socket2::Socket;

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::io::{self, ErrorKind, Read, Write};
use std::cell::RefCell;
use std::mem::{forget, MaybeUninit};
#[cfg(target_os = "windows")]
use std::os::windows::io::{FromRawSocket, AsRawSocket};
#[cfg(not(target_os = "windows"))]
use std::os::{fd::AsRawFd, unix::io::FromRawFd};
use std::sync::Arc;

const INPUT_BUFFER_SIZE: usize = u16::MAX as usize; // 2^16 - 1

#[derive(Clone, Debug, Default)]
pub struct AsyncFramedTcpConnectConfig {
    keepalive: Option<TcpKeepalive>,
    nodelay: Option<bool>,
}

impl AsyncFramedTcpConnectConfig {
    /// Enables TCP keepalive settings on the socket.
    pub fn with_keepalive(mut self, keepalive: TcpKeepalive) -> Self {
        self.keepalive = Some(keepalive);
        self
    }

    /// Sets TCP no_delay option
    pub fn with_nodelay(mut self, nodelay: bool) -> Self {
        self.nodelay = Some(nodelay);
        self
    }
}

#[derive(Clone, Debug, Default)]
pub struct AsyncFramedTcpListenConfig {
    keepalive: Option<TcpKeepalive>,
    nodelay: Option<bool>,
}

impl AsyncFramedTcpListenConfig {
    /// Enables TCP keepalive settings on client connection sockets.
    pub fn with_keepalive(mut self, keepalive: TcpKeepalive) -> Self {
        self.keepalive = Some(keepalive);
        self
    }

    /// Enables TCP keepalive settings on client connection sockets.
    pub fn with_nodelay(mut self, nodelay: bool) -> Self {
        self.nodelay = Some(nodelay);
        self
    }
}

pub(crate) struct AsyncFramedTcpAdapter;
impl Adapter for AsyncFramedTcpAdapter {
    type Remote = RemoteResource;
    type Local = LocalResource;
}

pub(crate) struct RemoteResource {
    stream: TcpStream,
    decoder: RefCell<Decoder>,
    queue: RefCell<VecDeque<EncodedMessage>>,
    keepalive: Option<TcpKeepalive>,
    nodelay: Option<bool>,
}

// SAFETY:
// That RefCell<Decoder> can be used with Sync because the decoder is only used in the read_event,
// that will be called always from the same thread. This way, we save the cost of a Mutex.
unsafe impl Sync for RemoteResource {}

impl RemoteResource {
    fn new(stream: TcpStream, keepalive: Option<TcpKeepalive>, nodelay: Option<bool>) -> Self {
        Self {
            stream,
            decoder: RefCell::new(Decoder::default()),
            queue: RefCell::new(Default::default()),
            keepalive,
            nodelay,
        }
    }
}

impl Resource for RemoteResource {
    fn source(&mut self) -> &mut dyn Source {
        &mut self.stream
    }
}

impl Remote for RemoteResource {
    fn connect_with(
        config: TransportConnect,
        remote_addr: RemoteAddr,
    ) -> io::Result<ConnectionInfo<Self>> {
        let config = match config {
            TransportConnect::AsyncFramedTcp(config) => config,
            _ => panic!("Internal error: Got wrong config"),
        };
        let peer_addr = *remote_addr.socket_addr();
        let stream = TcpStream::connect(peer_addr)?;
        let local_addr = stream.local_addr()?;
        Ok(ConnectionInfo {
            remote: RemoteResource::new(stream, config.keepalive, config.nodelay),
            local_addr,
            peer_addr,
        })
    }

    fn receive(&self, mut process_data: impl FnMut(&[u8])) -> ReadStatus {
        let buffer: MaybeUninit<[u8; INPUT_BUFFER_SIZE]> = MaybeUninit::uninit();
        let mut input_buffer = unsafe { buffer.assume_init() }; // Avoid to initialize the array

        loop {
            let mut stream = &self.stream;
            match stream.read(&mut input_buffer) {
                Ok(0) => break ReadStatus::Disconnected,
                Ok(size) => {
                    let data = &input_buffer[..size];
                    log::trace!("Decoding {} bytes", data.len());
                    self.decoder.borrow_mut().decode(data, |decoded_data| {
                        process_data(decoded_data);
                    });
                }
                Err(ref err) if err.kind() == ErrorKind::Interrupted => continue,
                Err(ref err) if err.kind() == ErrorKind::WouldBlock => {
                    break ReadStatus::WaitNextEvent
                }
                Err(ref err) if err.kind() == ErrorKind::ConnectionReset => {
                    break ReadStatus::Disconnected
                }
                Err(err) => {
                    log::error!("TCP receive error: {}", err);
                    break ReadStatus::Disconnected // should not happen
                }
            }
        }
    }

    fn send(&self, data: &[u8]) -> SendStatus {
        {
            let mut queue = self.queue.borrow_mut();
            queue.push_back(EncodedMessage::new(Arc::new(data.to_owned())));
        }

        self.try_write()
    }

    fn send_arc(&self, msg: Arc<Vec<u8>>) -> SendStatus {
        {
            let mut queue = self.queue.borrow_mut();
            queue.push_back(EncodedMessage::new(msg));
        }
        self.try_write()
    }

    fn pending(&self, _readiness: Readiness) -> PendingStatus {
        let status = super::tcp::check_stream_ready(&self.stream);

        if status == PendingStatus::Ready {
            if let Some(keepalive) = &self.keepalive {
                #[cfg(target_os = "windows")]
                let socket = unsafe { Socket::from_raw_socket(self.stream.as_raw_socket()) };
                #[cfg(not(target_os = "windows"))]
                let socket = unsafe { Socket::from_raw_fd(self.stream.as_raw_fd()) };

                if let Err(e) = socket.set_tcp_keepalive(keepalive) {
                    log::warn!("TCP set keepalive error: {}", e);
                }

                // Don't drop so the underlying socket is not closed.
                forget(socket);
            }

            if let Some(nodelay) = self.nodelay {
                #[cfg(target_os = "windows")]
                let socket = unsafe { Socket::from_raw_socket(self.stream.as_raw_socket()) };
                #[cfg(not(target_os = "windows"))]
                let socket = unsafe { Socket::from_raw_fd(self.stream.as_raw_fd()) };

                if let Err(e) = socket.set_nodelay(nodelay) {
                    log::warn!("TCP set nodelay error: {}", e);
                }

                // Don't drop so the underlying socket is not closed.
                forget(socket);
            }
        }

        status
    }

    fn ready_to_write(&self) -> bool {
        self.try_write() == SendStatus::Sent
    }
}

impl RemoteResource {
    pub fn try_write(&self) -> SendStatus {
        let mut queue = self.queue.borrow_mut();
        let mut stream = &self.stream;

        while let Some(msg) = queue.front_mut() {
            let bytes_to_send = match msg.bytes_to_send() {
                Some(bytes_to_send) => bytes_to_send,
                None => {
                    queue.pop_front();
                    break;
                }
            };

            match stream.write(bytes_to_send) {
                Ok(bytes_sent) => {
                    msg.bytes_sent(bytes_sent);
                }
                Err(ref err) if err.kind() == io::ErrorKind::WouldBlock => {
                    return SendStatus::Sent;
                }
                Err(err) => {
                    log::error!("TCP receive error: {}", err);
                    return SendStatus::ResourceNotFound;
                }
            }
        }
        SendStatus::Sent
    }
}

pub(crate) struct LocalResource {
    listener: TcpListener,
    keepalive: Option<TcpKeepalive>,
    nodelay: Option<bool>,
}

impl Resource for LocalResource {
    fn source(&mut self) -> &mut dyn Source {
        &mut self.listener
    }
}

impl Local for LocalResource {
    type Remote = RemoteResource;

    fn listen_with(config: TransportListen, addr: SocketAddr) -> io::Result<ListeningInfo<Self>> {
        let config = match config {
            TransportListen::AsyncFramedTcp(config) => config,
            _ => panic!("Internal error: Got wrong config"),
        };
        let listener = TcpListener::bind(addr)?;
        let local_addr = listener.local_addr().unwrap();
        Ok(ListeningInfo {
            local: {
                LocalResource { listener, keepalive: config.keepalive, nodelay: config.nodelay }
            },
            local_addr,
        })
    }

    fn accept(&self, mut accept_remote: impl FnMut(AcceptedType<'_, Self::Remote>)) {
        loop {
            match self.listener.accept() {
                Ok((stream, addr)) => accept_remote(AcceptedType::Remote(
                    addr,
                    RemoteResource::new(stream, self.keepalive.clone(), self.nodelay),
                )),
                Err(ref err) if err.kind() == ErrorKind::WouldBlock => break,
                Err(ref err) if err.kind() == ErrorKind::Interrupted => continue,
                Err(err) => break log::error!("TCP accept error: {}", err), // Should not happen
            }
        }
    }
}

pub struct EncodedMessage {
    encoded_size_buf: [u8; MAX_ENCODED_SIZE],
    encoded_size: u8,
    message: Arc<Vec<u8>>,
    total_bytes_sent: usize,
}

impl EncodedMessage {
    pub fn new(message: Arc<Vec<u8>>) -> Self {
        let mut length_buf = Default::default();
        let encoded_size = encoding::encode_size(&message, &mut length_buf).len() as u8;

        Self { encoded_size_buf: length_buf, encoded_size, message, total_bytes_sent: 0 }
    }

    pub fn total_bytes(&self) -> usize {
        self.encoded_size as usize + self.message.len()
    }

    pub fn bytes_to_send(&self) -> Option<&[u8]> {
        let encoded_size = self.encoded_size as usize;
        let total_bytes_sent = self.total_bytes_sent;
        let total_size = self.total_bytes();
        if total_bytes_sent >= total_size {
            return None;
        }

        Some(match total_bytes_sent < encoded_size {
            true => &self.encoded_size_buf[total_bytes_sent..encoded_size],
            false => &self.message.as_slice()[(total_bytes_sent - encoded_size)..],
        })
    }

    pub fn bytes_sent(&mut self, bytes_sent: usize) {
        self.total_bytes_sent += bytes_sent;
    }
}
