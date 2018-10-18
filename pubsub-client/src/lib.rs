use std::error::Error as StdError;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{Ordering, AtomicBool};
use std::time::{Duration, Instant};
use std::thread::{self, JoinHandle};
use std::io::{self, Read};
use std::net::{ToSocketAddrs, SocketAddr, UdpSocket};

#[macro_use]
extern crate quick_error;
#[macro_use]
extern crate debug_stub_derive;
#[macro_use]
extern crate log;
extern crate bytes;
#[macro_use]
extern crate crossbeam_channel;

extern crate pubsub_common as common;

use bytes::{BufMut, Bytes};
use crossbeam_channel as channel;
use channel::Sender;

use common::constants;
use common::util::{self, BufferProvider};
pub use common::timer::TimerManager;
use common::packet::{PacketType, Packet};
use common::protocol::{Timeout, KeepaliveManager, Jqtt};

pub mod messaging;
pub use messaging::{Message, MessageListener, MessageCollector, CompleteMessageListener};
use messaging::OutgoingMessage;

const BUFFER_PREALLOC_SIZE: usize = 1024 * constants::IPV6_MAX_PACKET_SIZE;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Io(err: io::Error) {
            from()
            display("io error: {}", err)
            description(err.description())
            cause(err)
        }
        Protocol(err: common::Error) {
            from()
            display("protocol error: {}", err)
            description(err.description())
            cause(err)
        }
        ConnectionFailed {
            description("connection failed")
        }
        ClientOnly(packet_type: PacketType) {
            description("received packet which can only be sent by clients")
            display("received packet which can only be sent by clients: {}", packet_type)
        }
        ReceivedDisconnect {
            description("received disconnect packet from server")
        }
        InvalidPublishState(started: bool) {
            description("incorrect type of incoming message packet for current state")
            display("incoming message {} started", match started {
                true => "already",
                false => "not",
            })
        }
        MessageTooBig(expected: u32, actual: u32) {
            description("message is bigger than specified")
            display("message is {} bytes, specified as {}", actual, expected)
        }
        MessageAlreadyQueued {
            description("a message is already queued for publishing")
        }
        Custom(err: Box<StdError>) {
            from()
            display("{}", err)
            description(err.description())
        }
    }
}

enum InternalMessage {
    Packet(Bytes),
    IoError(io::Error),
    Shutdown,
}

struct ClientInner<M: MessageListener + Send> {
    remote: SocketAddr,
    protocol: Arc<Mutex<Jqtt>>,

    keepalive: Option<KeepaliveManager>,
    listener: M,
}
impl<M: MessageListener + Send> ClientInner<M> {
    fn new(remote: SocketAddr, protocol: &Arc<Mutex<Jqtt>>, listener: M, keepalive: Option<KeepaliveManager>) -> ClientInner<M> {
        ClientInner {
            remote,
            protocol: Arc::clone(protocol),

            keepalive,
            listener,
        }
    }

    fn dispatch_error(&mut self, error: Error) {
        self.listener.on_error(error);
    }
    fn handle(&mut self, data: &Bytes) -> Result<bool, Error> {
        use Packet::*;

        let mut protocol = self.protocol.lock().unwrap();
        match protocol.decode(data) {
            Err(e@common::Error::InvalidAck(_, _, _)) | Err(e@common::Error::OutOfOrder(_, _)) => warn!("{}", e),
            Err(e) => return Err(e.into()),
            Ok(p) => match p {
                Connect => return Err(Error::ClientOnly(PacketType::Connect)),
                ConnAck(_) => {},
                Heartbeat => match self.keepalive {
                    Some(ref mut manager) => manager.heartbeat(),
                    None => warn!("received heartbeat from {}, but keepalive is disabled", self.remote),
                },
                Ack(_) => {},
                Disconnect => return Err(Error::ReceivedDisconnect),
                Subscribe(_) => return Err(Error::ClientOnly(PacketType::Subscribe)),
                Unsubscribe(_) => return Err(Error::ClientOnly(PacketType::Unsubscribe)),
                PublishStart(start) => self.listener.recv_start(start)?,
                PublishData(segment) => self.listener.recv_data(segment)?,
            },
        }

        Ok(false)
    }
}


#[derive(DebugStub)]
pub struct Client<'a> {
    buf_source: BufferProvider,
    protocol: Arc<Mutex<Jqtt>>,

    running: Arc<AtomicBool>,
    io_thread: JoinHandle<()>,
    inner_tx: Sender<InternalMessage>,
    inner_thread: JoinHandle<()>,
    #[debug_stub = "VecDeque<OutgoingMessage>"]
    messages: VecDeque<OutgoingMessage<'a>>,
}
impl<'a> Client<'a> {
    fn udp_connect<A: ToSocketAddrs>(addr: A) -> io::Result<(SocketAddr, UdpSocket)> {
        let mut remote = None;
        let mut final_socket = None;
        for addr in addr.to_socket_addrs()? {
            let socket = UdpSocket::bind(match addr {
                SocketAddr::V4(_) => "127.0.0.0:0",
                SocketAddr::V6(_) => ":::0",
            })?;
            socket.connect(addr)?;

            remote = Some(addr);
            final_socket = Some(socket);
            break;
        };

        Ok((remote.unwrap(), final_socket.unwrap()))
    }

    #[inline]
    pub fn connect<A: ToSocketAddrs, M: MessageListener + Send + Sync + 'static>(server: A, msg_listener: M) -> Result<Client<'a>, Error> {
        Client::connect_timers(&TimerManager::new(), server, msg_listener)
    }
    pub fn connect_timers<A: ToSocketAddrs, M: MessageListener + Send + Sync + 'static>(timers: &TimerManager<Timeout>, server: A, listener: M) -> Result<Client<'a>, Error> {
        let (remote, socket) = Client::udp_connect(server)?;
        socket.set_read_timeout(Some(Duration::from_millis(250)))?;

        let buf_source = BufferProvider::new(BUFFER_PREALLOC_SIZE, util::max_packet_size(remote));
        let (timeout_tx, timeout_rx) = channel::unbounded();

        let protocol = Arc::new(Mutex::new(Jqtt::new(timers, &buf_source, remote, socket.try_clone().unwrap(), &timeout_tx)));
        let mut keepalive = None;
        for _ in 0..constants::CONNECT_RETRIES {
            let mut protocol = protocol.lock().unwrap();
            protocol.send_connect()?;

            let mut response = buf_source.allocate(util::max_packet_size(remote));
            unsafe {
                let received = socket.recv(response.bytes_mut())?;
                response.advance_mut(received);
                response.truncate(received);
            }
            match protocol.decode(&response.freeze()) {
                Ok(Packet::ConnAck(ka)) => {
                    keepalive = Some(ka);
                    break;
                },
                Ok(p) => warn!("received unexpected packet from server while trying to connect: {:?}", p),
                Err(e) => error!("failed to connect to {}, retrying... ({})", remote, e),
            }
        }

        if let None = keepalive {
            return Err(Error::ConnectionFailed);
        }
        let keepalive = keepalive.unwrap();
        if keepalive {
            timeout_tx.send(Timeout::SendHeartbeat(remote));
        }

        let (inner_tx, inner_rx) = channel::unbounded();
        let running = Arc::new(AtomicBool::new(true));
        let io_thread = {
            let running = Arc::clone(&running);
            let buf_source = buf_source.clone();
            let inner_tx = inner_tx.clone();
            thread::spawn(move || {
                while running.load(Ordering::SeqCst) {
                    let mut packet_buffer = buf_source.allocate(util::max_packet_size(remote));

                    let size = unsafe {
                        let size = match socket.recv(packet_buffer.bytes_mut()) {
                            Ok(size) => size,
                            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                            Err(e) => {
                                inner_tx.send(InternalMessage::IoError(e));
                                running.store(false, Ordering::SeqCst);
                                break;
                            }
                        };
                        packet_buffer.advance_mut(size);
                        size
                    };

                    packet_buffer.truncate(size);
                    inner_tx.send(InternalMessage::Packet(packet_buffer.freeze()));
                }
            })
        };

        let inner_thread = {
            let io_running = Arc::clone(&running);

            let keepalive = match keepalive {
                true => Some(KeepaliveManager::new(&timers, &timeout_tx, remote)),
                false => None,
            };
            let mut inner = ClientInner::new(remote, &protocol, listener, keepalive);
            let protocol = Arc::clone(&protocol);

            let timers = timers.clone();
            let timers_id = timers.register(&timeout_tx);
            thread::spawn(move || {
                let mut running = true;
                while running {
                    select! {
                        recv(inner_rx, msg) => {
                            if let Some(msg) = msg {
                                match msg {
                                    InternalMessage::Packet(data) => if let Err(e) = inner.handle(&data) {
                                        io_running.store(false, Ordering::SeqCst);
                                        running = false;
                                        inner.dispatch_error(e);
                                    },
                                    InternalMessage::IoError(e) => {
                                        inner.dispatch_error(e.into());
                                        if let Err(e) = protocol.lock().unwrap().send_disconnect() {
                                            inner.dispatch_error(e.into());
                                        }
                                        running = false;
                                    },
                                    InternalMessage::Shutdown => {
                                        if let Err(e) = protocol.lock().unwrap().send_disconnect() {
                                            inner.dispatch_error(e.into());
                                        }
                                        running = false;
                                    },
                                }
                            }
                        },
                        recv(timeout_rx, msg) => {
                            if let Some(msg) = msg {
                                match msg {
                                    Timeout::Ack(_) => {
                                        warn!("server ack timed out, re-sending packets in window");
                                        if let Err(e) = protocol.lock().unwrap().handle_ack_timeout() {
                                            error!("error while handling ack timeout: {}", e);
                                        }
                                    },
                                    Timeout::Keepalive(_) => {
                                        error!("server timed out, disconnecting...");
                                        io_running.store(false, Ordering::SeqCst);
                                        if let Err(e) = protocol.lock().unwrap().send_disconnect() {
                                            inner.dispatch_error(e.into());
                                        }
                                        running = false;
                                    }
                                    Timeout::SendHeartbeat(_) => {
                                        if let Err(e) = protocol.lock().unwrap().send_heartbeat() {
                                            io_running.store(false, Ordering::SeqCst);
                                            running = false;
                                            inner.dispatch_error(e.into());
                                        } else {
                                            timers.post_message(timers_id, Timeout::SendHeartbeat(remote), Instant::now() + (constants::KEEPALIVE_TIMEOUT / 2));
                                        }
                                    },
                                }
                            }
                        }
                    }
                }
            })
        };

        Ok(Client {
            buf_source: buf_source.clone(),
            protocol,

            running,
            io_thread,
            inner_tx,
            inner_thread,
            messages: VecDeque::new(),
        })
    }
    pub fn stop(self) {
        self.running.store(false, Ordering::SeqCst);
        self.io_thread.join().unwrap();

        self.inner_tx.send(InternalMessage::Shutdown);
        self.inner_thread.join().unwrap();
    }

    pub fn subscribe(&self, topic: &str) -> Result<(), Error> {
        self.protocol.lock().unwrap().send_subscribe(topic)?;
        Ok(())
    }
    pub fn unsubscribe(&self, topic: &str) -> Result<(), Error> {
        self.protocol.lock().unwrap().send_unsubscribe(topic)?;
        Ok(())
    }

    pub fn queue_message<R: Read + Send + 'a>(&mut self, message: Message<R>) {
        self.messages.push_back(OutgoingMessage::new(&self.buf_source, message));
    }
    /*fn drain_messages(&mut self) -> Result<(), Error> {
        if self.protocol.buffer_full()() {
            return Ok(());
        }

        for _ in 0..self.messages.len() {
            if {
                let mut msg = self.messages.front_mut().unwrap();
                loop {
                    if !msg.first_sent() {
                        let data = msg.first_packet(self.protocol.send_seq());
                        self.protocol.gbn_send(data)?;
                    } else {
                        let (finished, data) = msg.next_packet(util::max_packet_size(self.remote), self.protocol.send_seq())?;
                        if let Some(data) = data {
                            if self.protocol.gbn_send(data)? {
                                break finished;
                            }
                        }
                    }
                }
            } {
                self.messages.pop_front().unwrap();
            }
        }

        Ok(())
    }*/
}
