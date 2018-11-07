use std::error::Error as StdError;
use std::sync::{Arc, Mutex, Condvar};
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

macro_rules! await_sendbuf_space {
    ($condvar:expr, $running:expr, $space:expr) => {{
        let &(ref lock, ref cvar) = &*$condvar;
        let mut avail = lock.lock().unwrap();
        while *avail < $space {
            avail = cvar.wait(avail).unwrap();
            if !$running.load(Ordering::SeqCst) {
                return Err(Error::Interrupted);
            }
        }
    }};
    ($condvar:expr, $running:expr) => (await_sendbuf_space!($condvar, $running, 1));
}
macro_rules! await_packet_ack {
    ($condvar:expr, $running:expr, $pid:expr) => (await_sendbuf_space!($condvar, $running, $pid));
}

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
        InvalidPublishState(id: u32, started: bool) {
            description("incorrect type of incoming message packet for current state")
            display("incoming message {} {} started", id, match started {
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
        Interrupted {
            description("action interrupted by shutdown")
        }
        Shutdown {
            description("client is shut down")
        }
        Std(err: Box<StdError>) {
            from()
            display("{}", err)
            description(err.description())
        }
        CustomStatic(msg: &'static str) {
            from()
            description(msg)
            display("{}", msg)
        }
        Custom(msg: String) {
            from()
            display("{}", msg)
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

    acked_pid: Arc<(Mutex<usize>, Condvar)>,
    send_space: Arc<(Mutex<usize>, Condvar)>,
    next_msg_id: u32,
    #[debug_stub = "VecDeque<OutgoingMessage>"]
    messages: Vec<OutgoingMessage<'a>>,
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

        let (inner_tx, inner_rx) = channel::unbounded();
        let running = Arc::new(AtomicBool::new(true));
        let io_thread = {
            let running = Arc::clone(&running);
            let buf_source = buf_source.clone();
            let inner_tx = inner_tx.clone();

            let (acked_pid, send_space) = {
                let protocol = protocol.lock().unwrap();
                (protocol.ack_condvar(), protocol.send_space_condvar())
            };
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

                let &(ref _lock, ref cvar) = &*send_space;
                cvar.notify_all();
                let &(ref _lock, ref cvar) = &*acked_pid;
                cvar.notify_all();
            })
        };

        let inner_thread = {
            let io_running = Arc::clone(&running);

            let heartbeat_timer = match keepalive {
                true => {
                    let timeout_tx = timeout_tx.clone();
                    Some(timers.post_new(Timeout::SendHeartbeat(remote), move |msg| {
                        timeout_tx.send(msg);
                    }, Instant::now()))
                },
                false => None,
            };
            let keepalive = match keepalive {
                true => Some(KeepaliveManager::new(&timers, &timeout_tx, remote)),
                false => None,
            };
            let mut inner = ClientInner::new(remote, &protocol, listener, keepalive);
            let protocol = Arc::clone(&protocol);

            let timers = timers.clone();
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
                                            let timer = heartbeat_timer.as_ref().unwrap();
                                            timer.set_message(Timeout::SendHeartbeat(remote));
                                            timers.reschedule(timer, Instant::now() + (constants::KEEPALIVE_TIMEOUT / 2));
                                        }
                                    },
                                }
                            }
                        }
                    }
                }
            })
        };

        let (acked_pid, send_space) = {
            let protocol = protocol.lock().unwrap();
            (protocol.ack_condvar(), protocol.send_space_condvar())
        };
        Ok(Client {
            buf_source: buf_source.clone(),
            protocol,

            running,
            io_thread,
            inner_tx,
            inner_thread,

            acked_pid,
            send_space,
            next_msg_id: 0,
            messages: Vec::new(),
        })
    }
    pub fn stop(self) {
        self.running.store(false, Ordering::SeqCst);
        self.io_thread.join().unwrap();

        self.inner_tx.send(InternalMessage::Shutdown);
        self.inner_thread.join().unwrap();
    }

    pub fn subscribe(&self, topic: &str) -> Result<(), Error> {
        if !self.running.load(Ordering::SeqCst) {
            return Err(Error::Shutdown);
        }
        await_sendbuf_space!(self.send_space, self.running);

        let pid = self.protocol.lock().unwrap().send_subscribe(topic)?;
        await_packet_ack!(self.acked_pid, self.running, pid);

        Ok(())
    }
    pub fn unsubscribe(&self, topic: &str) -> Result<(), Error> {
        if !self.running.load(Ordering::SeqCst) {
            return Err(Error::Shutdown);
        }
        await_sendbuf_space!(self.send_space, self.running);

        let pid = self.protocol.lock().unwrap().send_unsubscribe(topic)?;
        await_packet_ack!(self.acked_pid, self.running, pid);

        Ok(())
    }

    pub fn queue_message<R: Read + Send + 'a>(&mut self, message: Message<R>) -> Result<(), Error> {
        if !self.running.load(Ordering::SeqCst) {
            return Err(Error::Shutdown);
        }

        self.messages.push(OutgoingMessage::new(&self.buf_source, message, self.next_msg_id));
        self.next_msg_id = self.next_msg_id.wrapping_add(1);
        Ok(())
    }
    pub fn drain_messages(&mut self) -> Result<(), Error> {
        if !self.running.load(Ordering::SeqCst) {
            return Err(Error::Shutdown);
        }

        while !self.messages.is_empty() {
            await_sendbuf_space!(self.send_space, self.running, 32);
            let mut protocol = self.protocol.lock().unwrap();

            let mut finished = false;
            let mut i = 0;
            let mut pid = 0;
            while !protocol.send_buffer_full() {
                {
                    let msg = self.messages.get_mut(i).unwrap();
                    if !msg.first_sent() {
                        let data = msg.first_packet(protocol.send_seq());
                        pid = protocol.gbn_send(data)?;
                    } else {
                        let (fin, data) = msg.next_packet(protocol.max_packet_size(), protocol.send_seq())?;
                        pid = protocol.gbn_send(data.unwrap())?;
                        if fin {
                            finished = true;
                            break;
                        }
                    }
                }

                i += 1;
                if i == self.messages.len() {
                    i = 0;
                }
            }

            if finished {
                self.messages.remove(i);
            }

            drop(protocol);
            await_packet_ack!(self.acked_pid, self.running, pid);
        }

        Ok(())
    }
}
