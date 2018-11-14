use std::error::Error as StdError;
use std::collections::HashSet;
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
extern crate crossbeam;

extern crate pubsub_common as common;

use bytes::{BufMut, Bytes, BytesMut};
use crossbeam::queue::MsQueue;

use common::constants;
use common::util::BufferProvider;
pub use common::timer::TimerManager;
use common::packet::{PacketType, Packet};
use common::protocol::{self, KeepaliveManager, Jqtt};

pub mod messaging;
pub use messaging::{Message, MessageListener, MessageCollector, CompleteMessageListener};
use messaging::OutgoingMessage;

const BUFFER_PREALLOC_COUNT: usize = 4;

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
        AlreadySubscribed(topic: String) {
            description("already subscribed")
            display("already subscribed to {}", topic)
        }
        NotSubscribed(topic: String) {
            description("not subscribed")
            display("not subscribed to {}", topic)
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
    Timeout(protocol::Timeout),
    IoError(io::Error),
    SendHeartbeat,
    Shutdown,
}
impl From<protocol::Timeout> for InternalMessage {
    fn from(timeout: protocol::Timeout) -> Self {
        InternalMessage::Timeout(timeout)
    }
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
                ConnAck(_, _) => {},
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
    max_packet_size: u16,

    running: Arc<AtomicBool>,
    io_thread: JoinHandle<()>,
    #[debug_stub = "Arc<MsQueue<InternalMessage>>"]
    queue: Arc<MsQueue<InternalMessage>>,
    inner_thread: JoinHandle<()>,

    acked_pid: Arc<(Mutex<usize>, Condvar)>,
    send_space: Arc<(Mutex<usize>, Condvar)>,
    subscriptions: HashSet<String>,
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
                SocketAddr::V4(_) => "0.0.0.0:0",
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
    pub fn connect_timers<A: ToSocketAddrs, M: MessageListener + Send + Sync + 'static>(timers: &TimerManager<protocol::Timeout>, server: A, listener: M) -> Result<Client<'a>, Error> {
        let (remote, socket) = Client::udp_connect(server)?;
        socket.set_read_timeout(Some(Duration::from_millis(250)))?;

        let buf_source = BufferProvider::new(std::u16::MAX as usize * BUFFER_PREALLOC_COUNT, std::u16::MAX as usize);

        let queue = Arc::new(MsQueue::new());
        let protocol = {
            let queue = Arc::clone(&queue);
            let callback = move |msg: protocol::Timeout| queue.push(msg.into());
            Arc::new(Mutex::new(Jqtt::new(timers, &buf_source, remote, socket.try_clone().unwrap(), callback)))
        };
        let mut keepalive = None;
        let mut max_packet_size = 0;
        for _ in 0..constants::CONNECT_RETRIES {
            let mut protocol = protocol.lock().unwrap();
            protocol.send_connect()?;

            let mut response = BytesMut::with_capacity(std::u16::MAX as usize);
            unsafe {
                let received = socket.recv(response.bytes_mut())?;
                response.advance_mut(received);
                response.truncate(received);
            }
            match protocol.decode(&response.freeze()) {
                Ok(Packet::ConnAck(ka, mps)) => {
                    keepalive = Some(ka);
                    max_packet_size = mps;
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

        let running = Arc::new(AtomicBool::new(true));
        let io_thread = {
            let running = Arc::clone(&running);
            let buf_source = buf_source.clone();
            let queue = Arc::clone(&queue);

            let (acked_pid, send_space) = {
                let protocol = protocol.lock().unwrap();
                (protocol.ack_condvar(), protocol.send_space_condvar())
            };
            thread::spawn(move || {
                while running.load(Ordering::SeqCst) {
                    let mut packet_buffer = buf_source.allocate(max_packet_size as usize);

                    let size = unsafe {
                        let size = match socket.recv(packet_buffer.bytes_mut()) {
                            Ok(size) => size,
                            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                            Err(e) => {
                                queue.push(InternalMessage::IoError(e));
                                running.store(false, Ordering::SeqCst);
                                break;
                            },
                        };
                        packet_buffer.advance_mut(size);
                        size
                    };

                    packet_buffer.truncate(size);
                    queue.push(InternalMessage::Packet(packet_buffer.freeze()));
                }

                let &(ref _lock, ref cvar) = &*send_space;
                cvar.notify_all();
                let &(ref _lock, ref cvar) = &*acked_pid;
                cvar.notify_all();
            })
        };

        let inner_thread = {
            let heartbeat_timer = match keepalive {
                true => {
                    let queue = Arc::clone(&queue);
                    Some(timers.post_new(protocol::Timeout::Keepalive(remote), move |_| queue.push(InternalMessage::SendHeartbeat), Instant::now()))
                },
                false => None,
            };
            let keepalive = match keepalive {
                true => {
                    let queue = Arc::clone(&queue);
                    Some(KeepaliveManager::new(&timers, move |msg| queue.push(msg.into()), remote))
                },
                false => None,
            };

            let mut inner = ClientInner::new(remote, &protocol, listener, keepalive);
            let queue = Arc::clone(&queue);
            let io_running = Arc::clone(&running);
            let protocol = Arc::clone(&protocol);
            let timers = timers.clone();
            thread::spawn(move || {
                let mut running = true;
                while running {
                    match queue.pop() {
                        InternalMessage::Packet(data) => if let Err(e) = inner.handle(&data) {
                            io_running.store(false, Ordering::SeqCst);
                            running = false;
                            inner.dispatch_error(e);
                        },
                        InternalMessage::Timeout(protocol::Timeout::Ack(_)) => {
                            warn!("server ack timed out, re-sending packets in window");
                            if let Err(e) = protocol.lock().unwrap().handle_ack_timeout() {
                                error!("error while handling ack timeout: {}", e);
                            }
                        },
                        InternalMessage::Timeout(protocol::Timeout::Keepalive(_)) => {
                            error!("server timed out, disconnecting...");
                            io_running.store(false, Ordering::SeqCst);
                            if let Err(e) = protocol.lock().unwrap().send_disconnect() {
                                inner.dispatch_error(e.into());
                            }
                            running = false;
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
                        InternalMessage::SendHeartbeat => {
                            if let Err(e) = protocol.lock().unwrap().send_heartbeat() {
                                io_running.store(false, Ordering::SeqCst);
                                running = false;
                                inner.dispatch_error(e.into());
                            } else {
                                let timer = heartbeat_timer.as_ref().unwrap();
                                timer.set_message(protocol::Timeout::Keepalive(remote));
                                timers.reschedule(timer, Instant::now() + (constants::KEEPALIVE_TIMEOUT / 2));
                            }
                        },
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
            max_packet_size,

            running,
            io_thread,
            queue,
            inner_thread,

            acked_pid,
            send_space,
            subscriptions: HashSet::new(),
            next_msg_id: 0,
            messages: Vec::new(),
        })
    }
    pub fn stop(self) {
        self.running.store(false, Ordering::SeqCst);
        self.io_thread.join().unwrap();

        self.queue.push(InternalMessage::Shutdown);
        self.inner_thread.join().unwrap();
    }

    pub fn subscribe(&mut self, topic: &str) -> Result<(), Error> {
        if !self.running.load(Ordering::SeqCst) {
            return Err(Error::Shutdown);
        }
        if self.subscriptions.contains(topic) {
            return Err(Error::AlreadySubscribed(topic.to_owned()));
        }
        await_sendbuf_space!(self.send_space, self.running);

        let pid = self.protocol.lock().unwrap().send_subscribe(topic, self.max_packet_size)?;
        await_packet_ack!(self.acked_pid, self.running, pid);
        self.subscriptions.insert(topic.to_owned());

        Ok(())
    }
    pub fn unsubscribe(&mut self, topic: &str) -> Result<(), Error> {
        if !self.running.load(Ordering::SeqCst) {
            return Err(Error::Shutdown);
        }
        if !self.subscriptions.contains(topic) {
            return Err(Error::NotSubscribed(topic.to_owned()));
        }
        await_sendbuf_space!(self.send_space, self.running);

        let pid = self.protocol.lock().unwrap().send_unsubscribe(topic, self.max_packet_size)?;
        await_packet_ack!(self.acked_pid, self.running, pid);
        self.subscriptions.remove(topic);

        Ok(())
    }

    pub fn queue_message<R: Read + Send + 'a>(&mut self, message: Message<R>) -> Result<(), Error> {
        if !self.running.load(Ordering::SeqCst) {
            return Err(Error::Shutdown);
        }

        self.messages.push(OutgoingMessage::new(&self.buf_source, self.max_packet_size, message, self.next_msg_id));
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
                        let (fin, data) = msg.next_packet(self.max_packet_size, protocol.send_seq())?;
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
