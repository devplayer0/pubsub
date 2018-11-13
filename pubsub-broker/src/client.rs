use std::ops::{Deref, DerefMut};
use std::convert::TryFrom;
use std::hash::{Hash, Hasher};
use std::collections::{hash_map, HashMap};
use std::sync::{Arc, Mutex, MutexGuard};
use std::sync::atomic::{Ordering, AtomicU32};
use std::net::{SocketAddr, UdpSocket};

use bytes::Bytes;
use crossbeam::queue::MsQueue;

use common;
use common::util::BufferProvider;
use common::timer::TimerManager;
use common::packet::{PacketType, Packet};
use common::protocol::{Jqtt, Timeout, KeepaliveManager, MessageStart, MessageSegment};

use ::Error;
use worker::WorkerMessage;

#[derive(Debug)]
pub enum Action<'a> {
    Disconnect,
    Subscribe(&'a str),
    Unsubscribe(&'a str),
    DispatchStart(MessageStart),
    DispatchSegment(MessageSegment),
}
impl<'a> TryFrom<Packet<'a>> for Action<'a> {
    type Error = ();
    fn try_from(packet: Packet) -> Result<Action, Self::Error> {
        match packet {
            Packet::Disconnect => Ok(Action::Disconnect),
            Packet::Subscribe(topic) => Ok(Action::Subscribe(topic)),
            Packet::Unsubscribe(topic) => Ok(Action::Unsubscribe(topic)),
            _ => Err(()),
        }
    }
}

#[derive(Debug)]
struct MessageInfo {
    src: SocketAddr,
    start: MessageStart,

    mapped_id: u32,
    dispatched: u32,
}
impl MessageInfo {
    pub fn new(src: SocketAddr, start: MessageStart, mapped_id: u32) -> MessageInfo {
        MessageInfo {
            src,
            start,

            mapped_id,
            dispatched: 0,
        }
    }

    pub fn mapped_id(&self) -> u32 {
        self.mapped_id
    }
    pub fn finished(&self) -> bool {
        self.dispatched == self.start.size()
    }
    pub fn dispatched(&mut self, amount: u32) -> Result<bool, Error> {
        if self.dispatched + amount > self.start.size() {
            return Err(Error::MessageTooBig(self.src, self.mapped_id, self.start.size(), self.dispatched + amount));
        }

        self.dispatched += amount;
        Ok(self.finished())
    }
}
#[derive(Debug)]
struct MessageMapping {
    src: SocketAddr,
    next_id: Arc<AtomicU32>,
    inner: HashMap<u32, MessageInfo>,
}
impl MessageMapping {
    pub fn new(src: SocketAddr, next_id: &Arc<AtomicU32>) -> MessageMapping {
        MessageMapping {
            src,
            next_id: Arc::clone(next_id),
            inner: HashMap::new(),
        }
    }

    pub fn translate_start(&mut self, start: &mut MessageStart) -> Result<(), Error> {
        if self.inner.contains_key(&start.id()) {
            return Err(Error::PublishAlreadyStarted(self.src, start.id()));
        }

        let orig = start.id();
        let mapped = self.next_id.fetch_add(1, Ordering::Relaxed);
        start.set_id(mapped);
        self.inner.insert(orig, MessageInfo::new(self.src, start.clone(), mapped));
        Ok(())
    }
    pub fn translate_segment(&mut self, segment: &mut MessageSegment) -> Result<(), Error> {
        if !self.inner.contains_key(&segment.id()) {
            return Err(Error::PublishNotStarted(self.src, segment.id()));
        }

        if {
            let info = self.inner.get_mut(&segment.id()).unwrap();
            let finished = info.dispatched(segment.size())?;
            segment.link_start(&info.start);
            segment.set_id(info.mapped_id());
            finished
        } {
            self.inner.remove(&segment.id());
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct Client {
    addr: SocketAddr,
    protocol: Jqtt,
    buf_source: BufferProvider,

    max_packet_size: u16,
    connected: bool,
    keepalive: Option<KeepaliveManager>,
    msg_mapping: MessageMapping,
}
impl Hash for Client {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.addr.hash(state);
    }
}
impl PartialEq for Client {
    fn eq(&self, other: &Client) -> bool {
        other.addr == self.addr
    }
}
impl Eq for Client {}
impl Deref for Client {
    type Target = Jqtt;
    fn deref(&self) -> &Self::Target {
        &self.protocol
    }
}
impl DerefMut for Client {
    fn deref_mut(&mut self) -> &mut Jqtt {
        &mut self.protocol
    }
}
impl Client {
    pub fn new(timers: &TimerManager<Timeout>, addr: SocketAddr, socket: UdpSocket, buf_source: &BufferProvider, timeout_queue: &Arc<MsQueue<WorkerMessage>>, keepalive: bool, max_packet_size: u16, next_message_id: &Arc<AtomicU32>) -> Client {
        let keepalive = match keepalive {
            true => {
                let queue = Arc::clone(timeout_queue);
                Some(KeepaliveManager::new(timers, move |msg| queue.push(msg.into()), addr))
            },
            false => None
        };

        let timeout_queue = timeout_queue.clone();
        let timeout_callback = move |msg: Timeout| timeout_queue.push(msg.into());
        Client {
            addr,
            protocol: Jqtt::new(timers, buf_source, addr, socket, timeout_callback),
            buf_source: buf_source.clone(),

            max_packet_size,
            connected: false,
            keepalive,
            msg_mapping: MessageMapping::new(addr, next_message_id),
        }
    }
    #[inline]
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn handle<'a>(&mut self, data: &'a Bytes) -> Result<Option<Action<'a>>, Error> {
        use self::Packet::*;

        match self.protocol.decode(&data) {
            Err(e@common::Error::InvalidAck(_, _, _)) | Err(e@common::Error::OutOfOrder(_, _)) => warn!("client {}: {}", self.addr, e),
            Err(e) => return Err(e.into()),
            Ok(ref p) if !self.connected => match p {
                Connect => {
                    self.connected = true;
                    self.send_connack(self.keepalive.is_some(), self.max_packet_size)?
                },
                _ => return Err(Error::NotConnected),
            },
            Ok(p) => match p {
                Connect => return Err(Error::AlreadyConnected),
                ConnAck(_, _) => return Err(Error::ServerOnly(PacketType::ConnAck)),
                Heartbeat => match self.keepalive {
                    Some(ref mut manager) => {
                        manager.heartbeat();
                        self.protocol.send_heartbeat()?;
                    },
                    None => warn!("received heartbeat from {}, but keepalive is disabled", self.addr),
                },
                Ack(_) => {},
                PublishStart(mut start) => {
                    self.msg_mapping.translate_start(&mut start)?;
                    return Ok(Some(Action::DispatchStart(start)));
                },
                PublishData(mut segment) => {
                    self.msg_mapping.translate_segment(&mut segment)?;
                    return Ok(Some(Action::DispatchSegment(segment)));
                },

                p => return Ok(Some(Action::try_from(p).expect("impossible"))),
            },
        }

        Ok(None)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct CopyWrapper<C: Copy>(C);
impl<C: Copy> evmap::ShallowCopy for CopyWrapper<C> {
    unsafe fn shallow_copy(&mut self) -> Self {
        CopyWrapper(self.0)
    }
}
impl<C: Copy> From<C> for CopyWrapper<C> {
    fn from(c: C) -> Self {
        CopyWrapper(c)
    }
}

#[derive(DebugStub)]
pub(crate) struct Subscriptions {
    #[debug_stub = "evmap::ReadHandle<String, SocketAddr>"]
    read: evmap::ReadHandle<String, CopyWrapper<SocketAddr>, (), hash_map::RandomState>,
    #[debug_stub = "evmap::WriteHandle<String, SocketAddr>"]
    write: Arc<Mutex<evmap::WriteHandle<String, CopyWrapper<SocketAddr>, (), hash_map::RandomState>>>,
}
impl Clone for Subscriptions {
    fn clone(&self) -> Self {
        Subscriptions {
            read: self.read.clone(),
            write: Arc::clone(&self.write),
        }
    }
}
impl Subscriptions {
    pub fn new() -> Subscriptions {
        let (read, write) = evmap::new();
        Subscriptions {
            read,
            write: Arc::new(Mutex::new(write)),
        }
    }

    pub fn with_topic_each<F: FnMut(SocketAddr) -> Result<(), E>, E>(&self, topic: &str, mut fun: F) -> Option<Result<(), E>> {
        self.read.get_and(topic, |subs| {
            for sub in subs {
                let result = fun(sub.0);
                if result.is_err() {
                    return result;
                }
            }

            Ok(())
        })
    }
    pub fn is_subscribed(&self, topic: &str, subscriber: SocketAddr) -> bool {
        match self.read.get_and(topic, |subs| {
            for sub in subs {
                if sub.0 == subscriber {
                    return true;
                }
            }
            false
        }) {
            Some(is) => is,
            None => false,
        }
    }
    pub fn subscribe(&self, topic: &str, subscriber: SocketAddr) -> Result<(), Error> {
        if self.is_subscribed(topic, subscriber) {
            return Err(Error::AlreadySubscribed(topic.to_owned(), subscriber));
        }

        let mut write = self.write.lock().unwrap();
        write.insert(topic.to_owned(), subscriber.into());
        write.refresh();
        Ok(())
    }
    fn unsubscribe_internal(&self, topic: &str, subscriber: SocketAddr) -> bool {
        if !self.is_subscribed(topic, subscriber) {
            return false;
        }

        let mut write = self.write.lock().unwrap();
        let topic = topic.to_owned();
        match self.read.get_and(&topic, |subs| subs.len() == 1).unwrap() {
            false => write.remove(topic, subscriber.into()),
            true => write.empty(topic),
        }

        true
    }
    #[inline]
    pub fn unsubscribe(&self, topic: &str, subscriber: SocketAddr) -> Result<(), Error> {
        match self.unsubscribe_internal(topic, subscriber) {
            true => {
                self.write.lock().unwrap().refresh();
                Ok(())
            },
            false => Err(Error::NotSubscribed(topic.to_owned(), subscriber)),
        }
    }

    pub fn unsubscribe_all(&self, subscriber: SocketAddr) {
        self.read.for_each(|topic, _| { self.unsubscribe_internal(topic, subscriber); });
        self.write.lock().unwrap().refresh();
    }
}

struct ClientWrapper {
    addr: SocketAddr,
    client: Arc<Mutex<Client>>,
    queue: Arc<MsQueue<WorkerMessage>>,
}
impl ClientWrapper {
    pub fn new(client: Client, queue: &Arc<MsQueue<WorkerMessage>>) -> ClientWrapper {
        ClientWrapper {
            addr: client.addr(),
            client: Arc::new(Mutex::new(client)),
            queue: Arc::clone(queue),
        }
    }
}
impl PartialEq for ClientWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.addr == other.addr
    }
}
impl Eq for ClientWrapper {}
impl evmap::ShallowCopy for ClientWrapper {
    unsafe fn shallow_copy(&mut self) -> Self {
        ClientWrapper {
            addr: self.addr,
            client: self.client.shallow_copy(),
            queue: self.queue.shallow_copy(),
        }
    }
}

#[derive(DebugStub)]
pub(crate) struct Clients {
    #[debug_stub = "evmap::ReadHandle<SocketAddr, (Arc<Mutex<Client>>, Arc<MsQueue<WorkerMessage>>)"]
    read: evmap::ReadHandle<SocketAddr, ClientWrapper, (), hash_map::RandomState>,
    #[debug_stub = "evmap::WriteHandle<SocketAddr, (Arc<Mutex<Client>>, Arc<MsQueue<WorkerMessage>>)"]
    write: Arc<Mutex<evmap::WriteHandle<SocketAddr, ClientWrapper, (), hash_map::RandomState>>>,

    subscriptions: Subscriptions,
    next_message_id: Arc<AtomicU32>,
}
impl Clone for Clients {
    fn clone(&self) -> Self {
        Clients {
            read: self.read.clone(),
            write: Arc::clone(&self.write),

            subscriptions: self.subscriptions.clone(),
            next_message_id: Arc::clone(&self.next_message_id),
        }
    }
}
impl Clients {
    pub fn new() -> Clients {
        let (read, write) = evmap::new();
        Clients {
            read,
            write: Arc::new(Mutex::new(write)),

            subscriptions: Subscriptions::new(),
            next_message_id: Arc::new(AtomicU32::new(0)),
        }
    }

    #[inline]
    pub fn subscriptions(&self) -> &Subscriptions {
        &self.subscriptions
    }
    pub fn have_client(&self, addr: SocketAddr) -> bool {
        self.read.contains_key(&addr)
    }
    pub fn create(&self, queue: &Arc<MsQueue<WorkerMessage>>, timers: &TimerManager<Timeout>, addr: SocketAddr, socket: UdpSocket, buf_source: &BufferProvider, keepalive: bool, max_packet_size: u16) {
        let mut write = self.write.lock().unwrap();
        write.update(addr, ClientWrapper::new(Client::new(timers, addr, socket, buf_source, queue, keepalive, max_packet_size, &self.next_message_id), queue));
        write.refresh();
    }
    pub fn remove(&self, addr: SocketAddr) {
        self.subscriptions.unsubscribe_all(addr);

        let mut write = self.write.lock().unwrap();
        write.empty(addr);
        write.refresh();
    }
    pub fn with<F: FnOnce(MutexGuard<Client>) -> T, T>(&self, addr: SocketAddr, fun: F) -> Option<T> {
        self.read.get_and(&addr, |c| fun(c[0].client.lock().unwrap()))
    }
    pub fn dispatch(&self, addr: SocketAddr, message: WorkerMessage) -> bool {
        self.read.get_and(&addr, |c| c[0].queue.push(message)).is_some()
    }

    pub fn destroy_each<F: FnMut(MutexGuard<Client>) -> Result<(), E>, E>(self, mut fun: F) -> Result<(), E> {
        for client in self.read.map_into::<_, Vec<_>, _>(|_, c| Arc::clone(&c[0].client)) {
            let result = fun(client.lock().unwrap());
            if result.is_err() {
                return result;
            }
        }

        match Arc::try_unwrap(self.write) {
            Ok(mtx) => mtx.into_inner().unwrap().destroy(),
            Err(e) => panic!(e),
        }
        Ok(())
    }
}
