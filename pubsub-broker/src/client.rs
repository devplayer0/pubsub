use std::ops::{Deref, DerefMut};
use std::convert::TryFrom;
use std::hash::{Hash, Hasher};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{Ordering, AtomicU32};
use std::net::{SocketAddr, UdpSocket};

use bytes::Bytes;
use crossbeam_channel::Sender;

use common;
use common::util::BufferProvider;
use common::timer::TimerManager;
use common::packet::{PacketType, Packet};
use common::protocol::{Jqtt, Timeout, KeepaliveManager, MessageStart, MessageSegment};
use ::Error;

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

        let mapped = self.next_id.fetch_add(1, Ordering::Relaxed);
        start.set_id(mapped);
        self.inner.insert(start.id(), MessageInfo::new(self.src, start.clone(), mapped));
        Ok(())
    }
    pub fn translate_segment(&mut self, segment: &mut MessageSegment) -> Result<(), Error> {
        if !self.inner.contains_key(&segment.id()) {
            return Err(Error::PublishNotStarted(self.src, segment.id()));
        }

        let info = self.inner.get_mut(&segment.id()).unwrap();
        info.dispatched(segment.len() as u32)?;
        segment.link_start(&info.start);
        segment.set_id(info.mapped_id());
        Ok(())
    }
}

#[derive(Debug)]
pub struct Client {
    addr: SocketAddr,
    protocol: Jqtt,
    buf_source: BufferProvider,

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
    pub fn new(timers: &TimerManager<Timeout>, addr: SocketAddr, socket: UdpSocket, buf_source: &BufferProvider, timeout_tx: &Sender<Timeout>, keepalive: bool, next_message_id: &Arc<AtomicU32>) -> Client {
        let keepalive = match keepalive {
            true => Some(KeepaliveManager::new(timers, timeout_tx, addr)),
            false => None
        };

        Client {
            addr,
            protocol: Jqtt::new(timers, buf_source, addr, socket, timeout_tx),
            buf_source: buf_source.clone(),

            keepalive,
            msg_mapping: MessageMapping::new(addr, next_message_id),
        }
    }
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn handle<'a>(&mut self, data: &'a Bytes) -> Result<Option<Action<'a>>, Error> {
        use self::Packet::*;
        match self.protocol.decode(&data) {
            Err(e@common::Error::InvalidAck(_, _, _)) | Err(e@common::Error::OutOfOrder(_, _)) => warn!("client {}: {}", self.addr, e),
            Err(e) => return Err(e.into()),
            Ok(p) => match p {
                Connect => self.send_connack(self.keepalive.is_some())?,
                ConnAck(_) => return Err(Error::ServerOnly(PacketType::ConnAck)),
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
impl Eq for Client {}
