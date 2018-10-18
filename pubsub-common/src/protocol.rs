use std::ops::{Deref, DerefMut};
use std::cmp;
use std::hash::{Hash, Hasher};
use std::mem::size_of;
use std::str;
use std::collections::LinkedList;
use std::time::Instant;
use std::net::{SocketAddr, UdpSocket};

use bytes::{IntoBuf, Buf, BufMut, Bytes, BytesMut};
use crossbeam_channel::Sender;

use ::Error;
use constants::*;
use util::{self, BufferProvider};
use timer::{Timer, TimerManager, TimerManagerClient};
use packet::{Packet, PacketType};

#[inline]
pub(crate) fn window_end(window_start: u8) -> u8 {
    (window_start + WINDOW_SIZE) % SEQ_RANGE
}
#[inline]
pub(crate) fn seq_in_window(window_start: u8, seq: u8) -> bool {
    let end = window_end(window_start);
    (seq < window_start) != ((seq < end) != (end < window_start))
}
#[inline]
pub(crate) fn slide_amount(window_start: u8, seq: u8) -> u8 {
    (seq as i8 - window_start as i8).mod_euc(SEQ_RANGE as i8) as u8 + 1
}
#[inline]
pub(crate) fn next_seq(seq: u8) -> u8 {
    (seq + 1) % SEQ_RANGE
}

#[derive(Debug)]
pub struct MessageStart {
    id: u32,
    total_size: u32,
    headerless: Bytes,
}
impl Clone for MessageStart {
    fn clone(&self) -> MessageStart {
        MessageStart {
            id: self.id,
            total_size: self.total_size,
            headerless: self.headerless.clone(),
        }
    }
}
impl Hash for MessageStart {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}
impl PartialEq for MessageStart {
    fn eq(&self, other: &MessageStart) -> bool {
        self.id == other.id
    }
}
impl Eq for MessageStart {}
impl MessageStart {
    fn new(mut data: Bytes) -> Result<MessageStart, Error> {
        data.advance(1);
        let mut buf = data.into_buf();
        let id = buf.get_u32_be();
        let total_size = buf.get_u32_be();

        let mut data = buf.into_inner();
        data.advance(size_of::<u32>());
        str::from_utf8(&data[size_of::<u32>()..])?;

        Ok(MessageStart {
            id,
            total_size,
            headerless: data,
        })
    }

    pub fn id(&self) -> u32 {
        self.id
    }
    pub fn set_id(&mut self, id: u32) {
        self.id = id;
    }
    pub fn size(&self) -> u32 {
        self.total_size
    }
    pub fn topic(&self) -> &str {
        unsafe { str::from_utf8_unchecked(&self.headerless[size_of::<u32>()..]) }
    }
    pub fn take_topic(mut self) -> Bytes {
        self.headerless.advance(size_of::<u32>());
        self.headerless
    }

    pub fn with_seq(&self, buf_source: &BufferProvider, seq: u8) -> Bytes {
        let mut data = buf_source.allocate(1 + size_of::<u32>() + self.headerless.len());
        data.put_u8(Packet::encode_header(PacketType::PublishStart, seq));
        data.put_u32_be(self.id);
        data.put(&self.headerless);
        data.freeze()
    }
}

#[derive(Debug)]
pub struct MessageSegment {
    start: Option<MessageStart>,

    id: u32,
    headerless: Bytes,
}
impl Hash for MessageSegment {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}
impl PartialEq for MessageSegment {
    fn eq(&self, other: &MessageSegment) -> bool {
        self.id == other.id
    }
}
impl Eq for MessageSegment {}
impl Deref for MessageSegment {
    type Target = Bytes;
    fn deref(&self) -> &Self::Target {
        &self.headerless
    }
}
impl DerefMut for MessageSegment {
    fn deref_mut(&mut self) -> &mut Bytes {
        &mut self.headerless
    }
}
impl Clone for MessageSegment {
    fn clone(&self) -> MessageSegment {
        let start = match self.start {
            Some(ref s) => Some(s.clone()),
            None => None,
        };
        MessageSegment {
            start,

            id: self.id,
            headerless: self.headerless.clone(),
        }
    }
}
impl MessageSegment {
    fn new(mut data: Bytes) -> MessageSegment {
        data.advance(1);
        let mut buf = data.into_buf();
        let id = buf.get_u32_be();

        let mut data = buf.into_inner();
        data.advance(size_of::<u32>());

        MessageSegment {
            start: None,

            id,
            headerless: data,
        }
    }

    pub fn link_start(&mut self, start: &MessageStart) {
        self.start = Some(start.clone());
    }
    pub fn topic(&self) -> Option<&str> {
        match self.start {
            Some(ref s) => Some(s.topic()),
            None => None,
        }
    }

    pub fn id(&self) -> u32 {
        self.id
    }
    pub fn set_id(&mut self, id: u32) {
        self.id = id;
    }
    pub fn into_inner(self) -> Bytes {
        self.headerless
    }
    pub fn with_seq(&self, buf_source: &BufferProvider, seq: u8) -> Bytes {
        let mut data = buf_source.allocate(1 + size_of::<u32>() + self.len());
        data.put_u8(Packet::encode_header(PacketType::PublishData, seq));
        data.put_u32_be(self.id);
        data.put(&**self);
        data.freeze()
    }
}

#[derive(PartialEq, Eq, Hash, Debug)]
pub enum Timeout {
    Keepalive(SocketAddr),
    SendHeartbeat(SocketAddr),
    Ack(SocketAddr),
}


#[derive(Debug)]
pub struct KeepaliveManager {
    timers: TimerManager<Timeout>,
    timers_client: TimerManagerClient,
    addr: SocketAddr,

    timer: Timer,
}
impl Drop for KeepaliveManager {
    fn drop(&mut self) {
        self.timers.cancel_message(self.timer);
        self.timers.unregister(self.timers_client);
    }
}
impl KeepaliveManager {
    pub fn new(timers: &TimerManager<Timeout>, timeout_tx: &Sender<Timeout>, addr: SocketAddr) -> KeepaliveManager {
        let timers_client = timers.register(timeout_tx);
        let timer = timers.post_message(timers_client, Timeout::Keepalive(addr), Instant::now() + KEEPALIVE_TIMEOUT);

        KeepaliveManager {
            timers: timers.clone(),
            timers_client,
            addr,

            timer,
        }
    }

    pub fn heartbeat(&mut self) {
        self.timers.cancel_message(self.timer);
        self.timer = self.timers.post_message(self.timers_client, Timeout::Keepalive(self.addr), Instant::now() + KEEPALIVE_TIMEOUT);
    }
}

#[derive(Debug)]
pub struct Jqtt {
    timers: TimerManager<Timeout>,
    buf_source: BufferProvider,

    addr: SocketAddr,
    socket: UdpSocket,

    timers_client: TimerManagerClient,

    send_buffer: LinkedList<Bytes>,
    send_buffer_sseq: u8,
    send_seq: u8,
    ack_timeout: Option<Timer>,

    recv_seq: u8,
}
impl Drop for Jqtt {
    fn drop(&mut self) {
        if let Some(timer) = self.ack_timeout {
            self.timers.cancel_message(timer);
        }
        self.timers.unregister(self.timers_client);
    }
}
impl Jqtt {
    pub fn new(timers: &TimerManager<Timeout>, buf_source: &BufferProvider, addr: SocketAddr, socket: UdpSocket, timeout_tx: &Sender<Timeout>) -> Jqtt {
        let timers_client = timers.register(&timeout_tx);
        Jqtt {
            timers: timers.clone(),
            buf_source: buf_source.clone(),

            addr,
            socket,

            timers_client,

            send_buffer: LinkedList::new(),
            send_buffer_sseq: 0,
            send_seq: 0,
            ack_timeout: None,

            recv_seq: 0,
        }
    }

    pub fn send_seq(&self) -> u8 {
        self.send_seq
    }
    pub fn handle_ack_timeout(&mut self) -> Result<(), Error> {
        self.check_ack_timeout();
        {
            let mut iter = self.send_buffer.iter();
            for _ in 0..cmp::min(self.send_buffer.len(), WINDOW_SIZE as usize) {
                self.socket.send_to(iter.next().unwrap(), self.addr)?;
            }
        }

        Ok(())
    }
    fn check_ack_timeout(&mut self) {
        if !self.send_buffer.is_empty() && (self.ack_timeout.is_none() || (self.ack_timeout.is_some() && self.timers.expired(self.ack_timeout.unwrap()))) {
            self.ack_timeout = Some(self.timers.post_message(self.timers_client, Timeout::Ack(self.addr), Instant::now() + ACK_TIMEOUT));
        }
    }
    pub fn handle_ack(&mut self, seq: u8) -> Result<(), Error> {
        if !seq_in_window(self.send_buffer_sseq, seq) || slide_amount(self.send_buffer_sseq, seq) as usize > self.send_buffer.len() {
            return Err(Error::InvalidAck(self.send_buffer_sseq, cmp::min(WINDOW_SIZE as usize, self.send_buffer.len()) as u8, seq));
        }

        let split_amount = cmp::min(slide_amount(self.send_buffer_sseq, seq) as usize, self.send_buffer.len());
        self.send_buffer = self.send_buffer.split_off(split_amount);

        self.send_buffer_sseq = next_seq(seq);
        self.send_seq = self.send_buffer_sseq;
        if let Some(timer) = self.ack_timeout {
            self.timers.cancel_message(timer);
        }

        self.handle_ack_timeout()
    }
    #[inline]
    pub fn window_full(&self) -> bool {
        self.send_buffer.len() >= WINDOW_SIZE as usize
    }
    #[inline]
    pub fn send_buffer_full(&self) -> bool {
        self.send_buffer.len() == SEND_BUFFER_SIZE
    }
    pub fn gbn_send(&mut self, data: Bytes) -> Result<bool, Error> {
        assert!(data.len() <= util::max_packet_size(self.addr));
        if self.send_buffer_full() {
            return Err(Error::SendBufferFull);
        }

        self.send_buffer.push_back(data);
        self.send_seq = next_seq(self.send_seq);
        if !self.window_full() {
            self.socket.send_to(self.send_buffer.back().unwrap(), self.addr)?;
            self.check_ack_timeout();
        }

        Ok(self.window_full())
    }

    fn recv_slide(&mut self, seq: u8) -> Result<(), Error> {
        if seq != self.recv_seq {
            return Err(Error::OutOfOrder(self.recv_seq, seq));
        }

        self.socket.send_to(&[Packet::encode_header(PacketType::Ack, seq)], self.addr)?;
        self.recv_seq = next_seq(seq);
        Ok(())
    }
    pub fn decode<'b>(&mut self, data: &'b Bytes) -> Result<Packet<'b>, Error> {
        use Error::*;
        use PacketType::*;

        let mut data = data.into_buf();
        if data.remaining() == 0 {
            return Err(InvalidHeader);
        }

        let (t, seq) = Packet::decode_header(data.get_u8())?;
        match t {
            Connect => match data.bytes() {
                CONNECT_MAGIC => Ok(Packet::Connect),
                _ => Err(Malformed(Connect)),
            },
            ConnAck => match data.remaining() {
                0 if seq & 0b110 == 0 => Ok(Packet::ConnAck(match seq {
                    0 => false,
                    1 => true,
                    _ => panic!("impossible"),
                })),
                _ => Err(Malformed(ConnAck)),
            },
            Heartbeat => match data.remaining() {
                0 if seq == 0 => Ok(Packet::Heartbeat),
                _ => Err(Malformed(Heartbeat)),
            },
            Ack => match data.remaining() {
                0 => {
                    self.handle_ack(seq)?;
                    Ok(Packet::Ack(seq))
                },
                _ => Err(Malformed(Ack)),
            },
            Disconnect => match data.remaining() {
                0 => {
                    self.recv_slide(seq)?;
                    Ok(Packet::Disconnect)
                },
                _ => Err(Malformed(Disconnect)),
            },
            Subscribe | Unsubscribe => if data.remaining() > 0 {
                self.recv_slide(seq)?;
                let pos = data.position();
                let topic = str::from_utf8(&data.into_inner()[pos as usize..])?;
                Ok(match t {
                    Subscribe => Packet::Subscribe(topic),
                    Unsubscribe => Packet::Unsubscribe(topic),
                    _ => panic!("impossible"),
                })
            } else {
                Err(Malformed(Subscribe))
            },
            PublishStart => if data.remaining() > size_of::<u32>() + size_of::<u32>() {
                self.recv_slide(seq)?;
                Ok(Packet::PublishStart(MessageStart::new(data.into_inner().clone())?))
            } else {
                Err(Malformed(PublishStart))
            },
            PublishData => if data.remaining() >= size_of::<u32>() {
                self.recv_slide(seq)?;
                Ok(Packet::PublishData(MessageSegment::new(data.into_inner().clone())))
            } else {
                 Err(Malformed(PublishData))
            },
        }
    }

    pub fn send_connect(&self) -> Result<(), Error> {
        let mut data = self.buf_source.allocate(1 + CONNECT_MAGIC.len());
        data.put_u8(Packet::encode_header(PacketType::Connect, 0));
        data.put_slice(CONNECT_MAGIC);

        self.socket.send_to(&data, self.addr)?;
        Ok(())
    }
    pub fn send_connack(&self, keepalive: bool) -> Result<(), Error> {
        let header = Packet::encode_header(PacketType::ConnAck, match keepalive {
            false => 0,
            true => 1,
        });
        self.socket.send_to(&[header], self.addr)?;
        Ok(())
    }
    pub fn send_heartbeat(&self) -> Result<(), Error> {
        let header = Packet::encode_header(PacketType::Heartbeat, 0);
        self.socket.send_to(&[header], self.addr)?;
        Ok(())
    }
    pub fn send_disconnect(&mut self) -> Result<(), Error> {
        let mut bytes = BytesMut::with_capacity(1);
        bytes.put_u8(Packet::encode_header(PacketType::Disconnect, self.send_seq));

        self.gbn_send(bytes.freeze())?;
        Ok(())
    }
    pub fn send_subscribe(&mut self, topic: &str) -> Result<(), Error> {
        let topic = topic.as_bytes();
        assert!(topic.len() <= MAX_TOPIC_LENGTH);

        let mut data = self.buf_source.allocate(1 + topic.len());
        data.put_u8(Packet::encode_header(PacketType::Subscribe, self.send_seq));
        data.put(topic);
        self.gbn_send(data.freeze())?;
        Ok(())
    }
    pub fn send_unsubscribe(&mut self, topic: &str) -> Result<(), Error> {
        let topic = topic.as_bytes();
        assert!(topic.len() <= MAX_TOPIC_LENGTH);

        let mut data = self.buf_source.allocate(1 + topic.len());
        data.put_u8(Packet::encode_header(PacketType::Unsubscribe, self.send_seq));
        data.put(topic);
        self.gbn_send(data.freeze())?;
        Ok(())
    }
    pub fn send_msg_start(&mut self, start: &MessageStart) -> Result<(), Error> {
        let data = start.with_seq(&self.buf_source, self.send_seq());
        self.gbn_send(data)?;
        Ok(())
    }
    pub fn send_msg_segment(&mut self, segment: &MessageSegment) -> Result<(), Error> {
        let data = segment.with_seq(&self.buf_source, self.send_seq());
        self.gbn_send(data)?;
        Ok(())
    }
}
