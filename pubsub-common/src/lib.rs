#![feature(euclidean_division)]

use std::cmp;
use std::mem::size_of;
use std::str::{self, Utf8Error};
use std::fmt::{self, Display};
use std::collections::LinkedList;
use std::time::{Duration, Instant};
use std::io::{self, Read};
use std::net::{SocketAddr, UdpSocket};

#[macro_use]
extern crate enum_primitive;
#[macro_use]
extern crate debug_stub_derive;
#[macro_use]
extern crate quick_error;
#[macro_use]
extern crate log;
extern crate bytes;
extern crate crossbeam_channel;
extern crate priority_queue;

use enum_primitive::FromPrimitive;
use bytes::{IntoBuf, Buf, BufMut, Bytes, BytesMut};
use crossbeam_channel::Sender;

pub mod util;
pub mod timer;
use util::BufferProvider;
use timer::{TimerManager, TimerManagerClient, Timer};

pub const IPV4_MAX_PACKET_SIZE: usize = 508;
pub const IPV6_MAX_PACKET_SIZE: usize = 1212;

pub const SEQ_BITS: u8 = 3;
pub const SEQ_RANGE: u8 = 8;
pub const WINDOW_SIZE: u8 = SEQ_RANGE - 1;
pub const SEND_BUFFER_SIZE: usize = 128;
pub const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(5);
pub const ACK_TIMEOUT: Duration = Duration::from_millis(500);

pub const CONNECT_MAGIC: &'static [u8] = b"JQTT";
pub const MAX_TOPIC_LENGTH: usize = IPV4_MAX_PACKET_SIZE - 1 - size_of::<u32>();

#[inline]
pub fn window_end(window_start: u8) -> u8 {
    (window_start + WINDOW_SIZE) % SEQ_RANGE
}
#[inline]
pub fn seq_in_window(window_start: u8, seq: u8) -> bool {
    let end = window_end(window_start);
    (seq < window_start) != ((seq < end) != (end < window_start))
}
#[inline]
pub fn slide_amount(window_start: u8, seq: u8) -> u8 {
    (seq as i8 - window_start as i8).mod_euc(SEQ_RANGE as i8) as u8 + 1
}
#[inline]
pub fn next_seq(seq: u8) -> u8 {
    (seq + 1) % SEQ_RANGE
}

quick_error! {
    #[derive(Debug)]
    pub enum GbnError {
        InvalidConnect {
            description("invalid connection packet")
        }
        InvalidHeader {
            description("invalid packet header")
        }
        InvalidType(packet_type: u8) {
            description("invalid packet type")
            display("invalid packet type '{}'", packet_type)
        }
        Malformed(packet_type: PacketType) {
            description("malformed packet")
            display("malformed {} packet", packet_type)
        }
        AlreadyConnected {
            description("already connected")
        }
        OutOfOrder(expected: u8, seq: u8) {
            description("out of order packet")
            display("out of order packet seq {}, expected {}", seq, expected)
        }
        AckFail(err: io::Error) {
            from()
            cause(err)
            description(err.description())
            display("failed to send ack: {}", err)
        }
        Utf8Decode(err: Utf8Error) {
            from()
            cause(err)
            description(err.description())
            display("failed to decode string from packet: {}", err)
        }
        InvalidPublishState(started: bool) {
            description("incorrect type of message publish packet for current state")
            display("message publish {} started", match started {
                true => "already",
                false => "not",
            })
        }
        Partial(packet_type: PacketType) {
            description("partial message")
            display("partial {} message", packet_type)
        }
        MessageTooBig(expected: u32, actual: u32) {
            description("message is bigger than specified")
            display("message is {} bytes, specified as {}", actual, expected)
        }
        MessageAlreadyQueued {
            description("a message is already queued for publishing")
        }
    }
}

enum_from_primitive! {
    #[derive(Debug, PartialEq)]
    pub enum PacketType {
        Connect = 0,
        Heartbeat = 1,
        Ack = 2,
        Disconnect = 3,
        Subscribe = 4,
        Unsubscribe = 5,
        PublishStart = 6,
        PublishData = 7,
    }
}
impl Display for PacketType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use PacketType::*;
        write!(f, "{}", match self {
            Connect => "Connect",
            Heartbeat => "HEARTBEAT",
            Ack => "ACK",
            Disconnect => "DISCONNECT",
            Subscribe => "SUBSCRIBE",
            Unsubscribe => "UNSUBSCRIBE",
            PublishStart => "PUBLISH_START",
            PublishData => "PUBLISH_DATA",
        })
    }
}

#[derive(Debug)]
pub enum Packet<'a> {
    Connect,
    Heartbeat,
    Ack(u8),
    Disconnect,
    Subscribe(&'a str),
    Unsubscribe(&'a str),
    Message(IncomingMessage),
}
impl<'a> Packet<'a> {
    pub fn validate_connect(buffer: &Bytes) -> Result<Packet, GbnError> {
        if buffer.len() != 1 + CONNECT_MAGIC.len() || 
            PacketType::from_u8(buffer[0]) != Some(PacketType::Connect) || 
            &buffer[1..] != CONNECT_MAGIC {
            return Err(GbnError::InvalidConnect);
        }

        Ok(Packet::Connect)
    }

    fn encode_header(packet_type: PacketType, seq: u8) -> u8 {
        ((packet_type as u8) << SEQ_BITS) | seq
    }
    pub fn make_connect() -> Bytes {
        let mut data = BytesMut::with_capacity(1 + CONNECT_MAGIC.len());
        data.put_u8(Packet::encode_header(PacketType::Connect, 0));
        data.put_slice(CONNECT_MAGIC);
        data.freeze()
    }
    fn make_subscribe(buffer: &mut BytesMut, topic: &str, seq: u8) -> Bytes {
        let topic = topic.as_bytes();
        let mut data = buffer.split_to(1 + topic.len());
        data.put_u8(Packet::encode_header(PacketType::Subscribe, seq));
        data.put_slice(topic);
        data.freeze()
    }
}

#[derive(PartialEq, Eq, Hash, Debug)]
pub enum GbnTimeout {
    Heartbeat(SocketAddr),
    Ack(SocketAddr),
}
#[derive(DebugStub)]
pub struct IncomingMessage {
    topic_bytes: Bytes,
    total_size: u32,
    size: u32,
    #[debug_stub = "Chain"]
    buffer: Option<Box<Buf + Send>>,
}
impl IncomingMessage {
    fn new(mut data: Bytes) -> Result<IncomingMessage, GbnError> {
        data.advance(1);
        let mut buf = data.into_buf();
        let total_size = buf.get_u32_be();
        let pos = buf.position();
        let mut data = buf.into_inner();
        data.advance(pos as usize);
        str::from_utf8(&data)?;

        Ok(IncomingMessage {
            topic_bytes: data,
            total_size,
            size: 0,
            buffer: None,
        })
    }
    fn append(&mut self, mut data: Bytes) -> Result<bool, GbnError> {
        data.advance(1);
        let new_size = self.size + data.len() as u32;
        println!("new size: {}, total size: {}", new_size, self.total_size);
        if new_size > self.total_size {
            return Err(GbnError::MessageTooBig(self.total_size, new_size));
        }

        self.buffer = Some(if self.buffer.is_some() {
            let buffer = self.buffer.take().unwrap();
            Box::new(buffer.chain(data))
        } else {
            Box::new(data.into_buf())
        });

        self.size = new_size;
        Ok(new_size == self.total_size)
    }

    pub fn take(self) -> Box<Buf + Send> {
        assert_eq!(self.size, self.total_size);
        self.buffer.unwrap()
    }
    pub fn collect(self) -> Vec<u8> {
        self.buffer.unwrap().collect()
    }
    pub fn topic(&self) -> &str {
        unsafe { str::from_utf8_unchecked(&self.topic_bytes) }
    }
}
#[derive(Debug)]
pub struct OutgoingMessage<I: Read + Send> {
    buf_source: BufferProvider,
    publish_start: Option<BytesMut>,
    reader: I,
    size: u32,
    read: u32,
}
impl<I: Read + Send> OutgoingMessage<I> {
    pub fn new(buf_source: &BufferProvider, topic: &str, input: I, size: u32) -> OutgoingMessage<I> {
        let topic = topic.as_bytes();
        assert!(topic.len() <= MAX_TOPIC_LENGTH);

        let mut first_buffer = buf_source.allocate(1 + size_of::<u32>() + topic.len());
        first_buffer.put_u8(0);
        first_buffer.put_u32_be(size);
        first_buffer.put(topic);
        OutgoingMessage {
            buf_source: buf_source.clone(),
            publish_start: Some(first_buffer),
            reader: input,
            size,
            read: 0,
        }
    }
    fn into_boxed<'a, R: Read + Send + 'a>(msg: OutgoingMessage<R>) -> OutgoingMessage<Box<Read + Send + 'a>> {
        OutgoingMessage {
            buf_source: msg.buf_source,
            publish_start: msg.publish_start,
            reader: Box::new(msg.reader),
            size: msg.size,
            read: msg.read,
        }
    }

    fn first_sent(&self) -> bool {
        !self.publish_start.is_some()
    }
    fn first_packet(&mut self, seq: u8) -> Bytes {
        let mut data = self.publish_start.take().unwrap();
        data[0] = Packet::encode_header(PacketType::PublishStart, seq);
        data.freeze()
    }
    fn next_packet(&mut self, max_packet_size: usize, seq: u8) -> Result<(bool, Option<Bytes>), io::Error> {
        if self.read == self.size {
            return Ok((true, None));
        }

        let header_size = 1;
        let data_size = max_packet_size - header_size;
        let to_read = cmp::min((self.size - self.read) as usize, data_size);
        let mut data = self.buf_source.allocate(header_size + to_read);
        data.put_u8(Packet::encode_header(PacketType::PublishData, seq));

        data.truncate(header_size + to_read);

        unsafe {
            self.reader.read_exact(data.bytes_mut())?;
            data.advance_mut(to_read);
        }

        self.read += to_read as u32;
        Ok((self.read == self.size, Some(data.freeze())))
    }
}
#[derive(DebugStub)]
pub struct GoBackN {
    timers: TimerManager<GbnTimeout>,
    buf_source: BufferProvider,
    addr: SocketAddr,
    socket: UdpSocket,

    timers_client: TimerManagerClient,
    heartbeat_timeout: Option<Timer>,

    send_buffer: LinkedList<Bytes>,
    send_buffer_sseq: u8,
    send_seq: u8,
    ack_timeout: Option<Timer>,
    #[debug_stub = "OutgoingMessage"]
    send_publish_buf: Option<OutgoingMessage<Box<Read + Send>>>,

    recv_seq: u8,
    recv_publish_buf: Option<IncomingMessage>,
}
impl Drop for GoBackN {
    fn drop(&mut self) {
        if let Some(timer) = self.heartbeat_timeout {
            assert!(self.timers.cancel_message(timer));
        }
        self.timers.unregister(self.timers_client);
    }
}
impl GoBackN {
    pub fn new(timers: &TimerManager<GbnTimeout>, buf_source: &BufferProvider, addr: SocketAddr, socket: UdpSocket, timeout_tx: &Sender<GbnTimeout>, use_heartbeats: bool) -> GoBackN {
        let timers_client = timers.register(&timeout_tx);
        let heartbeat_timeout = match use_heartbeats {
            true => Some(timers.post_message(timers_client, GbnTimeout::Heartbeat(addr), Instant::now() + HEARTBEAT_TIMEOUT)),
            false => None
        };
        GoBackN {
            timers: timers.clone(),
            buf_source: buf_source.clone(),
            addr,
            socket,

            timers_client,
            heartbeat_timeout,

            send_buffer: LinkedList::new(),
            send_buffer_sseq: 0,
            send_seq: 0,
            ack_timeout: None,
            send_publish_buf: None,

            recv_seq: 0,
            recv_publish_buf: None,
        }
    }

    fn max_packet_size(&self) -> usize {
        match self.addr {
            SocketAddr::V4(_) => IPV4_MAX_PACKET_SIZE,
            SocketAddr::V6(_) => IPV6_MAX_PACKET_SIZE,
        }
    }
    pub fn handle_ack_timeout(&mut self) -> Result<(), GbnError> {
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
            self.ack_timeout = Some(self.timers.post_message(self.timers_client, GbnTimeout::Ack(self.addr), Instant::now() + ACK_TIMEOUT));
        }
    }
    pub fn handle_ack(&mut self, seq: u8) {
        if !seq_in_window(self.send_buffer_sseq, seq) || slide_amount(self.send_buffer_sseq, seq) as usize > self.send_buffer.len() {
            warn!("received ack from {} outside of window", self.addr);
            return;
        }

        let buffer_size = self.send_buffer.len();
        self.send_buffer.split_off(cmp::min(slide_amount(self.send_buffer_sseq, seq) as usize, buffer_size));

        self.send_buffer_sseq = next_seq(seq);
        self.send_seq = self.send_buffer_sseq;
        if let Some(timer) = self.ack_timeout {
            self.timers.cancel_message(timer);
        }

        self.check_ack_timeout();
    }
    fn window_full(&self) -> bool {
        self.send_buffer.len() >= WINDOW_SIZE as usize
    }
    fn send_now(&mut self, data: Bytes) -> Result<bool, GbnError> {
        assert!(self.send_buffer.len() < WINDOW_SIZE as usize);
        assert!(data.len() <= self.max_packet_size());

        self.send_buffer.push_back(data);
        self.socket.send_to(self.send_buffer.back().unwrap(), self.addr)?;
        self.send_seq = next_seq(self.send_seq);
        self.check_ack_timeout();
        Ok(self.window_full())
    }
    pub fn drain_message(&mut self) -> Result<(), GbnError> {
        let message = self.send_publish_buf.take();
        match message {
            None => Ok(()),
            Some(mut msg) => {
                let mut full = self.window_full();
                while !full {
                    if !msg.first_sent() {
                        let data = msg.first_packet(self.send_seq);
                        self.send_now(data)?;
                    } else {
                        let (finished, data) = msg.next_packet(self.max_packet_size(), self.send_seq)?;
                        if let Some(data) = data {
                            full = self.send_now(data)?;
                        }
                        if finished {
                            return Ok(())
                        }
                    }
                }

                self.send_publish_buf = Some(msg);
                Ok(())
            },
        }

    }
    pub fn queue_message<R: Read + Send + 'static>(&mut self, message: OutgoingMessage<R>) -> Result<(), GbnError> {
        if let Some(_) = self.send_publish_buf {
            return Err(GbnError::MessageAlreadyQueued);
        }

        self.send_publish_buf = Some(OutgoingMessage::<R>::into_boxed(message));
        self.drain_message()?;
        Ok(())
    }

    fn recv_slide(&mut self, seq: u8) -> Result<(), GbnError> {
        if seq != self.recv_seq {
            return Err(GbnError::OutOfOrder(self.recv_seq, seq));
        }

        self.socket.send_to(&[Packet::encode_header(PacketType::Ack, seq)], self.addr)?;
        self.recv_seq = next_seq(seq);
        Ok(())
    }
    pub fn decode<'b>(&mut self, data: &'b Bytes) -> Result<Packet<'b>, GbnError> {
        use GbnError::*;
        use PacketType::*;

        let mut data = data.into_buf();
        if data.remaining() == 0 {
            return Err(InvalidHeader);
        }
        let header = data.get_u8();
        let t = header >> SEQ_BITS;
        if let Some(t) = PacketType::from_u8(t) {
            let seq = header & (0xff >> (8-SEQ_BITS));
            return match t {
                Connect => Err(AlreadyConnected),
                Heartbeat => match data.remaining() {
                    0 if seq == 0 => {
                        if let Some(timer) = self.heartbeat_timeout {
                            self.timers.cancel_message(timer);
                            self.heartbeat_timeout = Some(self.timers.post_message(self.timers_client, GbnTimeout::Heartbeat(self.addr), Instant::now() + HEARTBEAT_TIMEOUT));
                        }

                        Ok(Packet::Heartbeat)
                    },
                    _ => Err(Malformed(Heartbeat)),
                },
                Ack => match data.remaining() {
                     0 => Ok(Packet::Ack(seq)),
                     _ => Err(Malformed(Ack)),
                },
                Disconnect => match data.remaining() {
                    0 if seq == 0 => Ok(Packet::Disconnect),
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
                PublishStart => Err(match self.recv_publish_buf {
                    None => {
                        self.recv_slide(seq)?;

                        let msg = IncomingMessage::new(data.into_inner().clone())?;
                        self.recv_publish_buf = Some(msg);
                        Partial(PublishData)
                    },
                    Some(_) => InvalidPublishState(true),
                }),
                PublishData => match self.recv_publish_buf {
                    Some(_) => {
                        self.recv_slide(seq)?;
                        match self.recv_publish_buf.as_mut().unwrap().append(data.into_inner().clone())? {
                            true => Ok(Packet::Message(self.recv_publish_buf.take().unwrap())),
                            false => Err(Partial(PublishData)),
                        }
                    },
                    None => Err(InvalidPublishState(false)),
                },
            }
        } else {
            Err(InvalidType(t))
        }
    }

    pub fn send_heartbeat(&self) -> Result<(), io::Error> {
        self.socket.send_to(&[Packet::encode_header(PacketType::Heartbeat, 0)], self.addr)?;
        Ok(())
    }
    pub fn send_disconnect(&self) -> Result<(), io::Error> {
        self.socket.send_to(&[Packet::encode_header(PacketType::Disconnect, 0)], self.addr)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
