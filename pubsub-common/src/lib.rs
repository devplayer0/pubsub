#![feature(euclidean_division)]

use std::cmp;
use std::str::{self, Utf8Error};
use std::fmt::{self, Display};
use std::collections::LinkedList;
use std::time::{Duration, Instant};
use std::mem;
use std::io;
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

pub mod timer;
use timer::{TimerManager, TimerManagerClient, Timer};

pub const IPV4_MAX_PACKET_SIZE: usize = 508;
pub const IPV6_MAX_PACKET_SIZE: usize = 1212;

pub const SEQ_BITS: u8 = 3;
pub const SEQ_RANGE: u8 = 8;
pub const WINDOW_SIZE: u8 = SEQ_RANGE - 1;
pub const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(5);

pub const CONNECT_MAGIC: &'static [u8] = b"JQTT";
pub const MAX_TOPIC_LENGTH: usize = IPV4_MAX_PACKET_SIZE - 1;

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
    pub enum DecodeError {
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
        MessageTooBig(expected: usize, actual: usize) {
            description("message is bigger than specified")
            display("message is {} bytes, specified as {}", actual, expected)
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
        PublishStart = 5,
        PublishData = 6,
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
            PublishStart => "PUBLISH_START",
            PublishData => "PUBLISH_DATA",
        })
    }
}

#[derive(DebugStub)]
pub enum Packet<'a> {
    Connect,
    Heartbeat,
    Ack(u8),
    Disconnect,
    Subscribe(&'a str),
    Message(#[debug_stub = "Buf"] Box<Buf + Send>),
}
impl<'a> Packet<'a> {
    pub fn validate_connect(buffer: &Bytes) -> Result<Packet, DecodeError> {
        if buffer.len() != 1 + CONNECT_MAGIC.len() || 
            PacketType::from_u8(buffer[0]) != Some(PacketType::Connect) || 
            &buffer[1..] != CONNECT_MAGIC {
            return Err(DecodeError::InvalidConnect);
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
}
#[derive(DebugStub)]
struct MessageBuffer {
    total_size: usize,
    size: usize,
    #[debug_stub = "Chain"]
    buffer: Box<Buf + Send>,
}
impl MessageBuffer {
    fn new(total_size: usize, data: Bytes) -> Result<MessageBuffer, DecodeError> {
        if data.len() > total_size {
            return Err(DecodeError::MessageTooBig(total_size, data.len()));
        }

        Ok(MessageBuffer {
            total_size,
            size: data.len(),
            buffer: Box::new(data.into_buf()),
        })
    }
    fn full(&self) -> bool {
        self.size == self.total_size
    }
    fn append(&mut self, data: Bytes) -> Result<bool, DecodeError> {
        let new_size = self.size + data.len();
        println!("new size: {}, total size: {}", new_size, self.total_size);
        if new_size > self.total_size {
            return Err(DecodeError::MessageTooBig(self.total_size, new_size));
        }

        let buffer = mem::replace(&mut self.buffer, unsafe {
            mem::uninitialized()
        });
        let null = mem::replace(&mut self.buffer, Box::new(buffer.chain(data)));
        mem::forget(null);

        self.size = new_size;
        Ok(new_size == self.total_size)
    }
    fn take(self) -> Box<Buf + Send> {
        assert_eq!(self.size, self.total_size);
        self.buffer
    }
}
#[derive(Debug)]
pub struct GoBackN {
    timers: TimerManager<GbnTimeout>,
    addr: SocketAddr,
    socket: UdpSocket,

    timers_client: TimerManagerClient,
    heartbeat_timeout: Option<Timer>,

    send_buffer: LinkedList<Bytes>,
    send_buffer_sseq: u8,
    send_seq: u8,

    recv_seq: u8,
    recv_publish_buf: Option<MessageBuffer>,
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
    pub fn new(timers: &TimerManager<GbnTimeout>, addr: SocketAddr, socket: UdpSocket, timeout_tx: &Sender<GbnTimeout>, use_heartbeats: bool) -> GoBackN {
        let timers_client = timers.register(&timeout_tx);
        let heartbeat_timeout = match use_heartbeats {
            true => Some(timers.post_message(timers_client, GbnTimeout::Heartbeat(addr), Instant::now() + HEARTBEAT_TIMEOUT)),
            false => None
        };
        GoBackN {
            timers: timers.clone(),
            addr,
            socket,

            timers_client,
            heartbeat_timeout,

            send_buffer: LinkedList::new(),
            send_buffer_sseq: 0,
            send_seq: 0,

            recv_seq: 0,
            recv_publish_buf: None,
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
    }
    fn recv_slide(&mut self, seq: u8) -> Result<(), DecodeError> {
        if seq != self.recv_seq {
            return Err(DecodeError::OutOfOrder(self.recv_seq, seq));
        }

        self.socket.send_to(&[Packet::encode_header(PacketType::Ack, seq)], self.addr)?;
        self.recv_seq = next_seq(seq);
        Ok(())
    }
    pub fn decode<'a>(&mut self, data: &'a Bytes) -> Result<Packet<'a>, DecodeError> {
        use DecodeError::*;
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
                Subscribe => if data.remaining() > 0 {
                    self.recv_slide(seq)?;
                    let pos = data.position();
                    Ok(Packet::Subscribe(str::from_utf8(&data.into_inner()[pos as usize..])?))
                } else {
                    Err(Malformed(Subscribe))
                },
                PublishStart => match self.recv_publish_buf {
                    None => {
                        self.recv_slide(seq)?;

                        let msg_len = data.get_u32_be() as usize;
                        let pos = data.position() as usize;
                        let msg_buf = MessageBuffer::new(msg_len, data.into_inner().slice_from(pos))?;

                        match msg_buf.full() {
                            false => {
                                self.recv_publish_buf = Some(msg_buf);
                                Err(Partial(PublishData))
                            },
                            true => Ok(Packet::Message(msg_buf.take()))
                        }
                    },
                    Some(_) => Err(InvalidPublishState(true)),
                },
                PublishData => match self.recv_publish_buf {
                    Some(_) => {
                        self.recv_slide(seq)?;
                        let pos = data.position() as usize;
                        match self.recv_publish_buf.as_mut().unwrap().append(data.into_inner().slice_from(pos))? {
                            true => Ok(Packet::Message(self.recv_publish_buf.take().unwrap().take())),
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

    pub fn send_heartbeat(&self, addr: SocketAddr) -> Result<(), io::Error> {
        self.socket.send_to(&[Packet::encode_header(PacketType::Heartbeat, 0)], addr)?;
        Ok(())
    }
    pub fn send_disconnect(&self, addr: SocketAddr) -> Result<(), io::Error> {
        self.socket.send_to(&[Packet::encode_header(PacketType::Disconnect, 0)], addr)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
