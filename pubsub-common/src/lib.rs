#![feature(euclidean_division)]

use std::ops::Deref;
use std::cmp;
use std::hash::{Hash, Hasher};
use std::fmt::{self, Display};
use std::string::FromUtf8Error;
use std::collections::LinkedList;
use std::time::{Duration, Instant};
use std::io::{self, Write, Cursor};
use std::net::{SocketAddr, UdpSocket};

#[macro_use]
extern crate quick_error;
#[macro_use]
extern crate log;
#[macro_use]
extern crate enum_primitive;
extern crate byteorder;
extern crate crossbeam_channel;
extern crate priority_queue;

#[cfg(test)]
#[macro_use]
extern crate lazy_static;

use enum_primitive::FromPrimitive;
use crossbeam_channel::Receiver;

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
        Utf8Decode(err: FromUtf8Error) {
            from()
            cause(err)
            description(err.description())
            display("failed to decode string from packet: {}", err)
        }
    }
}

#[derive(Eq, Debug)]
pub struct Data {
    buffer: Vec<u8>,
    size: usize,
}
impl Data {
    pub fn from_buffer(buffer: Vec<u8>, size: usize) -> Data {
        Data {
            buffer,
            size,
        }
    }
    pub fn take(self) -> Vec<u8> {
        self.buffer
    }
    pub fn cursor(&mut self) -> Cursor<&mut [u8]> {
        Cursor::new(&mut self.buffer[..self.size])
    }
}
impl Deref for Data {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.buffer[..self.size]
    }
}
impl From<Vec<u8>> for Data {
    fn from(buf: Vec<u8>) -> Self {
        let size = buf.len();
        Data::from_buffer(buf, size)
    }
}
impl Clone for Data {
    fn clone(&self) -> Self {
        Data::from_buffer(self.buffer.clone(), self.size)
    }
}
impl PartialEq for Data {
    fn eq(&self, other: &Data) -> bool {
        self.buffer == other.buffer
    }
}
impl Hash for Data {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.buffer.hash(state);
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
        })
    }
}

#[derive(Debug)]
pub enum Packet {
    Connect,
    Heartbeat,
    Ack(u8),
    Disconnect,
    Subscribe(String),
}
impl Packet {
    pub fn validate_connect(buffer: &Data) -> Result<Packet, DecodeError> {
        if buffer.len() != 1 + CONNECT_MAGIC.len() || 
            PacketType::from_u8(buffer[0]) != Some(PacketType::Connect) || 
            &buffer[1..] != CONNECT_MAGIC {
            return Err(DecodeError::InvalidConnect);
        }

        Ok(Packet::Connect)
    }

    pub fn make_connect(buffer: Vec<u8>) -> Data {
        let mut buffer = Data::from_buffer(buffer, 1 + CONNECT_MAGIC.len());
        {
            let mut cursor = buffer.cursor();
            cursor.write(&[0]).unwrap();
            cursor.write(&CONNECT_MAGIC).unwrap();
        }
        buffer
    }
    pub fn make_heartbeat(mut buffer: Vec<u8>) -> Data {
        buffer[0] = (PacketType::Heartbeat as u8) << SEQ_BITS;
        Data::from_buffer(buffer, 1)
    }
    pub fn make_disconnect(mut buffer: Vec<u8>) -> Data {
        buffer[0] = (PacketType::Disconnect as u8) << SEQ_BITS;
        Data::from_buffer(buffer, 1)
    }
    pub fn make_subscribe(buffer: Vec<u8>, topic: &str, seq: u8) -> Data {
        let mut cursor = Cursor::new(buffer);
        cursor.write(&[(PacketType::Subscribe as u8) << SEQ_BITS | seq]).unwrap();
        cursor.write(topic.as_bytes()).unwrap();
        let size = cursor.position() as usize;
        Data::from_buffer(cursor.into_inner(), size)
    }
}

#[derive(PartialEq, Eq, Hash, Debug)]
pub enum GbnTimeout {
    Heartbeat,
}
#[derive(Debug)]
pub struct GoBackN {
    timers: TimerManager<GbnTimeout>,
    socket: UdpSocket,

    timers_client: TimerManagerClient,
    timeout_rx: Receiver<GbnTimeout>,
    heartbeat_timeout: Option<Timer>,

    send_buffer: LinkedList<Data>,
    send_buffer_sseq: u8,
    send_seq: u8,

    recv_seq: u8,
}
impl GoBackN {
    pub fn new(timers: &TimerManager<GbnTimeout>, socket: UdpSocket, use_heartbeats: bool) -> GoBackN {
        let (timeout_tx, timeout_rx) = crossbeam_channel::unbounded();
        let timers_client = timers.register(&timeout_tx);
        let heartbeat_timeout = match use_heartbeats {
            true => Some(timers.post_message(timers_client, GbnTimeout::Heartbeat, Instant::now() + HEARTBEAT_TIMEOUT)),
            false => None
        };
        GoBackN {
            timers: timers.clone(),
            socket,

            timers_client,
            timeout_rx,
            heartbeat_timeout,

            send_buffer: LinkedList::new(),
            send_buffer_sseq: 0,
            send_seq: 0,

            recv_seq: 0,
        }
    }

    pub fn handle_ack(&mut self, src: SocketAddr, seq: u8) -> LinkedList<Data> {
        if !seq_in_window(self.send_buffer_sseq, seq) || slide_amount(self.send_buffer_sseq, seq) as usize > self.send_buffer.len() {
            warn!("received ack from {} outside of window", src);
            return LinkedList::new();
        }

        let buffer_size = self.send_buffer.len();
        let acknowledged = self.send_buffer.split_off(cmp::min(slide_amount(self.send_buffer_sseq, seq) as usize, buffer_size));

        self.send_buffer_sseq = next_seq(seq);
        self.send_seq = self.send_buffer_sseq;
        acknowledged
    }
    fn recv_slide(&mut self, src: SocketAddr, seq: u8) -> Result<(), DecodeError> {
        if seq != self.recv_seq {
            return Err(DecodeError::OutOfOrder(self.recv_seq, seq));
        }

        self.socket.send_to(&[(PacketType::Ack as u8) << SEQ_BITS | seq], src)?;
        self.recv_seq = next_seq(seq);
        Ok(())
    }
    pub fn decode(&mut self, src: SocketAddr, buffer: &Data) -> Result<Packet, DecodeError> {
        use DecodeError::*;
        use PacketType::*;

        if buffer.len() == 0 {
            return Err(InvalidHeader);
        }
        let header = buffer[0];
        let t = header >> SEQ_BITS;
        if let Some(t) = PacketType::from_u8(t) {
            let seq = header & (0xff >> (8-SEQ_BITS));
            return match t {
                Connect => Err(AlreadyConnected),
                Heartbeat => match buffer.len() {
                    1 if seq == 0 => {
                        if let Some(timer) = self.heartbeat_timeout {
                            self.timers.cancel_message(timer);
                            self.heartbeat_timeout = Some(self.timers.post_message(self.timers_client, GbnTimeout::Heartbeat, Instant::now() + HEARTBEAT_TIMEOUT));
                        }

                        Ok(Packet::Heartbeat)
                    },
                    _ => Err(Malformed(Heartbeat)),
                },
                Ack => match buffer.len() {
                     1 => Ok(Packet::Ack(seq)),
                     _ => Err(Malformed(Ack)),
                },
                Disconnect => match buffer.len() {
                    1 if seq == 0 => Ok(Packet::Disconnect),
                    _ => Err(Malformed(Disconnect)),
                },
                Subscribe => if buffer.len() > 1 {
                    self.recv_slide(src, seq)?;
                    Ok(Packet::Subscribe(String::from_utf8(buffer[1..].into())?))
                } else {
                    Err(Malformed(Subscribe))
                },
            }
        } else {
            Err(InvalidType(t))
        }
    }

    pub fn send_heartbeat(&self, addr: SocketAddr, buffer: Vec<u8>) -> Result<Vec<u8>, io::Error> {
        let heartbeat = Packet::make_heartbeat(buffer);
        self.socket.send_to(&heartbeat, addr)?;
        Ok(heartbeat.take())
    }
    pub fn send_disconnect(&self, addr: SocketAddr, buffer: Vec<u8>) -> Result<Vec<u8>, io::Error> {
        let disconnect = Packet::make_disconnect(buffer);
        self.socket.send_to(&disconnect, addr)?;
        Ok(disconnect.take())
    }
}

#[cfg(test)]
mod tests;
