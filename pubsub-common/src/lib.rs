#![feature(euclidean_division)]

use std::ops::Deref;
use std::cmp;
use std::fmt::{self, Display};
use std::collections::LinkedList;
use std::io::{self, Write, Cursor};
use std::net::{SocketAddr, UdpSocket};

#[macro_use]
extern crate quick_error;
#[macro_use]
extern crate log;
#[macro_use]
extern crate enum_primitive;
extern crate byteorder;

use enum_primitive::FromPrimitive;

pub const IPV4_MAX_PACKET_SIZE: usize = 508;
pub const IPV6_MAX_PACKET_SIZE: usize = 1212;

pub const SEQ_BITS: u8 = 3;
pub const SEQ_RANGE: u8 = 8;
pub const WINDOW_SIZE: u8 = SEQ_RANGE - 1;

pub const CONNECT_MAGIC: &'static [u8] = b"JQTT";
pub const HEARTBEAT_MAGIC: u8 = 5;
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
    }
}

#[derive(Debug)]
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

enum_from_primitive! {
    #[derive(Debug, PartialEq)]
    pub enum PacketType {
        Heartbeat = 0,
        Ack = 1,
        Disconnect = 2,
    }
}
impl Display for PacketType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use PacketType::*;
        write!(f, "{}", match self {
            Heartbeat => "HEARTBEAT",
            Ack => "ACK",
            Disconnect => "DISCONNECT",
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
        if &**buffer != CONNECT_MAGIC {
            return Err(DecodeError::InvalidConnect);
        }

        Ok(Packet::Connect)
    }

    pub fn make_connect(buffer: Vec<u8>) -> Data {
        let mut buffer = Data::from_buffer(buffer, CONNECT_MAGIC.len());
        buffer.cursor().write(&CONNECT_MAGIC).unwrap();
        buffer
    }
    pub fn make_heartbeat(mut buffer: Vec<u8>) -> Data {
        buffer[0] = (PacketType::Heartbeat as u8) << SEQ_BITS | HEARTBEAT_MAGIC;
        Data::from_buffer(buffer, 1)
    }
    pub fn make_disconnect(mut buffer: Vec<u8>) -> Data {
        buffer[0] = (PacketType::Disconnect as u8) << SEQ_BITS;
        Data::from_buffer(buffer, 1)
    }
}

#[derive(Debug)]
pub struct GoBackN {
    socket: UdpSocket,

    send_buffer: LinkedList<Data>,
    send_buffer_sseq: u8,
    send_seq: u8,

    recv_seq: u8,
}
impl GoBackN {
    pub fn new(socket: UdpSocket) -> GoBackN {
        GoBackN {
            socket,

            send_buffer: LinkedList::new(),
            send_buffer_sseq: 0,
            send_seq: 0,

            recv_seq: 0,
        }
    }

    pub fn handle_ack(&mut self, src: SocketAddr, seq: u8) -> LinkedList<Data> {
        if !seq_in_window(self.send_buffer_sseq, seq) {
            warn!("received ack from {} outside of window", src);
            return LinkedList::new();
        }

        let buffer_size = self.send_buffer.len();
        let acknowledged = self.send_buffer.split_off(cmp::min(slide_amount(self.send_buffer_sseq, seq) as usize, buffer_size));

        self.send_buffer_sseq = next_seq(seq);
        self.send_seq = self.send_buffer_sseq;
        acknowledged
    }
    pub fn decode(&self, buffer: &Data) -> Result<Packet, DecodeError> {
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
                Heartbeat => if buffer.len() == 1 && seq == HEARTBEAT_MAGIC {
                    Ok(Packet::Heartbeat)
                } else {
                    Err(Malformed(Heartbeat))
                },
                Ack => match buffer.len() {
                     1 => Ok(Packet::Ack(seq)),
                     _ => Err(Malformed(Ack)),
                },
                Disconnect => if buffer.len() == 1 && seq == 0 {
                    Ok(Packet::Disconnect)
                } else {
                    Err(Malformed(Disconnect))
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
