#![feature(euclidean_division)]
#![feature(try_from)]

use std::ops::Deref;
use std::convert::TryFrom;
use std::fmt::{self, Display};
use std::io::{Write, Cursor};

#[macro_use]
extern crate quick_error;
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
impl<'a> TryFrom<&'a Data> for Packet {
    type Error = DecodeError;
    fn try_from(buffer: &'a Data) -> Result<Self, Self::Error> {
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
}

#[cfg(test)]
mod tests;
