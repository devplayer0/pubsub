use std::str;
use std::fmt::{self, Display};

use enum_primitive::FromPrimitive;

use ::Error;
use constants::*;
use protocol::{MessageStart, MessageSegment};

enum_from_primitive! {
    #[derive(Debug, PartialEq)]
    pub enum PacketType {
        Connect = 0,
        ConnAck = 1,
        Heartbeat = 2,
        Ack = 3,
        Disconnect = 4,
        Subscribe = 5,
        Unsubscribe = 6,
        PublishStart = 7,
        PublishData = 8,
    }
}
impl Display for PacketType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use PacketType::*;
        write!(f, "{}", match self {
            Connect => "CONNECT",
            ConnAck => "CONNACK",
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
    ConnAck(bool, u16),
    Heartbeat,
    Ack(u8),
    Disconnect,
    Subscribe(&'a str),
    Unsubscribe(&'a str),
    PublishStart(MessageStart),
    PublishData(MessageSegment),
}
impl<'a> Packet<'a> {
    pub fn encode_header(packet_type: PacketType, seq: u8) -> u8 {
        ((packet_type as u8) << SEQ_BITS) | seq
    }
    pub fn decode_header(header: u8) -> Result<(PacketType, u8), Error> {
        let t = header >> SEQ_BITS;
        match PacketType::from_u8(t) {
            Some(t) => Ok((t, header & (0xff >> (8-SEQ_BITS)))),
            None => Err(Error::InvalidType(t)),
        }
    }
}
