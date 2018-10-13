use std::str;
use std::fmt::{self, Display};

use enum_primitive::FromPrimitive;
use bytes::{BufMut, Bytes, BytesMut};

use ::Error;
use constants::*;
use jqtt::IncomingMessage;

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
    pub fn make_connect() -> Bytes {
        let mut data = BytesMut::with_capacity(1 + CONNECT_MAGIC.len());
        data.put_u8(Packet::encode_header(PacketType::Connect, 0));
        data.put_slice(CONNECT_MAGIC);
        data.freeze()
    }
    pub fn validate_connect(buffer: &Bytes) -> Result<Packet, Error> {
        if buffer.len() != 1 + CONNECT_MAGIC.len() || 
            PacketType::from_u8(buffer[0]) != Some(PacketType::Connect) || 
            &buffer[1..] != CONNECT_MAGIC {
            return Err(Error::InvalidConnect);
        }

        Ok(Packet::Connect)
    }

    pub(crate) fn encode_header(packet_type: PacketType, seq: u8) -> u8 {
        ((packet_type as u8) << SEQ_BITS) | seq
    }
    pub(crate) fn make_subscribe(buffer: &mut BytesMut, topic: &str, seq: u8) -> Bytes {
        let topic = topic.as_bytes();
        let mut data = buffer.split_to(1 + topic.len());
        data.put_u8(Packet::encode_header(PacketType::Subscribe, seq));
        data.put_slice(topic);
        data.freeze()
    }
}
