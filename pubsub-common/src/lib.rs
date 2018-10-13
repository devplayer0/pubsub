#![feature(euclidean_division)]

use std::str::{self, Utf8Error};
use std::io;

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

pub mod util;
pub mod timer;
pub mod constants;
pub mod packet;
pub mod jqtt;
use packet::PacketType;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
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

#[cfg(test)]
mod tests;
