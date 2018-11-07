#![feature(euclidean_division)]
#![feature(wait_until, wait_timeout_until)]

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
extern crate priority_queue;

pub mod util;
pub mod timer;
pub mod constants;
pub mod packet;
pub mod protocol;
use packet::PacketType;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
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
        NotConnected {
            description("not connected")
        }
        ClientOnly(packet_type: PacketType) {
            description("received packet which can only be sent by clients")
            display("received packet which can only be sent by clients: {}", packet_type)
        }
        OutOfOrder(expected: u8, seq: u8) {
            description("out of order packet")
            display("out of order packet seq {}, expected {}", seq, expected)
        }
        InvalidAck(window_start: u8, window_end: u8, seq: u8) {
            description("received ack outside of window")
            display("received ack {} outside of window (start {}, size {})", seq, window_start, window_end)
        }
        AckFail(err: io::Error) {
            from()
            cause(err)
            description(err.description())
            display("failed to send ack: {}", err)
        }
        BadString(err: Utf8Error) {
            from()
            cause(err)
            description(err.description())
            display("failed to decode string from packet: {}", err)
        }
        SendBufferFull {
            description("send buffer is full")
        }
    }
}

#[cfg(test)]
mod tests;
