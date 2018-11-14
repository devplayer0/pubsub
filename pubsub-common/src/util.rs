use std::cmp;
use std::fmt::Display;
use std::str::FromStr;
use std::mem::size_of;
use std::sync::{Arc, Mutex};
use std::net::{IpAddr, AddrParseError, ToSocketAddrs, SocketAddr};

use log::LevelFilter;
use bytes::BytesMut;

use constants;

#[inline]
pub fn default_packet_size(addr: SocketAddr) -> u16 {
    match addr {
        SocketAddr::V4(_) => constants::IPV4_DEFAULT_PACKET_SIZE,
        SocketAddr::V6(_) => constants::IPV6_DEFAULT_PACKET_SIZE,
    }
}
#[inline]
pub fn max_topic_len(max_packet_size: u16) -> u16 {
    max_packet_size - 1 - size_of::<u32>() as u16 - size_of::<u32>() as u16
}

pub fn verbosity_to_log_level(verbosity: usize) -> LevelFilter {
    match verbosity {
        0 => LevelFilter::Info,
        1 => LevelFilter::Debug,
        2 | _ => LevelFilter::Trace,
    }
}
pub fn parse_addr(addr: &str) -> Result<std::vec::IntoIter<SocketAddr>, AddrParseError> {
    if let Ok(a) = addr.to_socket_addrs() {
        return Ok(a);
    }

    Ok(vec![(addr.parse::<IpAddr>()?, constants::DEFAULT_PORT).into()].into_iter())
}
pub fn validate_parse<V: FromStr<Err = E>, E: Display>(val: String) -> Result<(), String> {
    val.parse::<V>().map(|_| ()).map_err(|e| format!("{}", e))
}

#[derive(Debug)]
pub struct BufferProvider {
    hi: usize,
    lo: usize,
    buffer: Arc<Mutex<BytesMut>>,
}
impl Clone for BufferProvider {
    fn clone(&self) -> BufferProvider {
        BufferProvider {
            hi: self.hi,
            lo: self.lo,
            buffer: Arc::clone(&self.buffer),
        }
    }
}
impl BufferProvider {
    pub fn new(hi: usize, lo: usize) -> BufferProvider {
        BufferProvider {
            hi,
            lo,
            buffer: Arc::new(Mutex::new(BytesMut::with_capacity(hi))),
        }
    }

    #[inline]
    pub fn allocate(&self, size: usize) -> BytesMut {
        let mut buffer = self.buffer.lock().unwrap();
        if buffer.capacity() < cmp::max(size, self.lo) {
            buffer.reserve(cmp::max(size, self.hi));
        }

        buffer.split_to(size)
    }
}
