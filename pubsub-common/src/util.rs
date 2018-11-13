use std::cmp;
use std::mem::size_of;
use std::sync::{Arc, Mutex};
use std::net::SocketAddr;

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
