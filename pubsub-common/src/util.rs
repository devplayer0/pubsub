use std::cmp;
use std::sync::{Arc, Mutex};
use std::net::SocketAddr;

use bytes::BytesMut;

use constants;

#[inline]
pub fn max_packet_size(addr: SocketAddr) -> usize {
    match addr {
        SocketAddr::V4(_) => constants::IPV4_MAX_PACKET_SIZE,
        SocketAddr::V6(_) => constants::IPV6_MAX_PACKET_SIZE,
    }
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
            let ptr = buffer.as_ptr();
            buffer.reserve(cmp::max(size, self.hi));
            if buffer.as_ptr() != ptr {
                debug!("buffer realloc");
            }
        }

        buffer.split_to(size)
    }
}
