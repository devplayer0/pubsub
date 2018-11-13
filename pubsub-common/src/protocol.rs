use std::cmp;
use std::hash::{Hash, Hasher};
use std::mem::size_of;
use std::str;
use std::collections::LinkedList;
use std::sync::{Arc, Mutex, Condvar};
use std::time::Instant;
use std::net::{SocketAddr, UdpSocket};

use bytes::{IntoBuf, Buf, BufMut, Bytes, BytesMut};

use ::Error;
use constants::*;
use util::{self, BufferProvider};
use timer::{Timer, TimerManager};
use packet::{Packet, PacketType};

#[inline]
pub(crate) fn window_end(window_start: u8) -> u8 {
    (window_start + WINDOW_SIZE) % SEQ_RANGE
}
#[inline]
pub(crate) fn seq_in_window(window_start: u8, seq: u8) -> bool {
    let end = window_end(window_start);
    (seq < window_start) != ((seq < end) != (end < window_start))
}
#[inline]
pub(crate) fn slide_amount(window_start: u8, seq: u8) -> u8 {
    (seq as i8 - window_start as i8).mod_euc(SEQ_RANGE as i8) as u8 + 1
}
#[inline]
pub(crate) fn next_seq(seq: u8) -> u8 {
    (seq + 1) % SEQ_RANGE
}

#[derive(Debug)]
pub struct MessageStart {
    id: u32,
    total_size: u32,
    headerless: Bytes,
}
impl Clone for MessageStart {
    fn clone(&self) -> MessageStart {
        MessageStart {
            id: self.id,
            total_size: self.total_size,
            headerless: self.headerless.clone(),
        }
    }
}
impl Hash for MessageStart {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}
impl PartialEq for MessageStart {
    fn eq(&self, other: &MessageStart) -> bool {
        self.id == other.id
    }
}
impl Eq for MessageStart {}
impl MessageStart {
    fn new(mut data: Bytes) -> Result<MessageStart, Error> {
        data.advance(1);
        let mut buf = data.into_buf();
        let id = buf.get_u32_be();
        let total_size = buf.get_u32_be();

        let mut data = buf.into_inner();
        data.advance(size_of::<u32>());
        str::from_utf8(&data[size_of::<u32>()..])?;

        Ok(MessageStart {
            id,
            total_size,
            headerless: data,
        })
    }

    pub fn id(&self) -> u32 {
        self.id
    }
    pub fn set_id(&mut self, id: u32) {
        self.id = id;
    }
    pub fn size(&self) -> u32 {
        self.total_size
    }
    pub fn topic(&self) -> &str {
        unsafe { str::from_utf8_unchecked(&self.headerless[size_of::<u32>()..]) }
    }
    pub fn take_topic(mut self) -> Bytes {
        self.headerless.advance(size_of::<u32>());
        self.headerless
    }

    pub fn with_seq(&self, buf_source: &BufferProvider, seq: u8) -> Bytes {
        let mut data = buf_source.allocate(1 + size_of::<u32>() + self.headerless.len());
        data.put_u8(Packet::encode_header(PacketType::PublishStart, seq));
        data.put_u32_be(self.id);
        data.put(&self.headerless);
        data.freeze()
    }
}

#[derive(Debug)]
pub struct MessageSegment {
    start: Option<MessageStart>,

    id: u32,
    headerless: Bytes,
}
impl Hash for MessageSegment {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}
impl PartialEq for MessageSegment {
    fn eq(&self, other: &MessageSegment) -> bool {
        self.id == other.id
    }
}
impl Eq for MessageSegment {}
impl Clone for MessageSegment {
    fn clone(&self) -> MessageSegment {
        let start = match self.start {
            Some(ref s) => Some(s.clone()),
            None => None,
        };
        MessageSegment {
            start,

            id: self.id,
            headerless: self.headerless.clone(),
        }
    }
}
impl MessageSegment {
    fn new(mut data: Bytes) -> MessageSegment {
        data.advance(1);
        let mut buf = data.into_buf();
        let id = buf.get_u32_be();

        let mut data = buf.into_inner();
        data.advance(size_of::<u32>());

        MessageSegment {
            start: None,

            id,
            headerless: data,
        }
    }

    pub fn link_start(&mut self, start: &MessageStart) {
        self.start = Some(start.clone());
    }
    pub fn size(&self) -> u32 {
        self.headerless.len() as u32
    }
    pub fn topic(&self) -> Option<&str> {
        match self.start {
            Some(ref s) => Some(s.topic()),
            None => None,
        }
    }

    pub fn id(&self) -> u32 {
        self.id
    }
    pub fn set_id(&mut self, id: u32) {
        self.id = id;
    }
    pub fn into_inner(self) -> Bytes {
        self.headerless
    }
    pub fn with_seq(&self, buf_source: &BufferProvider, seq: u8) -> Bytes {
        let mut data = buf_source.allocate(1 + size_of::<u32>() + self.headerless.len());
        data.put_u8(Packet::encode_header(PacketType::PublishData, seq));
        data.put_u32_be(self.id);
        data.put(&self.headerless);
        data.freeze()
    }
}

#[derive(PartialEq, Eq, Hash, Debug)]
pub enum Timeout {
    Keepalive(SocketAddr),
    Ack(SocketAddr),
}


#[derive(Debug)]
pub struct KeepaliveManager {
    timers: TimerManager<Timeout>,
    addr: SocketAddr,

    timer: Timer<Timeout>,
}
impl Drop for KeepaliveManager {
    fn drop(&mut self) {
        self.timers.cancel(&self.timer);
    }
}
impl KeepaliveManager {
    pub fn new<C: FnMut(Timeout) + Send + Sync + 'static>(timers: &TimerManager<Timeout>, callback: C, addr: SocketAddr) -> KeepaliveManager {
        let timer = timers.post_new(Timeout::Keepalive(addr), callback, Instant::now() + KEEPALIVE_TIMEOUT);

        KeepaliveManager {
            timers: timers.clone(),
            addr,

            timer,
        }
    }

    pub fn heartbeat(&mut self) {
        if !self.timer.has_message() {
            self.timer.set_message(Timeout::Keepalive(self.addr));
        }
        self.timers.reschedule(&self.timer, Instant::now() + KEEPALIVE_TIMEOUT);
    }
}

#[derive(Debug)]
pub struct Jqtt {
    timers: TimerManager<Timeout>,
    buf_source: BufferProvider,

    addr: SocketAddr,
    socket: UdpSocket,

    send_buffer: LinkedList<Bytes>,
    send_window: LinkedList<Bytes>,
    send_window_sseq: u8,
    send_seq: u8,
    ack_timeout: Timer<Timeout>,
    next_pid: usize,
    acked_pid: Arc<(Mutex<usize>, Condvar)>,
    send_space: Arc<(Mutex<usize>, Condvar)>,

    recv_seq: u8,
}
impl Drop for Jqtt {
    fn drop(&mut self) {
        self.timers.cancel(&self.ack_timeout);
    }
}
impl Jqtt {
    pub fn new<C: FnMut(Timeout) + Send + Sync + 'static>(timers: &TimerManager<Timeout>, buf_source: &BufferProvider, addr: SocketAddr, socket: UdpSocket, timeout_callback: C) -> Jqtt {
        Jqtt {
            timers: timers.clone(),
            buf_source: buf_source.clone(),

            addr,
            socket,

            send_buffer: LinkedList::new(),
            send_window: LinkedList::new(),
            send_window_sseq: 0,
            send_seq: 0,
            ack_timeout: timers.create(Timeout::Ack(addr), timeout_callback),
            next_pid: 1,
            acked_pid: Arc::new((Mutex::new(0), Condvar::new())),
            send_space: Arc::new((Mutex::new(WINDOW_SIZE as usize + SEND_BUFFER_SIZE), Condvar::new())),

            recv_seq: 0,
        }
    }

    pub fn send_seq(&self) -> u8 {
        self.send_seq
    }
    pub fn handle_ack_timeout(&mut self) -> Result<(), Error> {
        self.reset_ack_timeout();
        {
            let mut iter = self.send_window.iter();
            for _ in 0..self.send_window.len() {
                let data = iter.next().unwrap();
                self.socket.send_to(data, self.addr)?;
            }
        }

        Ok(())
    }
    fn reset_ack_timeout(&self) {
        if !self.ack_timeout.has_message() {
            self.ack_timeout.set_message(Timeout::Ack(self.addr));
        }
        self.timers.reschedule(&self.ack_timeout, Instant::now() + ACK_TIMEOUT);
    }
    #[inline]
    fn window_full(&self) -> bool {
        self.send_window.len() >= WINDOW_SIZE as usize
    }
    pub fn handle_ack(&mut self, seq: u8) -> Result<(), Error> {
        let slide = slide_amount(self.send_window_sseq, seq) as usize;
        if !seq_in_window(self.send_window_sseq, seq) || slide > self.send_window.len() {
            return Err(Error::InvalidAck(self.send_window_sseq, cmp::min(WINDOW_SIZE as usize, self.send_window.len()) as u8, seq));
        }

        self.send_window = self.send_window.split_off(slide);
        self.send_window_sseq = next_seq(seq);
        self.timers.cancel(&self.ack_timeout);

        if !self.send_buffer.is_empty() {
            let buf_size = self.send_buffer.len();
            let new_send_buf = self.send_buffer.split_off(cmp::min(slide, buf_size));

            for data in &self.send_buffer {
                self.socket.send_to(data, self.addr)?;
            }
            self.send_window.append(&mut self.send_buffer);
            assert!(self.send_window.len() <= WINDOW_SIZE as usize);
            self.send_buffer = new_send_buf;
            self.reset_ack_timeout();
        }

        {
            let &(ref lock, ref cvar) = &*self.acked_pid;
            let mut acked = lock.lock().unwrap();
            *acked = acked.wrapping_add(slide);
            cvar.notify_all();
        }
        self.refresh_send_space();
        Ok(())
    }
    #[inline]
    pub fn send_buffer_full(&self) -> bool {
        self.send_buffer.len() == SEND_BUFFER_SIZE
    }
    pub fn gbn_send(&mut self, data: Bytes) -> Result<usize, Error> {
        if self.window_full() && self.send_buffer_full() {
            return Err(Error::SendBufferFull);
        }

        self.send_seq = next_seq(self.send_seq);
        if self.send_window.is_empty() {
            self.reset_ack_timeout();
        }
        if !self.window_full() {
            self.send_window.push_back(data);
            self.socket.send_to(self.send_window.back().unwrap(), self.addr)?;
        } else {
            self.send_buffer.push_back(data);
        }
        self.refresh_send_space();

        let pid = self.next_pid;
        self.next_pid = pid.wrapping_add(1);
        Ok(pid)
    }
    #[inline]
    fn refresh_send_space(&self) {
        let &(ref lock, ref cvar) = &*self.send_space;
        let mut space = lock.lock().unwrap();
        *space = WINDOW_SIZE as usize + SEND_BUFFER_SIZE - self.send_window.len() - self.send_buffer.len();
        cvar.notify_all();
    }
    pub fn ack_condvar(&self) -> Arc<(Mutex<usize>, Condvar)> {
        Arc::clone(&self.acked_pid)
    }
    pub fn send_space_condvar(&self) -> Arc<(Mutex<usize>, Condvar)> {
        Arc::clone(&self.send_space)
    }

    fn recv_slide(&mut self, seq: u8) -> Result<(), Error> {
        if seq != self.recv_seq {
            return Err(Error::OutOfOrder(self.recv_seq, seq));
        }

        self.socket.send_to(&[Packet::encode_header(PacketType::Ack, seq)], self.addr)?;
        self.recv_seq = next_seq(seq);
        Ok(())
    }
    pub fn decode<'b>(&mut self, data: &'b Bytes) -> Result<Packet<'b>, Error> {
        use Error::*;
        use PacketType::*;

        let mut data = data.into_buf();
        if data.remaining() == 0 {
            return Err(InvalidHeader);
        }

        let (t, seq) = Packet::decode_header(data.get_u8())?;
        match t {
            Connect => match data.bytes() {
                CONNECT_MAGIC => Ok(Packet::Connect),
                _ => Err(Malformed(Connect)),
            },
            ConnAck => match data.remaining() {
                2 if seq & 0b110 == 0 => Ok(Packet::ConnAck(match seq {
                    0 => false,
                    1 => true,
                    _ => panic!("impossible"),
                }, data.get_u16_be())),
                _ => Err(Malformed(ConnAck)),
            },
            Heartbeat => match data.remaining() {
                0 if seq == 0 => Ok(Packet::Heartbeat),
                _ => Err(Malformed(Heartbeat)),
            },
            Ack => match data.remaining() {
                0 => {
                    self.handle_ack(seq)?;
                    Ok(Packet::Ack(seq))
                },
                _ => Err(Malformed(Ack)),
            },
            Disconnect => match data.remaining() {
                0 => {
                    self.recv_slide(seq)?;
                    Ok(Packet::Disconnect)
                },
                _ => Err(Malformed(Disconnect)),
            },
            Subscribe | Unsubscribe => if data.remaining() > 0 {
                self.recv_slide(seq)?;
                let pos = data.position();
                let topic = str::from_utf8(&data.into_inner()[pos as usize..])?;
                Ok(match t {
                    Subscribe => Packet::Subscribe(topic),
                    Unsubscribe => Packet::Unsubscribe(topic),
                    _ => panic!("impossible"),
                })
            } else {
                Err(Malformed(Subscribe))
            },
            PublishStart => if data.remaining() > size_of::<u32>() + size_of::<u32>() {
                self.recv_slide(seq)?;
                Ok(Packet::PublishStart(MessageStart::new(data.into_inner().clone())?))
            } else {
                Err(Malformed(PublishStart))
            },
            PublishData => if data.remaining() >= size_of::<u32>() {
                self.recv_slide(seq)?;
                Ok(Packet::PublishData(MessageSegment::new(data.into_inner().clone())))
            } else {
                 Err(Malformed(PublishData))
            },
        }
    }

    pub fn send_connect(&self) -> Result<(), Error> {
        let mut data = self.buf_source.allocate(1 + CONNECT_MAGIC.len());
        data.put_u8(Packet::encode_header(PacketType::Connect, 0));
        data.put_slice(CONNECT_MAGIC);

        self.socket.send_to(&data, self.addr)?;
        Ok(())
    }
    pub fn send_connack(&self, keepalive: bool, max_packet_size: u16) -> Result<(), Error> {
        let mut bytes = BytesMut::with_capacity(3);
        let header = Packet::encode_header(PacketType::ConnAck, match keepalive {
            false => 0,
            true => 1,
        });
        bytes.put(header);
        bytes.put_u16_be(max_packet_size);

        self.socket.send_to(&bytes, self.addr)?;
        Ok(())
    }
    pub fn send_heartbeat(&self) -> Result<(), Error> {
        let header = Packet::encode_header(PacketType::Heartbeat, 0);
        self.socket.send_to(&[header], self.addr)?;
        Ok(())
    }
    pub fn send_disconnect(&mut self) -> Result<(), Error> {
        let mut bytes = BytesMut::with_capacity(1);
        bytes.put_u8(Packet::encode_header(PacketType::Disconnect, self.send_seq));

        self.send_buffer.clear();
        self.gbn_send(bytes.freeze())?;
        Ok(())
    }
    pub fn send_subscribe(&mut self, topic: &str, max_packet_size: u16) -> Result<usize, Error> {
        let topic = topic.as_bytes();
        assert!(topic.len() <= util::max_topic_len(max_packet_size) as usize);

        let mut data = self.buf_source.allocate(1 + topic.len());
        data.put_u8(Packet::encode_header(PacketType::Subscribe, self.send_seq));
        data.put(topic);
        self.gbn_send(data.freeze())
    }
    pub fn send_unsubscribe(&mut self, topic: &str, max_packet_size: u16) -> Result<usize, Error> {
        let topic = topic.as_bytes();
        assert!(topic.len() <= util::max_topic_len(max_packet_size) as usize);

        let mut data = self.buf_source.allocate(1 + topic.len());
        data.put_u8(Packet::encode_header(PacketType::Unsubscribe, self.send_seq));
        data.put(topic);
        self.gbn_send(data.freeze())
    }
    pub fn send_msg_start(&mut self, start: &MessageStart) -> Result<usize, Error> {
        let data = start.with_seq(&self.buf_source, self.send_seq());
        self.gbn_send(data)
    }
    pub fn send_msg_segment(&mut self, segment: &MessageSegment) -> Result<usize, Error> {
        let data = segment.with_seq(&self.buf_source, self.send_seq());
        self.gbn_send(data)
    }
}
