use std::ops::{Deref, DerefMut};
use std::cmp;
use std::mem::size_of;
use std::str;
use std::collections::HashMap;
use std::io::{self, Read};

use bytes::{IntoBuf, Buf, BufMut, Bytes, BytesMut, Reader};

use ::Error;
use common::util::{self, BufferProvider};
use common::packet::{PacketType, Packet};
pub use common::protocol::{MessageStart, MessageSegment};

pub trait MessageListener {
    fn recv_start(&mut self, start: MessageStart) -> Result<(), Error>;
    fn recv_data(&mut self, data: MessageSegment) -> Result<(), Error>;
    fn on_error(&mut self, error: Error);
}

#[derive(Debug)]
pub struct Message<R: Read + Send> {
    size: u32,
    topic_bytes: Bytes,
    input: R,
}
impl<R: Read + Send> Deref for Message<R> {
    type Target = R;
    fn deref(&self) -> &Self::Target {
        &self.input
    }
}
impl<R: Read + Send> DerefMut for Message<R> {
    fn deref_mut(&mut self) -> &mut R {
        &mut self.input
    }
}
impl<R: Read + Send> Message<R> {
    pub fn new(size: u32, topic: &str, input: R) -> Message<R> {
        Message {
            size,
            topic_bytes: topic.as_bytes().into(),
            input,
        }
    }
    pub fn into_boxed<'a, B: Read + Send + 'a>(message: Message<B>) -> Message<Box<Read + Send + 'a>> {
        Message {
            size: message.size,
            topic_bytes: message.topic_bytes,
            input: Box::new(message.input),
        }
    }

    pub fn size(&self) -> u32 {
        self.size
    }
    pub fn topic(&self) -> &str {
        unsafe { str::from_utf8_unchecked(&self.topic_bytes) }
    }
}

#[derive(DebugStub)]
pub(crate) struct OutgoingMessage<'a> {
    buf_source: BufferProvider,

    #[debug_stub = "Message"]
    message: Message<Box<Read + Send + 'a>>,
    id: u32,
    publish_start: Option<BytesMut>,
    read: u32,
}
impl<'a> OutgoingMessage<'a> {
    pub(crate) fn new<R: Read + Send + 'a>(buf_source: &BufferProvider, max_packet_size: u16, message: Message<R>, id: u32) -> OutgoingMessage<'a> {
        let topic_len = message.topic().as_bytes().len();
        assert!(topic_len <= util::max_topic_len(max_packet_size) as usize);

        let mut first_buffer = buf_source.allocate(1 + size_of::<u32>() + size_of::<u32>() + topic_len);
        first_buffer.put_u8(0);
        first_buffer.put_u32_be(id);
        first_buffer.put_u32_be(message.size());
        first_buffer.put(message.topic());
        OutgoingMessage {
            buf_source: buf_source.clone(),

            message: Message::<R>::into_boxed(message),
            id,
            publish_start: Some(first_buffer),
            read: 0,
        }
    }

    pub(crate) fn first_sent(&self) -> bool {
        !self.publish_start.is_some()
    }
    pub(crate) fn first_packet(&mut self, seq: u8) -> Bytes {
        let mut data = self.publish_start.take().unwrap();
        data[0] = Packet::encode_header(PacketType::PublishStart, seq);
        data.freeze()
    }
    pub(crate) fn next_packet(&mut self, max_packet_size: u16, seq: u8) -> Result<(bool, Option<Bytes>), io::Error> {
        if self.read == self.message.size {
            return Ok((true, None));
        }

        let header_size = 1 + size_of::<u32>();
        let data_size = max_packet_size as usize - header_size;
        let to_read = cmp::min((self.message.size - self.read) as usize, data_size);
        let mut data = self.buf_source.allocate(header_size + to_read);
        data.put_u8(Packet::encode_header(PacketType::PublishData, seq));
        data.put_u32_be(self.id);

        data.truncate(header_size + to_read);
        unsafe {
            self.message.read_exact(data.bytes_mut())?;
            data.advance_mut(to_read);
        }

        self.read += to_read as u32;
        Ok((self.read == self.message.size, Some(data.freeze())))
    }
}

#[derive(DebugStub)]
struct IncomingMessage {
    start: MessageStart,
    size: u32,
    #[debug_stub = "Chain"]
    buffer: Option<Box<Buf + Send + Sync>>,
}
impl Deref for IncomingMessage {
    type Target = MessageStart;
    fn deref(&self) -> &Self::Target {
        &self.start
    }
}
impl IncomingMessage {
    fn new(start: MessageStart) -> IncomingMessage {
        IncomingMessage {
            start,
            size: 0,
            buffer: None,
        }
    }
    fn append(&mut self, data: MessageSegment) -> Result<bool, Error> {
        let new_size = self.size + data.size();
        if new_size > self.start.size() {
            return Err(Error::MessageTooBig(self.start.size(), new_size));
        }

        self.buffer = Some(if self.buffer.is_some() {
            let buffer = self.buffer.take().unwrap();
            Box::new(buffer.chain(data.into_inner()))
        } else {
            Box::new(data.into_inner().into_buf())
        });

        self.size = new_size;
        Ok(new_size == self.start.size())
    }

    fn into_message(mut self) -> Message<Reader<Box<Buf + Send + Sync>>> {
        assert_eq!(self.size, self.start.size());
        Message {
            size: self.start.size(),
            topic_bytes: self.start.take_topic(),
            input: self.buffer.take().unwrap().reader(),
        }
    }
}

pub trait CompleteMessageListener {
    fn recv_message<'a>(&mut self, message: Message<Reader<Box<Buf + Send + Sync + 'a>>>) -> Result<(), Error>;
    fn on_error(&mut self, error: Error);
}
pub struct MessageCollector<L: CompleteMessageListener> {
    listener: L,
    messages: HashMap<u32, IncomingMessage>,
}
impl<L: CompleteMessageListener> MessageCollector<L> {
    pub fn new(listener: L) -> MessageCollector<L> {
        MessageCollector {
            listener,
            messages: HashMap::new(),
        }
    }
}
impl<L: CompleteMessageListener> MessageListener for MessageCollector<L> {
    fn recv_start(&mut self, start: MessageStart) -> Result<(), Error> {
        match self.messages.get(&start.id()) {
            None => {
                self.messages.insert(start.id(), IncomingMessage::new(start));
                Ok(())
            },
            Some(_) => Err(Error::InvalidPublishState(start.id(), true)),
        }
    }
    fn recv_data(&mut self, data: MessageSegment) -> Result<(), Error> {
        let id = data.id();
        if match self.messages.get_mut(&id) {
            Some(info) => info.append(data)?,
            None => return Err(Error::InvalidPublishState(data.id(), false)),
        } {
            self.listener.recv_message(self.messages.remove(&id).unwrap().into_message())?;
        }

        Ok(())
    }
    fn on_error(&mut self, error: Error) {
        self.listener.on_error(error);
    }
}
