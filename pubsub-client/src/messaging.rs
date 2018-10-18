use std::ops::{Deref, DerefMut};
use std::cmp;
use std::mem::size_of;
use std::str;
use std::io::{self, Read};

use bytes::{IntoBuf, Buf, BufMut, Bytes, BytesMut, Reader};

use ::Error;
use common::constants::*;
use common::util::BufferProvider;
use common::packet::{PacketType, Packet};
use common::protocol::{MessageStart, MessageSegment};

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
    publish_start: Option<BytesMut>,
    read: u32,
}
impl<'a> OutgoingMessage<'a> {
    pub(crate) fn new<R: Read + Send + 'a>(buf_source: &BufferProvider, message: Message<R>) -> OutgoingMessage<'a> {
        let topic_len = message.topic().as_bytes().len();
        assert!(topic_len <= MAX_TOPIC_LENGTH);

        let mut first_buffer = buf_source.allocate(1 + size_of::<u32>() + topic_len);
        first_buffer.put_u8(0);
        first_buffer.put_u32_be(message.size());
        first_buffer.put(message.topic());
        OutgoingMessage {
            buf_source: buf_source.clone(),

            message: Message::<R>::into_boxed(message),
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
    pub(crate) fn next_packet(&mut self, max_packet_size: usize, seq: u8) -> Result<(bool, Option<Bytes>), io::Error> {
        if self.read == self.message.size {
            return Ok((true, None));
        }

        let header_size = 1;
        let data_size = max_packet_size - header_size;
        let to_read = cmp::min((self.message.size - self.read) as usize, data_size);
        let mut data = self.buf_source.allocate(header_size + to_read);
        data.put_u8(Packet::encode_header(PacketType::PublishData, seq));

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
    fn append(&mut self, mut data: MessageSegment) -> Result<bool, Error> {
        data.advance(1);
        let new_size = self.size + data.len() as u32;
        println!("new size: {}, total size: {}", new_size, self.start.size());
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
    message: Option<IncomingMessage>,
}
impl<L: CompleteMessageListener> MessageCollector<L> {
    pub fn new(listener: L) -> MessageCollector<L> {
        MessageCollector {
            listener,
            message: None,
        }
    }
}
impl<L: CompleteMessageListener> MessageListener for MessageCollector<L> {
    fn recv_start(&mut self, start: MessageStart) -> Result<(), Error> {
        match self.message {
            None => {
                self.message = Some(IncomingMessage::new(start));
                Ok(())
            },
            Some(_) => Err(Error::InvalidPublishState(true)),
        }
    }
    fn recv_data(&mut self, data: MessageSegment) -> Result<(), Error> {
        if self.message.is_some() {
            if self.message.as_mut().unwrap().append(data)? {
                self.listener.recv_message(self.message.take().unwrap().into_message())?;
            }

            Ok(())
        } else {
            Err(Error::InvalidPublishState(false))
        }
    }
    fn on_error(&mut self, error: Error) {
        self.listener.on_error(error);
    }
}
