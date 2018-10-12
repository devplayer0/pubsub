use std::ops::{Deref, DerefMut};
use std::hash::{Hash, Hasher};
use std::net::{SocketAddr, UdpSocket};

use crossbeam_channel::Sender;

use common::util::BufferProvider;
use common::timer::TimerManager;
use common::{Packet, GoBackN, GbnTimeout, OutgoingMessage};
use ::Error;

#[derive(Debug)]
pub struct Client {
    addr: SocketAddr,
    gbn: GoBackN,

    pub connected: bool,
}
impl Hash for Client {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.addr.hash(state);
    }
}
impl PartialEq for Client {
    fn eq(&self, other: &Client) -> bool {
        other.addr == self.addr
    }
}
impl Deref for Client {
    type Target = GoBackN;
    fn deref(&self) -> &Self::Target {
        &self.gbn
    }
}
impl DerefMut for Client {
    fn deref_mut(&mut self) -> &mut GoBackN {
        &mut self.gbn
    }
}
impl Client {
    pub fn new(timers: &TimerManager<GbnTimeout>, addr: SocketAddr, socket: UdpSocket, buf_source: &BufferProvider, timeout_tx: &Sender<GbnTimeout>, use_heartbeats: bool) -> Client {
        Client {
            addr,
            gbn: GoBackN::new(timers, buf_source, addr, socket, timeout_tx, use_heartbeats),

            connected: false,
        }
    }
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn handle(&mut self, packet: Packet) -> Result<(), Error> {
        match packet {
            Packet::Ack(seq) => self.gbn.handle_ack(seq),
            Packet::Heartbeat => debug!("got heartbeat from {}", self.addr),
            Packet::Subscribe(topic) => {
                info!("{} wants to subscribe to '{}'", self.addr, topic);
                if topic == "memes" {
                    let lipsum = include_str!("lipsum.txt");
                    let bytes = std::io::Cursor::new(lipsum.to_owned().into_bytes());
                    self.gbn.queue_message(OutgoingMessage::new(bytes, lipsum.len() as u32))?;
                }
            },
            _ => return Err(Error::InvalidPacketType)
        }

        Ok(())
    }
}
impl Eq for Client {}
