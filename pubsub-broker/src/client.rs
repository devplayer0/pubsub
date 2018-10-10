use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::net::{SocketAddr, UdpSocket};

use crossbeam::queue::MsQueue;

use common::{TimerManager, Packet, GoBackN, GbnTimeout};
use ::Error;

#[derive(Debug)]
pub struct Client {
    addr: SocketAddr,
    buffers: Arc<MsQueue<Vec<u8>>>,
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
impl Client {
    pub fn new(timers: &TimerManager<GbnTimeout>, addr: SocketAddr, socket: UdpSocket, buffers: Arc<MsQueue<Vec<u8>>>) -> Client {
        Client {
            addr,
            buffers,
            gbn: GoBackN::new(timers, socket, true),

            connected: false,
        }
    }
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
    pub fn gbn(&mut self) -> &mut GoBackN {
        &mut self.gbn
    }

    pub fn send_heartbeat(&self) -> Result<(), Error> {
        self.buffers.push(self.gbn.send_heartbeat(self.addr, self.buffers.pop())?);
        Ok(())
    }
    pub fn send_disconnect(&self) -> Result<(), Error> {
        self.buffers.push(self.gbn.send_disconnect(self.addr, self.buffers.pop())?);
        Ok(())
    }
    pub fn handle(&mut self, packet: Packet) -> Result<(), Error> {
        match packet {
            Packet::Ack(seq) => {
                for buffer in self.gbn.handle_ack(self.addr, seq) {
                    self.buffers.push(buffer.take());
                }
            },
            Packet::Heartbeat => {
                // TODO: timeout
                debug!("got heartbeat from {}", self.addr);
            },
            Packet::Subscribe(topic) => {
                info!("{} wants to subscribe to '{}'", self.addr, topic)
            },
            _ => return Err(Error::InvalidPacketType)
        }

        Ok(())
    }
}
impl Eq for Client {}
