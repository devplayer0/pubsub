use std::hash::{Hash, Hasher};
use std::net::{SocketAddr, UdpSocket};

use bytes::BytesMut;
use crossbeam_channel::Sender;

use common::timer::TimerManager;
use common::{Packet, GoBackN, GbnTimeout};
use ::{Error, check_alloc};

#[derive(Debug)]
pub struct Client {
    addr: SocketAddr,
    buffer: BytesMut,
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
    pub fn new(timers: &TimerManager<GbnTimeout>, addr: SocketAddr, socket: UdpSocket, buffer: &BytesMut, timeout_tx: &Sender<GbnTimeout>, use_heartbeats: bool) -> Client {
        Client {
            addr,
            buffer: buffer.clone(),
            gbn: GoBackN::new(timers, addr, socket, timeout_tx, use_heartbeats),

            connected: false,
        }
    }
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
    pub fn gbn(&mut self) -> &mut GoBackN {
        &mut self.gbn
    }

    pub fn send_heartbeat(&mut self) -> Result<(), Error> {
        check_alloc(&mut self.buffer);
        self.gbn.send_heartbeat(self.addr, &mut self.buffer)?;
        Ok(())
    }
    pub fn send_disconnect(&mut self) -> Result<(), Error> {
        check_alloc(&mut self.buffer);
        self.gbn.send_disconnect(self.addr, &mut self.buffer)?;
        Ok(())
    }
    pub fn handle(&mut self, packet: Packet) -> Result<(), Error> {
        match packet {
            Packet::Ack(seq) => self.gbn.handle_ack(seq),
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
