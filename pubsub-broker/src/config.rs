use std::net::{SocketAddr, ToSocketAddrs};

pub struct Config {
    bind_addrs: Vec<SocketAddr>,
}
impl Default for Config {
    fn default() -> Config {
        Config {
            bind_addrs: "localhost:26999".to_socket_addrs().unwrap().collect(),
        }
    }
}
impl Config {
    pub fn bind_addrs(&self) -> &Vec<SocketAddr> {
        &self.bind_addrs
    }
}
