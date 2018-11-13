use std::time::Duration;

pub const IPV4_DEFAULT_PACKET_SIZE: u16 = 508;
pub const IPV6_DEFAULT_PACKET_SIZE: u16 = 1212;

pub const SEQ_BITS: u8 = 3;
pub const SEQ_RANGE: u8 = 8;
pub const WINDOW_SIZE: u8 = SEQ_RANGE - 1;
pub const SEND_BUFFER_SIZE: usize = 1024;
pub const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(5);
pub const ACK_TIMEOUT: Duration = Duration::from_millis(500);
pub const CONNECT_RETRIES: u8 = 5;

pub const CONNECT_MAGIC: &'static [u8] = b"JQTT";

