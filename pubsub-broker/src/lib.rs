use std::error::Error as StdError;
use std::any::Any;
use std::time::Duration;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::io;
use std::net::UdpSocket;

#[macro_use]
extern crate quick_error;
#[macro_use]
extern crate log;
extern crate ctrlc;

pub mod config;

use config::Config;

quick_error! {
    #[derive(Debug)]
    enum Error {
        ThreadPanic(err: Box<dyn Any + Send>) {
            display(e) -> ("thread panicked: {:?}", err)
        }
    }
}

pub fn run(config: Config) -> Result<(), Box<dyn StdError>> {
    info!("starting broker");

    let running = Arc::new(AtomicBool::new(true));
    let mut threads = Vec::with_capacity(config.bind_addrs().len());
    for addr in config.bind_addrs().iter().cloned() {
        info!("binding on {}...", addr);

        let socket = UdpSocket::bind(addr)?;
        socket.set_read_timeout(Some(Duration::from_millis(250)))?;

        let r = Arc::clone(&running);
        threads.push(thread::spawn(move || {
            let mut buf = [0; 65536];
            while r.load(Ordering::SeqCst) {
                let (size, src) = match socket.recv_from(&mut buf) {
                    Ok((size, src)) => (size, src),
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                    Err(e) => {
                        error!("failed to recv from socket bound to {}: {}", addr, e);
                        r.store(false, Ordering::SeqCst);
                        return;
                    }
                };

                debug!("received packet of size {} bytes from {}!", size, src);
            }
        }));
    }

    {
        let r = Arc::clone(&running);
        ctrlc::set_handler(move || {
            info!("shutting down...");
            r.store(false, Ordering::SeqCst);
        })?;
    }

    for thread in threads {
        thread.join().map_err(|e| Error::ThreadPanic(Box::new(e)))?
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
