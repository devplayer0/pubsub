use std::error::Error as StdError;
use std::any::Any;
use std::mem;
use std::time::Duration;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::io;
use std::net::{SocketAddr, UdpSocket};

#[macro_use]
extern crate quick_error;
#[macro_use]
extern crate log;
extern crate ctrlc;

pub mod config;

use config::Config;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        ThreadPanic(err: Box<dyn Any + Send>) {
            display("thread panicked: {:?}", err)
        }
        Io(err: io::Error) {
            from()
            display("io error: {}", err)
            description(err.description())
            cause(err)
        }
    }
}

pub struct Broker {
    running: Arc<AtomicBool>,
    socket_threads: Vec<thread::JoinHandle<Result<(), Error>>>,
}
impl Broker {
    pub fn bind<'a, I>(addrs: I) -> Result<Broker, io::Error>
    where I: IntoIterator<Item = &'a SocketAddr>
    {
        let running = Arc::new(AtomicBool::new(true));
        let addrs = addrs.into_iter();
        let (l, _) = addrs.size_hint();
        let mut socket_threads = Vec::with_capacity(l);
        for addr in addrs {
            info!("binding on {}...", addr);

            let socket = UdpSocket::bind(addr)?;
            socket.set_read_timeout(Some(Duration::from_millis(250)))?;

            let r = Arc::clone(&running);
            socket_threads.push(thread::spawn(move || {
                let mut buf = [0; 65536];
                while r.load(Ordering::SeqCst) {
                    let (size, src) = match socket.recv_from(&mut buf) {
                        Ok((size, src)) => (size, src),
                        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                        Err(e) => return Err(Error::Io(e))
                    };

                    debug!("received packet of size {} bytes from {}!", size, src);
                }

                Ok(())
            }));
        }

        Ok(Broker {
            running,
            socket_threads,
        })
    }

    pub fn running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
    pub fn join(&mut self) -> Result<(), Error> {
        let mut threads = Vec::with_capacity(0);
        mem::swap(&mut threads, &mut self.socket_threads);

        for thread in threads.into_iter() {
            thread.join().map_err(|e| Error::ThreadPanic(Box::new(e)))??;
        }

        Ok(())
    }
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }
}

pub fn run(config: Config) -> Result<(), Box<dyn StdError>> {
    info!("starting broker");

    let broker = Arc::new(Mutex::new(Broker::bind(config.bind_addrs())?));
    {
        let b = Arc::clone(&broker);
        ctrlc::set_handler(move || {
            info!("shutting down...");
            b.lock().unwrap().stop();
        })?;
    }

    broker.lock().unwrap().join()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
