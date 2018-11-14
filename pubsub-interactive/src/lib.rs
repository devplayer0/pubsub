#[macro_use]
extern crate quick_error;
extern crate ctrlc;
extern crate cursive;

extern crate pubsub_client;

use std::collections::VecDeque;
use std::rc::Rc;
use std::cell::RefCell;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{Ordering, AtomicBool};
use std::thread;
use std::time::Duration;
use std::io::{self, Read};
use std::net::SocketAddr;

use cursive::{Cursive, Printer};
use cursive::direction::Orientation;
use cursive::align::HAlign;
use cursive::view::View;
use cursive::views::{IdView, BoxView, LinearLayout, Panel, EditView};
use cursive::theme::Color;

use pubsub_client::bytes::{IntoBuf, Buf, Bytes, Reader};
use pubsub_client::{Client, Message, MessageCollector, CompleteMessageListener};

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Io(err: io::Error) {
            from()
            description(err.description())
            display("io error: {}", err)
        }
        Ctrlc(err: ctrlc::Error) {
            from()
            description(err.description())
            display("ctrlc: {}", err)
        }
        Library(err: pubsub_client::Error) {
            from()
            description(err.description())
            display("library error: {}", err)
        }
        AlreadyRunning {
            description("shell already running")
        }
        Fatal {
            description("fatal error, shutting down...")
        }
    }
}

enum ConsoleMessage {
    Help,
    Error(Error),
    CommandResult(String),
    Published(String, String),
}
impl ConsoleMessage {
    pub fn print(&self, printer: &Printer, pos: usize) {
        match self {
            ConsoleMessage::Help => {
                printer.with_color(Color::RgbLowRes(0, 0, 5).into(), |printer| {
                    printer.print((0, pos), "HELP");
                });
                printer.print((4, pos), ": Commands: `subscribe <topic>`, `unsubscribe <topic>`, `publish <topic> <message>`");
            },
            ConsoleMessage::Error(e) => {
                printer.with_color(Color::RgbLowRes(5, 0, 0).into(), |printer| {
                    printer.print((0, pos), "ERROR");
                });
                printer.print((5, pos), &format!(": {}", e));
            },
            ConsoleMessage::CommandResult(msg) => {
                printer.with_color(Color::RgbLowRes(0, 5, 0).into(), |printer| {
                    printer.print((0, pos), "OK");
                });
                printer.print((2, pos), &format!(": {}", msg));
            },
            ConsoleMessage::Published(topic, message) => {
                printer.with_color(Color::RgbLowRes(5, 5, 5).into(), |printer| {
                    printer.print((0, pos), &topic);
                });
                printer.print((topic.len(), pos), &format!(": {}", message));
            },
        }
    }
}

struct ConsoleView {
    size: usize,
    messages: Arc<RwLock<VecDeque<ConsoleMessage>>>,
}
impl Clone for ConsoleView {
    fn clone(&self) -> ConsoleView {
        ConsoleView {
            size: self.size,
            messages: Arc::clone(&self.messages),
        }
    }
}
impl ConsoleView {
    pub fn new(size: usize) -> ConsoleView {
        ConsoleView {
            size,
            messages: Arc::new(RwLock::new(VecDeque::new())),
        }
    }

    fn push(&self, msg: ConsoleMessage) {
        let mut messages = self.messages.write().unwrap();
        messages.push_back(msg);
        if messages.len() > self.size {
            messages.pop_front();
        }
    }
    pub fn push_help(&self) {
        self.push(ConsoleMessage::Help);
    }
    pub fn push_error(&self, err: Error) {
        self.push(ConsoleMessage::Error(err));
    }
    pub fn push_command_result<M: Into<String>>(&self, message: M) {
        self.push(ConsoleMessage::CommandResult(message.into()));
    }
    pub fn push_message<'a>(&self, mut msg: Message<Reader<Box<Buf + Send + Sync + 'a>>>) {
        let mut data = Vec::with_capacity(msg.size() as usize);
        if let Err(e) = msg.read_to_end(&mut data) {
            self.push_error(e.into());
        }
        let text = match String::from_utf8(data) {
            Ok(m) => m,
            Err(e) => format!("{:?}", Bytes::from(e.into_bytes())),
        };

        self.push(ConsoleMessage::Published(msg.topic().to_owned(), text));
    }
}
impl View for ConsoleView {
    fn draw(&self, printer: &Printer) {
        let messages = self.messages.read().unwrap();
        for (i, msg) in messages.iter().rev().take(printer.size.y).enumerate() {
            msg.print(printer, printer.size.y - 1 - i);
        }
    }
}

struct ShellListener {
    running: Arc<AtomicBool>,
    console: ConsoleView
}
impl ShellListener {
    pub fn new(running: &Arc<AtomicBool>, console: &ConsoleView) -> ShellListener {
        ShellListener {
            running: Arc::clone(running),
            console: console.clone()
        }
    }
}
impl CompleteMessageListener for ShellListener {
    fn recv_message<'a>(&mut self, msg: Message<Reader<Box<Buf + Send + Sync + 'a>>>) -> Result<(), pubsub_client::Error> {
        self.console.push_message(msg);
        Ok(())
    }
    fn on_error(&mut self, error: pubsub_client::Error) {
        self.console.push_error(error.into());
        self.console.push_error(Error::Fatal);

        thread::sleep(Duration::from_secs(2));
        self.running.store(false, Ordering::Relaxed);
    }
}

fn parse_command<'a>(name: &str, input: &'a str, arg_count: usize) -> Option<Vec<&'a str>> {
    if input.len() <= name.len() + 1 || !input.starts_with(&format!("{} ", name)) {
        return None;
    }

    let args: Vec<_> = input[name.len() + 1..].split(" ").collect();
    if args.len() < arg_count {
        return None;
    }

    Some(args)
}
fn handle_command(console: &ConsoleView, client: &mut Client, cmd: &str) -> Result<(), Error> {
    if let Some(args) = parse_command("subscribe", cmd, 1) {
        let topic = args.join(" ");
        client.subscribe(&topic)?;
        console.push_command_result(format!("subscribed to '{}'", topic));
    } else if let Some(args) = parse_command("unsubscribe", cmd, 1) {
        let topic = args.join(" ");
        client.unsubscribe(&topic)?;
        console.push_command_result(format!("unsubscribed from '{}'", topic));
    } else if let Some(args) = parse_command("publish", cmd, 2) {
        let message: Bytes = args[1..].join(" ").into_bytes().into();
        let len = message.len() as u32;
        client.queue_message(Message::new(len, args[0], message.into_buf()))?;
        client.drain_messages()?;
        console.push_command_result(format!("published message to '{}'", args[0]));
    } else {
        console.push_help();
    }
    Ok(())
}
struct Shell {
    running: Arc<AtomicBool>,
    siv: Cursive,
    console: ConsoleView,
}
impl Shell {
    pub fn new(siv: Cursive) -> Shell {
        Shell {
            running: Arc::new(AtomicBool::new(false)),
            siv,
            console: ConsoleView::new(128),
        }
    }

    pub fn listener(&self) -> MessageCollector<ShellListener> {
        MessageCollector::new(ShellListener::new(&self.running, &self.console))
    }
    pub fn running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    pub fn run(&mut self, client: Client<'static>) -> Result<(), Error> {
        if self.running() {
            return Err(Error::AlreadyRunning);
        }
        {
            let running = Arc::clone(&self.running);
            ctrlc::set_handler(move || {
                running.store(false, Ordering::Relaxed);
            })?;
        }

        let client = Rc::new(RefCell::new(client));
        let on_command = {
            let client = Rc::downgrade(&Rc::clone(&client));
            let console = self.console.clone();
            move |siv: &mut Cursive, cmd: &str| {
                siv.find_id::<EditView>("readline").unwrap().set_content("");
                let cmd = cmd.trim();
                if cmd.len() == 0 {
                    return;
                }

                let client = client.upgrade().unwrap();
                let mut client = client.borrow_mut();
                if let Err(e) = handle_command(&console, &mut client, cmd) {
                    console.push_error(e.into());
                }
            }
        };
        self.siv.set_fps(10);
        self.siv.add_layer(
            BoxView::with_full_screen(
                LinearLayout::new(Orientation::Vertical)
                    .child(
                        BoxView::with_full_screen(
                            Panel::new(
                                self.console.clone()
                            )
                                .title("Messages")
                        )
                    )
                    .child(
                        Panel::new(
                            IdView::new("readline",
                                EditView::new()
                                    .on_submit(on_command))
                        )
                            .title("Command")
                            .title_position(HAlign::Left)
                    )
            )
        );

        self.running.store(true, Ordering::Relaxed);
        while self.running() {
            self.siv.step();
        }

        Rc::try_unwrap(client).unwrap().into_inner().stop();
        Ok(())
    }
}
pub fn run(siv: Cursive, addrs: std::vec::IntoIter<SocketAddr>) -> Result<(), Error> {
    let mut shell = Shell::new(siv);
    let client = Client::connect(&*addrs.collect::<Vec<_>>(), shell.listener())?;
    shell.run(client)?;

    Ok(())
}
