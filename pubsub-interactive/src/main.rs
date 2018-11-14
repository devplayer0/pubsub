extern crate clap;
extern crate cursive;

extern crate pubsub_client;
extern crate pubsub_interactive;

use cursive::Cursive;

use pubsub_client::util;

fn args<'a>() -> clap::ArgMatches<'a> {
    clap::App::new("JQTT interactive client")
        .version("0.1")
        .author("Jack O'Sullivan <jackos1998@gmail.com>")
        .arg(clap::Arg::with_name("address")
             .short("a")
             .long("address")
             .value_name("address[:port]")
             .help("Set server address")
             .default_value("localhost:26999")
             .takes_value(true)
             .validator(|val| util::parse_addr(&val).map(|_| ()).map_err(|e| format!("{}", e))))
        .get_matches()
}

fn main() {
    if let Err(e) = pubsub_interactive::run(Cursive::default(), util::parse_addr(args().value_of("address").unwrap()).unwrap()) {
        println!("error: {}", e);
    }
}
