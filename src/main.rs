extern crate core;
extern crate mmap;
extern crate mio;
extern crate bytes;
#[macro_use]
extern crate log;
extern crate env_logger;

#[macro_use]
extern crate nom;
extern crate crc;

mod parser;
mod storage;
mod network;
mod responses;
mod util;
mod proust;

fn main() {
  println!("Le peintre original procède à la façon des oculistes.");

  env_logger::init().expect("Can't init env_logger");

  storage::storage_test();

  let jg = network::kafka::start_listener("127.0.0.1:8080".to_string()).expect("start kafka");

  jg.join();
}