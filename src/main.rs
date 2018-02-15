extern crate core;
extern crate memmap;
extern crate mio;
extern crate bytes;
#[macro_use]
extern crate log;
extern crate env_logger;

#[macro_use]
extern crate nom;
extern crate crc;
extern crate slab;

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

  let jg = network::kafka::start_listener("127.0.0.1:9092".to_string()).expect("start kafka");

  jg.join();
}