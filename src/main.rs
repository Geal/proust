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
  env_logger::init().expect("Can't init env_logger");

  storage::storage_test();

  let zjg = network::zookeeper::start_listener("127.0.0.1:2088".to_string()).expect("start zookeeper");
  let jg = network::kafka::start_listener("127.0.0.1:8080".to_string()).expect("start kafka");
  zjg.join();
  jg.join();
}