#![allow(dead_code)]
#![allow(unused_imports)]
#![feature(core)]
#![cfg_attr(feature = "nightly", feature(std_misc))]
extern crate core;
extern crate mmap;
extern crate mio;

#[macro_use] extern crate nom;
extern crate crc;

mod parser;
use parser::request::*;
use std::error::Error;
use std::thread;
use std::sync::mpsc::{channel,Sender,Receiver};

mod storage;
mod network;
mod responses;
use responses::response::*;

mod util;

fn main() {
  //storage::storage_test();

  let (tx, jg) = network::kafka::start_listener("abcd");
  jg.join();

  println!("Hello, world!");
}

