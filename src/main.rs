#![allow(dead_code)]
#![allow(unused_imports)]
#![feature(core,std_misc)]
extern crate core;
extern crate mmap;
extern crate mio;

#[macro_use] extern crate nom;
extern crate crc;

mod parser;
use parser::request::*;
use std::error::Error;

mod storage;
mod network;
mod responses;
use responses::response::*;

mod parser;
mod storage;
mod network;


fn main() {
  storage::storage_test();

  let rx = network::start_listener("abcd");
  loop {
    select! {
      val  = rx.recv() => match val {
        Ok(a)  => println!("Received val {}", a),
        Err(e) => {
          println!("Received err {:?}", e.description());
          break;
        }
      }
      /*,
      () = timeout.recv() => {
        println!("timed out, total time was more than 10 seconds");
        break;
      }*/
    }
  }

  println!("Hello, world!");
}

