#![allow(dead_code)]
#![allow(unused_imports)]
#![feature(core)]
extern crate core;
extern crate mmap;

#[macro_use] extern crate nom;
extern crate crc;

mod parser;
use parser::request::*;

mod responses;
use responses::response::*;
mod storage;

fn main() {
  storage::storage_test();
  println!("Hello, world!");
}

