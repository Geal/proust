#![allow(dead_code)]
#![allow(unused_imports)]
#![feature(core)]
extern crate core;
extern crate mmap;

#[macro_use]
extern crate nom;

use parser::request::*;

mod parser;
mod storage;

fn main() {
  storage::storage_test();
  println!("Hello, world!");
}

