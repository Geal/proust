#![allow(dead_code)]
#![allow(unused_imports)]

#[macro_use] extern crate nom;
extern crate crc;

mod parser;
use parser::request::*;

mod responses;
use responses::response::*;


fn main() {
    println!("Hello, world!");
}

