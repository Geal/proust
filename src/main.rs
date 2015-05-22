#![allow(dead_code)]
#![allow(unused_imports)]

#[macro_use]
extern crate nom;

use parser::primitive::*;
use parser::request::*;
use parser::metadata::*;
use parser::produce::*;

mod parser;

fn main() {
    println!("Hello, world!");
}

