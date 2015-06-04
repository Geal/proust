#![allow(dead_code)]
#![allow(unused_imports)]

use nom::{HexDisplay,Needed,IResult,FileProducer,be_i8,be_i16,be_i32,be_i64};
use nom::{Consumer,ConsumerState};
use nom::IResult::*;
use nom::Err::*;

use std::str;

use parser::request::*;
use parser::primitive::*;

pub fn ser_i8<'a>(v: i8, output: &mut Vec<u8>) -> () {
  output.push(v as u8);
}

pub fn ser_i16<'a>(v: i16, output: &mut Vec<u8>) -> () {
  let bits: u16 = v as u16;
  output.push((bits >> 8 ) as u8);
  output.push( bits        as u8);
}

pub fn ser_i32<'a>(v: i32, output: &mut Vec<u8>) -> () {
  let bits: u32 = v as u32;
  output.push((bits >> 24) as u8);
  output.push((bits >> 16) as u8);
  output.push((bits >> 8 ) as u8);
  output.push( bits        as u8);
}

pub fn ser_i32_ref<'a>(v: &i32, output: &mut Vec<u8>) -> () {
  ser_i32(*v, output);
}

pub fn ser_i64<'a>(v: i64, output: &mut Vec<u8>) -> () {
  let bits: u64 = v as u64;
  output.push((bits >> 56) as u8);
  output.push((bits >> 48) as u8);
  output.push((bits >> 40) as u8);
  output.push((bits >> 32) as u8);
  output.push((bits >> 24) as u8);
  output.push((bits >> 16) as u8);
  output.push((bits >> 8 ) as u8);
  output.push( bits        as u8);
}

pub fn ser_i64_ref<'a>(v: &i64, output: &mut Vec<u8>) -> () {
  ser_i64(*v, output);
}

pub fn ser_kafka_bytes(bs: KafkaBytes, output: &mut Vec<u8>) -> () {
  ser_i32(bs.len() as i32, output);

  output.extend(bs.iter().cloned());
}

pub fn ser_kafka_string(string: KafkaString, output: &mut Vec<u8>) -> () {
  ser_i16(string.len() as i16, output);

  for b in string.bytes() {
    output.push(b);
  }
}

pub fn ser_kafka_array<F,O>(elems: &Vec<O>, closure: F, output: &mut Vec<u8>) -> ()
 where F : Fn(&O, &mut Vec<u8>) -> () {
  ser_i32(elems.len() as i32, output);
  for e in elems.iter() {
    closure(e, output);
  }
}

#[cfg(test)]

mod tests {
  use super::*;
  use nom::*;
  use nom::IResult::*;

  #[test]
  fn ser_i8_test() {
    let mut v: Vec<u8> = vec![];
    ser_i8(127, &mut v);
    assert_eq!(&v[..], &[0x7f][..]);
    v.clear();
    ser_i8(-128, &mut v);
    assert_eq!(&v[..], &[0x80][..]);
  }

  #[test]
  fn ser_i16_test() {
    let mut v: Vec<u8> = vec![];
    ser_i16(32767_i16, &mut v);
    assert_eq!(&v[..], &[0x7f, 0xff][..]);
    v.clear();
    ser_i16(-32768_i16, &mut v);
    assert_eq!(&v[..], &[0x80, 0x00][..]);
  }

  #[test]
  fn ser_i32_test() {
    let mut v: Vec<u8> = vec![];
    ser_i32(2147483647_i32, &mut v);
    assert_eq!(&v[..], &[0x7f, 0xff, 0xff, 0xff]);
    v.clear();
    ser_i32(-2147483648_i32, &mut v);
    assert_eq!(&v[..], &[0x80, 0x00, 0x00, 0x00][..]);
  }

  #[test]
  fn ser_i64_test() {
    let mut v: Vec<u8> = vec![];
    ser_i64(9223372036854775807_i64, &mut v);
    assert_eq!(&v[..], &[0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);
    v.clear();
    ser_i64(-9223372036854775808_i64, &mut v);
    assert_eq!(&v[..], &[0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
  }

  #[test]
  fn ser_kafka_bytes_test() {
    let mut v: Vec<u8> = vec![];
    ser_kafka_bytes(&[][..], &mut v);
    assert_eq!(&v[..], &[0x00, 0x00, 0x00, 0x00][..]);
  }

  #[test]
  fn ser_kafka_string_test() {
    let mut v: Vec<u8> = vec![];
    ser_kafka_string("", &mut v);
    assert_eq!(&v[..], &[0x00, 0x00][..]);
    v.clear();
    ser_kafka_string("ABCD", &mut v);
    assert_eq!(&v[..], &[0x00, 0x04, 65, 66, 67, 68][..]);
  }

  #[test]
  fn ser_kafka_array_test() {
    let mut v: Vec<u8> = vec![];
    let i: Vec<i8> = vec![127];
    ser_kafka_array(&i, |ii, o| { ser_i8(*ii,o); }, &mut v);
    assert_eq!(&v[..], &[0x00, 0x00, 0x00, 0x01, 0x7f][..]);
  }
}


