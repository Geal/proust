#![allow(dead_code)]
#![allow(unused_imports)]

#[macro_use]

extern crate nom;

use nom::{HexDisplay,Needed,IResult,FileProducer,be_u8,be_u16,be_u32,be_u64,be_f32};
use nom::{Consumer,ConsumerState};
use nom::IResult::*;
use nom::Err::*;

use std::str;
use std::collections::HashMap;

enum KafkaValue<'a> {
    Int8(u8),
    Int16(u16),
    Int32(u32),
    Int64(u64),
    Bytes(&'a[u8]),
    String(&'a[u8]),
    Array(&'a[KafkaValue<'a>])
}

fn kafka_value<'a>(input: &'a[u8]) -> IResult<&'a[u8], KafkaValue<'a>> {
    alt!(
      input,
      be_u8             => { |x| KafkaValue::Int8(x)   } |
      be_u16            => { |x| KafkaValue::Int16(x)  } |
      be_u32            => { |x| KafkaValue::Int32(x)  } |
      be_u64            => { |x| KafkaValue::Int64(x)  } |
      counted_bytes_u32 => { |x| KafkaValue::Bytes(x)  } |
      counted_bytes_u16 => { |x| KafkaValue::String(x) }

    )
}

fn counted_bytes_u32<'a>(input:&'a [u8]) -> IResult<&'a [u8], &'a [u8]> {
  match be_u32(input) {
    Done(i, length) => {
      let sz: usize = (length - 4) as usize;
      if i.len() >= sz {
        return Done(&i[sz..], &i[0..sz])
      } else {
        return Incomplete(Needed::Size(length))
      }
    }
    Error(e)      => Error(e),
    Incomplete(e) => Incomplete(e)
  }
}

fn counted_bytes_u16<'a>(input:&'a [u8]) -> IResult<&'a [u8], &'a [u8]> {
  match be_u16(input) {
    Done(i, length) => {
      let sz: usize = (length - 4) as usize;
      if i.len() >= sz {
        return Done(&i[sz..], &i[0..sz])
      } else {
        return Incomplete(Needed::Size(length as u32))
      }
    }
    Error(e)      => Error(e),
    Incomplete(e) => Incomplete(e)
  }
}


fn main() {
    println!("Hello, world!");
}

