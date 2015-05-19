#![allow(dead_code)]
#![allow(unused_imports)]

use nom::{HexDisplay,Needed,IResult,FileProducer,be_u8,be_u16,be_u32,be_u64,be_f32};
use nom::{Consumer,ConsumerState};
use nom::IResult::*;
use nom::Err::*;

use std::str;

pub enum KafkaValue<'a> {
    Int8(KafkaInt8),
    Int16(KafkaInt8),
    Int32(KafkaInt32),
    Int64(KafkaInt64),
    Bytes(&'a KafkaBytes<'a>),
    String(&'a KafkaString<'a>),
    Array(&'a Vec<KafkaValue<'a>>)
}

pub type KafkaInt8 = i8;
pub type KafkaInt16 = i16;
pub type KafkaInt32 = i32;
pub type KafkaInt64 = i64;
pub type KafkaBytes<'a> = &'a [u8];
pub type KafkaString<'a> = &'a [u8];


// ToDo remove these once they get integrated in nom
pub fn be_i8<'a>(i:&'a [u8]) -> IResult<&'a [u8], i8> {
  map!(i, be_u8, | x | { x as i8 })
}

pub fn be_i16<'a>(i:&'a [u8]) -> IResult<&'a [u8], i16> {
  map!(i, be_u16, | x | { x as i16 })
}

pub fn be_i32<'a>(i:&'a [u8]) -> IResult<&'a [u8], i32> {
  map!(i, be_u32, | x | { x as i32 })
}

pub fn be_i64<'a>(i:&'a [u8]) -> IResult<&'a [u8], i64> {
  map!(i, be_u64, | x | { x as i64 })
}


#[test]
fn i8_tests() {
  assert_eq!(be_i8(&[0x00]), Done(&b""[..], 0));
  assert_eq!(be_i8(&[0x7f]), Done(&b""[..], 127));
  assert_eq!(be_i8(&[0xff]), Done(&b""[..], -1));
  assert_eq!(be_i8(&[0x80]), Done(&b""[..], -128));
}

#[test]
fn i16_tests() {
  assert_eq!(be_i16(&[0x00, 0x00]), Done(&b""[..], 0));
  assert_eq!(be_i16(&[0x7f, 0xff]), Done(&b""[..], 32767_i16));
  assert_eq!(be_i16(&[0xff, 0xff]), Done(&b""[..], -1));
  assert_eq!(be_i16(&[0x80, 0x00]), Done(&b""[..], -32768_i16));
}

#[test]
fn i32_tests() {
  assert_eq!(be_i32(&[0x00, 0x00, 0x00, 0x00]), Done(&b""[..], 0));
  assert_eq!(be_i32(&[0x7f, 0xff, 0xff, 0xff]), Done(&b""[..], 2147483647_i32));
  assert_eq!(be_i32(&[0xff, 0xff, 0xff, 0xff]), Done(&b""[..], -1));
  assert_eq!(be_i32(&[0x80, 0x00, 0x00, 0x00]), Done(&b""[..], -2147483648_i32));
}

#[test]
fn i64_tests() {
  assert_eq!(be_i64(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]), Done(&b""[..], 0));
  assert_eq!(be_i64(&[0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]), Done(&b""[..], 9223372036854775807_i64));
  assert_eq!(be_i64(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]), Done(&b""[..], -1));
  assert_eq!(be_i64(&[0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]), Done(&b""[..], -9223372036854775808_i64));
}

pub fn kafka_bytes<'a>(input:&'a [u8]) -> IResult<&'a [u8], KafkaBytes<'a>> {
  match be_i32(input) {
    Done(i, length) => {
      let sz: usize = length as usize;
      // ToDo handle -1 (null)
      // ToDo handle other negative values (it's an error)
      println!("length: {}", length);
      println!("sz: {}, len: {}", sz, i.len());
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

pub fn kafka_string<'a>(input:&'a [u8]) -> IResult<&'a [u8], KafkaString<'a>> {
  match be_i16(input) {
    Done(i, length) => {
      // ToDo handle -1 (null)
      // ToDo handle other negative values (it's an error)
      let sz: usize = length as usize;
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

pub fn kafka_array<'a, F,O>(input: &'a[u8], closure: F) -> IResult<&'a[u8], Vec<O> >
 where F : Fn(&'a[u8]) -> IResult<&'a[u8], O> {
   match be_i32(input) {
    Done(i, size) => {
      // ToDo handle negative values (it's an error)
      count!(i, closure, size)
    }
    Error(e)      => Error(e),
    Incomplete(e) => Incomplete(e)
   }
 }

#[cfg(test)]

mod tests {
  use super::*;
  use nom::*;
  use nom::IResult::*;

  #[test]
  fn kafka_bytes_test() {
    assert_eq!(kafka_bytes(&[0x00, 0x00, 0x00, 0x00]), Done(&[][..], &[][..]));
    assert_eq!(kafka_bytes(&[0x00, 0x00, 0x00, 0x01]), Incomplete(Needed::Size(1)));
    assert_eq!(kafka_bytes(&[0x00, 0x00, 0x00, 0x01, 0x00]), Done(&[][..], &[0x00][..]));
    assert_eq!(kafka_bytes(&[0x00, 0x00, 0x00, 0x01, 0x00, 0x00]), Done(&[0x00][..], &[0x00][..]));
  }

  #[test]
  fn kafka_string_test() {
    assert_eq!(kafka_string(&[0x00, 0x00]), Done(&[][..], &[][..]));
    assert_eq!(kafka_string(&[0x00, 0x01]), Incomplete(Needed::Size(1)));
    assert_eq!(kafka_string(&[0x00, 0x01, 0x00]), Done(&[][..], &[0x00][..]));
    assert_eq!(kafka_string(&[0x00, 0x01, 0x00, 0x00]), Done(&[0x00][..], &[0x00][..]));
  }

  #[test]
  fn kafka_array_test() {
    assert_eq!(kafka_array(&[0x00, 0x00, 0x00, 0x00], be_i8), Done(&[][..], vec![]));
    assert_eq!(kafka_array(&[0x00, 0x00, 0x00, 0x01], be_i8), Incomplete(Needed::Unknown));
    assert_eq!(kafka_array(&[0x00, 0x00, 0x00, 0x01, 0x00], be_i8), Done(&[][..], vec![0x00]));
    assert_eq!(kafka_array(&[0x00, 0x00, 0x00, 0x01, 0x00, 0x00], be_i8), Done(&[0x00][..], vec![0x00]));
  }
}


