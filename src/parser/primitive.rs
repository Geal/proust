#![allow(dead_code)]
#![allow(unused_imports)]

use nom::{HexDisplay,Needed,IResult,FileProducer,be_i8,be_i16,be_i32,be_i64};
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


