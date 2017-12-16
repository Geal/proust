#![allow(dead_code)]
#![allow(unused_imports)]

use nom::{HexDisplay,Needed,IResult,ErrorKind,FileProducer,be_i8,be_i16,be_i32,be_i64};
use nom::{Consumer,ConsumerState};
use nom::IResult::*;
use nom::ErrorKind::Custom;

use std::str;

use parser::errors::*;

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
pub type KafkaString<'a> = &'a str;

pub fn kafka_bytes<'a>(input:&'a [u8]) -> IResult<&'a [u8], KafkaBytes<'a>> {
  match be_i32(input) {
    Done(i, length) => {
      if length >= 0 {
        let sz: usize = length as usize;
        if i.len() >= sz {
          return Done(&i[sz..], &i[0..sz])
        } else {
          return Incomplete(Needed::Size(length as usize))
        }
      } else if length == -1 {
        Error(Custom(InputError::NotImplemented.to_int())) // TODO: maybe make an optional parser which returns an option?
      } else {
        Error(Custom(InputError::ParserError.to_int()))
      }
    }
    Error(e)      => Error(e),
    Incomplete(e) => Incomplete(e)
  }
}

pub fn kafka_bytestring<'a>(input:&'a [u8]) -> IResult<&'a [u8], &'a [u8]> {
  match be_i16(input) {
    Done(i, length) => {
      if length >= 0 {
        let sz: usize = length as usize;
        if i.len() >= sz {
          return Done(&i[sz..], &i[0..sz])
        } else {
          return Incomplete(Needed::Size(length as usize))
        }
      } else if length == -1 {
        Error(Custom(InputError::NotImplemented.to_int())) //TODO: maybe make an optional parser which returns an option?
      } else {
        Error(Custom(InputError::ParserError.to_int()))
      }
    }
    Error(e)      => Error(e),
    Incomplete(e) => Incomplete(e)
  }
}

pub fn kafka_string<'a>(input:&'a [u8]) -> IResult<&'a [u8], KafkaString<'a>> {
  map_res!(input, kafka_bytestring, |bs| {
    str::from_utf8(bs)
  })
}

pub fn kafka_array<'a, F,O>(input: &'a[u8], closure: F) -> IResult<&'a[u8], Vec<O> >
 where F : Fn(&'a[u8]) -> IResult<&'a[u8], O> {
   match be_i32(input) {
    Done(i, size) => {
      if size >= 0 {
        count!(i, closure, size as usize)
      } else {
        Error(Custom(InputError::ParserError.to_int()))
      }
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
  use nom::ErrorKind::Custom;

  use parser::errors::*;

  #[test]
  fn kafka_bytes_test() {
    assert_eq!(kafka_bytes(&[0x00, 0x00, 0x00, 0x00]), Done(&[][..], &[][..]));
    assert_eq!(kafka_bytes(&[0x00, 0x00, 0x00, 0x01]), Incomplete(Needed::Size(1)));
    assert_eq!(kafka_bytes(&[0x00, 0x00, 0x00, 0x01, 0x00]), Done(&[][..], &[0x00][..]));
    assert_eq!(kafka_bytes(&[0x00, 0x00, 0x00, 0x01, 0x00, 0x00]), Done(&[0x00][..], &[0x00][..]));
  }

  #[test]
  fn kafka_string_test() {
    assert_eq!(kafka_string(&[0x00, 0x00]), Done(&[][..], ""));
    assert_eq!(kafka_string(&[0x00, 0x01]), Incomplete(Needed::Size(1)));
    assert_eq!(kafka_string(&[0x00, 0x02, 65, 66]), Done(&[][..], "AB"));
    assert_eq!(kafka_string(&[0x00, 0x01, 65, 0x00]), Done(&[0x00][..], "A"));
    assert_eq!(kafka_string(&[0x80, 0x00]), Error(Custom(InputError::ParserError.to_int())));
    // TODO: test invalid utf8 strings
  }

  #[test]
  fn kafka_array_test() {
    assert_eq!(kafka_array(&[0x00, 0x00, 0x00, 0x00], be_i8), Done(&[][..], vec![]));
    assert_eq!(kafka_array(&[0x00, 0x00, 0x00, 0x01], be_i8), Incomplete(Needed::Unknown)); //FIXME: test fail
    assert_eq!(kafka_array(&[0x00, 0x00, 0x00, 0x01, 0x00], be_i8), Done(&[][..], vec![0x00]));
    assert_eq!(kafka_array(&[0x00, 0x00, 0x00, 0x01, 0x00, 0x00], be_i8), Done(&[0x00][..], vec![0x00]));
    assert_eq!(kafka_array(&[0x80, 0x00, 0x00, 0x00], be_i8), Error(Custom(InputError::ParserError.to_int())));
  }
}


