#![allow(dead_code)]
#![allow(unused_imports)]

use parser::primitive::*;
use parser::message::*;

use nom::{HexDisplay,Needed,IResult,FileProducer, be_i16, be_i32};
use nom::{Consumer,ConsumerState};
use nom::IResult::*;
use nom::Err::*;

use responses::primitive::*;

//ProduceResponse => [TopicName [Partition ErrorCode Offset]]
//  TopicName => string
//  Partition => int32
//  ErrorCode => int16
//  Offset => int64

pub type ProduceResponse<'a> = Vec<(KafkaString<'a>, Vec<(i32, i16, i64)>)>;

pub fn ser_produce_response<'a>(r: &ProduceResponse<'a>, o: &mut Vec<u8>) -> () {
  ser_kafka_array(r, |t, oo| {
    let (topic_name, ref partitions) = *t;
    ser_kafka_string(topic_name, oo);

    ser_kafka_array(partitions, |p, ooo| {
        let (pid, error_code, offset) = *p;
        ser_i32(pid, ooo);
        ser_i16(error_code, ooo);
        ser_i64(offset, ooo);
    }, oo);
  }, o);
}

#[cfg(test)]

mod tests {
  use super::*;
  use nom::*;
  use nom::IResult::*;

  #[test]
  fn ser_produce_response_test() {
    let mut v: Vec<u8> = vec![];


    ser_produce_response(&vec![(
      "",
      vec![(
        127,
        0,
        1337
      )]
    )], &mut v);

    assert_eq!(&v[..], &[
      0x00, 0x00, 0x00, 0x01, // array length = 1
        0x00, 0x00,             // topic_name = ""
        0x00, 0x00, 0x00, 0x01, // array length = 1
          0x00, 0x00, 0x00, 0x7f,                        // partition_id = 127
          0x00, 0x00,                                    // error_code = 0
          0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x39 // offset = 1337
    ][..]);
  }
}
