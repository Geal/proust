#![allow(dead_code)]
#![allow(unused_imports)]

use parser::primitive::*;

use nom::{HexDisplay,Needed,IResult,FileProducer, be_i16, be_i32, be_i64};
use nom::{Consumer,ConsumerState};
use nom::IResult::*;

use responses::primitive::*;

/*
OffsetCommitResponse => [TopicName [Partition ErrorCode]]
  TopicName => string
  Partition => int32
  ErrorCode => int16
  */

pub type OffsetCommitResponse<'a> = Vec<(KafkaString<'a>, Vec<(i32, i16)>)>;

pub fn ser_offset_commit_response<'a>(r: OffsetCommitResponse<'a>, output: &mut Vec<u8>) -> () {
  ser_kafka_array(&r, |topic, oo| {
    let (ref name, ref ps) = *topic;
    ser_kafka_string(name, oo);
    ser_kafka_array(ps, |p, ooo| {
      let (partition_id, error_code) = *p;
      ser_i32(partition_id, ooo);
      ser_i16(error_code, ooo);
    }, oo);
  }, output);
}

#[cfg(test)]
mod tests {
  use super::*;
  use nom::*;
  use nom::IResult::*;

  #[test]
  fn ser_offset_commit_response_tests() {
    let mut v: Vec<u8> = vec![];
    ser_offset_commit_response(vec![("", vec![(0, 0)])], &mut v);
    assert_eq!(&v[..], &[
      0x00, 0x00, 0x00, 0x01, // topics array length = 1
          0x00, 0x00,             // topic_name = ""
          0x00, 0x00, 0x00, 0x01, // partitions array length = 1
              0x00, 0x00, 0x00, 0x00, // partition_id = 0
              0x00, 0x00              // error_code = 0
    ][..]);
  }
}
