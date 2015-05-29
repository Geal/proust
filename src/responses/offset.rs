#![allow(dead_code)]
#![allow(unused_imports)]

use parser::primitive::*;

use nom::{HexDisplay,Needed,IResult,FileProducer, be_i16, be_i32, be_i64};
use nom::{Consumer,ConsumerState};
use nom::IResult::*;
use nom::Err::*;

use responses::primitive::*;

/*
OffsetResponse => [TopicName [PartitionOffsets]]
  PartitionOffsets => Partition ErrorCode [Offset]
  Partition => int32
  ErrorCode => int16
  Offset => int64
  */

pub type OffsetResponse<'a> = Vec<(KafkaString<'a>, Vec<(i32, i16, Vec<i64>)>)>;

pub fn ser_offset_response<'a>(r: OffsetResponse<'a>, output: &mut Vec<u8>) -> () {
  ser_kafka_array(&r, |topic, oo| {
    let (ref name, ref ps) = *topic;
    ser_kafka_string(name, oo);
    ser_kafka_array(ps, |p, ooo| {
      let (partition_id, error_code, ref offsets) = *p;
      ser_i32(partition_id, ooo);
      ser_i16(error_code, ooo);
      ser_kafka_array(offsets, ser_i64_ref, ooo);
    }, oo);
  }, output);
}

#[cfg(test)]
mod tests {
  use super::*;
  use nom::*;
  use nom::IResult::*;

  #[test]
  fn ser_offset_response_tests() {
    let mut v: Vec<u8> = vec![];
    ser_offset_response(vec![(&[][..], vec![(0, 0, vec![0])])], &mut v);
    assert_eq!(&v[..], &[
      0x00, 0x00, 0x00, 0x01, // topics array length = 1
          0x00, 0x00,             // topic_name = ""
          0x00, 0x00, 0x00, 0x01, // partitions array length = 1
              0x00, 0x00, 0x00, 0x00, // partition_id = 0
              0x00, 0x00,             // error_code = 0
              0x00, 0x00, 0x00, 0x01, // offsets array length = 1
                  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 // partition_id = 0
    ][..]);
  }
}
