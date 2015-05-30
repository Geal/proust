#![allow(dead_code)]
#![allow(unused_imports)]

use parser::primitive::*;

use nom::{HexDisplay,Needed,IResult,FileProducer, be_i16, be_i32, be_i64};
use nom::{Consumer,ConsumerState};
use nom::IResult::*;
use nom::Err::*;

#[derive(PartialEq,Debug)]
pub struct OffsetRequest<'a> {
  replica_id: i32,
  topics: Vec<TopicOffset<'a>>
}

pub fn offset_request<'a>(input:&'a [u8]) -> IResult<&'a [u8], OffsetRequest<'a>> {
  chain!(
    input,
    replica_id: be_i32 ~
    topics: apply!(kafka_array, topic_offset), || {
      OffsetRequest {
        replica_id: replica_id,
        topics: topics
      }
    }
  )
}

#[derive(PartialEq,Debug)]
pub struct TopicOffset<'a> {
  topic_name: KafkaString<'a>,
  partitions: Vec<PartitionOffset>
}

pub fn topic_offset<'a>(input:&'a [u8]) -> IResult<&'a [u8], TopicOffset<'a>> {
  chain!(
    input,
    topic_name: kafka_string ~
    partitions: apply!(kafka_array, partition_offset), || {
      TopicOffset {
        topic_name: topic_name,
        partitions: partitions
      }
    }
  )
}

#[derive(PartialEq,Debug)]
pub struct PartitionOffset {
  partition: i32,
  time: i64,
  max_number_of_offsets: i32
}

pub fn partition_offset<'a>(input:&'a [u8]) -> IResult<&'a [u8], PartitionOffset> {
  chain!(
    input,
    partition: be_i32 ~
    time: be_i64 ~
    max_number_of_offsets: be_i32, || {
      PartitionOffset {
        partition: partition,
        time: time,
        max_number_of_offsets: max_number_of_offsets
      }
    }
  )
}

#[cfg(test)]
mod tests {
  use super::*;
  use nom::*;
  use nom::IResult::*;

  #[test]
  fn offset_request_tests() {
      let input = &[
        0x00, 0x00, 0x00, 0x00, // replica_id = 0
        0x00, 0x00, 0x00, 0x01, // topics array length
            0x00, 0x00,             // topic_name = ""
            0x00, 0x00, 0x00, 0x01, // partitions array length
                0x00, 0x00, 0x00, 0x00,                         // partition = 0
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // time = 0
                0x00, 0x00, 0x00, 0x00                          // max_number_of_offsets = 0
      ];
      let result = offset_request(input);
      let expected = OffsetRequest {
        replica_id: 0,
        topics: vec![
          TopicOffset {
            topic_name: "",
            partitions: vec![
              PartitionOffset {
                partition: 0,
                time: 0,
                max_number_of_offsets: 0
              }
            ]
          }
        ]
      };

      assert_eq!(result, Done(&[][..], expected))
  }
}
