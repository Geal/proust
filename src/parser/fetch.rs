#![allow(dead_code)]
#![allow(unused_imports)]

use parser::primitive::*;

use nom::{HexDisplay,Needed,IResult,FileProducer, be_i16, be_i32, be_i64};
use nom::{Consumer,ConsumerState};
use nom::IResult::*;
use nom::Err::*;

#[derive(PartialEq,Debug)]
pub struct FetchRequest<'a> {
  replica_id: i32,
  max_wait_time: i32,
  min_bytes: i32,
  topics: Vec<TopicFetch<'a>>
}

pub fn fetch_request<'a>(input:&'a [u8]) -> IResult<&'a [u8], FetchRequest<'a>> {
  chain!(
    input,
    replica_id: be_i32 ~
    max_wait_time: be_i32 ~
    min_bytes: be_i32 ~
    topics: apply!(kafka_array, topic_fetch), || {
      FetchRequest {
        replica_id: replica_id,
        max_wait_time: max_wait_time,
        min_bytes: min_bytes,
        topics: topics
      }
    }
  )
}

#[derive(PartialEq,Debug)]
pub struct TopicFetch<'a> {
  topic_name: KafkaString<'a>,
  partitions: Vec<PartitionFetch>
}

pub fn topic_fetch<'a>(input:&'a [u8]) -> IResult<&'a [u8], TopicFetch<'a>> {
  chain!(
    input,
    topic_name: kafka_string ~
    partitions: apply!(kafka_array, partition_fetch), || {
      TopicFetch {
        topic_name: topic_name,
        partitions: partitions
      }
    }
  )
}

#[derive(PartialEq,Debug)]
pub struct PartitionFetch {
  partition: i32,
  fetch_offset: i64,
  max_bytes: i32
}

pub fn partition_fetch<'a>(input:&'a [u8]) -> IResult<&'a [u8], PartitionFetch> {
  chain!(
    input,
    partition: be_i32 ~
    fetch_offset: be_i64 ~
    max_bytes: be_i32, || {
      PartitionFetch {
        partition: partition,
        fetch_offset: fetch_offset,
        max_bytes: max_bytes
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
  fn fetch_request_tests() {
      let input = &[
        0x00, 0x00, 0x00, 0x00, // replica_id = 0
        0x00, 0x00, 0x00, 0x00, // max_wait_time = 0
        0x00, 0x00, 0x00, 0x00, // min_bytes = 0
        0x00, 0x00, 0x00, 0x01, // topics array length
            0x00, 0x00,             //topic_name = ""
            0x00, 0x00, 0x00, 0x01, // partitions array length
                0x00, 0x00, 0x00, 0x00,                         //  partition = 0
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //  fetch_offset = 0
                0x00, 0x00, 0x00, 0x00                          //  max_bytes = 0
      ];
      let result = fetch_request(input);
      let expected = FetchRequest {
        replica_id: 0,
        max_wait_time: 0,
        min_bytes: 0,
        topics: vec![
          TopicFetch {
            topic_name: "",
            partitions: vec![
              PartitionFetch {
                partition: 0,
                fetch_offset: 0,
                max_bytes: 0
              }
            ]
          }
        ]
      };

      assert_eq!(result, Done(&[][..], expected))
  }
}
