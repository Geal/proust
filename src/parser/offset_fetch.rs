#![allow(dead_code)]
#![allow(unused_imports)]

use parser::primitive::*;

use nom::{HexDisplay,Needed,IResult,FileProducer, be_i16, be_i32, be_i64};
use nom::{Consumer,ConsumerState};
use nom::IResult::*;
use nom::Err::*;

#[derive(PartialEq,Debug)]
pub struct OffsetFetchRequest<'a> {
  consumer_group: KafkaString<'a>,
  topics: Vec<TopicOffsetFetch<'a>>
}

pub fn offset_fetch_request<'a>(input:&'a [u8]) -> IResult<&'a [u8], OffsetFetchRequest<'a>> {
  chain!(
    input,
    consumer_group: kafka_string ~
    topics: apply!(kafka_array, topic_offset_fetch), || {
      OffsetFetchRequest{
        consumer_group: consumer_group,
        topics: topics
      }
    }
  )
}

#[derive(PartialEq,Debug)]
pub struct TopicOffsetFetch<'a> {
  topic_name: KafkaString<'a>,
  partitions: Vec<PartitionOffsetFetch>
}

pub fn topic_offset_fetch<'a>(input:&'a [u8]) -> IResult<&'a [u8], TopicOffsetFetch<'a>> {
  chain!(
    input,
    topic_name: kafka_string ~
    partitions: apply!(kafka_array, partition_offset_fetch), || {
      TopicOffsetFetch {
        topic_name: topic_name,
        partitions: partitions
      }
    }
  )
}

#[derive(PartialEq,Debug)]
pub struct PartitionOffsetFetch {
  partition: i32
}

pub fn partition_offset_fetch<'a>(input:&'a [u8]) -> IResult<&'a [u8], PartitionOffsetFetch> {
  map!(input, be_i32, |p| { PartitionOffsetFetch { partition: p } })
}

#[cfg(test)]
mod tests {
  use super::*;
  use nom::*;
  use nom::IResult::*;

/*
OffsetFetchRequest => ConsumerGroup [TopicName [Partition]]
  ConsumerGroup => string
  TopicName => string
  Partition => int32
  */

  #[test]
  fn offset_request_tests() {
      let input = &[
        0x00, 0x00,             // consumer_group = ""
        0x00, 0x00, 0x00, 0x01, // topics array length
            0x00, 0x00,             // topic_name = ""
            0x00, 0x00, 0x00, 0x01, // partitions array length
                0x00, 0x00, 0x00, 0x00 // partition = 0
      ];
      let result = offset_fetch_request(input);
      let expected = OffsetFetchRequest {
        consumer_group: "",
        topics: vec![
          TopicOffsetFetch {
            topic_name: "",
            partitions: vec![
              PartitionOffsetFetch {
                partition: 0
              }
            ]
          }
        ]
      };

      assert_eq!(result, Done(&[][..], expected))
  }
}
