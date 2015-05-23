#![allow(dead_code)]
#![allow(unused_imports)]

use parser::primitive::*;

use nom::{HexDisplay,Needed,IResult,FileProducer, be_i16, be_i32, be_i64};
use nom::{Consumer,ConsumerState};
use nom::IResult::*;
use nom::Err::*;


#[derive(PartialEq,Debug)]
pub struct OffsetCommitRequest<'a> {
  consumer_group: &'a [u8],
  consumer_group_generation_id: Option<i32>,
  consumer_id: Option<&'a [u8]>,
  retention_time: Option<i64>,
  topics: Vec<TopicOffsetCommit<'a>>
}

pub fn offset_commit_request_v2<'a>(input:&'a [u8]) -> IResult<&'a [u8], OffsetCommitRequest<'a>> {
  chain!(
    input,
    consumer_group: kafka_string ~
    consumer_group_generation_id: be_i32 ~
    consumer_id: kafka_string ~
    retention_time: be_i64 ~
    topics: call!(|i| { kafka_array(i, topic_offset_commit) }), || {
      OffsetCommitRequest {
        consumer_group: consumer_group,
        consumer_group_generation_id: Option::Some(consumer_group_generation_id),
        consumer_id: Option::Some(consumer_id),
        retention_time: Option::Some(retention_time),
        topics: topics
      }
    }
  )
}

#[derive(PartialEq,Debug)]
pub struct TopicOffsetCommit<'a> {
  topic_name: &'a [u8],
  partitions: Vec<PartitionOffsetCommit<'a>>
}

pub fn topic_offset_commit<'a>(input:&'a [u8]) -> IResult<&'a [u8], TopicOffsetCommit<'a>> {
  chain!(
    input,
    topic_name: kafka_string ~
    partitions: call!(|i| { kafka_array(i, partition_offset_commit) }), || {
      TopicOffsetCommit {
        topic_name: topic_name,
        partitions: partitions
      }
    }
  )
}

#[derive(PartialEq,Debug)]
pub struct PartitionOffsetCommit<'a> {
  partition: i32,
  offset: i64,
  metadata: &'a [u8]
}

pub fn partition_offset_commit<'a>(input:&'a [u8]) -> IResult<&'a [u8], PartitionOffsetCommit> {
  chain!(
    input,
    partition: be_i32 ~
    offset: be_i64 ~
    metadata: kafka_string, || {
      PartitionOffsetCommit {
        partition: partition,
        offset: offset,
        metadata: metadata
      }
    }
  )
}

#[cfg(test)]
mod tests {
  use super::*;
  use nom::*;
  use nom::IResult::*;

/*
 * v2
OffsetCommitRequest => ConsumerGroup ConsumerGroupGenerationId ConsumerId RetentionTime [TopicName [Partition Offset Metadata]]
  ConsumerGroupId => string
  ConsumerGroupGenerationId => int32
  ConsumerId => string
  RetentionTime => int64
  TopicName => string
  Partition => int32
  Offset => int64
  Metadata => string
  */

  #[test]
  fn offset_request_tests() {
      let input = &[
        0x00, 0x00,                                     // consumer_group = ""
        0x00, 0x00, 0x00, 0x00,                         // consumer_group_generation_id = 0
        0x00, 0x00,                                     // consumer_id = ""
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // retention_time = 0
        0x00, 0x00, 0x00, 0x01, // topics array length
            0x00, 0x00,             // topic_name = ""
            0x00, 0x00, 0x00, 0x01, // partitions array length
                0x00, 0x00, 0x00, 0x00,                         // partition = 0
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // offset = 0
                0x00, 0x00                                      // metadata = ""
      ];
      let result = offset_commit_request_v2(input);
      let expected = OffsetCommitRequest {
        consumer_group: &[][..],
        consumer_group_generation_id: Option::Some(0),
        consumer_id: Option::Some(&[][..]),
        retention_time: Option::Some(0),
        topics: vec![
          TopicOffsetCommit {
            topic_name: &[][..],
            partitions: vec![
              PartitionOffsetCommit {
                partition: 0,
                offset: 0,
                metadata: &[][..]
              }
            ]
          }
        ]
      };

      assert_eq!(result, Done(&[][..], expected))
  }
}
