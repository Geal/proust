#![allow(dead_code)]
#![allow(unused_imports)]

use parser::primitive::*;

use nom::{HexDisplay,Needed,IResult,ErrorKind,FileProducer, be_i16, be_i32, be_i64};
use nom::{Consumer,ConsumerState};
use nom::IResult::*;
use nom::Err::*;

use parser::errors::*;

#[derive(PartialEq,Debug)]
pub enum OffsetCommitRequest<'a> {
  V0(OffsetCommitRequestV0<'a>),
  V1(OffsetCommitRequestV1<'a>),
  V2(OffsetCommitRequestV2<'a>)
}

pub fn offset_commit_request<'a>(input:&'a [u8], api_version: i16) -> IResult<&'a [u8], OffsetCommitRequest<'a>> {
  match api_version {
    0 => map!(input, offset_commit_request_v0, |p| { OffsetCommitRequest::V0(p) }),
    1 => map!(input, offset_commit_request_v1, |p| { OffsetCommitRequest::V1(p) }),
    2 => map!(input, offset_commit_request_v2, |p| { OffsetCommitRequest::V2(p) }),
    _ => Error(Code(ErrorKind::Custom(InputError::ParserError.to_int())))
  }
}

// v0
#[derive(PartialEq,Debug)]
pub struct OffsetCommitRequestV0<'a> {
  consumer_group: KafkaString<'a>,
  topics: Vec<TopicOffsetCommitV0<'a>>
}

pub fn offset_commit_request_v0<'a>(input:&'a [u8]) -> IResult<&'a [u8], OffsetCommitRequestV0<'a>> {
  chain!(
    input,
    consumer_group: kafka_string ~
    topics: apply!(kafka_array, topic_offset_commit_v0), || {
      OffsetCommitRequestV0 {
        consumer_group: consumer_group,
        topics: topics
      }
    }
  )
}

#[derive(PartialEq,Debug)]
pub struct TopicOffsetCommitV0<'a> {
  topic_name: KafkaString<'a>,
  partitions: Vec<PartitionOffsetCommitV0<'a>>
}

pub fn topic_offset_commit_v0<'a>(input:&'a [u8]) -> IResult<&'a [u8], TopicOffsetCommitV0<'a>> {
  chain!(
    input,
    topic_name: kafka_string ~
    partitions: apply!(kafka_array, partition_offset_commit_v0), || {
      TopicOffsetCommitV0 {
        topic_name: topic_name,
        partitions: partitions
      }
    }
  )
}

#[derive(PartialEq,Debug)]
pub struct PartitionOffsetCommitV0<'a> {
  partition: i32,
  offset: i64,
  metadata: KafkaString<'a>
}

pub fn partition_offset_commit_v0<'a>(input:&'a [u8]) -> IResult<&'a [u8], PartitionOffsetCommitV0> {
  chain!(
    input,
    partition: be_i32 ~
    offset: be_i64 ~
    metadata: kafka_string, || {
      PartitionOffsetCommitV0 {
        partition: partition,
        offset: offset,
        metadata: metadata
      }
    }
  )
}

// v1
#[derive(PartialEq,Debug)]
pub struct OffsetCommitRequestV1<'a> {
  consumer_group: KafkaString<'a>,
  consumer_group_generation_id: i32,
  consumer_id: KafkaString<'a>,
  topics: Vec<TopicOffsetCommitV1<'a>>
}

pub fn offset_commit_request_v1<'a>(input:&'a [u8]) -> IResult<&'a [u8], OffsetCommitRequestV1<'a>> {
  chain!(
    input,
    consumer_group: kafka_string ~
    consumer_group_generation_id: be_i32 ~
    consumer_id: kafka_string ~
    topics: apply!(kafka_array, topic_offset_commit_v1), || {
      OffsetCommitRequestV1 {
        consumer_group: consumer_group,
        consumer_group_generation_id: consumer_group_generation_id,
        consumer_id: consumer_id,
        topics: topics
      }
    }
  )
}

#[derive(PartialEq,Debug)]
pub struct TopicOffsetCommitV1<'a> {
  topic_name: KafkaString<'a>,
  partitions: Vec<PartitionOffsetCommitV1<'a>>
}

pub fn topic_offset_commit_v1<'a>(input:&'a [u8]) -> IResult<&'a [u8], TopicOffsetCommitV1<'a>> {
  chain!(
    input,
    topic_name: kafka_string ~
    partitions: apply!(kafka_array, partition_offset_commit_v1), || {
      TopicOffsetCommitV1 {
        topic_name: topic_name,
        partitions: partitions
      }
    }
  )
}

#[derive(PartialEq,Debug)]
pub struct PartitionOffsetCommitV1<'a> {
  partition: i32,
  offset: i64,
  timestamp: i64,
  metadata: KafkaString<'a>
}

pub fn partition_offset_commit_v1<'a>(input:&'a [u8]) -> IResult<&'a [u8], PartitionOffsetCommitV1> {
  chain!(
    input,
    partition: be_i32 ~
    offset: be_i64 ~
    timestamp: be_i64 ~
    metadata: kafka_string, || {
      PartitionOffsetCommitV1 {
        partition: partition,
        offset: offset,
        timestamp: timestamp,
        metadata: metadata
      }
    }
  )
}

// v2
#[derive(PartialEq,Debug)]
pub struct OffsetCommitRequestV2<'a> {
  consumer_group: KafkaString<'a>,
  consumer_group_generation_id: i32,
  consumer_id: KafkaString<'a>,
  retention_time: i64,
  topics: Vec<TopicOffsetCommitV0<'a>>
}

pub fn offset_commit_request_v2<'a>(input:&'a [u8]) -> IResult<&'a [u8], OffsetCommitRequestV2<'a>> {
  chain!(
    input,
    consumer_group: kafka_string ~
    consumer_group_generation_id: be_i32 ~
    consumer_id: kafka_string ~
    retention_time: be_i64 ~
    topics: apply!(kafka_array, topic_offset_commit_v0), || {
      OffsetCommitRequestV2 {
        consumer_group: consumer_group,
        consumer_group_generation_id: consumer_group_generation_id,
        consumer_id: consumer_id,
        retention_time: retention_time,
        topics: topics
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
  fn offset_request_v0_tests() {
      let input = &[
        0x00, 0x00, // consumer_group = ""
        0x00, 0x00, 0x00, 0x01, // topics array length
            0x00, 0x00,             // topic_name = ""
            0x00, 0x00, 0x00, 0x01, // partitions array length
                0x00, 0x00, 0x00, 0x00,                         // partition = 0
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // offset = 0
                0x00, 0x00                                      // metadata = ""
      ];
      let result = offset_commit_request_v0(input);
      let expected = OffsetCommitRequestV0 {
        consumer_group: "",
        topics: vec![
          TopicOffsetCommitV0 {
            topic_name: "",
            partitions: vec![
              PartitionOffsetCommitV0 {
                partition: 0,
                offset: 0,
                metadata: ""
              }
            ]
          }
        ]
      };

      assert_eq!(result, Done(&[][..], expected))
  }

  #[test]
  fn offset_request_v1_tests() {
      let input = &[
        0x00, 0x00,                                     // consumer_group = ""
        0x00, 0x00, 0x00, 0x00,                         // consumer_group_generation_id = 0
        0x00, 0x00,                                     // consumer_id = ""
        0x00, 0x00, 0x00, 0x01, // topics array length
            0x00, 0x00,             // topic_name = ""
            0x00, 0x00, 0x00, 0x01, // partitions array length
                0x00, 0x00, 0x00, 0x00,                         // partition = 0
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // offset = 0
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // timestamp = 0
                0x00, 0x00                                      // metadata = ""
      ];
      let result = offset_commit_request_v1(input);
      let expected = OffsetCommitRequestV1 {
        consumer_group: "",
        consumer_group_generation_id: 0,
        consumer_id: "",
        topics: vec![
          TopicOffsetCommitV1 {
            topic_name: "",
            partitions: vec![
              PartitionOffsetCommitV1 {
                partition: 0,
                offset: 0,
                timestamp: 0,
                metadata: ""
              }
            ]
          }
        ]
      };

      assert_eq!(result, Done(&[][..], expected))
  }

  #[test]
  fn offset_request_v2_tests() {
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
      let expected = OffsetCommitRequestV2 {
        consumer_group: "",
        consumer_group_generation_id: 0,
        consumer_id: "",
        retention_time: 0,
        topics: vec![
          TopicOffsetCommitV0 {
            topic_name: "",
            partitions: vec![
              PartitionOffsetCommitV0 {
                partition: 0,
                offset: 0,
                metadata: ""
              }
            ]
          }
        ]
      };

      assert_eq!(result, Done(&[][..], expected))
  }
}
