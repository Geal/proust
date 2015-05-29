#![allow(dead_code)]
#![allow(unused_imports)]

use parser::primitive::*;

use nom::{HexDisplay,Needed,IResult,FileProducer};
use nom::{Consumer,ConsumerState};
use nom::IResult::*;
use nom::Err::*;

use responses::primitive::*;

/*
MetadataResponse => [Broker][TopicMetadata]
  Broker => NodeId Host Port  (any number of brokers may be returned)
    NodeId => int32
    Host => string
    Port => int32
  TopicMetadata => TopicErrorCode TopicName [PartitionMetadata]
    TopicErrorCode => int16
  PartitionMetadata => PartitionErrorCode PartitionId Leader Replicas Isr
    PartitionErrorCode => int16
    PartitionId => int32
    Leader => int32
    Replicas => [int32]
    Isr => [int32]
*/

#[derive(Debug,PartialEq)]
pub struct MetadataResponse<'a> {
  brokers: Vec<Broker<'a>>,
  topics: Vec<TopicMetadata<'a>>
}

#[derive(Debug,PartialEq)]
pub struct Broker<'a> {
  node_id: i32,
  host: KafkaString<'a>,
  port: i32
}

#[derive(Debug,PartialEq)]
pub struct TopicMetadata<'a> {
  topic_error_code: i16,
  topic_name: KafkaString<'a>,
  partitions: Vec<PartitionMetadata>
}

#[derive(Debug,PartialEq)]
pub struct PartitionMetadata {
  partition_error_code: i16,
  partition_id: i32,
  leader: i32,
  replicas: Vec<i32>,
  isr: Vec<i32>
}


pub fn ser_metadata_response<'a>(r: &MetadataResponse<'a>, o: &mut Vec<u8>) -> () {
  ser_kafka_array(&r.brokers, ser_broker, o);
  ser_kafka_array(&r.topics, ser_topic_metadata, o);
}

pub fn ser_broker<'a>(b: &Broker<'a>, o: &mut Vec<u8>) -> () {
  ser_i32(b.node_id, o);
  ser_kafka_string(b.host, o);
  ser_i32(b.port, o);
}

pub fn ser_topic_metadata<'a>(tm: &TopicMetadata<'a>, o: &mut Vec<u8>) -> () {
  ser_i16(tm.topic_error_code, o);
  ser_kafka_string(tm.topic_name, o);
  ser_kafka_array(&tm.partitions, ser_partition_metadata, o);
}

pub fn ser_partition_metadata(pm: &PartitionMetadata, o: &mut Vec<u8>) -> () {
  ser_i16(pm.partition_error_code, o);
  ser_i32(pm.partition_id, o);
  ser_i32(pm.leader, o);
  ser_kafka_array(&pm.replicas, ser_i32_ref, o);
  ser_kafka_array(&pm.isr, ser_i32_ref, o);
}

#[cfg(test)]
mod tests {
  use super::*;
  use nom::*;
  use nom::IResult::*;

  #[test]
  fn ser_metadata_response_tests() {
    let mut v: Vec<u8> = vec![];
    ser_metadata_response(&MetadataResponse {
      brokers: vec![Broker {
        node_id: 0,
        host: "",
        port: 0
      }],
      topics: vec![TopicMetadata {
        topic_error_code: 0,
        topic_name: "",
        partitions: vec![PartitionMetadata {
          partition_error_code: 0,
          partition_id: 0,
          leader: 0,
          replicas: vec![0],
          isr: vec![0]
        }]
      }]
    }, &mut v);
    assert_eq!(&v[..], &[
        0x00, 0x00, 0x00, 0x01, // brokers array length = 1
            0x00, 0x00, 0x00, 0x00, // node_id = 0
            0x00, 0x00,             // host = ""
            0x00, 0x00, 0x00, 0x00, // port = 0
        0x00, 0x00, 0x00, 0x01, // topics array length = 1
            0x00, 0x00,             // topic_error_code = 0
            0x00, 0x00,             // topic_name = ""
            0x00, 0x00, 0x00, 0x01, // partitions array length = 1
                0x00, 0x00,             // partition_error_code = 0
                0x00, 0x00, 0x00, 0x00, // partition_id = 0
                0x00, 0x00, 0x00, 0x00, // leader = 0
                0x00, 0x00, 0x00, 0x01, // replicas = [0]
                    0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x01,  // isr = [0]
                0x00, 0x00, 0x00, 0x00
      ][..]);
  }

  #[test]
  fn ser_broker_tests() {
    let mut v: Vec<u8> = vec![];
    ser_broker(&Broker {
      node_id: 0,
      host: "",
      port: 0
    }, &mut v);
    assert_eq!(&v[..], &[
        0x00, 0x00, 0x00, 0x00, // node_id = 0
        0x00, 0x00,             // host = ""
        0x00, 0x00, 0x00, 0x00  // port = 0
      ][..]);
  }

  #[test]
  fn ser_topic_metadata_tests() {
    let mut v: Vec<u8> = vec![];
    ser_topic_metadata(&TopicMetadata {
      topic_error_code: 0,
      topic_name: "",
      partitions: vec![]
    }, &mut v);
    assert_eq!(&v[..], &[
        0x00, 0x00,             // topic_error_code = 0
        0x00, 0x00,             // topic_name = ""
        0x00, 0x00, 0x00, 0x00  // partitions = []
      ][..]);
  }

  #[test]
  fn ser_partition_metadata_tests() {
    let mut v: Vec<u8> = vec![];
    ser_partition_metadata(&PartitionMetadata {
      partition_error_code: 0,
      partition_id: 0,
      leader: 0,
      replicas: vec![0],
      isr: vec![0]
    }, &mut v);
    assert_eq!(&v[..], &[
        0x00, 0x00,             // partition_error_code = 0
        0x00, 0x00, 0x00, 0x00, // partition_id = 0
        0x00, 0x00, 0x00, 0x00, // leader = 0
        0x00, 0x00, 0x00, 0x01, // replicas = [0]
            0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x01,  // isr = [0]
        0x00, 0x00, 0x00, 0x00
      ][..]);
  }
}
