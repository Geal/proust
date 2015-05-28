#![allow(dead_code)]
#![allow(unused_imports)]

use parser::primitive::*;

use nom::{HexDisplay,Needed,IResult,FileProducer, be_i8, be_i16, be_i32, be_i64};
use nom::{Consumer,ConsumerState};
use nom::IResult::*;
use nom::Err::*;

#[derive(PartialEq, Debug)]
pub struct TopicMessageSet<'a> {
    pub topic_name: &'a [u8],
    pub partitions: Vec<PartitionMessageSet<'a>>
}

pub fn topic_message_set<'a>(input: &'a [u8]) -> IResult<&'a [u8], TopicMessageSet<'a>> {
  chain!(
    input,
    topic_name: kafka_string ~
    partitions: call!(|i| { kafka_array(i, partition_message_set) }), || {
      TopicMessageSet {
        topic_name: topic_name,
        partitions: partitions
      }
    })
}

#[derive(PartialEq, Debug)]
pub struct PartitionMessageSet<'a> {
    pub partition: i32,
    pub message_set: MessageSet<'a>
}

pub fn partition_message_set<'a>(input: &'a [u8]) -> IResult<&'a [u8], PartitionMessageSet<'a>> {
  chain!(
    input,
    partition: be_i32 ~
    message_set_size: be_i32 ~
    message_set: message_set, || {
      // TODO use message_set_size
      PartitionMessageSet {
        partition: partition,
        message_set: message_set
      }
    })
}

pub type MessageSet<'a> = Vec<OMsMessage<'a>>;

pub fn message_set<'a>(input: &'a [u8]) -> IResult<&'a [u8], MessageSet<'a>> {
  kafka_array(input, o_ms_message)
}

#[derive(PartialEq, Debug)]
pub struct OMsMessage<'a> {
    pub offset: i64,
    pub message: Message<'a>
}

pub fn o_ms_message<'a>(input: &'a [u8]) -> IResult<&'a [u8], OMsMessage<'a>> {
  chain!(
    input,
    offset: be_i64 ~
    message_size: be_i32 ~
    message: message, || {
      // TODO use message_size
      OMsMessage {
        offset: offset,
        message: message
      }
    })
}

#[derive(PartialEq, Debug)]
pub struct Message<'a> {
  pub crc: i32,
  pub magic_byte: i8,
  pub attributes: i8,
  pub key: &'a [u8],
  pub value: &'a [u8]
}

pub fn message<'a>(input: &'a [u8]) -> IResult<&'a [u8], Message<'a>> {
  chain!(
    input,
    crc: be_i32 ~
    magic_byte: be_i8 ~
    attributes: be_i8 ~
    key: kafka_bytes ~
    value: kafka_bytes, || {
      Message {
        crc: crc,
        magic_byte: magic_byte,
        attributes: attributes,
        key: key,
        value: value
      }
    })
}

#[cfg(test)]
mod tests {
  use super::*;
  use nom::*;
  use nom::IResult::*;

  #[test]
  fn message_tests() {
      let input = &[
        0x00, 0x00, 0x00, 0x00, // crc = 0
        0x00,                   // magic_byte = 0
        0x00,                   // attributes = 0
        0x00, 0x00, 0x00, 0x00, // key = []
        0x00, 0x00, 0x00, 0x00  // value = []
      ];
      let result = message(input);
      let expected = Message {
        crc: 0,
        magic_byte: 0,
        attributes: 0,
        key: &[][..],
        value: &[][..]
      };

      assert_eq!(result, Done(&[][..], expected))
  }
}
