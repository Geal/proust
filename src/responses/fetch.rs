#![allow(dead_code)]
#![allow(unused_imports)]

use parser::primitive::*;
use parser::message::*;

use nom::{HexDisplay,Needed,IResult,FileProducer, be_i16, be_i32, be_i64};
use nom::{Consumer,ConsumerState};
use nom::IResult::*;
use nom::Err::*;

use responses::primitive::*;

/*
FetchResponse => [TopicName [Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet]]
  TopicName => string
  Partition => int32
  ErrorCode => int16
  HighwaterMarkOffset => int64
  MessageSetSize => int32
  */

pub type FetchResponse<'a> = Vec<(KafkaString<'a>, Vec<(i32, i16, i64, MessageSet<'a>)>)>;

pub fn ser_fetch_response(response: FetchResponse, output: &mut Vec<u8>) -> () {
  ser_kafka_array(&response, |topic, oo| {
    let (name, ref ps) = *topic;
    ser_kafka_string(name, oo);
    ser_kafka_array(ps, |p, ooo| {
      let (partition_id, error_code, highwater_mark_offset, ref ms) = *p;
      let mut ms_output: Vec<u8> = vec![];
      ser_message_set(ms, &mut ms_output);

      ser_i32(partition_id, ooo);
      ser_i16(error_code, ooo);
      ser_i64(highwater_mark_offset, ooo);
      ser_i32(ms_output.len() as i32, ooo);
      ooo.extend(ms_output);
    }, oo);
  }, output);
}

pub fn ser_message_set(message_set: &MessageSet, output: &mut Vec<u8>) -> () {
  ser_kafka_array(message_set, |oms, oo| {
    let OMsMessage { offset, ref message } = *oms;
    let mut m_output: Vec<u8> = vec![];
    ser_message(message, &mut m_output);

    ser_i64(offset, oo);
    ser_i32(m_output.len() as i32, oo);
    oo.extend(m_output);
  }, output);
}

pub fn ser_message(message: &Message, output: &mut Vec<u8>) -> () {
  let Message { crc, magic_byte, attributes, ref key, ref value } = *message;

  ser_i32(crc, output);
  ser_i8(magic_byte, output);
  ser_i8(attributes, output);
  ser_kafka_bytes(key, output);
  ser_kafka_bytes(value, output);
}



#[cfg(test)]
mod tests {
  use super::*;
  use nom::*;
  use nom::IResult::*;

  use parser::message::*;

  #[test]
  fn ser_fetch_response_test() {
    let mut v: Vec<u8> = vec![];
    ser_fetch_response(vec![(
      &[][..],
      vec![(
        0,
        0,
        0,
        vec![OMsMessage {
              offset: 0,
              message: Message {
                crc: 0,
                magic_byte: 0,
                attributes: 0,
                key: &[][..],
                value: &[][..]
              }
            }]
      )]
    )], &mut v);

    assert_eq!(&v[..], &[
      0x00, 0x00, 0x00, 0x01, // topics array length = 1
          0x00, 0x00,             // topic_name = ""
          0x00, 0x00, 0x00, 0x01, // partitions array length = 1
              0x00, 0x00, 0x00, 0x00,                         // partition_id = 0
              0x00, 0x00,                                     // error_code = 0
              0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // highwater_mark_offset = 0
              0x00, 0x00, 0x00, 0x1e,                         // message_set_size = 30
                  0x00, 0x00, 0x00, 0x01, // message_set array length = 1
                      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // offset = 0
                      0x00, 0x00, 0x00, 0x0e,                         // message_size = 14
                          0x00, 0x00, 0x00, 0x00, // crc = 0
                          0x00,                   // magic_byte = 0
                          0x00,                   // attributes = 0
                          0x00, 0x00, 0x00, 0x00, // key = []
                          0x00, 0x00, 0x00, 0x00  // value = []
    ][..]);
  }

  #[test]
  fn ser_message_set_test() {
    let mut v: Vec<u8> = vec![];
    ser_message_set(&vec![OMsMessage {
      offset: 0,
      message: Message {
        crc: 0,
        magic_byte: 0,
        attributes: 0,
        key: &[][..],
        value: &[][..]
      }
    }], &mut v);

    assert_eq!(&v[..], &[
      0x00, 0x00, 0x00, 0x01, // message_set array length = 1
          0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // offset = 0
          0x00, 0x00, 0x00, 0x0e,                         // message_size = 14
              0x00, 0x00, 0x00, 0x00, // crc = 0
              0x00,                   // magic_byte = 0
              0x00,                   // attributes = 0
              0x00, 0x00, 0x00, 0x00, // key = []
              0x00, 0x00, 0x00, 0x00  // value = []
    ][..]);
  }

  #[test]
  fn ser_message_test() {
    let mut v: Vec<u8> = vec![];
    ser_message(&Message {
      crc: 0,
      magic_byte: 0,
      attributes: 0,
      key: &[][..],
      value: &[][..]
    }, &mut v);

    assert_eq!(&v[..], &[
      0x00, 0x00, 0x00, 0x00, // crc = 0
      0x00,                   // magic_byte = 0
      0x00,                   // attributes = 0
      0x00, 0x00, 0x00, 0x00, // key = []
      0x00, 0x00, 0x00, 0x00  // value = []
    ][..]);
  }
}
