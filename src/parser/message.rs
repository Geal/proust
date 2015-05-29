#![allow(dead_code)]
#![allow(unused_imports)]

use parser::primitive::*;

use nom::{HexDisplay,Needed,IResult,FileProducer, be_i8, be_i16, be_i32, be_i64, eof};
use nom::{Consumer,ConsumerState};
use nom::IResult::*;
use nom::Err::*;

use parser::errors::*;

#[derive(PartialEq, Debug)]
pub struct TopicMessageSet<'a> {
    pub topic_name: KafkaString<'a>,
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
    message_set: call!(|i| { message_set(message_set_size, i) }),
    || {
      PartitionMessageSet {
        partition: partition,
        message_set: message_set
      }
    })
}

pub type MessageSet<'a> = Vec<OMsMessage<'a>>;

pub fn message_set<'a>(size: i32, input: &'a [u8]) -> IResult<&'a [u8], MessageSet<'a>> {
  let ms_bytes = |i: &'a [u8]| {
    if size >= 0 {
      take!(i, size as usize)
    } else {
      Error(Code(InputError::InvalidMessageSetSize.to_int()))
    }
  };

  flat_map!(input, ms_bytes, |msb| {
    chain!(
      msb,
      ms: call!(|i| { kafka_array(i, o_ms_message) }) ~
      eof, || {
        ms
      })
  })
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
    message: call!(|i| { message(message_size, i) }), || {
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

pub fn message<'a>(size: i32, input: &'a [u8]) -> IResult<&'a [u8], Message<'a>> {
  let message_bytes = |i: &'a [u8]| {
    if size >= 0 {
      take!(i, size as usize)
    } else {
      Error(Code(InputError::InvalidMessageSize.to_int()))
    }
  };

  flat_map!(input, message_bytes, |mb| {
    chain!(
      mb,
      crc: be_i32 ~
      magic_byte: be_i8 ~
      attributes: be_i8 ~
      key: kafka_bytes ~
      value: kafka_bytes ~
      eof, || {
        Message {
          crc: crc,
          magic_byte: magic_byte,
          attributes: attributes,
          key: key,
          value: value
        }
      })
  })
}

#[cfg(test)]
mod tests {
  use super::*;
  use nom::*;
  use nom::IResult::*;

  #[test]
  fn topic_message_set_tests() {
      let input = &[
        0x00, 0x00,             // topic_name = ""
        0x00, 0x00, 0x00, 0x01, // partitions array length = 1
            0x00, 0x00, 0x00, 0x01, // partition = 1
            0x00, 0x00, 0x00, 0x1e, // message_set size = 30
            0x00, 0x00, 0x00, 0x01, // message_set array length = 1
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // offset = 1
                0x00, 0x00, 0x00, 0x0e,                         // message_size = 14
                    0x00, 0x00, 0x00, 0x00, // crc = 0
                    0x00,                   // magic_byte = 0
                    0x00,                   // attributes = 0
                    0x00, 0x00, 0x00, 0x00, // key = []
                    0x00, 0x00, 0x00, 0x00  // value = []
      ];
      let result = topic_message_set(input);
      let expected = TopicMessageSet {
        topic_name: "",
        partitions: vec![PartitionMessageSet {
          partition: 1,
          message_set: vec![OMsMessage {
            offset: 1,
            message: Message {
              crc: 0,
              magic_byte: 0,
              attributes: 0,
              key: &[][..],
              value: &[][..]
            }
          }]
        }]
      };

      assert_eq!(result, Done(&[][..], expected))
  }

  #[test]
  fn partition_message_set_tests() {
      let input = &[
        0x00, 0x00, 0x00, 0x01, // partition = 1
        0x00, 0x00, 0x00, 0x1e, // message_set size = 30
        0x00, 0x00, 0x00, 0x01, // message_set array length = 1
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // offset = 1
            0x00, 0x00, 0x00, 0x0e,                         // message_size = 14
                0x00, 0x00, 0x00, 0x00, // crc = 0
                0x00,                   // magic_byte = 0
                0x00,                   // attributes = 0
                0x00, 0x00, 0x00, 0x00, // key = []
                0x00, 0x00, 0x00, 0x00  // value = []
      ];
      let result = partition_message_set(input);
      let expected = PartitionMessageSet {
        partition: 1,
        message_set: vec![OMsMessage {
          offset: 1,
          message: Message {
            crc: 0,
            magic_byte: 0,
            attributes: 0,
            key: &[][..],
            value: &[][..]
          }
        }]
      };

      assert_eq!(result, Done(&[][..], expected))
  }

  #[test]
  fn message_set_tests() {
      let input = &[
        0x00, 0x00, 0x00, 0x01, // message_set array length = 1
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // offset = 1
            0x00, 0x00, 0x00, 0x0e,                         // message_size = 14
                0x00, 0x00, 0x00, 0x00, // crc = 0
                0x00,                   // magic_byte = 0
                0x00,                   // attributes = 0
                0x00, 0x00, 0x00, 0x00, // key = []
                0x00, 0x00, 0x00, 0x00  // value = []
      ];
      let result = message_set(30, input);
      let expected = vec![
        OMsMessage {
          offset: 1,
          message: Message {
            crc: 0,
            magic_byte: 0,
            attributes: 0,
            key: &[][..],
            value: &[][..]
          }
        }
      ];

      assert_eq!(result, Done(&[][..], expected))
  }

  #[test]
  fn message_set_trailing_tests() {
      let input = &[
        0x00, 0x00, 0x00, 0x01, // message_set array length = 1
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // offset = 1
            0x00, 0x00, 0x00, 0x0e,                         // message_size = 14
                0x00, 0x00, 0x00, 0x00, // crc = 0
                0x00,                   // magic_byte = 0
                0x00,                   // attributes = 0
                0x00, 0x00, 0x00, 0x00, // key = []
                0x00, 0x00, 0x00, 0x00,  // value = []
        0x00, 0x00, 0x00, 0x00, // trailing
      ];
      let result = message_set(30, input);
      let expected = vec![
        OMsMessage {
          offset: 1,
          message: Message {
            crc: 0,
            magic_byte: 0,
            attributes: 0,
            key: &[][..],
            value: &[][..]
          }
        }
      ];

      assert_eq!(result, Done(&[0x00, 0x00, 0x00, 0x00][..], expected))
  }

  #[test]
  fn message_set_too_short_tests() {
      let input = &[
        0x00, 0x00, 0x00, 0x01, // message_set array length = 1
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // offset = 1
            0x00, 0x00, 0x00, 0x0e,                         // message_size = 14
                0x00, 0x00, 0x00, 0x00, // crc = 0
                0x00,                   // magic_byte = 0
                0x00,                   // attributes = 0
                0x00, 0x00, 0x00, 0x00, // key = []
                0x00, 0x00, 0x00, 0x00  // value = []
      ];
      let result = message_set(32, input);

      assert_eq!(result, Incomplete(Needed::Size(32)));
  }

  #[test]
  fn message_set_too_long_tests() {
      let input = &[
        0x00, 0x00, 0x00, 0x01, // message_set array length = 1
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // offset = 1
            0x00, 0x00, 0x00, 0x0e,                         // message_size = 14
                0x00, 0x00, 0x00, 0x00, // crc = 0
                0x00,                   // magic_byte = 0
                0x00,                   // attributes = 0
                0x00, 0x00, 0x00, 0x00, // key = []
                0x00, 0x00, 0x00, 0x00  // value = []
      ];
      let result = message_set(28, input);

      assert_eq!(result, Incomplete(Needed::Unknown));
  }

  #[test]
  fn message_tests() {
      let input = &[
        0x00, 0x00, 0x00, 0x00, // crc = 0
        0x00,                   // magic_byte = 0
        0x00,                   // attributes = 0
        0x00, 0x00, 0x00, 0x00, // key = []
        0x00, 0x00, 0x00, 0x00  // value = []
      ];
      let result = message(14, input);
      let expected = Message {
        crc: 0,
        magic_byte: 0,
        attributes: 0,
        key: &[][..],
        value: &[][..]
      };

      assert_eq!(result, Done(&[][..], expected))
  }

  #[test]
  fn message_trailing_tests() {
      let input = &[
        0x00, 0x00, 0x00, 0x00, // crc = 0
        0x00,                   // magic_byte = 0
        0x00,                   // attributes = 0
        0x00, 0x00, 0x00, 0x00, // key = []
        0x00, 0x00, 0x00, 0x00, // value = []
        0x00, 0x00, 0x00, 0x00  // trailing data
      ];
      let result = message(14, input);
      let expected = Message {
        crc: 0,
        magic_byte: 0,
        attributes: 0,
        key: &[][..],
        value: &[][..]
      };

      assert_eq!(result, Done(&[0x00, 0x00, 0x00, 0x00][..], expected))
  }

  #[test]
  fn message_too_short_tests() {
      let input = &[
        0x00, 0x00, 0x00, 0x00, // crc = 0
        0x00,                   // magic_byte = 0
        0x00,                   // attributes = 0
        0x00, 0x00, 0x00, 0x00, // key = []
        0x00, 0x00, 0x00, 0x00  // value = []
      ];
      let result = message(18, input);

      assert_eq!(result, Incomplete(Needed::Size(18)));
  }

  #[test]
  fn message_too_long_tests() {
      let input = &[
        0x00, 0x00, 0x00, 0x00, // crc = 0
        0x00,                   // magic_byte = 0
        0x00,                   // attributes = 0
        0x00, 0x00, 0x00, 0x00, // key = []
        0x00, 0x00, 0x00, 0x00  // value = []
      ];
      let result = message(12, input);

      assert_eq!(result, Incomplete(Needed::Size(4)));
  }
}
