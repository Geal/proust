#![allow(dead_code)]
#![allow(unused_imports)]

use parser::primitive::*;

use nom::{HexDisplay,Needed,IResult,ErrorKind,FileProducer, be_i8, be_i16, be_i32, be_i64};
use nom::{Consumer,ConsumerState};
use nom::IResult::*;

use crc::{crc32, Hasher32};

use parser::errors::*;

const EMPTY_KEY: &[u8] = &[255, 255, 255, 255];

#[derive(PartialEq, Debug)]
pub struct TopicMessageSet<'a> {
    pub topic_name: KafkaString<'a>,
    pub partitions: Vec<PartitionMessageSet<'a>>
}

pub fn topic_message_set<'a>(input: &'a [u8]) -> IResult<&'a [u8], TopicMessageSet<'a>> {
  do_parse!(
    input,
    topic_name: kafka_string >>
    partitions: apply!(kafka_array, partition_message_set) >>
    (
      TopicMessageSet {
        topic_name,
        partitions,
      }
    )
  )
}

#[derive(PartialEq, Debug)]
pub struct PartitionMessageSet<'a> {
    pub partition: i32,
    pub message_set: MessageSet<'a>
}

pub fn partition_message_set<'a>(input: &'a [u8]) -> IResult<&'a [u8], PartitionMessageSet<'a>> {
  do_parse!(
    input,
    partition: be_i32 >>
    message_set_size: be_i32 >>
    message_set: apply!(message_set, message_set_size) >>
    (
      PartitionMessageSet {
        partition,
        message_set,
      }
    )
  )
}

pub type MessageSet<'a> = Vec<OMsMessage<'a>>;

pub fn message_set<'a>(input: &'a [u8], size: i32) -> IResult<&'a [u8], MessageSet<'a>> {
  let ms_bytes = |i: &'a [u8]| {
    if size >= 0 {
      take!(i, size as usize)
    } else {
      Error(ErrorKind::Custom(InputError::InvalidMessageSetSize.to_int()))
    }
  };

  flat_map!(input, ms_bytes, message_set_messages)
}

pub fn message_set_message<'a>(input: &'a [u8]) -> IResult<&'a [u8], MessageSet<'a>> {
  do_parse!(
    input,
    m: o_ms_message >>
    rest: message_set_messages >>
    ({
      let mut a = vec![m];
      a.extend(rest);
      a
    })
  )
}

pub fn message_set_messages<'a>(input: &'a [u8]) -> IResult<&'a [u8], MessageSet<'a>> {
  alt!(input,
    eof!() =>  { |_| vec![] }
    | message_set_message)
}

#[derive(PartialEq, Debug)]
pub struct OMsMessage<'a> {
    pub offset: i64,
    pub message: Message<'a>
}

pub fn o_ms_message<'a>(input: &'a [u8]) -> IResult<&'a [u8], OMsMessage<'a>> {
  do_parse!(
    input,
    offset: be_i64 >>
    message_size: be_i32 >>
    message: apply!(message, message_size) >>
    (
      OMsMessage {
        offset,
        message,
      }
    )
  )
}

#[derive(PartialEq, Debug)]
pub struct Message<'a> {
  pub magic_byte: i8,
  pub attributes: i8,
  pub key: &'a [u8],
  pub value: &'a [u8]
}

pub fn message<'a>(input: &'a [u8], size: i32) -> IResult<&'a [u8], Message<'a>> {
  let sz = size as usize; // Only valid if size >= 0

  let message_bytes = |i: &'a [u8]| {
    if size >= 0 {
      take!(i, sz)
    } else {
      Error(ErrorKind::Custom(InputError::InvalidMessageSize.to_int()))
    }
  };


  // TODO make the code more robust / simple
  // Right now the code in `crc_parser` is based on asumptions valid only because `message_bytes`
  // is before in the parser do_parse
  let crc_parser = |i: &'a [u8]| {
    // The message_bytes parser has already checked the bound>>
    let computed_crc = crc32::checksum_ieee(&input[4..sz]);
    flat_map!(i, be_i32, |given_crc| {
      if given_crc as u32 == computed_crc {
        Done(i, given_crc as u32)
      } else {
        Error(ErrorKind::Custom(InputError::InvalidMessage.to_int()))
      }
    })
  };

  flat_map!(input, message_bytes, |mb| parse_message(mb, &crc_parser))
}

fn parse_message<'a>(mb: &'a [u8], crc_parser: &Fn(&'a [u8]) -> IResult<&'a [u8], u32>) -> IResult<&'a [u8], Message<'a>> {
  do_parse!(
    mb,
    crc_parser >>
    magic_byte: be_i8 >>
    attributes: be_i8 >>
    key: alt!(tag!(EMPTY_KEY) | kafka_bytes) >> //FIXME: tag! set the value of max i32 to the key.
    value: kafka_bytes >>
    eof!() >>
    (
      Message {
        magic_byte,
        attributes,
        key,
        value,
      }
    )
  )
}

#[cfg(test)]
mod tests {
  use super::*;
  use nom::*;
  use nom::IResult::*;

  use parser::errors::*;

  #[test]
  fn parse_message_without_key_tests() {
    let input = &[
      0xfd, 0x6e, 0xbd, 0xdb, // crc
      0x00,                   // magic_byte = 0
      0x00,                   // attributes = 0
      0xff, 0xff, 0xff, 0xff, // empty key
      0x00, 0x00, 0x00, 0x02, // message size = 2
      0x68, 0x69              // message = 'hi' = (68, 69) in ASCII
    ];

    let result = message(input, input.len() as i32);

    let expected = Message {
      magic_byte: 0,
      attributes: 0,
      key: &[255, 255, 255, 255],
      value: "hi".as_bytes(),
    };

    assert_eq!(result, Done(&[][..], expected))
  }

    #[test]
  fn parse_message_with_key_tests() {
    let input = &[
      0x74, 0xd1, 0xed, 0x70, // crc
      0x00,                   // magic_byte = 0
      0x00,                   // attributes = 0
      0x00, 0x00, 0x00, 0x01, // key size = 1
      0x61,                   // key = 'a'
      0x00, 0x00, 0x00, 0x02, // message size = 2
      0x68, 0x69              // message = 'hi' = (68, 69) in ASCII
    ];

    let result = message(input, input.len() as i32);

    let expected = Message {
      magic_byte: 0,
      attributes: 0,
      key: "a".as_bytes(),
      value: "hi".as_bytes(),
    };

    assert_eq!(result, Done(&[][..], expected))
  }

  #[test]
  fn topic_message_set_tests() {
      let input = &[
        0x00, 0x00,             // topic_name = ""
        0x00, 0x00, 0x00, 0x01, // partitions array length = 1
            0x00, 0x00, 0x00, 0x01, // partition = 1
            0x00, 0x00, 0x00, 0x1a, // message_set size = 26
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // offset = 1
                0x00, 0x00, 0x00, 0x0e,                         // message_size = 14
                    0xe3, 0x8a, 0x68, 0x76, // crc
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
        0x00, 0x00, 0x00, 0x1a, // message_set size = 26
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // offset = 1
            0x00, 0x00, 0x00, 0x0e,                         // message_size = 14
                0xe3, 0x8a, 0x68, 0x76, // crc
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
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // offset = 1
            0x00, 0x00, 0x00, 0x0e,                         // message_size = 14
                0xe3, 0x8a, 0x68, 0x76, // crc
                0x00,                   // magic_byte = 0
                0x00,                   // attributes = 0
                0x00, 0x00, 0x00, 0x00, // key = []
                0x00, 0x00, 0x00, 0x00  // value = []
      ];
      let result = message_set(input, 26);
      let expected = vec![
        OMsMessage {
          offset: 1,
          message: Message {
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
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // offset = 1
            0x00, 0x00, 0x00, 0x0e,                         // message_size = 14
                0xe3, 0x8a, 0x68, 0x76, // crc
                0x00,                   // magic_byte = 0
                0x00,                   // attributes = 0
                0x00, 0x00, 0x00, 0x00, // key = []
                0x00, 0x00, 0x00, 0x00,  // value = []
        0x00, 0x00, 0x00, 0x00, // trailing
      ];
      let result = message_set(input, 26);
      let expected = vec![
        OMsMessage {
          offset: 1,
          message: Message {
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
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // offset = 1
            0x00, 0x00, 0x00, 0x0e,                         // message_size = 14
                0xe3, 0x8a, 0x68, 0x76, // crc
                0x00,                   // magic_byte = 0
                0x00,                   // attributes = 0
                0x00, 0x00, 0x00, 0x00, // key = []
                0x00, 0x00, 0x00, 0x00  // value = []
      ];
      let result = message_set(input, 32);

      assert_eq!(result, Incomplete(Needed::Size(32)));
  }

  #[test]
  fn message_set_too_long_tests() {
      let input = &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // offset = 1
            0x00, 0x00, 0x00, 0x0e,                         // message_size = 14
                0xe3, 0x8a, 0x68, 0x76, // crc
                0x00,                   // magic_byte = 0
                0x00,                   // attributes = 0
                0x00, 0x00, 0x00, 0x00, // key = []
                0x00, 0x00, 0x00, 0x00  // value = []
      ];
      let result = message_set(input, 20);

      assert_eq!(result, Incomplete(Needed::Size(26)));
  }

  #[test]
  fn message_tests() {
      let input = &[
        0xe3, 0x8a, 0x68, 0x76, // crc
        0x00,                   // magic_byte = 0
        0x00,                   // attributes = 0
        0x00, 0x00, 0x00, 0x00, // key = []
        0x00, 0x00, 0x00, 0x00  // value = []
      ];
      let result = message(input, 14);
      let expected = Message {
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
        0xe3, 0x8a, 0x68, 0x76, // crc
        0x00,                   // magic_byte = 0
        0x00,                   // attributes = 0
        0x00, 0x00, 0x00, 0x00, // key = []
        0x00, 0x00, 0x00, 0x00, // value = []
        0x00, 0x00, 0x00, 0x00  // trailing data
      ];
      let result = message(input, 14);
      let expected = Message {
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
        0xe3, 0x8a, 0x68, 0x76, // crc
        0x00,                   // magic_byte = 0
        0x00,                   // attributes = 0
        0x00, 0x00, 0x00, 0x00, // key = []
        0x00, 0x00, 0x00, 0x00  // value = []
      ];
      let result = message(input, 18);

      assert_eq!(result, Incomplete(Needed::Size(18)));
  }

  #[test]
  fn message_too_long_tests() {
      let input = &[
        0xe3, 0x8a, 0x68, 0x76, // crc
        0x00,                   // magic_byte = 0
        0x00,                   // attributes = 0
        0x00, 0x00, 0x00, 0x00, // key = []
        0x00, 0x00, 0x00, 0x00  // value = []
      ];
      let result = message(input, 12);

      // The CRC doesn't include the last two bytes so the check fails
      assert_eq!(result, Error(ErrorKind::Custom(InputError::InvalidMessage.to_int())));
  }

  #[test]
  fn message_invalid_crc_tests() {
      let input = &[
        0x00, 0x00, 0x00, 0x00, // invalid CRC
        0x00,                   // magic_byte = 0
        0x00,                   // attributes = 0
        0x00, 0x00, 0x00, 0x00, // key = []
        0x00, 0x00, 0x00, 0x00  // value = []
      ];
      let result = message(input, 14);

      assert_eq!(result, Error(ErrorKind::Custom(InputError::InvalidMessage.to_int())));
  }
}
