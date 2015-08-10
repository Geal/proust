#![allow(dead_code)]
#![allow(unused_imports)]

use parser::primitive::*;
use parser::message::*;

use nom::{HexDisplay,Needed,IResult,FileProducer, be_i16, be_i32};
use nom::{Consumer,ConsumerState};
use nom::IResult::*;
use nom::Err::*;

#[derive(PartialEq, Debug)]
pub struct ProduceRequest<'a> {
    required_acks: i16,
    timeout: i32,
    topics: Vec<TopicMessageSet<'a>>
}

pub fn produce_request<'a>(input:&'a [u8]) -> IResult<&'a [u8], ProduceRequest<'a>> {
  chain!(
    input,
    acks: be_i16 ~
    timeout: be_i32 ~
    topics: apply!(kafka_array, topic_message_set), || {
      ProduceRequest {
        required_acks: acks,
        timeout: timeout,
        topics: topics
      }
    }
  )
}

#[cfg(test)]
mod tests {
  use super::*;
  use parser::message::*;
  use nom::*;
  use nom::IResult::*;

  #[test]
  fn produce_request_tests() {
      let input = &[
        0x00, 0x00,             // required_acks = 0
        0x00, 0x00, 0x00, 0x00, // timeout = 0
        0x00, 0x00, 0x00, 0x01, // TopicMessageSet array length
            // TopicMessageSet
            0x00, 0x00,             // topic_name = ""
            0x00, 0x00, 0x00, 0x01, // PartitionMessageSet array length
                // PartitionMessageSet
                0x00, 0x00, 0x00, 0x00, // partition = 0
                0x00, 0x00, 0x00, 0x1a, // message_set_size = 26
                    // OMsMessage
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // offset = 0
                    0x00, 0x00, 0x00, 0x0e,                         // message_size = 14
                    // Message
                    0xe3, 0x8a, 0x68, 0x76, // crc
                    0x00,                   // magic_byte = 0
                    0x00,                   // attributes = 0
                    0x00, 0x00, 0x00, 0x00, // key = []
                    0x00, 0x00, 0x00, 0x00  // value = []

      ];
      let result = produce_request(input);

      assert_eq!(result, Done(&[][..], ProduceRequest {
        required_acks: 0,
        timeout: 0,
        topics: vec![
          TopicMessageSet {
            topic_name: "",
            partitions: vec![
              PartitionMessageSet {
                partition: 0,
                message_set: vec![
                  OMsMessage {
                    offset: 0,
                    message: Message {
                      magic_byte: 0,
                      attributes: 0,
                      key: &[][..],
                      value: &[][..]
                    }
                  }
                ]
              }
            ]
          }
        ]
      }));
  }
}
