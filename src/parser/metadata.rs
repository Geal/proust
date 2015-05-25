#![allow(dead_code)]
#![allow(unused_imports)]

use parser::primitive::*;

use nom::{HexDisplay,Needed,IResult,FileProducer};
use nom::{Consumer,ConsumerState};
use nom::IResult::*;
use nom::Err::*;

pub type TopicMetadataRequest<'a> = Vec<KafkaString<'a>>;

pub fn topic_metadata_request<'a>(input:&'a [u8]) -> IResult<&'a [u8], TopicMetadataRequest<'a>> {
  kafka_array(input, kafka_string)
}

#[cfg(test)]
mod tests {
  use super::*;
  use nom::*;
  use nom::IResult::*;

  #[test]
  fn topic_metadata_request_tests() {
      let input = &[
        0x00, 0x00, 0x00, 0x01, 0x00, 0x00  //  [""]
      ];
      let result = topic_metadata_request(input);
      let expected = vec![&[][..]];

      assert_eq!(result, Done(&[][..], expected))
  }
}
