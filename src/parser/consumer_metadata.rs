#![allow(dead_code)]
#![allow(unused_imports)]

use parser::primitive::*;

use nom::{HexDisplay,Needed,IResult,FileProducer};
use nom::{Consumer,ConsumerState};
use nom::IResult::*;
use nom::Err::*;

pub type ConsumerMetadataRequest<'a> = KafkaString<'a>;

pub fn consumer_metadata_request<'a>(input:&'a [u8]) -> IResult<&'a [u8], ConsumerMetadataRequest<'a>> {
  kafka_string(input)
}

#[cfg(test)]
mod tests {
  use super::*;
  use nom::*;
  use nom::IResult::*;

  #[test]
  fn consumer_metadata_request_tests() {
      let input = &[
        0x00, 0x00  //  ""
      ];
      let result = consumer_metadata_request(input);
      let expected = "";

      assert_eq!(result, Done(&[][..], expected))
  }
}
