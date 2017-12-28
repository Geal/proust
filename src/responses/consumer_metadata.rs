#![allow(dead_code)]
#![allow(unused_imports)]

use parser::primitive::*;

use nom::{HexDisplay,Needed,IResult,FileProducer};
use nom::{Consumer,ConsumerState};
use nom::IResult::*;

use responses::primitive::*;

#[derive(Debug,PartialEq)]
pub struct ConsumerMetadataResponse<'a> {
  pub error_code: i16,
  pub coordinator_id: i32,
  pub coordinator_host: KafkaString<'a>,
  pub coordinator_port: i32
}

pub fn ser_consumer_metadata_response<'a>(r: ConsumerMetadataResponse<'a>, output: &mut Vec<u8>) -> () {
  ser_i16(r.error_code, output);
  ser_i32(r.coordinator_id, output);
  ser_kafka_string(r.coordinator_host, output);
  ser_i32(r.coordinator_port, output);
}

#[cfg(test)]

mod tests {
  use super::*;
  use nom::*;
  use nom::IResult::*;

  #[test]
  fn ser_consumer_metadata_response_test() {
    let mut v: Vec<u8> = vec![];
    ser_consumer_metadata_response(
      ConsumerMetadataResponse {
        error_code: 0,
        coordinator_id: 1337,
        coordinator_host: "",
        coordinator_port: 9000
      }, &mut v);
    assert_eq!(&v[..], &[
      0x00, 0x00,             // error_code = 0
      0x00, 0x00, 0x05, 0x39, // coordinator_id = 1337
      0x00, 0x00,             // coordinator_host = ""
      0x00, 0x00, 0x23, 0x28  // coordinator_port = 9000
    ][..]);
  }
}
