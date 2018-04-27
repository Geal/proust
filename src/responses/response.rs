#![allow(dead_code)]
#![allow(unused_imports)]


use nom::{HexDisplay,Needed,IResult,FileProducer,be_i8,be_i16,be_i32,be_i64,be_f32};
use nom::{Consumer,ConsumerState};
use nom::IResult::*;

use parser::request::*;
use parser::primitive::*;

use responses::primitive::*;
use responses::consumer_metadata::*;
use responses::metadata::*;
use responses::produce::*;
use responses::fetch::*;
use responses::offset::*;
use responses::offset_commit::*;
use responses::offset_fetch::*;
use responses::api_versions::*;

#[derive(Debug,PartialEq)]
pub struct ResponseMessage<'a> {
  pub correlation_id: i32,
  pub response_payload: ResponsePayload<'a>
}

#[derive(Debug,PartialEq)]
pub enum ResponsePayload<'a> {
  ConsumerMetadataResponse(ConsumerMetadataResponse<'a>),
  MetadataResponse(MetadataResponse<'a>),
  ProduceResponse(ProduceResponse<'a>),
  FetchResponse(FetchResponse<'a>),
  OffsetResponse(OffsetResponse<'a>),
  OffsetCommitResponse(OffsetCommitResponse<'a>),
  OffsetFetchResponse(OffsetFetchResponse<'a>),
  ApiVersionsResponse(ApiVersionsResponse<'a>),
}

pub fn ser_response_message(response: ResponseMessage, output: &mut Vec<u8>) -> () {
  let mut r_output: Vec<u8> = vec![];
  ser_i32(response.correlation_id, &mut r_output);

  match response.response_payload {
    ResponsePayload::ConsumerMetadataResponse(p) => ser_consumer_metadata_response(p, &mut r_output),
    ResponsePayload::MetadataResponse(p) => ser_metadata_response(&p, &mut r_output),
    ResponsePayload::ProduceResponse(p) => ser_produce_response(&p, &mut r_output),
    ResponsePayload::FetchResponse(p) => ser_fetch_response(p, &mut r_output),
    ResponsePayload::OffsetResponse(p) => ser_offset_response(p, &mut r_output),
    ResponsePayload::OffsetCommitResponse(p) => ser_offset_commit_response(p, &mut r_output),
    ResponsePayload::OffsetFetchResponse(p) => ser_offset_fetch_response(p, &mut r_output),
    ResponsePayload::ApiVersionsResponse(p) => ser_api_versions_response(p, &mut r_output),
  }

  ser_i32(r_output.len() as i32, output);
  output.extend(r_output);
}

#[cfg(test)]

mod tests {
  use super::*;
  use nom::*;
  use nom::IResult::*;

  use responses::consumer_metadata::*;

  #[test]
  fn ser_response_message_test() {
    let mut v: Vec<u8> = vec![];
    ser_response_message(
      ResponseMessage {
        correlation_id: 0,
        response_payload: ResponsePayload::ConsumerMetadataResponse(ConsumerMetadataResponse {
          error_code: 0,
          coordinator_id: 1337,
          coordinator_host: "",
          coordinator_port: 9000
        })
      }, &mut v);
    assert_eq!(&v[..], &[
      0x00, 0x00, 0x00, 0x10, // size = 16
      0x00, 0x00, 0x00, 0x00, // correlation_id = 0
      0x00, 0x00,             // error_code = 0
      0x00, 0x00, 0x05, 0x39, // coordinator_id = 1337
      0x00, 0x00,             // coordinator_host = ""
      0x00, 0x00, 0x23, 0x28  // coordinator_port = 9000
    ][..]);
  }
}
