#![allow(dead_code)]
#![allow(unused_imports)]


use nom::{HexDisplay,Needed,IResult,FileProducer,be_i8,be_i16,be_i32,be_i64,be_f32};
use nom::{Consumer,ConsumerState};
use nom::IResult::*;
use nom::Err::*;

use parser::request::*;
use parser::primitive::*;

use responses::primitive::*;
use responses::consumer_metadata::*;
use responses::metadata::*;
use responses::produce::*;
use responses::fetch::*;
use responses::offset::*;
use responses::offset_commit::*;


#[derive(Debug,PartialEq)]
pub struct ResponseMessage<'a> {
  correlation_id: i32,
  response_payload: ResponsePayload<'a>
}

#[derive(Debug,PartialEq)]
pub enum ResponsePayload<'a> {
  ConsumerMetadataResponse(ConsumerMetadataResponse<'a>),
  MetadataResponse(MetadataResponse<'a>),
  ProduceResponse(ProduceResponse<'a>),
  FetchResponse(FetchResponse<'a>),
  OffsetResponse(OffsetResponse<'a>),
  OffsetCommitResponse(OffsetCommitResponse<'a>)
}

pub fn ser_response_message(response: ResponseMessage, output: &mut Vec<u8>) -> () {
  ser_i32(response.correlation_id, output);

  match response.response_payload {
    ResponsePayload::ConsumerMetadataResponse(p) => ser_consumer_metadata_response(p, output),
    ResponsePayload::MetadataResponse(p) => ser_metadata_response(&p, output),
    ResponsePayload::ProduceResponse(p) => ser_produce_response(&p, output),
    ResponsePayload::FetchResponse(p) => ser_fetch_response(p, output),
    ResponsePayload::OffsetResponse(p) => ser_offset_response(p, output),
    ResponsePayload::OffsetCommitResponse(p) => ser_offset_commit_response(p, output)
  }
}
