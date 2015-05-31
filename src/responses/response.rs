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

#[derive(Debug,PartialEq)]
pub struct ResponseMessage<'a> {
  correlation_id: i32,
  response_payload: ResponsePayload<'a>
}

#[derive(Debug,PartialEq)]
pub enum ResponsePayload<'a> {
  ConsumerMetadataResponse(ConsumerMetadataResponse<'a>)
}

pub fn ser_response_message(response: ResponseMessage, output: &mut Vec<u8>) -> () {
  ser_i32(response.correlation_id, output);

  match response.response_payload {
    ResponsePayload::ConsumerMetadataResponse(p) => ser_consumer_metadata_response(p, output)
  }
}
