#![allow(dead_code)]
#![allow(unused_imports)]

use parser::primitive::*;

use nom::{HexDisplay,Needed,IResult,FileProducer,be_u8,be_u16,be_u32,be_u64,be_f32};
use nom::{Consumer,ConsumerState};
use nom::IResult::*;
use nom::Err::*;

use parser::metadata::*;

pub struct RequestMessage<'a> {
    api_version: i16,
    correlation_id: i32,
    client_id: &'a [u8],
    request_payload: &'a RequestPayload<'a>
}

// ToDo other requests
pub enum RequestPayload<'a> {
    MetadataRequest(TopicMetadataRequest<'a>)
}

pub fn parse_request_payload<'a>(api_key: i16, input:&'a [u8]) -> IResult<&'a [u8], RequestPayload<'a>> {
    match api_key {
        0  => Error(Code(1)), // Produce
        1  => Error(Code(1)), // Fetch
        2  => Error(Code(1)), // Offsets
        3  => map!(input, topic_metadata_request, |p| { RequestPayload::MetadataRequest(p) }),
        4  => Error(Code(1)), // LeaderAndIsr
        5  => Error(Code(1)), // StopReplica
        6  => Error(Code(1)), // UpdateMetadata
        7  => Error(Code(1)), // ControlledShutdown
        8  => Error(Code(1)), // OffsetCommit
        9  => Error(Code(1)), // OffsetFetch
        10 => Error(Code(1)), // ConsumerMetadata
        11 => Error(Code(1)), // JoinGroup
        12 => Error(Code(1)), // Heartbeat
        _  => Error(Code(2)) // ToDo proper error code
    }
}

// ToDo understand why the chain! macro doesn't work
/*
pub fn request_message<'a>(input:&'a [u8]) -> IResult<&'a [u8], &'a RequestMessage<'a>> {
    chain!(
      input,
      key: be_i16 ~
      version: be_i16 ~
      correlation_id: be_i32 ~
      client_id: kafka_string ~
      payload: topic_metadata_request, || {
          & RequestMessage {
              api_key: key,
              api_version: version,
              correlation_id: correlation_id,
              client_id: client_id,
              request_payload: & RequestPayload::MetadataRequest(payload)
          }
      }
    )
}
*/
