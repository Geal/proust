#![allow(dead_code)]
#![allow(unused_imports)]

use parser::primitive::*;

use nom::{HexDisplay,Needed,IResult,FileProducer,be_u8,be_u16,be_u32,be_u64,be_f32};
use nom::{Consumer,ConsumerState};
use nom::IResult::*;
use nom::Err::*;

// ToDo according to the doc, api_key denotes the type of the request payload.
// Need to investigate on this since the documentation is unclear
pub struct RequestMessage<'a> {
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    client_id: &'a [u8],
    request_payload: &'a RequestPayload<'a>
}

// ToDo other requests
pub enum RequestPayload<'a> {
    MetadataRequest(TopicMetadataRequest<'a>)
}

pub type TopicMetadataRequest<'a> = Vec<KafkaString<'a>>;

pub fn topic_metadata_request<'a>(input:&'a [u8]) -> IResult<&'a [u8], TopicMetadataRequest<'a>> {
  kafka_array(input, kafka_string)
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
