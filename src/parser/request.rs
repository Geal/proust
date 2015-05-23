#![allow(dead_code)]
#![allow(unused_imports)]

use parser::primitive::*;

use nom::{HexDisplay,Needed,IResult,FileProducer,be_i8,be_i16,be_i32,be_i64,be_f32};
use nom::{Consumer,ConsumerState};
use nom::IResult::*;
use nom::Err::*;

use parser::produce::*;
use parser::fetch::*;
use parser::offset::*;
use parser::metadata::*;
use parser::offset_commit::*;
use parser::offset_fetch::*;

#[derive(PartialEq,Debug)]
pub struct RequestMessage<'a> {
    api_version: i16,
    correlation_id: i32,
    client_id: &'a [u8],
    request_payload: RequestPayload<'a>
}

// ToDo other requests
#[derive(PartialEq,Debug)]
pub enum RequestPayload<'a> {
    ProduceRequest(ProduceRequest<'a>),
    FetchRequest(FetchRequest<'a>),
    OffsetRequest(OffsetRequest<'a>),
    MetadataRequest(TopicMetadataRequest<'a>),
    OffsetCommitRequest(OffsetCommitRequest<'a>),
    OffsetFetchRequest(OffsetFetchRequest<'a>)
}

pub fn parse_request_payload<'a>(api_version: i16, api_key: i16, input:&'a [u8]) -> IResult<&'a [u8], RequestPayload<'a>> {
    match api_key {
        0  => map!(input, produce_request, |p| { RequestPayload::ProduceRequest(p) }),
        1  => map!(input, fetch_request, |p| { RequestPayload::FetchRequest(p) }),
        2  => map!(input, offset_request, |p| { RequestPayload::OffsetRequest(p) }),
        3  => map!(input, topic_metadata_request, |p| { RequestPayload::MetadataRequest(p) }),
        4  => Error(Code(1)), // LeaderAndIsr
        5  => Error(Code(1)), // StopReplica
        6  => Error(Code(1)), // UpdateMetadata
        7  => Error(Code(1)), // ControlledShutdown
        8  => {
          // ToDo move it back to parser::offset_commit
          match api_version {
            0 => Error(Code(1)),
            1 => Error(Code(1)),
            2 => map!(input, offset_commit_request_v2, |p| { RequestPayload::OffsetCommitRequest(p) }),
            _ => Error(Code(2))
          }
        }
        9  => map!(input, offset_fetch_request, |p| { RequestPayload::OffsetFetchRequest(p) }),
        10 => Error(Code(1)), // ConsumerMetadata
        11 => Error(Code(1)), // JoinGroup
        12 => Error(Code(1)), // Heartbeat
        _  => Error(Code(2)) // ToDo proper error code
    }
}

pub fn request_message<'a>(input:&'a [u8]) -> IResult<&'a [u8], RequestMessage<'a>> {
    chain!(
      input,
      key: be_i16 ~
      version: be_i16 ~
      correlation_id: be_i32 ~
      client_id: kafka_string ~
      payload: call!(|i| { parse_request_payload(version, key, i) }), || {
          RequestMessage {
              api_version: version,
              correlation_id: correlation_id,
              client_id: client_id,
              request_payload: payload
          }
      }
    )
}

#[cfg(test)]
mod tests {
  use super::*;
  use nom::*;
  use nom::IResult::*;

  #[test]
  fn request_message_test() {
      let input = &[
        0x00, 0x03,             // api_key = 3
        0x00, 0x00,             // api_version = 0
        0x00, 0x00, 0x00, 0x00, // correlation_id = 0
        0x00, 0x00,             // client_id = ""
        0x00, 0x00, 0x00, 0x00  // request_payload = []
      ];
      let result = request_message(input);
      let expected = RequestMessage {
        api_version: 0,
        correlation_id: 0,
        client_id: &[][..],
        request_payload: RequestPayload::MetadataRequest(vec![])
      };

      assert_eq!(result, Done(&[][..], expected))
  }
}
