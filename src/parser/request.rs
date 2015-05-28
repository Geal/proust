#![allow(dead_code)]
#![allow(unused_imports)]

use parser::primitive::*;

use nom::{HexDisplay,Needed,IResult,FileProducer,be_i8,be_i16,be_i32,be_i64,be_f32, eof};
use nom::{Consumer,ConsumerState};
use nom::IResult::*;
use nom::Err::*;

use parser::produce::*;
use parser::fetch::*;
use parser::offset::*;
use parser::metadata::*;
use parser::offset_commit::*;
use parser::offset_fetch::*;
use parser::consumer_metadata::*;

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
    OffsetFetchRequest(OffsetFetchRequest<'a>),
    ConsumerMetadataRequest(ConsumerMetadataRequest<'a>)
}

pub fn parse_request_payload<'a>(api_version: i16, api_key: i16, input:&'a [u8]) -> IResult<&'a [u8], RequestPayload<'a>> {
    match api_key {
        0  => map!(input, produce_request, |p| { RequestPayload::ProduceRequest(p) }),
        1  => map!(input, fetch_request, |p| { RequestPayload::FetchRequest(p) }),
        2  => map!(input, offset_request, |p| { RequestPayload::OffsetRequest(p) }),
        3  => map!(input, topic_metadata_request, |p| { RequestPayload::MetadataRequest(p) }),

        // Non user-facing control APIs
        // Given proust topology, implementing all of them may not be necessary
        4  => Error(Code(1)), // LeaderAndIsr
        5  => Error(Code(1)), // StopReplica
        6  => Error(Code(1)), // UpdateMetadata
        7  => Error(Code(1)), // ControlledShutdown

        8  => {
           let pp = |i| { offset_commit_request(i, api_version) };
           map!(input, pp, |p| { RequestPayload::OffsetCommitRequest(p) })
        }
        9  => map!(input, offset_fetch_request, |p| { RequestPayload::OffsetFetchRequest(p) }),
        10 => map!(input, consumer_metadata_request, |p| { RequestPayload::ConsumerMetadataRequest(p) }),

        // Not documented, but those exist in the code
        // Given proust topology, implementing all of them may not be necessary
        11 => Error(Code(1)), // JoinGroup
        12 => Error(Code(1)), // Heartbeat

        _  => Error(Code(2)) // ToDo proper error code. A generic error type would be nicer.
                             // I'm looking at you, @Geal
    }
}

pub fn request_message<'a>(input:&'a [u8]) -> IResult<&'a [u8], RequestMessage<'a>> {
    chain!(
      input,
      size: be_i32 ~
      key: be_i16 ~
      version: be_i16 ~
      correlation_id: be_i32 ~
      client_id: kafka_string ~
      payload: call!(|i| { parse_request_payload(version, key, i) }), || {
          // TODO use size
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
        0x00, 0x00, 0x00, 0x0e, // size = 14
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

  #[test]
  fn request_message_trailing_tests() {
      let input = &[
        0x00, 0x00, 0x00, 0x0e, // size = 14
        0x00, 0x03,             // api_key = 3
        0x00, 0x00,             // api_version = 0
        0x00, 0x00, 0x00, 0x00, // correlation_id = 0
        0x00, 0x00,             // client_id = ""
        0x00, 0x00, 0x00, 0x00, // request_payload = []
        0x00, 0x00, 0x00, 0x00  // trailing data
      ];
      let result = request_message(input);
      let expected = RequestMessage {
        api_version: 0,
        correlation_id: 0,
        client_id: &[][..],
        request_payload: RequestPayload::MetadataRequest(vec![])
      };

      assert_eq!(result, Done(&[0x00, 0x00, 0x00, 0x00][..], expected))
  }

  #[test]
  fn request_message_too_short_tests() {
      let input = &[
        0x00, 0x00, 0x00, 0x12, // size = 18
        0x00, 0x03,             // api_key = 3
        0x00, 0x00,             // api_version = 0
        0x00, 0x00, 0x00, 0x00, // correlation_id = 0
        0x00, 0x00,             // client_id = ""
        0x00, 0x00, 0x00, 0x00, // request_payload = []
        0x00, 0x00, 0x00, 0x00  // trailing data
      ];
      let result = request_message(input);
      let expected = RequestMessage {
        api_version: 0,
        correlation_id: 0,
        client_id: &[][..],
        request_payload: RequestPayload::MetadataRequest(vec![])
      };

      assert_eq!(result, Incomplete(Needed::Unknown))
  }

  #[test]
  fn request_message_too_long_tests() {
      let input = &[
        0x00, 0x00, 0x00, 0x0c, // size = 12
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

      // Will fail trying to parse request_payload's array length (4 bytes)
      assert_eq!(result, Incomplete(Needed::Size(4)))
  }
}
