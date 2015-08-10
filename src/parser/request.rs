#![allow(dead_code)]
#![allow(unused_imports)]

use parser::primitive::*;
use parser::errors::*;

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
    client_id: KafkaString<'a>,
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

pub fn parse_request_payload<'a>(input:&'a [u8], api_version: i16, api_key: i16) -> IResult<&'a [u8], RequestPayload<'a>> {
    match api_key {
        0  => map!(input, produce_request, |p| { RequestPayload::ProduceRequest(p) }),
        1  => map!(input, fetch_request, |p| { RequestPayload::FetchRequest(p) }),
        2  => map!(input, offset_request, |p| { RequestPayload::OffsetRequest(p) }),
        3  => map!(input, topic_metadata_request, |p| { RequestPayload::MetadataRequest(p) }),

        // Non user-facing control APIs
        // Given proust topology, implementing all of them may not be necessary
        4  => Error(Code(InputError::NotImplemented.to_int())), // LeaderAndIsr
        5  => Error(Code(InputError::NotImplemented.to_int())), // StopReplica
        6  => Error(Code(InputError::NotImplemented.to_int())), // UpdateMetadata
        7  => Error(Code(InputError::NotImplemented.to_int())), // ControlledShutdown

        8  => {
           let pp = |i| { offset_commit_request(i, api_version) };
           map!(input, pp, |p| { RequestPayload::OffsetCommitRequest(p) })
        }
        9  => map!(input, offset_fetch_request, |p| { RequestPayload::OffsetFetchRequest(p) }),
        10 => map!(input, consumer_metadata_request, |p| { RequestPayload::ConsumerMetadataRequest(p) }),

        // Not documented, but those exist in the code
        // Given proust topology, implementing all of them may not be necessary
        11 => Error(Code(InputError::NotImplemented.to_int())), // JoinGroup
        12 => Error(Code(InputError::NotImplemented.to_int())), // Heartbeat

        _  => Error(Code(InputError::ParserError.to_int()))
    }
}

pub fn request_message_with_length<'a>(input:&'a [u8]) -> IResult<&'a [u8], RequestMessage<'a>> {
  match be_i32(input) {
    Done(i, size) => {
      let request_bytes = |i: &'a [u8]| {
        if size >= 0 {
          take!(i, size as usize)
        } else {
          Error(Code(InputError::InvalidRequestSize.to_int()))
        }
      };
      flat_map!(i, request_bytes, |rb| {
          request_message(rb)
      })
    }
    Error(e)      => Error(e),
    Incomplete(e) => Incomplete(e)
  }
}

pub fn request_message<'a>(input:&'a [u8]) -> IResult<&'a [u8], RequestMessage<'a>> {
  chain!(
    input,
    key: be_i16 ~
    version: be_i16 ~
    correlation_id: be_i32 ~
    client_id: kafka_string ~
    payload: apply!(parse_request_payload, version, key) ~
    eof, || {
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

  use nom::Err::*;

  use parser::errors::*;

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
      let result = request_message_with_length(input);
      let expected = RequestMessage {
        api_version: 0,
        correlation_id: 0,
        client_id: "",
        request_payload: RequestPayload::MetadataRequest(vec![])
      };

      assert_eq!(result, Done(&[][..], expected))
  }

  #[test]
  fn request_message_wrong_size_test() {
      let input = &[
        0x80, 0x00, 0x00, 0x00, // size = 14
        0x00, 0x03,             // api_key = 3
        0x00, 0x00,             // api_version = 0
        0x00, 0x00, 0x00, 0x00, // correlation_id = 0
        0x00, 0x00,             // client_id = ""
        0x00, 0x00, 0x00, 0x00  // request_payload = []
      ];
      let result = request_message_with_length(input);

      assert_eq!(result, Error(Code(InputError::InvalidRequestSize.to_int())));
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
      let result = request_message_with_length(input);
      let expected = RequestMessage {
        api_version: 0,
        correlation_id: 0,
        client_id: "",
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
      let result = request_message_with_length(input);

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
      let result = request_message_with_length(input);

      // Will fail trying to parse request_payload's array length (4 bytes)
      assert_eq!(result, Incomplete(Needed::Size(4)))
  }
}
