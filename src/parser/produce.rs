#![allow(dead_code)]
#![allow(unused_imports)]

use parser::primitive::*;
use parser::message::*;

use nom::{HexDisplay,Needed,IResult,FileProducer, be_i16, be_i32};
use nom::{Consumer,ConsumerState};
use nom::IResult::*;
use nom::Err::*;

pub struct ProduceRequest<'a> {
    required_acks: i16,
    timeout: i32,
    topics: Vec<TopicMessageSet<'a>>
}

pub fn produce_request<'a>(input:&'a [u8]) -> IResult<&'a [u8], ProduceRequest<'a>> {
  chain!(
    input,
    acks: be_i16 ~
    timeout: be_i32 ~
    topics: call!(|i| { kafka_array(i, topic_message_set) }), || {
      ProduceRequest {
        required_acks: acks,
        timeout: timeout,
        topics: topics
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
  fn produce_request_tests() {
    
  }
}
