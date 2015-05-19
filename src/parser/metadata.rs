#![allow(dead_code)]
#![allow(unused_imports)]

use parser::primitive::*;

use nom::{HexDisplay,Needed,IResult,FileProducer,be_u8,be_u16,be_u32,be_u64,be_f32};
use nom::{Consumer,ConsumerState};
use nom::IResult::*;
use nom::Err::*;

pub type TopicMetadataRequest<'a> = Vec<KafkaString<'a>>;

pub fn topic_metadata_request<'a>(input:&'a [u8]) -> IResult<&'a [u8], TopicMetadataRequest<'a>> {
  kafka_array(input, kafka_string)
}
