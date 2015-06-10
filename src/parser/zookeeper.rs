use parser::primitive::*;

use nom::{HexDisplay,Needed,IResult,FileProducer, be_i8, be_i16, be_i32, be_i64, eof};
use nom::{Consumer,ConsumerState};
use nom::IResult::*;
use nom::Err::*;

use parser::errors::*;
use responses::primitive::{ser_i32,ser_i64};

#[derive(Debug)]
pub struct ReplyHeader {
  pub xid:  i32,
  pub zxid: i64,
  pub err:  i32
}

#[derive(Debug)]
pub struct ConnectRequest<'a> {
  pub protocol_version: i32,
  pub last_zxid_seen:   i64,
  pub timeout:          i32,
  pub session_id:       i64,
  pub password:         &'a[u8] // 16 bytes
}

#[derive(Debug)]
pub struct ConnectResponse<'a> {
  pub protocol_version: i32,
  pub timeout:          i32,
  pub session_id:       i64,
  pub password:         &'a[u8]
}


pub fn connection_request<'a>(input: &'a [u8]) -> IResult<&'a [u8], ConnectRequest<'a>> {
  chain!(
    input,
    size:             be_i32    ~ // should be 44
    protocol_version: be_i32    ~
    last_zxid_seen:   be_i64    ~
    timeout:          be_i32    ~
    session_id:       be_i64    ~
    password:         take!(16) ,
    //                  eof       ,
    || {
      println!("connection request size: {}", size);
      ConnectRequest {
        protocol_version: protocol_version,
        last_zxid_seen:   last_zxid_seen,
        timeout:          timeout,
        session_id:       session_id,
        password:         password
      }
    }
  )
}

pub fn ser_connection_response<'a>(c: &ConnectResponse<'a>, o: &mut Vec<u8>) -> () {
  ser_i32(32, o);
  ser_i32(c.protocol_version, o);
  ser_i32(c.timeout, o);
  ser_i64(c.session_id, o);
  o.extend(c.password.iter().cloned());
}

pub fn ser_reply_header(r: &ReplyHeader, o: &mut Vec<u8>) -> () {
  ser_i32(r.xid, o);
  ser_i64(r.zxid, o);
  ser_i32(r.err, o);
}
