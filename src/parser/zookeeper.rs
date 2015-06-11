use parser::primitive::*;

use nom::{HexDisplay,Needed,IResult,FileProducer, be_u8, be_i8, be_i16, be_i32, be_i64, eof};
use nom::{Consumer,ConsumerState};
use nom::IResult::*;
use nom::Err::*;

use parser::errors::*;
use responses::primitive::{ser_i32,ser_i64};

use std::str;

#[repr(i32)]
pub enum OpCodes {
  NOTIFICATION   = 0,
  CREATE         = 1,
  DELETE         = 2,
  EXISTS         = 3,
  GET_DATA       = 4,
  SET_DATA       = 5,
  GET_ACL        = 6,
  SET_ACL        = 7,
  GET_CHILDREN   = 8,
  SYNC           = 9,
  PING           = 11,
  GET_CHILDREN2  = 12,
  CHECK          = 13,
  MULTI          = 14,
  AUTH           = 100,
  SET_WATCHES    = 101,
  SASL           = 102,
  CREATE_SESSION = -10,
  CLOSE_SESSION  = -11,
  ERROR          = -1
}

#[derive(Debug)]
pub struct RequestHeader {
  pub xid:   i32,
  pub _type: i32
}

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

#[derive(Debug)]
pub struct Stat {
  pub czxid:          i64,
  pub mzxid:          i64,
  pub ctime:          i64,
  pub mtime:          i64,
  pub version:        i32,
  pub cversion:       i32,
  pub aversion:       i32,
  pub ephemeralOwner: i64,
  pub datalength:     i32,
  pub numChildren:    i32,
  pub pzxid:          i64
}

#[derive(Debug)]
pub struct GetChildrenRequest<'a> {
  pub path:  &'a str,
  pub watch: bool
}

#[derive(Debug)]
pub struct GetChildrenResponse<'a> {
  pub children:  Vec<&'a str>
}

#[derive(Debug)]
pub struct GetChildren2Response<'a> {
  pub children:  Vec<&'a str>,
  pub stat:      Stat
}

pub fn connection_request<'a>(input: &'a [u8]) -> IResult<&'a [u8], ConnectRequest<'a>> {
  chain!(
    input,
    protocol_version: be_i32  ~
    last_zxid_seen:   be_i64  ~
    timeout:          be_i32  ~
    session_id:       be_i64  ~
    password:         buffer  ~
                      eof     ,
    || {
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

pub fn request_header(input: &[u8]) -> IResult<&[u8], RequestHeader> {
  chain!(input,
    xid:   be_i32 ~
    _type: be_i32 ,
    || { RequestHeader { xid: xid, _type: _type } }
  )
}

pub enum Message {
  GetChildren2,
  Ping,
}

pub fn get_children(input: &[u8]) -> IResult<&[u8], GetChildrenRequest> {
  chain!(input,
    path:   ustring ~
    watch:  be_u8   ~
            eof     ,
    || { GetChildrenRequest { path: path, watch: watch == 1 } }
  )
}

/*
pub fn message(input: &[u8]) -> IResult<&[u8], Message> {
  alt!(input,

}
*/

pub fn buffer<'a>(input:&'a [u8]) -> IResult<&'a [u8], &'a [u8]> {
  match be_i32(input) {
    Done(i, length) => {
      if length >= 0 {
        let sz: usize = length as usize;
        if i.len() >= sz {
          return Done(&i[sz..], &i[0..sz])
        } else {
          return Incomplete(Needed::Size(length as usize))
        }
      } else if length == -1 {
        Error(Code(InputError::NotImplemented.to_int())) // TODO maybe make an optional parser which returns an option?
      } else {
        Error(Code(InputError::ParserError.to_int()))
      }
    }
    Error(e)      => Error(e),
    Incomplete(e) => Incomplete(e)
  }
}

named!(pub ustring<&str>, map_res!(buffer, str::from_utf8));

named!(pub vector_ustring< Vec<&str> >, length_value!(be_i32, ustring));

pub fn ser_connection_response<'a>(c: &ConnectResponse<'a>, o: &mut Vec<u8>) -> () {
  ser_i32(32, o);
  ser_i32(c.protocol_version, o);
  ser_i32(c.timeout, o);
  ser_i64(c.session_id, o);
  o.extend(c.password.iter().cloned());
}

pub fn ser_reply_header(r: &ReplyHeader, o: &mut Vec<u8>) -> () {
  ser_i32(16, o);
  ser_i32(r.xid, o);
  ser_i64(r.zxid, o);
  ser_i32(r.err, o);
}
