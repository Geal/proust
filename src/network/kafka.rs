use mio::net::{TcpListener, TcpStream};
use mio::*;
use bytes::{BytesMut, BufMut};
use nom::{IResult, HexDisplay};

use std::error::Error;
use std::str;
use std::thread;

//use util::monitor;
use storage::{self,storage};
use network::handler::*;
use network::handler::Client as ClientTrait;
//use parser::request::request_message;
//use responses::response::ser_response_message;
//use proust::handle_request;

const SERVER: Token = Token(0);
const EVENTS_BUF_SIZE: usize = 1024;

struct Client {
  session: Session
}

impl ClientTrait for Client {
  fn new(stream: TcpStream, index: usize) -> Client {
    Client{
      session: Session {
        socket: stream,
        state: ClientState::Normal,
        token: index,
        buffer: None
      }
    }
  }

  fn session(&mut self) -> &mut Session {
    &mut self.session
  }

  fn handle_message(&mut self, buffer: &mut BytesMut) -> ClientErr {
    let size = buffer.remaining_mut();
    let mut res: Vec<u8> = Vec::with_capacity(size);
    unsafe {
      res.set_len(size);
    }

    /* buffer.read_slice(&mut res[..]);

    let parsed_request_message = request_message(&res[..]);
    if let IResult::Done(_, req) = parsed_request_message {
      println!("Got request: {:?}", req);
      let response = handle_request(req);
      if let Ok(res) = response {
        println!("Writing response: {:?}", res);
        let mut v: Vec<u8> = Vec::new();
        ser_response_message(res, &mut v);
        let write_res = self.write(&v[..]);
      } else {
        println!("Got request handling error {:?}", response);
      }
    } else {
      println!("Got request parsing error {:?}\n{}", parsed_request_message, (&res[..]).to_hex(8));
    } */

    ClientErr::Continue
  }
}

pub fn start_listener(address: String) -> Result<thread::JoinHandle<()>, Box<Error>> {
  let poll = Poll::new()?;

  let jg = thread::spawn(move || {
    let mut server = Server::<Client>::new(address.parse().unwrap(), poll);
    server.run();
  });

  Ok(jg)
}

