use mio::*;
use mio::tcp::TcpStream;
use bytes::BytesMut;

use std::thread;
use std::error::Error;

//use parser::zookeeper;
//use responses;
use network::handler::*;
use network::handler::Client as ClientTrait;


#[derive(Debug)]
enum ZookeeperState {
  Connecting,
  Normal
}

struct Client {
  zookeeper_state: ZookeeperState,
  session:   Session
}

impl ClientTrait for Client {
  fn new(stream: TcpStream, index: usize) -> Client {
    Client {
      zookeeper_state: ZookeeperState::Connecting,
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
    unimplemented!();
    /* match self.zookeeper_state {
      ZookeeperState::Connecting => {
        let size = buffer.remaining_mut();
        let mut res: Vec<u8> = Vec::with_capacity(size);
        unsafe {
          res.set_len(size);
        }
        buffer.read_slice(&mut res[..]);

        if let IResult::Done(i, o) =  zookeeper::connection_request(&res[..size]) {
          println!("connection request: {:?}", o);

          let c = zookeeper::ConnectResponse{
            protocol_version: o.protocol_version,
            timeout:          o.timeout,
            session_id:       o.session_id,
            password:         o.password
          };

          let mut v: Vec<u8> = Vec::new();
          zookeeper::ser_connection_response(&c, &mut v);
          let write_res = self.write(&v[..]);
          println!("sent connection response");
          self.zookeeper_state = ZookeeperState::Normal;
        }  else {
          println!("could not parse");
        }
      },
      _ => {
        let size = buffer.remaining();
        let mut res: Vec<u8> = Vec::with_capacity(size);
        unsafe {
          res.set_len(size);
        }
        buffer.read_slice(&mut res[..]);
        if let IResult::Done(i, header) =  zookeeper::request_header(&res[..size]) {
          if header.xid == -2 && header._type == 11 {
            println!("ping");
            let r = zookeeper::ReplyHeader{
              xid: -2,
              zxid: 1,
              err:  0
            };
            let mut v: Vec<u8> = Vec::new();
            let send_size = 16;
            responses::primitive::be_i32(send_size, &mut v);
            zookeeper::ser_reply_header(&r, &mut v);
            let write_res = self.write(&v[..]);
          } else {
            match header._type {
              x if x == zookeeper::OpCodes::GET_CHILDREN as i32 => {
                println!("got get_children");
                if let IResult::Done(i2, request) = zookeeper::get_children(i) {
                  println!("got children request: {:?}", request);
                }
              },
              x if x == zookeeper::OpCodes::GET_CHILDREN2 as i32 => {
                println!("got get_children2");
                if let IResult::Done(i2, request) = zookeeper::get_children(i) {
                  println!("got children2 request: {:?}", request);
                  if request.path == "/brokers/ids" {
                    let rp = zookeeper::GetChildren2Response{
                      //children: vec!["{\"jmx_port\":-1,\"timestamp\":\"1428512949385\",\"host\":\"127.0.0.1\",\"version\":1,\"port\":9092}"],
                      children: vec!["1234"],
                      //children: vec!["abcd"],
                      stat: zookeeper::Stat {
                        czxid: 0, mzxid: 0, ctime: 0, mtime: 0, version: 0, cversion: 0, aversion: 0, ephemeralOwner: 0,
                        datalength: 1, numChildren: 1, pzxid: 0 }
                    };
                    let r = zookeeper::ReplyHeader{
                      xid: header.xid,
                      zxid: 1,
                      err:  0 //ZOK
                    };
                    let send_size = 88+16-4;
                    let mut v: Vec<u8> = Vec::new();
                    responses::primitive::be_i32(send_size, &mut v);
                    zookeeper::ser_reply_header(&r, &mut v);
                    zookeeper::ser_get_children2_response(&rp, &mut v);
                    let write_res = self.write(&v[..]);
                  } else {
                    println!("unknown GetChildren2 path: {}", request.path);
                  }
                }
              },
              x if x == zookeeper::OpCodes::GET_DATA as i32 => {
                println!("got get_data");
                if let IResult::Done(i2, request) = zookeeper::get_data(i) {
                  println!("got data request: {:?}", request);
                  if request.path == "/brokers/ids/1234" {
                    let rp = zookeeper::GetDataResponse{
                      data: &b"{\"jmx_port\":-1,\"timestamp\":\"1428512949385\",\"host\":\"127.0.0.1\",\"version\":1,\"port\":9092}"[..],
                      stat: zookeeper::Stat {
                        czxid: 0, mzxid: 0, ctime: 0, mtime: 0, version: 0, cversion: 0, aversion: 0, ephemeralOwner: 0,
                        datalength: 1, numChildren: 1, pzxid: 0 }
                    };
                    let r = zookeeper::ReplyHeader{
                      xid: header.xid,
                      zxid: 1,
                      err:  0
                    };
                    let stat_size = 68;
                    let send_size = 4 + 16 + ((4 + rp.data.len()) + stat_size);
                    let mut v: Vec<u8> = Vec::new();
                    responses::primitive::be_i32(send_size as i32, &mut v);
                    zookeeper::ser_reply_header(&r, &mut v);
                    zookeeper::ser_get_data_response(&rp, &mut v);
                    let write_res = self.write(&v[..]);
                  } else {
                    println!("unknown GetData path: {}", request.path);
                  }
                }
              },
              _ => {
                println!("invalid state (for now)");
                println!("got this request header: {:?}", header);
                println!("remaining input ( {} bytes ):\n{}", i.len(), i.to_hex(8));
              }
            }
          }
        }
      }
    }
    ClientErr::Continue*/
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