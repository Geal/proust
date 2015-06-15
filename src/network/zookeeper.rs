use std::thread::{self,Thread,Builder};
use std::string::String;
use std::sync::mpsc;
use std::sync::mpsc::{channel,Receiver};
use std::collections::HashMap;

use std::str;
use std::io::{self,Read,ErrorKind};
use std::error::Error;
use mio::tcp::*;
use mio::*;
use mio::buf::{RingBuf,ByteBuf,MutByteBuf,SliceBuf,MutSliceBuf};
use util::monitor;
use std::marker::PhantomData;
use nom::HexDisplay;
use nom::IResult;
use parser::zookeeper;
use responses;
use network::handler::*;

const SERVER: Token = Token(0);

#[derive(Debug)]
enum ZookeeperState {
  Connecting,
  Normal
}

struct Client {
  socket:          NonBlock<TcpStream>,
  state:           ClientState,
  zookeeper_state: ZookeeperState,
  token:           usize,
  buffer:          Option<MutByteBuf>
}

impl NetworkClient for Client {
  fn new(stream: NonBlock<TcpStream>, index: usize) -> Client {
    Client{ socket: stream, state: ClientState::Normal, zookeeper_state: ZookeeperState::Connecting, token: index, buffer: None }
  }

  fn state(&self) -> ClientState {
    self.state.clone()
  }

  fn set_state(&mut self, st: ClientState) {
    self.state = st;
  }

  fn buffer(&mut self) -> Option<MutByteBuf> {
    self.buffer.take()
  }

  fn set_buffer(&mut self, buf: MutByteBuf) {
    self.buffer = Some(buf);
  }

  fn read_size(&mut self) -> ClientResult {
    let mut size_buf = ByteBuf::mut_with_capacity(4);
    match self.socket.read(&mut size_buf) {
      Ok(Some(size)) => {
        if size != 4 {
          Err(ClientErr::Continue)
        } else {
          let mut b = size_buf.flip();
          let b1 = b.read_byte().unwrap();
          let b2 = b.read_byte().unwrap();
          let b3 = b.read_byte().unwrap();
          let b4 = b.read_byte().unwrap();
          let sz = ((b1 as u32) << 24) + ((b2 as u32) << 16) + ((b3 as u32) << 8) + b4 as u32;
          //println!("found size: {}", sz);
          Ok(sz as usize)
        }
      },
      Ok(None) => Err(ClientErr::Continue),
      Err(e) => {
        match e.kind() {
          ErrorKind::BrokenPipe => {
            println!("broken pipe, removing client");
            Err(ClientErr::ShouldClose)
          },
          _ => {
            println!("error writing: {:?} | {:?} | {:?} | {:?}", e, e.description(), e.cause(), e.kind());
            Err(ClientErr::Continue)
          }
        }
      }
    }
  }

  fn read_to_buf(&mut self, buffer: &mut MutByteBuf) -> ClientResult {
    let mut bytes_read: usize = 0;
    loop {
      //println!("remaining space: {}", buffer.remaining());
      match self.socket.read(buffer) {
        Ok(a) => {
          if a == None || a == Some(0) {
            //println!("breaking because a == {:?}", a);
            break;
          }
          //println!("Ok({:?})", a);
          if let Some(just_read) = a {
            bytes_read += just_read;
          }
        },
        Err(e) => {
          match e.kind() {
            ErrorKind::BrokenPipe => {
              println!("broken pipe, removing client");
              return Err(ClientErr::ShouldClose)
            },
            _ => {
              println!("error writing: {:?} | {:?} | {:?} | {:?}", e, e.description(), e.cause(), e.kind());
              return Err(ClientErr::Continue)
            }
          }
        }
      }
    }
    Ok(bytes_read)
  }

  fn write(&mut self, msg: &[u8]) -> ClientResult {
    match self.socket.write_slice(msg) {
      Ok(Some(o))  => {
        println!("sent message: {:?}", o);
        Ok(o)
      },
      Ok(None) => Err(ClientErr::Continue),
      Err(e) => {
        match e.kind() {
          ErrorKind::BrokenPipe => {
            println!("broken pipe, removing client");
            Err(ClientErr::ShouldClose)
          },
          _ => {
            println!("error writing: {:?} | {:?} | {:?} | {:?}", e, e.description(), e.cause(), e.kind());
            Err(ClientErr::Continue)
          }
        }
      }
    }
  }

  fn handle_message(&mut self, buffer: &mut ByteBuf) -> ClientErr {
    match self.zookeeper_state {
      ZookeeperState::Connecting => {
        let size = buffer.remaining();
        let mut res: Vec<u8> = Vec::with_capacity(size);
        unsafe {
          res.set_len(size);
        }
        buffer.read_slice(&mut res[..]);
        //println!("connect state: {} bytes:\n{}", (&res[..]).len(), (&res[..]).to_hex(8));

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
              err:  0 //ZOK
            };
            let mut v: Vec<u8> = Vec::new();
            let send_size = 16;
            responses::primitive::ser_i32(send_size, &mut v);
            zookeeper::ser_reply_header(&r, &mut v);
            //println!("sending {} bytes for ping:\n{}", v.len(), (&v[..]).to_hex(8));
            let write_res = self.write(&v[..]);
          } else {
            match header._type {
              x if x == zookeeper::OpCodes::GET_CHILDREN as i32 => {
                println!("got get_children");
                if let IResult::Done(i2, request) = zookeeper::get_children(i) {
                  println!("got children request: {:?}", request);
                  /*let r = zookeeper::GetChildrenResponse{
                    children: vec!["{\"jmx_port\":-1,\"timestamp\":\"1428512949385\",\"host\":\"127.0.0.1\",\"version\":1,\"port\":2181}"]
                  };
                  let mut v: Vec<u8> = Vec::new();
                  zookeeper::ser_get_children_response(&r, &mut v);
                  let write_res = self.write(&v[..]);
                  */
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
                    //let send_size = 4+88+16+4;
                    responses::primitive::ser_i32(send_size, &mut v);
                    //println!("size tag: {}", send_size);
                    zookeeper::ser_reply_header(&r, &mut v);
                    //println!("reply header:\n{}", (&v[..]).to_hex(8));
                    zookeeper::ser_get_children2_response(&rp, &mut v);
                    //println!("sending {} bytes for get_children2:\n{}", v.len(), (&v[..]).to_hex(8));
                    let write_res = self.write(&v[..]);
                  } else {
                    println!("unknown GetChildren2 path: {}", request.path);
                  }
                }
              },
              x if x == zookeeper::OpCodes::GET_DATA as i32 => {
                println!("got get_data");
                //println!("remaining input ( {} bytes ):\n{}", i.len(), i.to_hex(8));
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
                      err:  0 //ZOK
                    };
                    let stat_size = 68;
                    let send_size = 4 + 16 + ((4 + rp.data.len()) + stat_size);
                    let mut v: Vec<u8> = Vec::new();
                    responses::primitive::ser_i32(send_size as i32, &mut v);
                    //println!("size tag: {}", send_size);
                    zookeeper::ser_reply_header(&r, &mut v);
                    //println!("reply header:\n{}", (&v[..]).to_hex(8));
                    zookeeper::ser_get_data_response(&rp, &mut v);
                    //println!("sending {} bytes for get_data:\n{}", v.len(), (&v[..]).to_hex(8));
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
    ClientErr::Continue
  }
}

pub struct ListenerMessage {
  a: u8
}

pub struct Listener {
  t: Thread,
  rx: Receiver<u8>
}

pub fn start_listener(address: &str) -> (Sender<Message>,thread::JoinHandle<()>)  {
  let mut event_loop:EventLoop<ProustHandler<Client>> = EventLoop::new().unwrap();
  let t2 = event_loop.channel();
  let jg = thread::spawn(move || {
    let listener = NonBlock::new(TcpListener::bind("127.0.0.1:2181").unwrap());
    event_loop.register(&listener, SERVER).unwrap();
    //let t = storage(&event_loop.channel(), "pouet");

    event_loop.run(&mut ProustHandler {
      listener: listener,
      //storage_tx: t,
      counter: 0,
      token_index: 1, // 0 is the server socket
      clients: HashMap::new(),
      available_tokens: Vec::new()
    }).unwrap();

  });

  (t2, jg)
}

