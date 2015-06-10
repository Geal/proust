use std::thread::{self,Thread,Builder};
use std::string::String;
use std::sync::mpsc;
use std::sync::mpsc::{channel,Receiver};
use std::collections::HashMap;

use std::str;
use std::io::{self,Read,ErrorKind};
use std::error::Error;
//use std::net::TcpStream;
use mio::tcp::*;
use mio::*;
use mio::buf::{RingBuf,ByteBuf,MutByteBuf,SliceBuf,MutSliceBuf};
use util::monitor;
//use storage::{self,storage};
use std::marker::PhantomData;
use nom::HexDisplay;
use nom::IResult;
use parser::zookeeper;

const SERVER: Token = Token(0);

#[derive(Debug)]
pub enum Message {
  Stop,
  Data(Vec<u8>),
  Close(usize)
}

enum ClientErr {
  Continue,
  ShouldClose
}

type ClientResult = Result<usize, ClientErr>;

enum ClientState {
  Normal,
  Await(usize)
}

struct Client {
  socket: NonBlock<TcpStream>,
  state:  ClientState,
  token:  usize,
  buffer: Option<MutByteBuf>
}

pub struct ListenerMessage {
  a: u8
}

pub struct Listener {
  t: Thread,
  rx: Receiver<u8>
}

pub struct ZookeeperHandler {
  listener:    NonBlock<TcpListener>,
  //storage_tx : mpsc::Sender<storage::Request>,
  counter:     u8,
  token_index: usize,
  clients:     HashMap<usize, Client>,
  available_tokens: Vec<usize>
}

impl Client {
  fn new(stream: NonBlock<TcpStream>, index: usize) -> Client {
      Client{ socket: stream, state: ClientState::Normal, token: index, buffer: None }
  }

  fn read_size(&mut self) -> ClientResult {
    let mut size_buf = ByteBuf::mut_with_capacity(4);
    match self.socket.read(&mut size_buf) {
      Ok(Some(size)) => {
        // FIXME: should parse 32 bit size here
        let mut b = size_buf.flip();
        let sz = b.read_byte().unwrap() as usize;
        Ok(sz)
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
      println!("remaining space: {}", buffer.remaining());
      match self.socket.read(buffer) {
        Ok(a) => {
          if a == None || a == Some(0) {
            println!("breaking because a == {:?}", a);
            break;
          }
          println!("Ok({:?})", a);
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
}


impl ZookeeperHandler {
  fn accept(&mut self, event_loop: &mut EventLoop<ZookeeperHandler>) {
    if let Ok(Some(stream)) = self.listener.accept() {
      let index = self.next_token();
      println!("got client n°{:?}", index);
      let token = Token(index);
      event_loop.register_opt(&stream, token, Interest::all(), PollOpt::edge());
      self.clients.insert(index, Client::new(stream, index));
    } else {
      println!("invalid connection");
    }
  }

  fn client_read(&mut self, event_loop: &mut EventLoop<ZookeeperHandler>, tk: usize) {
    println!("client n°{:?} readable", tk);
    if let Some(mut client) = self.clients.get_mut(&tk) {

      let sz: usize = 44;
      let mut buffer = ByteBuf::mut_with_capacity(1000);
      if let Ok(size) = client.read_to_buf(&mut buffer) {

        let mut res: [u8; 1000] = unsafe{[::std::mem::uninitialized(); 1000]};
        let mut buf = buffer.flip();
        buf.read_slice(&mut res);
        println!("{} bytes:\n{}", (&res[..size]).len(), (&res[..size]).to_hex(8));

        if let IResult::Done(i, o) =  zookeeper::connection_request(&res[..size]) {
          println!("connection request: {:?}", o);
          println!("{} bytes:\n{}", i.len(), i.to_hex(8));

          let c = zookeeper::ConnectResponse{
            protocol_version: o.protocol_version,
            timeout:          o.timeout,
            session_id:       o.session_id,
            password:         o.password
          };

          let mut v: Vec<u8> = Vec::new();
          zookeeper::ser_connection_response(&c, &mut v);
          client.write(&v[..]);
        }
      }
    }
  }

  fn next_token(&mut self) -> usize {
    match self.available_tokens.pop() {
      None        => {
        let index = self.token_index;
        self.token_index += 1;
        index
      },
      Some(index) => {
        index
      }
    }
  }

  fn close(&mut self, token: usize) {
    self.clients.remove(&token);
    self.available_tokens.push(token);
  }
}

impl Handler for ZookeeperHandler {
  type Timeout = ();
  type Message = Message;

  fn readable(&mut self, event_loop: &mut EventLoop<ZookeeperHandler>, token: Token, _: ReadHint) {
    println!("readable");
    match token {
      SERVER => {
        self.accept(event_loop);
      },
      Token(tk) => {
        self.client_read(event_loop, tk);
      }
    }
  }

  fn writable(&mut self, event_loop: &mut EventLoop<Self>, token: Token) {
    println!("writable");
    match token {
      SERVER => {
        println!("server writeable");
      },
      Token(x) => {
        println!("client n°{:?} writeable", x);
      }
    }
  }

  fn notify(&mut self, _reactor: &mut EventLoop<ZookeeperHandler>, msg: Message) {
    println!("notify: {:?}", msg);
    match msg {
      Message::Close(token) => {
        println!("closing client n°{:?}", token);
        self.close(token)
      },
      _                     => println!("unknown message: {:?}", msg)
    }
  }


  fn timeout(&mut self, event_loop: &mut EventLoop<Self>, timeout: Self::Timeout) {
    println!("timeout");
  }

  fn interrupted(&mut self, event_loop: &mut EventLoop<Self>) {
    println!("interrupted");
  }
}


pub fn start_listener(address: &str) -> (Sender<Message>,thread::JoinHandle<()>)  {
  let mut event_loop = EventLoop::new().unwrap();
  let t2 = event_loop.channel();
  let jg = thread::spawn(move || {
    let listener = NonBlock::new(TcpListener::bind("127.0.0.1:2181").unwrap());
    event_loop.register(&listener, SERVER).unwrap();
    //let t = storage(&event_loop.channel(), "pouet");

    event_loop.run(&mut ZookeeperHandler {
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

