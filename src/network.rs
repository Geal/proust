use std::thread::{self,Thread,Builder};
use std::string::String;
use std::sync::mpsc;
use std::sync::mpsc::{channel,Receiver};
use std::rt::unwind::try;
use std::collections::HashMap;

use std::str;
use std::io::{self,Read,ErrorKind};
use std::error::Error;
//use std::net::TcpStream;
use mio::tcp::*;
use mio::*;
use mio::buf::{ByteBuf,MutByteBuf,SliceBuf,MutSliceBuf};
use util::monitor;
use storage::{self,storage};
use std::marker::PhantomData;

const SERVER: Token = Token(0);

#[derive(Debug)]
pub enum Message {
  Stop,
  Data(Vec<u8>),
  Close(usize)
}

enum ClientState {
  Normal,
  Await(usize)
}

struct Client {
  socket: NonBlock<TcpStream>,
  state:  ClientState,
  token:  usize,
  buffer: Option<ByteBuf>
}

pub struct ListenerMessage {
  a: u8
}

pub struct Listener {
  t: Thread,
  rx: Receiver<u8>
}

pub struct KafkaHandler {
  listener:    NonBlock<TcpListener>,
  storage_tx : mpsc::Sender<storage::Request>,
  counter:     u8,
  token_index: usize,
  clients:     HashMap<usize, Client>,
  available_tokens: Vec<usize>
}

impl Client {
  fn read_size(&mut self) -> Result<usize, io::Error> {
    let mut size_buf = ByteBuf::mut_with_capacity(4);
    //if let Ok(Some(size)) = client.socket.read(&mut size_buf) {
    match self.socket.read(&mut size_buf) {
      Ok(Some(size)) => {
        // FIXME: should parse 32 size here
        let mut b = size_buf.flip();
        let sz = b.read_byte().unwrap() as usize;
        Ok(sz)
      },
      Ok(None) => Ok(0),
      Err(e) => Err(e)
    }
  }

  fn read_to_buf(&mut self, event_loop: &mut EventLoop<KafkaHandler>, buffer: &mut MutByteBuf) -> Option<usize> {
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
              event_loop.channel().send(Message::Close(self.token));
            },
            _ => println!("error writing: {:?} | {:?} | {:?} | {:?}", e, e.description(), e.cause(), e.kind())
          }
          return None;
        }
      }
    }
    Some(bytes_read)
  }

  fn write(&mut self, event_loop: &mut EventLoop<KafkaHandler>, msg: &[u8]) {
    match self.socket.write_slice(msg) {
      Ok(o)  => {
        println!("sent message: {:?}", o);
      },
      Err(e) => {
        match e.kind() {
          ErrorKind::BrokenPipe => {
            println!("broken pipe, removing client");
            event_loop.channel().send(Message::Close(self.token));
          },
          _ => println!("error writing: {:?} | {:?} | {:?} | {:?}", e, e.description(), e.cause(), e.kind())
        }
      }
    }
  }
}

impl KafkaHandler {
  fn accept(&mut self, event_loop: &mut EventLoop<KafkaHandler>) {
    if let Ok(Some(stream)) = self.listener.accept() {
      let index = self.next_token();
      println!("got client n°{:?}", index);
      let token = Token(index);
      event_loop.register_opt(&stream, token, Interest::all(), PollOpt::edge());
      self.clients.insert(index, Client{ socket:stream, state: ClientState::Normal, token: index, buffer: None });
    } else {
      println!("invalid connection");
    }
  }

  fn client_read(&mut self, event_loop: &mut EventLoop<KafkaHandler>, tk: usize) {
    println!("client n°{:?} readable", tk);
    if let Some(mut client) = self.clients.get_mut(&tk) {
      match client.state {
        ClientState::Normal => {
          if let Ok(sz) = client.read_size() {
            println!("allocating buffer of size {}", sz);
            let mut read_buf = ByteBuf::mut_with_capacity(sz);

            client.read_to_buf(event_loop, &mut read_buf);

            let mut text = String::new();
            let mut buf = read_buf.flip();
            buf.read_to_string(&mut text);
            println!("content ({} bytes): {}", text.len(), text);
          }
        },
        ClientState::Await(nb) => println!("nb: {}", nb)
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

impl Handler for KafkaHandler {
  type Timeout = ();
  type Message = Message;

  fn readable(&mut self, event_loop: &mut EventLoop<KafkaHandler>, token: Token, _: ReadHint) {
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

  fn notify(&mut self, _reactor: &mut EventLoop<KafkaHandler>, msg: Message) {
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
    let listener = NonBlock::new(TcpListener::bind("127.0.0.1:4242").unwrap());
    event_loop.register(&listener, SERVER).unwrap();
    let t = storage(&event_loop.channel(), "pouet");

    event_loop.run(&mut KafkaHandler {
      listener: listener,
      storage_tx: t,
      counter: 0,
      token_index: 1, // 0 is the server socket
      clients: HashMap::new(),
      available_tokens: Vec::new()
    }).unwrap();

  });

  (t2, jg)
}

