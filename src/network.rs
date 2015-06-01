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
use mio::buf::{ByteBuf};
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

struct Client {
  sock: NonBlock<TcpStream>
}

pub struct ListenerMessage {
  a: u8
}

pub struct Listener {
  t: Thread,
  rx: Receiver<u8>
}

pub struct MyHandler {
  listener:    NonBlock<TcpListener>,
  storage_tx : mpsc::Sender<storage::Request>,
  counter:     u8,
  token_index: usize,
  clients:     HashMap<usize, NonBlock<TcpStream>>,
  available_tokens: Vec<usize>
}

impl MyHandler {
  fn accept(&mut self, event_loop: &mut EventLoop<MyHandler>) {
    if let Ok(Some(stream)) = self.listener.accept() {
      let index = self.next_token();
      println!("got client n°{:?}", index);
      let token = Token(index);
      event_loop.register_opt(&stream, token, Interest::all(), PollOpt::edge());
      self.clients.insert(index, stream);
    } else {
      println!("invalid connection");
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

impl Handler for MyHandler {
  type Timeout = ();
  type Message = Message;

  fn readable(&mut self, event_loop: &mut EventLoop<MyHandler>, token: Token, _: ReadHint) {
    println!("readable");
    match token {
      SERVER => {
        self.accept(event_loop);
      },
      Token(x) => {
        println!("client n°{:?} readable", x);
        if let Some(client) = self.clients.get_mut(&x) {
          let mut read_buf = ByteBuf::mut_with_capacity(2048);
          match client.read(&mut read_buf) {
            Ok(a) => {
              println!("Ok{:?}", a);
              let mut buf = read_buf.flip();
              let mut text = String::new();
              buf.read_to_string(&mut text);
              println!("Received: {}", text);
              let msg = "hello\n".as_bytes();
              match client.write_slice(msg) {
                Ok(o)  => {
                  println!("sent message: {:?}", o);
                },
                Err(e) => {
                  match e.kind() {
                    ErrorKind::BrokenPipe => {
                      println!("broken pipe, removing client");
                      event_loop.channel().send(Message::Close(x));
                    },
                    _ => println!("error writing: {:?} | {:?} | {:?} | {:?}", e, e.description(), e.cause(), e.kind())
                  }
                }
              }
            },
            Err(e) => {
              match e.kind() {
                ErrorKind::BrokenPipe => {
                  println!("broken pipe, removing client");
                  event_loop.channel().send(Message::Close(x));
                },
                _ => println!("error writing: {:?} | {:?} | {:?} | {:?}", e, e.description(), e.cause(), e.kind())
              }
            }
          }
        }
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

  fn notify(&mut self, _reactor: &mut EventLoop<MyHandler>, msg: Message) {
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

    event_loop.run(&mut MyHandler {
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

