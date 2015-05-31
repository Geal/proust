use std::thread::{self,Thread,Builder};
use std::string::String;
use std::sync::mpsc;
use std::sync::mpsc::{channel,Receiver};
use std::rt::unwind::try;

use std::str;
use std::io::Read;
//use std::net::TcpStream;
use mio::tcp::*;//{TcpListener};
use mio::*;
use mio::buf::{ByteBuf};
use util::monitor;
use storage::{self,storage};
use std::marker::PhantomData;

const SERVER: Token = Token(0);
const CLIENT: Token = Token(1);

#[derive(Debug)]
pub enum Message {
  Stop,
  Data(Vec<u8>)
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
  clients:     Vec<NonBlock<TcpStream>>
}


impl Handler for MyHandler {
  type Timeout = ();
  type Message = Message;

  fn readable(&mut self, event_loop: &mut EventLoop<MyHandler>, token: Token, _: ReadHint) {
    println!("readable");
    match token {
      SERVER => {
        let s = self as &mut MyHandler;
        if let Ok(Some(stream)) = s.listener.accept() {
          println!("got client n°{:?}", s.token_index);
          //s.storage_tx.send(s.counter)
          //let index = s.token_index;
          //let token = Token(index);
          //s.token_index += 1;
          let i = s.clients.len() +1;
          let token = Token(i);
          event_loop.register_opt(&stream, token, Interest::all(), PollOpt::edge());
          event_loop.register(&stream, token);
          s.clients.push(stream);
          /*s.counter = s.counter + 1;
          if s.counter > 1 {
          panic!();
          }*/
        } else {
          println!("invalid connection");
        }
      },
      Token(x) => {
        println!("client n°{:?} readable", x);
        let client = &mut self.clients[x - 1];
        //let mut buf = buf::ByteBuf::mut_with_capacity(1024);
        // let mut v: Vec<u8> = Vec::with_capacity(2048);
        let mut read_buf = ByteBuf::mut_with_capacity(2048);
        if let Ok(_) =  client.read(&mut read_buf) {
          let mut buf = read_buf.flip();
          let mut text = String::new();
          buf.read_to_string(&mut text);
          println!("Received: {}", text);
        }
        //let mut sl = [0; 2048];
        //read_buf.read_slice(sl);
        //println!("read: {:?}", str::from_utf8(sl));
      }
      //_ => panic!("unexpected token"),
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

    event_loop.run(&mut MyHandler {listener: listener, storage_tx: t, counter: 0, token_index: 0, clients: Vec::new()}).unwrap();

  });

  (t2, jg)
}

