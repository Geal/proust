use std::thread::{self,Thread,Builder};
use std::string::String;
use std::sync::mpsc;
use std::sync::mpsc::{channel,Receiver};
use std::rt::unwind::try;
use mio::*;
use mio::tcp::{TcpListener, TcpStream};
use util::monitor;
use storage::{self,storage};

const SERVER: Token = Token(0);

pub type Message = u8;

pub struct ListenerMessage {
  a: u8
}

pub struct Listener {
  t: Thread,
  rx: Receiver<u8>
}

pub struct MyHandler {
  listener:    TcpListener,
  storage_tx : mpsc::Sender<storage::Request>,
  counter:     u8
}


impl Handler for MyHandler {
  type Timeout = ();
  type Message = Message;

  fn readable(&mut self, event_loop: &mut EventLoop<MyHandler>, token: Token, _: ReadHint) {
    match token {
      SERVER => {
        let s = self as &mut MyHandler;
        let _ = s.listener.accept();
        println!("accepted message");
        s.storage_tx.send(s.counter);
        s.counter = s.counter + 1;
        if s.counter > 1 {
          panic!();
        }
      }
      _ => panic!("unexpected token"),
    }
  }

  fn notify(&mut self, _reactor: &mut EventLoop<MyHandler>, msg: Message) {
    println!("message: {:?}", msg);
  }

}

pub fn start_listener(address: &str) -> (Sender<Message>,thread::JoinHandle<()>)  {
  let mut event_loop = EventLoop::new().unwrap();
  let t2 = event_loop.channel().clone();
  let jg = thread::spawn(move || {
    let listener = TcpListener::bind("127.0.0.1:4242").unwrap();
    event_loop.register(&listener, SERVER).unwrap();
    let t = storage(&event_loop.channel(), "pouet");

    event_loop.run(&mut MyHandler {listener: listener, storage_tx: t, counter: 0}).unwrap();

  });

  (t2, jg)
}

