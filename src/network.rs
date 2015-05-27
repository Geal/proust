use std::thread::{self,Thread,Builder};
use std::string::String;
use std::sync::mpsc;
use std::sync::mpsc::{channel,Receiver};
use std::rt::unwind::try;
use mio::*;
use mio::tcp::{TcpListener, TcpStream};

const SERVER: Token = Token(0);

pub struct ListenerMessage {
  a: u8
}

pub fn monitor<F, R>(f: F, restart_count: u8) -> Result<R, u8>
where F: Fn() -> R + Send + 'static {

  thread::spawn( move || {
    let mut count = 0;

    while count <= restart_count {
      unsafe {
        let mut res: Option<R> = None;

        if let Err(e) = try(|| res = Some(f())) {
          count = count + 1;
          println!("restarting");
        }
      }
    }
    println!("failed too much");
  });
  Err(0)
}

pub struct Listener {
  t: Thread,
  rx: Receiver<u8>
}

pub struct MyHandler<'a> {
  listener: &'a TcpListener,
  sender:   &'a mpsc::Sender<u8>,
  counter:  u8
}


impl<'a> Handler for MyHandler<'a> {
  type Timeout = ();
  type Message = ();

  fn readable(&mut self, event_loop: &mut EventLoop<MyHandler>, token: Token, _: ReadHint) {
    match token {
      SERVER => {
        let s = self as &mut MyHandler<'a>;
        let _ = s.listener.accept();
        println!("accepted message");
        s.sender.send(s.counter);
        s.counter = s.counter + 1;
        if s.counter > 1 {
          panic!();
        }
      }
      _ => panic!("unexpected token"),
    }
  }
}

pub fn start_listener(address: &str) -> Receiver<u8> {
  let (tx, rx) = channel::<u8>();

  //let jg = thread::spawn(move || {
  let jg = monitor(move || {
    let listener = TcpListener::bind("127.0.0.1:4242").unwrap();
    let mut event_loop = EventLoop::new().unwrap();
    event_loop.register(&listener, SERVER).unwrap();
    let mut counter: u8 = 0;

    event_loop.run(&mut MyHandler {listener: &listener, sender: &tx, counter: 0}).unwrap();

  }, 1);
  //});

  rx
}

