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
use storage::{self,storage};
use std::marker::PhantomData;
use nom::HexDisplay;
use network::handler::*;

const SERVER: Token = Token(0);

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

impl NetworkClient for Client {
  fn new(stream: NonBlock<TcpStream>, index: usize) -> Client {
      Client{ socket: stream, state: ClientState::Normal, token: index, buffer: None }
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

  fn socket(&mut self) -> &mut NonBlock<TcpStream> {
    &mut self.socket
  }

  fn handle_message(&mut self, buffer: &mut ByteBuf) ->ClientErr {
    let size = buffer.remaining();
    let mut res: Vec<u8> = Vec::with_capacity(size);
    unsafe {
      res.set_len(size);
    }
    buffer.read_slice(&mut res[..]);
    println!("handle_message got {} bytes:\n{}", (&res[..]).len(), (&res[..]).to_hex(8));
    ClientErr::Continue
  }
}

pub fn start_listener(address: &str) -> (Sender<Message>,thread::JoinHandle<()>)  {
  let mut event_loop:EventLoop<ProustHandler<Client>> = EventLoop::new().unwrap();
  let t2 = event_loop.channel();
  let jg = thread::spawn(move || {
    let listener = NonBlock::new(TcpListener::bind("127.0.0.1:9092").unwrap());
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

