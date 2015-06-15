use mio::tcp::*;
use mio::*;
use mio::buf::{ByteBuf,MutByteBuf};
use std::collections::HashMap;

const SERVER: Token = Token(0);

pub trait NetworkClient {
  fn new(stream: NonBlock<TcpStream>, index: usize) -> Self;
  fn handle_message(&mut self, buffer: &mut ByteBuf) -> ClientErr;
  fn read_size(&mut self) -> ClientResult;
  fn read_to_buf(&mut self, buffer: &mut MutByteBuf) -> ClientResult;
  fn write(&mut self, msg: &[u8]) -> ClientResult;
  fn state(&self) -> ClientState;
  fn set_state(&mut self, st: ClientState);
  fn buffer(&mut self) -> Option<MutByteBuf>;
  fn set_buffer(&mut self, buf: MutByteBuf);

}

#[derive(Debug)]
pub enum Message {
  Stop,
  Data(Vec<u8>),
  Close(usize)
}

#[derive(Debug,Clone)]
pub enum ClientState {
  Normal,
  Await(usize)
}

#[derive(Debug)]
pub enum ClientErr {
  Continue,
  ShouldClose
}

pub type ClientResult = Result<usize, ClientErr>;

pub struct ProustHandler<Client: NetworkClient> {
  pub listener:    NonBlock<TcpListener>,
  //pub storage_tx : mpsc::Sender<storage::Request>,
  pub counter:     u8,
  pub token_index: usize,
  pub clients:     HashMap<usize, Client>,
  pub available_tokens: Vec<usize>
}


impl<Client: NetworkClient> ProustHandler<Client> {
  fn accept(&mut self, event_loop: &mut EventLoop<Self>) {
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

  fn client_read(&mut self, event_loop: &mut EventLoop<Self>, tk: usize) {
    //println!("client n°{:?} readable", tk);
    if let Some(mut client) = self.clients.get_mut(&tk) {

      match client.state() {
        ClientState::Normal => {
          //println!("state normal");
          match client.read_size() {
            Ok(size) => {
              let mut buffer = ByteBuf::mut_with_capacity(size);

              let capacity = buffer.remaining();  // actual buffer capacity may be higher
              //println!("capacity: {}", capacity);
              if let Err(ClientErr::ShouldClose) = client.read_to_buf(&mut buffer) {
                event_loop.channel().send(Message::Close(tk));
              }

              if capacity - buffer.remaining() < size {
                //println!("read {} bytes", capacity - buffer.remaining());
                //client.state = ClientState::Await(size - (capacity - buffer.remaining()));
                client.set_state(ClientState::Await(size - (capacity - buffer.remaining())));
                //client.buffer = Some(buffer);
                client.set_buffer(buffer);
              } else {
                //println!("got enough bytes: {}", capacity - buffer.remaining());
                let mut text = String::new();
                let mut buf = buffer.flip();
                if let ClientErr::ShouldClose = client.handle_message(&mut buf) {
                  event_loop.channel().send(Message::Close(tk));
                }
              }
            },
            Err(ClientErr::ShouldClose) => {
              println!("should close");
              event_loop.channel().send(Message::Close(tk));
            },
            a => {
              println!("other error: {:?}", a);
            }
          }
        },
        ClientState::Await(sz) => {
          println!("awaits {} bytes", sz);
          //let mut buffer = client.buffer.take().unwrap();
          let mut buffer = client.buffer().unwrap();
          let capacity = buffer.remaining();

          if let Err(ClientErr::ShouldClose) = client.read_to_buf(&mut buffer) {
            event_loop.channel().send(Message::Close(tk));
          }
          if capacity - buffer.remaining() < sz {
            //client.state  = ClientState::Await(sz - (capacity - buffer.remaining()));
            client.set_state(ClientState::Await(sz - (capacity - buffer.remaining())));
            //client.buffer = Some(buffer);
            client.set_buffer(buffer);
          } else {
            let mut text = String::new();
            let mut buf = buffer.flip();
            if let ClientErr::ShouldClose = client.handle_message(&mut buf) {
              event_loop.channel().send(Message::Close(tk));
            }
          }
        }
      }
    }
  }

  fn client_write(&mut self, event_loop: &mut EventLoop<Self>, tk: usize) {
    //println!("client n°{:?} readable", tk);
    if let Some(mut client) = self.clients.get_mut(&tk) {
      let s = b"";
      client.write(s);
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

impl<Client:NetworkClient> Handler for ProustHandler<Client> {
  type Timeout = ();
  type Message = Message;

  fn readable(&mut self, event_loop: &mut EventLoop<Self>, token: Token, _: ReadHint) {
    //println!("readable");
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
    match token {
      SERVER => {
        println!("server writeable");
      },
      Token(tk) => {
        //println!("client n°{:?} writeable", tk);
        //self.client_write(event_loop, tk);
      }
    }
  }

  fn notify(&mut self, _reactor: &mut EventLoop<Self>, msg: Message) {
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
