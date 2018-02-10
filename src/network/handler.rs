use mio::*;
use mio::net::{TcpListener, TcpStream};
use mio::unix::UnixReady;
use bytes::{BytesMut, BufMut};
use nom::be_u32;
use nom::IResult::*;
use std::collections::HashMap;
use std::io::{Read, Write, ErrorKind};
use std::net::SocketAddr;
use std::error::Error;
use responses::metadata::*;
use responses::response::*;

const SERVER: Token = Token(0);

pub struct Session {
  pub socket: TcpStream,
  pub state:  ClientState,
  pub token:  usize,
  pub buffer: Option<BytesMut>
}

#[derive(Debug,Clone)]
pub enum ClientState {
  Connect, // Actually the client just ask the metadatas
  Normal,
  Await(usize)
}

pub trait Client {

  fn new(stream: TcpStream, index: usize) -> Self;
  fn handle_message(&mut self, buffer: &mut BytesMut) -> ClientErr;
  fn session(&mut self) -> &mut Session;

  #[inline]
  fn state(&mut self) -> ClientState {
    self.session().state.clone()
  }

  #[inline]
  fn set_state(&mut self, st: ClientState) {
    self.session().state = st;
  }

  #[inline]
  fn buffer(&mut self) -> Option<BytesMut> {
    self.session().buffer.take()
  }

  #[inline]
  fn set_buffer(&mut self, buf: BytesMut) {
    self.session().buffer = Some(buf);
  }

  #[inline]
  fn socket(&mut self) -> &mut TcpStream {
    &mut self.session().socket
  }

  fn read_size(&mut self) -> ClientResult {
    let mut size_buf: [u8; 4] = [0; 4];

    match self.socket().read(&mut size_buf) {
      Ok(size) => {
        if size != 4 {
          Err(ClientErr::Continue)
        }
        else {
          match be_u32(&size_buf) {
            Done(buf, size) => Ok(size as usize),
            _ => Err(ClientErr::Continue),
          }
        }
      },
      Err(e) => {
        match e.kind() {
          ErrorKind::BrokenPipe => {
            error!("broken pipe, removing client");
            Err(ClientErr::ShouldClose)
          },
          _ => {
            error!("error writing: {:?} | {:?} | {:?} | {:?}", e, e.description(), e.cause(), e.kind());
            Err(ClientErr::Continue)
          }
        }
      }
    }
  }

  fn read_to_buf(&mut self, buffer: &mut BytesMut) -> ClientResult {
    let mut bytes_read: usize = 0;

    loop {
      println!("remaining space: {}", buffer.remaining_mut());

      match self.socket().read(buffer) {
        Ok(just_read) => {
          if just_read == 0 {
            println!("breaking because just_read == {}", just_read);
            break;
          }
          else {
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
    match self.socket().write_all(msg) {
      Ok(_)  => Ok(msg.len()),
      Err(e) => {
        match e.kind() {
          ErrorKind::Interrupted  => {
            error!("Interrupted during write message, removing client");
            Err(ClientErr::Continue)
          },
          ErrorKind::BrokenPipe => {
            error!("broken pipe, removing client");
            Err(ClientErr::ShouldClose)
          },
          _ => {
            error!("error writing: {:?} | {:?} | {:?} | {:?}", e, e.description(), e.cause(), e.kind());
            Err(ClientErr::Continue)
          }
        }
      }
    }
  }
}

#[derive(Debug)]
pub enum Message {
  Stop,
  Data(Vec<u8>),
  Close(usize)
}

#[derive(Debug)]
pub enum ClientErr {
  Continue,
  ShouldClose
}

pub type ClientResult = Result<usize, ClientErr>;

pub struct Server<C: Client> {
  pub tcp_listener: TcpListener,
  pub token_index:  usize,
  pub clients:      HashMap<usize, C>,
  pub poll:         Poll,
  pub available_tokens: Vec<usize>
}


impl<C: Client> Server<C> {

  pub fn new(addr: SocketAddr, poll: Poll) -> Self {
    Server {
      tcp_listener: TcpListener::bind(&addr.into()).unwrap(),
      token_index: 1,
      clients: HashMap::new(),
      poll,
      available_tokens: Vec::new()
    }
  }

  pub fn run(&mut self) {
    let mut events = Events::with_capacity(1024);

    self.poll.register(&self.tcp_listener, SERVER, Ready::readable(), PollOpt::edge()).unwrap();

    'main: loop {
      self.poll.poll(&mut events, None).unwrap();

      for event in events.iter() {
        match event.token() {
          SERVER => {
            self.accept();
          },
          Token(t) => {
            let kind = event.readiness();

            if UnixReady::from(kind).is_hup() {
              self.close(t);
            }
            else if UnixReady::from(kind).is_readable() {
              self.client_read(t);
            }
            else {
              self.client_write(t);
            }
          }
          _ => unimplemented!(),
        }
      }
    }
  }

  fn accept(&mut self) {
    if let Ok((stream, addr)) = self.tcp_listener.accept() {
      let index = self.next_token();
      info!("got client n°{:?}", index);
      let token = Token(index);

      self.poll.register(&stream, token, Ready::all(), PollOpt::edge());

      self.clients.insert(index, Client::new(stream, index));
    }
    else {
      error!("Invalid connection");
    }
  }

  fn next_token(&mut self) -> usize {
    match self.available_tokens.pop() {
      None => {
        let index = self.token_index;
        self.token_index += 1;
        index
      },
      Some(index) => {
        index
      }
    }
  }

  fn client_read(&mut self, tk: usize) {
    let mut error = false;

    if let Some(mut client) = self.clients.get_mut(&tk) {
      match client.state() {
        ClientState::Normal => {
          match client.read_size() {
            Ok(size) => {
              let mut buffer = BytesMut::with_capacity(size);
              let capacity = buffer.remaining_mut();  // actual buffer capacity may be higher
              if let Err(ClientErr::ShouldClose) = client.read_to_buf(&mut buffer) {
                error = true;
              }

              if capacity - buffer.remaining_mut() < size {
                client.set_state(ClientState::Await(size - (capacity - buffer.remaining_mut())));
                client.set_buffer(buffer);
              }
              else {
                let mut text = String::new();
                if let ClientErr::ShouldClose = client.handle_message(&mut buffer) {
                  error = true;
                }
              }
            },
            Err(ClientErr::ShouldClose) => {
              println!("should close");
              error = true;
            },
            a => {
              println!("other error: {:?}", a);
            }
          }
        },
        ClientState::Await(sz) => {
          println!("awaits {} bytes", sz);
          let mut buffer = client.buffer().unwrap();
          let capacity = buffer.remaining_mut();

          if let Err(ClientErr::ShouldClose) = client.read_to_buf(&mut buffer) {
            error = true;
          }
          if capacity - buffer.remaining_mut() < sz {
            client.set_state(ClientState::Await(sz - (capacity - buffer.remaining_mut())));
            client.set_buffer(buffer);
          }
          else {
            let mut text = String::new();
            if let ClientErr::ShouldClose = client.handle_message(&mut buffer) {
              error = true;
            }
          }
        },
       _ => { /* do nothing */ },
      };
    }

    // Here to fix "multiple mutable borrows occurs" if we call self.close() directly in
    // the match above.
    if error {
      self.close(tk);
    }
  }

  fn client_write(&mut self, token: usize) {
    if let Some(client) = self.clients.get_mut(&token) {
      match client.state() {
        ClientState::Connect => {
          //TODO: Create a metadata registry.
          // The definition will look like: Rc<RefCell<Slab<Topic>>>
          let mut v: Vec<u8> = vec![];
          let meta = MetadataResponse {
            brokers: vec![Broker {
              node_id: 1,
              host: "127.0.0.1",
              port: 8080
            }],
            topics: vec![TopicMetadata {
              topic_error_code: 0,
              topic_name: "my-topic",
              partitions: vec![PartitionMetadata {
                partition_error_code: 0,
                partition_id: 0,
                leader: 1,
                replicas: vec![0],
                isr: vec![0]
              }]
            }]
          };

          ser_response_message(
            ResponseMessage {
              correlation_id: 0,
              response_payload: ResponsePayload::MetadataResponse(meta)
            },
            &mut v
          );

          client.write(&v).expect("Can't writte to the socket");
          client.set_state(ClientState::Normal);
        },
        ClientState::Normal => {
          //TODO: Look if we have something to write.
        },
        ClientState::Await(_) => {
          // Do nothing, we are looking to read
        },
      }
    }
  }

  fn close(&mut self, token: usize) {
    self.clients.remove(&token);
    self.available_tokens.push(token);

    if let Some(client) = self.clients.get_mut(&token) {
      self.poll.deregister(client.socket());
    }
  }
}