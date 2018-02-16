use slab::Slab;
use mio::*;
use mio::net::{TcpListener, TcpStream};
use mio::unix::UnixReady;
use bytes::{BytesMut, BufMut};
use nom::be_u32;
use nom::IResult::*;
use std::io::{Read, Write, ErrorKind};
use std::net::SocketAddr;
use std::error::Error;

const SERVER: Token = Token(usize::max_value() - 1);

pub struct Session {
  pub socket: TcpStream,
  pub state:  ClientState,
  pub token:  usize,
  pub buffer: Option<BytesMut>
}

#[derive(Debug,Clone)]
pub enum ClientState {
  Normal,
  Await(usize)
}

pub trait Client {

  fn new(stream: TcpStream, index: usize) -> Self;
  fn handle_message(&mut self, buffer: &mut [u8]) -> ClientErr;
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
  fn set_buffer(&mut self, buf: &[u8]) {
    self.session().buffer = Some(BytesMut::from(buf));
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
            Done(_, size) => Ok(size as usize),
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

  fn read_to_buf(&mut self, buffer: &mut BytesMut, size: usize) -> ClientResult {
    let mut bytes_read: usize = 0;
    loop {
      match self.socket().read(unsafe { buffer.bytes_mut() }) {
        Ok(just_read) => {
          if just_read == 0 {
            println!("breaking because just_read == {}", just_read);
            break;
          }
          else {
            bytes_read += just_read;
            unsafe { buffer.advance_mut(just_read) };
          }
        },
        Err(e) => {
          match e.kind() {
            ErrorKind::BrokenPipe => {
              println!("broken pipe, removing client");
              return Err(ClientErr::ShouldClose)
            },
            ErrorKind::WouldBlock => {
              break;
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
  pub clients:      Slab<C>,
  pub poll:         Poll,
}


impl<C: Client> Server<C> {

  pub fn new(addr: SocketAddr, poll: Poll) -> Self {
    Server {
      tcp_listener: TcpListener::bind(&addr.into()).unwrap(),
      clients: Slab::new(),
      poll,
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

            if UnixReady::from(kind).is_writable() {
              self.client_write(t);
            }

            if UnixReady::from(kind).is_readable() {
              self.client_read(t);
            }
          }
        }
      }
    }
  }

  fn accept(&mut self) {
    match self.tcp_listener.accept() {
      Ok((stream, addr)) => {
        let entry = self.clients.vacant_entry();
        let key = entry.key();

        let token = Token(key);

        if let Err(e) = self.poll.register(&stream, token, Ready::all(), PollOpt::edge()) {
          error!("Can't register {}: {}", addr, e);
        }

        entry.insert(Client::new(stream, key));
      },
      Err(e) => {
        error!("Invalid connection: {}", e);
      }
    }
  }

  fn client_read(&mut self, tk: usize) {
    let mut error = false;

    if let Some(client) = self.clients.get_mut(tk) {
      match client.state() {
        ClientState::Normal => {
          match client.read_size() {
            Ok(size) => {
              let mut buffer = BytesMut::with_capacity(size);
              let capacity = buffer.remaining_mut();  // actual buffer capacity may be higher
              if let Err(ClientErr::ShouldClose) = client.read_to_buf(&mut buffer, size) {
                error = true;
              }

              if capacity - buffer.remaining_mut() < size {
                client.set_state(ClientState::Await(size - (capacity - buffer.remaining_mut())));
                client.set_buffer(&buffer);
              }
              else {
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

          if let Err(ClientErr::ShouldClose) = client.read_to_buf(&mut buffer, sz) {
            error = true;
          }
          if capacity - buffer.remaining_mut() < sz {
            client.set_state(ClientState::Await(sz - (capacity - buffer.remaining_mut())));
            client.set_buffer(&buffer);
          }
          else {
            if let ClientErr::ShouldClose = client.handle_message(&mut buffer) {
              error = true;
            }
          }
        },
      };
    }

    // Here to fix "multiple mutable borrows occurs" if we call self.close() directly in
    // the match above.
    if error {
      self.close(tk);
    }
  }

  fn client_write(&mut self, token: usize) {
    if let Some(client) = self.clients.get_mut(token) {
      match client.state() {
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
    self.clients.remove(token);

    if let Some(client) = self.clients.get_mut(token) {
      if let Err(e) = self.poll.deregister(client.socket()) {
        if let Ok(addr) = client.socket().peer_addr() {
          error!("Can't deregister {}: {}", addr, e);
        }
        else {
          error!("Can't deregister: {}", e);
        }
      }
    }
  }
}
