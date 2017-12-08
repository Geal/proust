//#![feature(core)]

//extern crate core;
extern crate mmap;

use std::str;
use std::os;

use std::os::unix::io::AsRawFd;
use std::fs::File;
use std::fs::OpenOptions;
use mmap::{MemoryMap,MapOption};
use core::slice::from_raw_parts_mut;
use std;
use std::sync::mpsc;
use std::sync::mpsc::{channel,Sender,Receiver};
use std::thread::{self,Thread,Builder};
use std::io::Write;
use util::monitor;
use mio;
use network;

pub type Request  = u8;
pub type Response = u8;

pub struct Storage<'a> {
  filename: &'a str,
  file:     File,
  data:     &'a mut [u8],
  size:     usize,
  map:      MemoryMap
}

impl<'a> Storage<'a> {

  pub fn create(filename: &str) -> Option<Storage> {
    if let Ok(mut file) = OpenOptions::new().read(true).write(true).create(true).open(filename) {
      let mut size: usize = 0;
      // fills the file with 0
      if let Ok(meta) = file.metadata() {
        if meta.len() == 0 {
          file.set_len(4096);
          size = 4096;
        } else {
          size = meta.len() as usize;
        }
      }

      let fd = file.as_raw_fd();
      let mut options: Vec<MapOption> = Vec::new();
      options.push(MapOption::MapWritable);
      options.push(MapOption::MapFd(fd));
      options.push(MapOption::MapOffset(0));
      options.push(MapOption::MapReadable);
      let mut mm = MemoryMap::new(size, &options[..]).unwrap();
      let mut sl: &mut[u8] = unsafe {from_raw_parts_mut(mm.data(), mm.len())};

      println!("data2:\n{}", str::from_utf8(sl).unwrap());
      sl[0] = 0x41;
      sl[1] = 0x42;
      println!("data3:\n{}", str::from_utf8(sl).unwrap());
      println!("msync result: {:?}", mm.msync());
      println!("data4:\n{}", str::from_utf8(sl).unwrap());

      Some(Storage{ filename: filename, file: file, data: sl, size: size, map: mm })
    } else {
      None
    }
  }

  pub fn read(&self, position: usize, length: usize) -> Option<&[u8]> {
    //println!("position: {}, length: {}, self.size: {}, slice length: {}", position, length, self.size, self.data.len());
    if position > self.size || length > self.size || self.size - length < position {
      None
    } else {
      Some(&(self.data)[position..(position+length)])
    }
  }

  pub fn write(&mut self, position: usize, src: &[u8]) -> Option<()> {
    let length = src.len();
    while position + length > self.size {
      self.grow();
    }

    (self.data)[position..(position+length)].copy_from_slice(src);
    Some(())
  }

  pub fn grow(&mut self) {
    self.file.set_len((self.size + 4096) as u64);
    self.size = self.size + 4096;
    let fd = self.file.as_raw_fd();
    let mut options: Vec<MapOption> = Vec::new();
    options.push(MapOption::MapWritable);
    options.push(MapOption::MapFd(fd));
    options.push(MapOption::MapOffset(0));
    options.push(MapOption::MapReadable);
    println!("creating a map of size {}", self.size);
    let mut mm = MemoryMap::new(self.size, &options[..]).unwrap();
    let mut sl: &mut[u8] = unsafe {from_raw_parts_mut(mm.data(), mm.len())};
    self.map  = mm;
    self.data = sl;
  }
}

pub fn storage(out:&Sender<network::handler::Message>, name: &str) -> Sender<Request> {
  let (tx,rx) = channel::<u8>();
  let mut v: Vec<u8> = Vec::new();
  let t2 = out.clone();
  thread::spawn(move || {
    v.push(1);
    v.push(2);
    v.push(3);
    loop {
      if let Ok(count) = rx.recv() {
        //t2.send(count + 1);
        //t2.send(network::Message::Data(Vec::new()));
        t2.send(network::handler::Message::Data(v.clone()));
        //t2.send(network::Message::Data(&v[..]));
      }
    }
  });

  tx
}


pub fn storage_test() {
  if let Some(mut st) = Storage::create("pouet.txt") {
    //println!("data: {:?}", st.data);
    println!("{:?}", str::from_utf8(st.read(0, 2).unwrap()));
    println!("{:?}", str::from_utf8(st.read(0, 10).unwrap()));
    println!("{:?}", str::from_utf8(st.read(5, 10).unwrap()));
    println!("{:?}", str::from_utf8(st.read(10, 4).unwrap()));
    if let Some(a) = st.read(4090, 30) {
      println!("{:?}", str::from_utf8(a));
    } else {
      println!("less than 4097 bytes");
    }

    let s:&[u8] = b"pouet";
    st.write(2, s);
    println!("{:?}", str::from_utf8(st.read(0, 10).unwrap()));
    st.write(4095, s);
    println!("grow");
    //st.grow();
    println!("{:?}", str::from_utf8(st.read(0, 2).unwrap()));
    println!("{:?}", str::from_utf8(st.read(0, 10).unwrap()));
    println!("{:?}", str::from_utf8(st.read(5, 10).unwrap()));
    println!("{:?}", str::from_utf8(st.read(4090, 4).unwrap()));
    println!("{:?}", str::from_utf8(st.read(4090, 30).unwrap()));
  }
}
