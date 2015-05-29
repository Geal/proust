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
use std::sync::mpsc;
use std::sync::mpsc::{channel,Sender,Receiver};
use std::thread::{self,Thread,Builder};
use util::monitor;
use mio;
use network;

pub type Request  = u8;
pub type Response = u8;

pub fn storage(out:&mio::Sender<network::Message>, name: &str) -> Sender<Request> {
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
        t2.send(network::Message::Data(v.clone()));
        //t2.send(network::Message::Data(&v[..]));
      }
    }
  });

  tx
}


pub fn storage_test() {

  let mut file = match OpenOptions::new().read(true).write(true).create(true).open("pouet.txt") {
    Ok(f) => f,
    Err(e) => panic!("file error: {}", e),
  };

  // fills the file with 0
  file.set_len(4000);

  let fd = file.as_raw_fd();
  let mut options: Vec<MapOption> = Vec::new();
  options.push(MapOption::MapWritable);
  options.push(MapOption::MapFd(fd));
  options.push(MapOption::MapOffset(0));
  options.push(MapOption::MapReadable);
  let mut mm = MemoryMap::new(4096, &options[..]).unwrap();
  let mut sl: &mut[u8] = unsafe {from_raw_parts_mut(mm.data(), mm.len())};

  println!("data1:\n{}", str::from_utf8(sl).unwrap());
  let mut d = mm.data();
  unsafe {
    *d = 0x41;
  }
  println!("mmap length: {}", mm.len());
  println!("data2:\n{}", str::from_utf8(sl).unwrap());
  sl[0] = 0x41;
  sl[1] = 0x42;
  println!("data3:\n{}", str::from_utf8(sl).unwrap());
  println!("msync result: {:?}", mm.msync());
  println!("data4:\n{}", str::from_utf8(sl).unwrap());
  //file.sync_all();
  drop(sl);
  drop(file);
}
