use std::thread::{self,Thread,Builder};

#[cfg(not(feature = "nightly"))]
pub fn monitor<F, R>(f: F, restart_count: u8) -> Result<R, u8>
where F: Fn() -> R + Send + 'static {

  thread::spawn( move || {
    f();
  });
  Err(0)
}

#[cfg(feature = "nightly")]
pub fn monitor<F, R>(f: F, restart_count: u8) -> Result<R, u8>
where F: Fn() -> R + Send + 'static {
  use std::rt::unwind::try;

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

