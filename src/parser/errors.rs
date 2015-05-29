pub enum InputError {
  ParserError,
  NotImplemented,
  InvalidRequestSize,
  InvalidMessageSize,
  InvalidMessageSetSize,
  InvalidMessage
}

impl InputError {
  #[inline]
  pub fn to_int(&self) -> u32 {
    match *self {
      InputError::ParserError           => 1,
      InputError::NotImplemented        => 2,
      InputError::InvalidRequestSize    => 3,
      InputError::InvalidMessageSetSize => 4,
      InputError::InvalidMessageSize    => 5,
      InputError::InvalidMessage        => 6
    }
  }
}

#[inline]
pub fn from_int(code: u32) -> Option<InputError> {
  match code {
    1 => Option::Some(InputError::ParserError),
    2 => Option::Some(InputError::NotImplemented),
    3 => Option::Some(InputError::InvalidRequestSize),
    4 => Option::Some(InputError::InvalidMessageSetSize),
    5 => Option::Some(InputError::InvalidMessageSize),
    6 => Option::Some(InputError::InvalidMessage),
    _ => Option::None
  }
}

