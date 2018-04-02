use storage::Storage;

pub struct TopicConfig {

}

pub struct Topic<'a> {
  pub name: String,
  pub storage: Storage<'a>,
  pub offset: usize,
}

impl <'a>Topic<'a> {

  pub fn new(name: String) -> Option<Self> {
    Storage::create("test")
      .and_then(|storage|
        Some(Self {
          name,
          storage,
          offset: 0,
        })
      )
  }
}