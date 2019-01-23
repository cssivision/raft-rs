use std::collections::HashMap;

#[derive(Debug)]
struct Storage {
    kv_store: HashMap<String, Vec<u8>>,
}
