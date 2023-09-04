use std::collections::HashMap;

pub struct KvStore {
    data: HashMap<String, String>,
}

impl KvStore {
    pub fn new() -> Self {
        let data = HashMap::new();
        KvStore { data }
    }
    pub fn get(&self, key: String) -> Option<String> {
        self.data.get(&key).cloned()
    }
    pub fn set(&mut self, key: String, value: String) {
        self.data.insert(key, value);
    }
    pub fn remove(&mut self, key: String) {
        self.data.remove(&key);
    }
}
