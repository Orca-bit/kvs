use std::collections::HashMap;

pub struct KvStore {
    map: HashMap<String, String>,
}

impl KvStore {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    pub fn set(&mut self, key: String, value: String) {
        self.map.insert(key, value);
    }

    pub fn get(&self, key: String) -> Option<String> {
        if let Some(value) = self.map.get(&key) {
            Some(value.clone())
        } else {
            None
        }
    }

    pub fn remove(&mut self, key: String) {
        self.map.remove(&key);
    }
}
