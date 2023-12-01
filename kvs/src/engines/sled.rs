use std::path;

use super::KvsEngine;
use crate::Result;

#[derive(Clone)]
pub struct SledKvsEngine {
    db: sled::Db,
}

impl SledKvsEngine {
    pub fn open(p: &path::Path) -> Result<Self> {
        let db = sled::open(p);
        match db {
            Ok(db) => Ok(SledKvsEngine { db }),
            Err(e) => Err(Box::new(e)),
        }
    }
}

impl KvsEngine for SledKvsEngine {
    fn set(&self, key: String, value: String) -> Result<()> {
        // println!("set key: {} value: {}",key,value);
        let x = self.db.insert(key, value.as_bytes());
        self.db.flush().unwrap();
        match x {
            Ok(_) => Ok(()),
            Err(e) => Err(Box::new(e)),
        }
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        // println!("get key: {}",key);
        let x = self.db.get(key);
        match x {
            Ok(v) => match v {
                Some(value) => {
                    let v2 = String::from_utf8(value.to_vec());
                    match v2 {
                        Ok(v3) => Ok(Some(v3)),
                        Err(e) => Err(Box::new(e)),
                    }
                }
                None => Ok(None),
            },
            Err(e) => Err(Box::new(e)),
        }
    }

    fn remove(&self, key: String) -> Result<()> {
        // println!("rm key: {}",key);
        let x = self.db.remove(key);
        self.db.flush().unwrap();
        match x {
            Ok(v) => match v {
                Some(_) => Ok(()),
                None => Err("Key not found".into()),
            },
            Err(e) => Err(Box::new(e)),
        }
    }
}
