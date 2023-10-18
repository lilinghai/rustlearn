use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fs::{self},
    io::{BufRead, BufReader, Seek, Write},
    os::unix::prelude::FileExt,
    path,
};

pub struct KvStore {
    // key -> log position, content length
    data: HashMap<String, (u64, usize)>,
    // 记录日志文件中 keys 的数量，判断 compaction 的时机
    log_keys: usize,
    wlog: fs::File,
    rlog: fs::File,
    dir: String,
}

// std::result::Result 是 preinclude 到项目中的，为了防止歧义显示制定了 package
// 给 result 起别名，主要用户同一个包内部相同 Err<T> 类型多次使用场景
// 为了防止歧义，type Result<T> 可以换个名称，如 type KVResult<T>
pub type Result<T> = std::result::Result<T, Box<dyn Error>>;

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Rm(String),
    Set(String, String),
}

// log file 分隔符号
const DELIMITER: u8 = b'#';
const COMPACTION_RATIO: usize = 3;
const COMPACTION_KEYS: usize = 100;
const LOGFILENAM: &str = "kvs.log";

// 打开一个已有的之前写入过的日志文件，读需要重建内存表，写需要正确记录新的起始位置
impl KvStore {
    pub fn open(p: &path::Path) -> Result<Self> {
        let wf: fs::File = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(p.join(LOGFILENAM).to_str().unwrap())?;
        let mut rf: fs::File = fs::OpenOptions::new()
            .read(true)
            .open(p.join(LOGFILENAM).to_str().unwrap())?;
        let mut data = HashMap::new();
        let mut log_keys = 0;
        let mut r = BufReader::new(&rf);
        loop {
            let mut buf = vec![];
            let pos = r.stream_position()?;
            let s = r.read_until(DELIMITER, &mut buf).unwrap();
            // println!("buf {}, len {}", String::from_utf8(buf.to_vec()).unwrap(),s);
            if s == 0 {
                break;
            }
            buf.pop();
            log_keys += 1;
            let c: Command = serde_json::from_slice(&buf)?;
            if let Command::Set(k, _value) = c {
                data.insert(k, (pos, s));
            } else if let Command::Rm(k) = c {
                data.remove(&k);
            }
        }
        rf.rewind().unwrap();

        Ok(KvStore {
            data,
            wlog: wf,
            rlog: rf,
            log_keys,
            dir: p.to_str().unwrap().to_owned(),
        })
    }
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.data.get(&key) {
            Some(v) => {
                // println!("debug get key {}", key);
                let mut buf: Vec<u8> = vec![0; v.1];
                self.rlog.read_exact_at(&mut buf, v.0)?;
                // rm delimiter
                buf.pop();
                let c: Command = serde_json::from_slice(&buf)?;
                // println!("debug store key {:#?},pos {}, len{}", c, v.0, v.1);

                if let Command::Set(k, value) = c {
                    assert_eq!(key, k);
                    Ok(Some(value))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let sc = Command::Set(key.clone(), value);
        let mut s = serde_json::to_string(&sc)?;
        s.push(char::from_u32(DELIMITER.into()).unwrap());
        let b = s.as_bytes();
        let len = b.len();
        self.wlog.write_all(b)?;
        let pos = self.wlog.stream_position()?;
        self.data
            .insert(key, (pos - u64::try_from(len).unwrap(), len));
        self.log_keys += 1;
        self.compaction();
        Ok(())
    }
    pub fn remove(&mut self, key: String) -> Result<()> {
        match self.data.remove(&key) {
            Some(_) => {
                let sc = Command::Rm(key.clone());
                let mut s = serde_json::to_string(&sc)?;
                s.push(char::from_u32(DELIMITER.into()).unwrap());
                let b = s.as_bytes();
                self.wlog.write_all(b)?;
                self.log_keys += 1;
                self.compaction();
                Ok(())
            }
            None => Err("Key not found".into()),
        }
    }

    // 新开辟一个文件写入
    fn compaction(&mut self) {
        if self.log_keys / (self.data.len() + 1) < COMPACTION_RATIO
            || self.log_keys < COMPACTION_KEYS
        {
            return;
        }
        println!(
            "start compaction [log keys] {} , [real keys] {}",
            self.log_keys,
            self.data.len()
        );
        let mut wf: fs::File = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(self.dir.to_owned() + "/" + LOGFILENAM + "1")
            .unwrap();
        for v in self.data.values_mut() {
            let mut buf: Vec<u8> = vec![0; v.1];
            self.rlog.read_exact_at(&mut buf, v.0).unwrap();
            v.0 = wf.stream_position().unwrap();
            wf.write_all(&buf).unwrap();
        }
        fs::rename(
            self.dir.to_owned() + "/" + LOGFILENAM + "1",
            self.dir.to_owned() + "/" + LOGFILENAM,
        )
        .unwrap();
        self.wlog = wf;

        self.rlog = fs::OpenOptions::new()
            .read(true)
            .open(self.dir.to_owned() + "/" + LOGFILENAM)
            .unwrap();

        self.log_keys = self.data.len();
    }

    // 遍历 log
    // 如果是 set，data 中存在，且没有写入过，读取开始的位置和写入的位置一样，不需要重新写入，同时往前推进写入位置
    // 如果是 set，data 中存在，且没有写入过，读取开始的位置和写入的位置不一样，需要重新写入，同时往前推进写入位置
    // 如果是 set，data 中存在，已经写入过，不需要往前推进写入位置
    // 如果是 set，且 data 不存在，写入的位置不推进
    // 如果是 dm，写入位置不推进
    // 最后 truncate 一下日志
    // 放弃该办法，会出现同样 key 修改，最新的得不到写入的情况
    fn compaction2(&mut self) {
        if self.log_keys / (self.data.len() + 1) < COMPACTION_RATIO
            || self.log_keys < COMPACTION_KEYS
        {
            return;
        }
        println!(
            "start compaction [log keys] {} , [real keys] {}",
            self.log_keys,
            self.data.len()
        );
        // self.rlog.rewind().unwrap();
        // let mut buffer = String::new();
        // self.rlog.read_to_string(&mut buffer).unwrap();
        // println!("content {}",buffer);

        self.rlog.rewind().unwrap();
        let mut r = BufReader::new(&self.rlog);
        let mut flags = HashSet::new();
        let mut write_pos = 0;
        loop {
            let mut buf = vec![];
            let read_pos = r.stream_position().unwrap();
            let s = r.read_until(DELIMITER, &mut buf).unwrap();
            if s == 0 {
                break;
            }
            let l = buf.len();
            buf.pop();
            let c: Command = serde_json::from_slice(&buf).unwrap();
            if let Command::Set(k, _value) = c {
                let sv = self.data.get(&k);
                match sv {
                    Some(_) => {
                        let f = flags.insert(k);
                        // 没有写入过
                        if f {
                            if read_pos != write_pos {
                                buf.push(DELIMITER);
                                self.wlog.write_all_at(&buf, write_pos).unwrap();
                            }
                            write_pos += u64::try_from(l).unwrap()
                        }
                    }
                    None => {}
                }
            }
        }
        self.wlog.set_len(write_pos).unwrap();
        self.log_keys = flags.len();
        // self.rlog.rewind().unwrap();
        // let mut buffer = String::new();
        // self.rlog.read_to_string(&mut buffer).unwrap();
        // println!("content {}",buffer);
    }
}

#[cfg(test)]
mod tests {
    // use super::*;
    use core::time;
    use std::{
        fs,
        io::{BufRead, BufReader, Read, Seek, Write},
        thread,
    };

    use tempfile::TempDir;

    // #[test]
    // fn write3(){
    //     let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    //     let mut store = KvStore::open(temp_dir.path()).unwrap();
    //     for key_id in 0..1000 {
    //         let key = format!("key{}", key_id);
    //         let value = format!("{}", iter);
    //         store.set(key, value)?;
    //     }
    // }

    #[test]
    fn tmp_path() {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let p = temp_dir.path();
        let pb = p.join("kvs");
        match pb.to_str() {
            Some(s) => println!("path is {}", s),
            None => todo!(),
        };
        thread::sleep(time::Duration::from_secs(1));
        println!("is file {}", pb.is_file());
        // assert_eq!(pb.to_str(), Some("bad"));
    }

    #[test]
    fn writec() {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let p = temp_dir.path();
        let mut f = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(p.join("kvs").to_str().unwrap())
            .unwrap();
        f.write_all(b"dfdf").unwrap();
        f.sync_all().unwrap();
        let mut buf = String::new();
        f.read_to_string(&mut buf).unwrap();
        println!("path {}", p.join("kvs").to_str().unwrap());
        println!("read {}", buf);
    }

    #[test]
    fn read_write() {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let p = temp_dir.path();
        let mut f = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(p.join("kvs").to_str().unwrap())
            .unwrap();
        f.write_all(b"ab_fe_").unwrap();
        f.sync_all().unwrap();
        f.rewind().unwrap();
        let mut _buffer = [0; 10];

        // read exactly 10 bytes
        // f.read_exact(&mut buffer).unwrap();
        // println!("buf {}", String::from_utf8(buffer.to_vec()).unwrap());

        f.rewind().unwrap();
        let mut r = BufReader::new(f);
        loop {
            let mut buf = vec![];
            let s = r.read_until(b'_', &mut buf).unwrap();
            if s != 0 {
                buf.pop();
            }
            println!("buf {}, len {}", String::from_utf8(buf).unwrap(), s);
            if s == 0 {
                break;
            }
        }
    }

    #[test]
    fn write2c() {
        let mut f = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open("/tmp/2")
            .unwrap();
        println!("pos1 {}", f.stream_position().unwrap());
        f.write_all(b"zzzxxxdfdf").unwrap();
        println!("pos1.1 {}", f.stream_position().unwrap());

        let mut buf = String::new();
        f.rewind().unwrap();
        f.read_to_string(&mut buf).unwrap();
        println!("read {}", buf);
        println!("pos {}", f.stream_position().unwrap());

        f.rewind().unwrap();
        println!("pos2 {}", f.stream_position().unwrap());

        // f.set_len(40).unwrap();
        // f.write_all_at(b"buffer", 20);
        f.write_all(b"begin").unwrap();
        println!("pos3 {}", f.stream_position().unwrap());
    }
}
