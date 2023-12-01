use crate::Result;
use serde::{Deserialize, Serialize};

mod kvs;
mod sled;
pub use self::kvs::KvStore;
pub use self::kvs::LOGFILENAM;
pub use self::sled::SledKvsEngine;

pub trait KvsEngine:   Send + 'static {
    fn set(&self, key: String, value: String) -> Result<()>;

    fn get(&self, key: String) -> Result<Option<String>>;

    fn remove(&self, key: String) -> Result<()>;
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Command {
    Get(String),
    Rm(String),
    Set(String, String),
}

#[cfg(test)]
mod test {

    fn t1(a: &String) -> &String {
        let b = String::from("b");
        // cannot return reference to local variable `b` returns a reference to data owned by the current function
        // b
        a
    }

    fn t2<'a>(s: &'a String) -> &'a String {
        s
    }

    // missing lifetime specifier
    // this function's return type contains a borrowed value,
    // but the signature does not say whether it is borrowed from `s1` or `s2`
    // fn t3(s1: &String, s2: &String) -> &String {
    //     s1
    // }

    fn t4<'a>(s1: &'a String, s2: &'a String) -> &'a String {
        s2
    }

    fn t5() {
        // missing type for `const` or `static`
        // const 和 static 变量需要显示声明类型
        static mut b: i32 = 10;
        const c: i32 = 30;
        // mutable 的 static 变量需要 unsafe 来使用
        unsafe { b = 10 };
        unsafe {
            println!("{}", b);
        }
        static d: i32 = 10;
        println!("{}", d);
    }

    #[test]
    fn lifecycle() {}
}
