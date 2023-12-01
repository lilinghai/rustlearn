mod engines;
pub mod thread_pool;

pub use self::engines::*;

use std::error::Error;
// std::result::Result 是 preinclude 到项目中的，为了防止歧义显示制定了 package
// 给 result 起别名，主要用户同一个包内部相同 Err<T> 类型多次使用场景
// 为了防止歧义，type Result<T> 可以换个名称，如 type KVResult<T>
pub type Result<T> = std::result::Result<T, Box<dyn Error>>;
