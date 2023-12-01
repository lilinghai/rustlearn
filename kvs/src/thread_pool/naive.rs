use std::thread;

use super::ThreadPool;
use crate::Result;
pub struct NaiveThreadPool {}
impl ThreadPool for NaiveThreadPool {
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized,
    {
        return Ok(NaiveThreadPool{})
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        thread::spawn(job);
    }
}
