use super::ThreadPool;
use crate::Result;
pub struct SharedQueueThreadPool {}
impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized,
    {
        todo!()
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        todo!()
    }
}
