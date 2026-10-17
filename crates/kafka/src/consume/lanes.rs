use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::oneshot;

// Per-key FIFO. A record waits on the record for its key pulled before it,
// and on nothing else; the order is fixed at the pull, under one lock, so
// two records for a key can never swap. A lane is forgotten once its last
// record is done.
#[derive(Default)]
pub struct Lanes {
    tails: HashMap<Arc<[u8]>, (u64, oneshot::Receiver<()>)>,
    next: u64,
}

pub struct Lane {
    key: Arc<[u8]>,
    seq: u64,
    // the predecessor's completion; `None` at the head of the lane
    behind: Option<oneshot::Receiver<()>>,
    // dropped when this record is done, waking the record behind
    _done: oneshot::Sender<()>,
}

impl Lanes {
    pub fn enqueue(&mut self, key: Arc<[u8]>) -> Lane {
        let seq = self.next;
        self.next += 1;
        let (done, wakes_next) = oneshot::channel();
        let behind =
            self.tails.insert(Arc::clone(&key), (seq, wakes_next)).map(|(_, behind)| behind);
        Lane {
            key,
            seq,
            behind,
            _done: done,
        }
    }

    pub fn release(&mut self, lane: &Lane) {
        if self.tails.get(&lane.key).is_some_and(|(tail, _)| *tail == lane.seq) {
            self.tails.remove(&lane.key);
        }
    }
}

impl Lane {
    pub const fn is_head(&self) -> bool {
        self.behind.is_none()
    }

    // Resolves once the record ahead is done, however it finished.
    pub async fn turn(&mut self) {
        if let Some(behind) = self.behind.take() {
            let _ = behind.await;
        }
    }
}

// Per-key FIFO lane enqueue and release invariants without a consumer loop.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lane_release() {
        let mut lanes = Lanes::default();
        let first = lanes.enqueue(Arc::from(b"a".as_slice()));
        let second = lanes.enqueue(Arc::from(b"a".as_slice()));
        assert!(first.is_head());
        assert!(!second.is_head());

        lanes.release(&first);
        assert_eq!(lanes.tails.len(), 1, "the lane still has a record behind");
        lanes.release(&second);
        assert!(lanes.tails.is_empty());
    }
}
