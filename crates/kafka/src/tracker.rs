use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

// One record the host has been given, or that is waiting on an earlier
// record in its partition. `sends_before` is meaningful once `acked` is set:
// every produce numbered below it has to be delivered before the record is
// done.
struct Pending {
    acked: bool,
    sends_before: u64,
}

// The highest offset with every record before it done. Records finish out
// of order (up to the in-flight bound), and librdkafka only commits a higher
// offset than the last one, so a finished-but-gapped offset is remembered
// here until the gap closes.
#[derive(Default)]
pub struct Tracker {
    partitions: HashMap<(String, i32), BTreeMap<i64, Pending>>,
    // every send numbered below this has a delivery report
    delivered: u64,
    out_of_order: BTreeSet<u64>,
    // partitions revoked and not yet reassigned; a late ack for one is ignored
    revoked: HashSet<(String, i32)>,
    stored: HashMap<(String, i32), i64>,
}

impl Tracker {
    pub fn insert(&mut self, topic: &str, partition: i32, offset: i64) {
        let key = (topic.to_owned(), partition);
        if self.revoked.contains(&key) {
            return;
        }
        self.partitions.entry(key).or_default().insert(
            offset,
            Pending {
                acked: false,
                sends_before: 0,
            },
        );
    }

    pub fn ack(
        &mut self, topic: &str, partition: i32, offset: i64, sends_before: u64,
    ) -> Vec<(String, i32, i64)> {
        let key = (topic.to_owned(), partition);
        if let Some(pending) = self.partitions.get_mut(&key)
            && let Some(state) = pending.get_mut(&offset)
        {
            state.acked = true;
            state.sends_before = sends_before;
        }
        self.resolve()
    }

    // A delivery report, including a send that never left the queue: skipping
    // a number would leave `delivered` stuck below it.
    pub fn note_delivered(&mut self, id: u64) -> Vec<(String, i32, i64)> {
        if id < self.delivered {
            return Vec::new();
        }
        if id == self.delivered {
            self.advance();
        } else {
            self.out_of_order.insert(id);
        }
        self.resolve()
    }

    pub fn revoke(&mut self, topic: &str, partition: i32) {
        let key = (topic.to_owned(), partition);
        self.partitions.remove(&key);
        self.revoked.insert(key);
    }

    pub fn assign(&mut self, topic: &str, partition: i32) {
        self.revoked.remove(&(topic.to_owned(), partition));
    }

    #[cfg(test)]
    pub(crate) fn stored_offset(&self, topic: &str, partition: i32) -> Option<i64> {
        self.stored.get(&(topic.to_owned(), partition)).copied()
    }

    fn advance(&mut self) {
        self.delivered += 1;
        while self.out_of_order.remove(&self.delivered) {
            self.delivered += 1;
        }
    }

    fn resolve(&mut self) -> Vec<(String, i32, i64)> {
        let mut stored = Vec::new();
        let keys: Vec<(String, i32)> = self.partitions.keys().cloned().collect();
        for key in keys {
            let (last, empty) = {
                let Some(pending) = self.partitions.get_mut(&key) else {
                    continue;
                };
                let mut last = None;
                while let Some((&offset, state)) = pending.first_key_value() {
                    if state.acked && state.sends_before <= self.delivered {
                        last = Some(offset);
                        pending.pop_first();
                    } else {
                        break;
                    }
                }
                (last, pending.is_empty())
            };
            if let Some(offset) = last {
                self.stored.insert(key.clone(), offset);
                stored.push((key.0.clone(), key.1, offset));
            }
            if empty {
                self.partitions.remove(&key);
            }
        }
        stored
    }
}

// Resolved offsets when records finish out of order, with no broker. The
// gate is `dispatch`'s tests; a real group commit is `tests/live.rs`.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn out_of_order_acks_store_the_resolved_offset() {
        let mut tracker = Tracker::default();
        tracker.insert("t", 3, 100);
        tracker.insert("t", 3, 101);
        tracker.insert("t", 3, 102);

        assert_eq!(tracker.ack("t", 3, 101, 0), []);
        assert_eq!(tracker.stored.get(&("t".to_owned(), 3)), None);

        let stored = tracker.ack("t", 3, 100, 0);
        assert_eq!(stored, vec![("t".to_owned(), 3, 101)]);

        let stored = tracker.ack("t", 3, 102, 0);
        assert_eq!(stored, vec![("t".to_owned(), 3, 102)]);
    }

    #[test]
    fn record_acked_before_its_sends_are_delivered() {
        let mut tracker = Tracker::default();
        tracker.insert("t", 0, 7);
        assert_eq!(tracker.ack("t", 0, 7, 2), []);

        assert_eq!(tracker.note_delivered(0), []);
        assert_eq!(tracker.note_delivered(1), vec![("t".to_owned(), 0, 7)]);
    }

    #[test]
    fn revoked_partition_ignores_a_late_ack() {
        let mut tracker = Tracker::default();
        tracker.insert("t", 1, 5);
        tracker.revoke("t", 1);
        assert_eq!(tracker.ack("t", 1, 5, 0), []);
        assert!(tracker.stored.is_empty());
    }

    #[test]
    fn enqueue_failure_does_not_stall_delivered() {
        let mut tracker = Tracker::default();
        assert_eq!(tracker.note_delivered(1), []);
        assert_eq!(tracker.delivered, 0);

        // the send numbered 0 never queued, and reports itself here
        assert_eq!(tracker.note_delivered(0), []);
        assert_eq!(tracker.delivered, 2);

        tracker.insert("t", 0, 10);
        assert_eq!(tracker.ack("t", 0, 10, 2), vec![("t".to_owned(), 0, 10)]);
    }
}
