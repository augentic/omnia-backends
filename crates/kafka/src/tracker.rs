use std::collections::{BTreeMap, BTreeSet};

// A resolved offset as `store_offset` takes it: topic, partition, offset.
pub type Stored = (String, i32, i64);

// One record the host has been given, or that is waiting on an earlier
// record in its partition. `sends_before` is meaningful once `done` is set:
// every produce numbered below it has to be delivered before the record is
// resolved.
#[derive(Default)]
struct Pending {
    done: bool,
    sends_before: usize,
}

impl Pending {
    const fn resolved(&self, delivered: usize) -> bool {
        self.done && self.sends_before <= delivered
    }
}

// A partition's state, kept from its first record or assignment for the
// life of the tracker. A revoke empties it rather than removing it, so a
// record pulled before the revoke is still dropped when it comes.
#[derive(Default)]
struct Partition {
    revoked: bool,
    pending: BTreeMap<i64, Pending>,
    stored: Option<i64>,
}

impl Partition {
    // The leading records that are done with their sends delivered leave;
    // the last of them is the new resolved offset.
    fn resolve(&mut self, delivered: usize) -> Option<i64> {
        let mut last = None;
        while let Some(entry) = self.pending.first_entry()
            && entry.get().resolved(delivered)
        {
            last = Some(*entry.key());
            entry.remove();
        }
        if last.is_some() {
            self.stored = last;
        }
        last
    }
}

// The highest offset with every record before it done. Records finish out
// of order (up to the in-flight bound), and librdkafka only commits a higher
// offset than the last one, so a finished-but-gapped offset is remembered
// here until the gap closes.
#[derive(Default)]
pub struct Tracker {
    topics: BTreeMap<String, BTreeMap<i32, Partition>>,
    // every send numbered below this has a delivery report
    delivered: usize,
    out_of_order: BTreeSet<usize>,
}

impl Tracker {
    pub fn insert(&mut self, topic: &str, partition: i32, offset: i64) {
        let state = self.partition_or_insert(topic, partition);
        if !state.revoked {
            state.pending.insert(offset, Pending::default());
        }
    }

    // Whether the record is still this consumer's to deliver. A revoke
    // between the pull and the delivery drops it.
    pub fn holds(&self, topic: &str, partition: i32, offset: i64) -> bool {
        self.partition(topic, partition).is_some_and(|state| state.pending.contains_key(&offset))
    }

    // Only the record's own partition can resolve here: the delivered
    // watermark has not moved.
    pub fn done(
        &mut self, topic: &str, partition: i32, offset: i64, sends_before: usize,
    ) -> Option<Stored> {
        let delivered = self.delivered;
        let state = self.partition_mut(topic, partition)?;
        if let Some(pending) = state.pending.get_mut(&offset) {
            pending.done = true;
            pending.sends_before = sends_before;
        }
        state.resolve(delivered).map(|offset| (topic.to_owned(), partition, offset))
    }

    // A delivery report, including a send that never left the queue: skipping
    // a number would leave `delivered` stuck below it. Only a report that
    // moves the watermark can resolve a record.
    pub fn note_delivered(&mut self, id: usize) -> Vec<Stored> {
        if id < self.delivered {
            return Vec::new();
        }
        if id > self.delivered {
            self.out_of_order.insert(id);
            return Vec::new();
        }
        self.advance();
        self.resolve_all()
    }

    pub fn revoke(&mut self, topic: &str, partition: i32) {
        *self.partition_or_insert(topic, partition) = Partition {
            revoked: true,
            ..Partition::default()
        };
    }

    pub fn assign(&mut self, topic: &str, partition: i32) {
        self.partition_or_insert(topic, partition).revoked = false;
    }

    #[cfg(test)]
    pub(crate) fn stored_offset(&self, topic: &str, partition: i32) -> Option<i64> {
        self.partition(topic, partition).and_then(|state| state.stored)
    }

    fn partition(&self, topic: &str, partition: i32) -> Option<&Partition> {
        self.topics.get(topic)?.get(&partition)
    }

    fn partition_mut(&mut self, topic: &str, partition: i32) -> Option<&mut Partition> {
        self.topics.get_mut(topic)?.get_mut(&partition)
    }

    // The topic's name is allocated once, at its first partition.
    fn partition_or_insert(&mut self, topic: &str, partition: i32) -> &mut Partition {
        if !self.topics.contains_key(topic) {
            self.topics.insert(topic.to_owned(), BTreeMap::new());
        }
        self.topics.get_mut(topic).expect("inserted above").entry(partition).or_default()
    }

    fn advance(&mut self) {
        self.delivered += 1;
        while self.out_of_order.remove(&self.delivered) {
            self.delivered += 1;
        }
    }

    fn resolve_all(&mut self) -> Vec<Stored> {
        let delivered = self.delivered;
        let mut stored = Vec::new();
        for (topic, partitions) in &mut self.topics {
            for (&partition, state) in partitions.iter_mut() {
                if let Some(offset) = state.resolve(delivered) {
                    stored.push((topic.clone(), partition, offset));
                }
            }
        }
        stored
    }
}

// Resolved offsets when records finish out of order, with no broker. The
// loop over it is `consume`'s tests; a real group commit is `tests/live.rs`.
#[cfg(test)]
mod tests {
    use super::*;

    fn stored(topic: &str, partition: i32, offset: i64) -> Stored {
        (topic.to_owned(), partition, offset)
    }

    #[test]
    fn out_of_order_completions_store_the_resolved_offset() {
        let mut tracker = Tracker::default();
        tracker.insert("t", 3, 100);
        tracker.insert("t", 3, 101);
        tracker.insert("t", 3, 102);

        assert_eq!(tracker.done("t", 3, 101, 0), None);
        assert_eq!(tracker.stored_offset("t", 3), None);

        assert_eq!(tracker.done("t", 3, 100, 0), Some(stored("t", 3, 101)));
        assert_eq!(tracker.done("t", 3, 102, 0), Some(stored("t", 3, 102)));
    }

    #[test]
    fn record_done_before_its_sends_are_delivered() {
        let mut tracker = Tracker::default();
        tracker.insert("t", 0, 7);
        assert_eq!(tracker.done("t", 0, 7, 2), None);

        assert_eq!(tracker.note_delivered(0), []);
        assert_eq!(tracker.note_delivered(1), [stored("t", 0, 7)]);
    }

    #[test]
    fn one_delivery_awaited_by_two_partitions() {
        let mut tracker = Tracker::default();
        tracker.insert("t", 0, 1);
        tracker.insert("u", 2, 9);
        assert_eq!(tracker.done("t", 0, 1, 1), None);
        assert_eq!(tracker.done("u", 2, 9, 1), None);

        assert_eq!(tracker.note_delivered(0), [stored("t", 0, 1), stored("u", 2, 9)]);
    }

    #[test]
    fn revoked_partition_drops_its_records() {
        let mut tracker = Tracker::default();
        tracker.insert("t", 1, 5);
        assert!(tracker.holds("t", 1, 5));

        tracker.revoke("t", 1);
        assert!(!tracker.holds("t", 1, 5));
        assert_eq!(tracker.done("t", 1, 5, 0), None);
        assert_eq!(tracker.stored_offset("t", 1), None);

        // a record pulled before the reassignment is still dropped
        tracker.insert("t", 1, 6);
        assert!(!tracker.holds("t", 1, 6));
        tracker.assign("t", 1);
        tracker.insert("t", 1, 7);
        assert!(tracker.holds("t", 1, 7));
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
        assert_eq!(tracker.done("t", 0, 10, 2), Some(stored("t", 0, 10)));
    }
}
