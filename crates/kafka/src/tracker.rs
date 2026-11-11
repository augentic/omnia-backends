use std::collections::{BTreeMap, BTreeSet};

// A resolved offset as `store_offset` takes it: topic, partition, offset.
pub type Stored = (String, i32, i64);

// One pull of a record. A revoke drops a pull in flight, and the offset comes
// back as a new pull once the partition is assigned again; the earlier pull's
// completion must not stand in for the new one, so the two are told apart by
// ticket rather than by offset.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Ticket(u64);

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
    tickets: u64,
}

impl Tracker {
    pub fn insert(&mut self, topic: &str, partition: i32, offset: i64) -> Ticket {
        let ticket = Ticket(self.tickets);
        self.tickets += 1;
        let state = self.partition_or_insert(topic, partition);
        if !state.revoked {
            state.pending.insert(
                offset,
                Pending {
                    ticket,
                    done: false,
                    sends_before: 0,
                },
            );
        }
        ticket
    }

    // Whether the record is still this consumer's to deliver. A revoke
    // between the pull and the delivery drops it, and a redelivery of its
    // offset pulled since does not stand in for it.
    pub fn holds(&self, topic: &str, partition: i32, offset: i64, ticket: Ticket) -> bool {
        self.partition(topic, partition)
            .and_then(|state| state.pending.get(&offset))
            .is_some_and(|pending| pending.ticket == ticket)
    }

    // Only the record's own partition can resolve here: the delivered
    // watermark has not moved.
    pub fn done(
        &mut self, topic: &str, partition: i32, offset: i64, ticket: Ticket, sends_before: usize,
    ) -> Option<Stored> {
        let delivered = self.delivered;
        let state = self.partition_mut(topic, partition)?;
        let pending = state.pending.get_mut(&offset).filter(|pending| pending.ticket == ticket)?;
        pending.done = true;
        pending.sends_before = sends_before;
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

// A partition's state, kept from its first record or assignment for the
// life of the tracker. A revoke empties it rather than removing it, so a
// record pulled before the revoke is still dropped when it comes.
#[derive(Default)]
struct Partition {
    revoked: bool,
    pending: BTreeMap<i64, Pending>,
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
        last
    }
}

// One record the host has been given, or that is waiting on an earlier
// record in its partition. `sends_before` is meaningful once `done` is set:
// every produce numbered below it has to be delivered before the record is
// resolved.
struct Pending {
    ticket: Ticket,
    done: bool,
    sends_before: usize,
}

impl Pending {
    const fn resolved(&self, delivered: usize) -> bool {
        self.done && self.sends_before <= delivered
    }
}
