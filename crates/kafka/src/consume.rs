use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use omnia_wasi_messaging::{Ack, Message, Metadata};
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{Headers, OwnedMessage};
use rdkafka::{ClientContext, Message as _};
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;

use crate::registry::Registry;
use crate::tracker::Tracker;

// The most records this backend hands the host at once. The runtime's
// instance pool is `POOL_TOTAL_CORE_INSTANCES` (1000); 64 stays well under it.
const IN_FLIGHT: usize = 64;

// Total records pulled and waiting behind a key already in flight. A memory
// ceiling for a stream with fewer distinct keys than slots, where
// `IN_FLIGHT` is never reached and nothing else would stop the pull. It
// must sit well above a few partition fetches (64 KiB each), or it trips on
// an ordinary stream and the pull stops with most slots idle.
const PARKED: usize = 4096;

pub type KafkaConsumer = StreamConsumer<KafkaContext>;

pub struct KafkaContext {
    shared: Arc<Shared>,
}

impl KafkaContext {
    pub const fn new(shared: Arc<Shared>) -> Self {
        Self { shared }
    }
}

impl ClientContext for KafkaContext {}

impl ConsumerContext for KafkaContext {
    // Gate and tracker are held together on revoke: an admit between the two
    // locks could park a record whose tracker entry the revoke then deletes.
    #[expect(
        clippy::significant_drop_tightening,
        reason = "gate and tracker stay locked together across the revoke"
    )]
    fn pre_rebalance(&self, _base: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        match rebalance {
            // librdkafka commits stored offsets on revoke itself. Gate before
            // tracker, the same order as an ack.
            Rebalance::Revoke(partitions) => {
                let mut gate = self.shared.gate.lock().expect("gate");
                let mut tracker = self.shared.tracker.lock().expect("tracker");
                for element in partitions.elements() {
                    gate.drop_parked(element.topic(), element.partition());
                    tracker.revoke(element.topic(), element.partition());
                }
            }
            Rebalance::Assign(partitions) => {
                let mut tracker = self.shared.tracker.lock().expect("tracker");
                for element in partitions.elements() {
                    tracker.assign(element.topic(), element.partition());
                }
            }
            Rebalance::Error(_) => {}
        }
    }
}

// A record the forwarder has decoded and not yet handed to the host.
struct Incoming {
    message: Message,
    key: Option<Vec<u8>>,
    topic: String,
    partition: i32,
    offset: i64,
}

#[derive(Default)]
struct Gate {
    in_flight: usize,
    keys: HashSet<Vec<u8>>,
    parked: HashMap<Vec<u8>, VecDeque<Incoming>>,
    parked_count: usize,
    ended: bool,
}

impl Gate {
    const fn can_pull(&self) -> bool {
        !self.ended && self.in_flight < IN_FLIGHT && self.parked_count < PARKED
    }

    // `Some` is yielded. `None` is parked behind the key already in flight.
    fn admit(&mut self, record: Incoming) -> Option<Incoming> {
        if let Some(key) = record.key.as_deref()
            && self.keys.contains(key)
        {
            self.parked.entry(key.to_vec()).or_default().push_back(record);
            self.parked_count += 1;
            self.report();
            return None;
        }
        if let Some(key) = record.key.clone() {
            self.keys.insert(key);
        }
        self.in_flight += 1;
        self.report();
        Some(record)
    }

    // The next parked record for `key`, now in flight in the released one's place.
    fn release(&mut self, key: Option<&[u8]>) -> Option<Incoming> {
        self.in_flight = self.in_flight.saturating_sub(1);
        let next = key.and_then(|key| {
            self.keys.remove(key);
            let next = self.parked.get_mut(key).and_then(VecDeque::pop_front);
            if self.parked.get(key).is_some_and(VecDeque::is_empty) {
                self.parked.remove(key);
            }
            if next.is_some() {
                self.parked_count -= 1;
                self.keys.insert(key.to_vec());
                self.in_flight += 1;
            }
            next
        });
        self.report();
        next
    }

    // A revoked partition is redelivered from the committed offset. The record
    // already with the host stays; the ones still queued behind it do not.
    // No `KafkaAck` yet — the token is attached in `hand_off` — so dropping
    // these while the gate lock is held cannot re-enter it from `KafkaAck::drop`.
    fn drop_parked(&mut self, topic: &str, partition: i32) {
        self.parked.retain(|_, queue| {
            let before = queue.len();
            queue.retain(|record| record.topic != topic || record.partition != partition);
            self.parked_count -= before - queue.len();
            !queue.is_empty()
        });
        self.report();
    }

    fn release_slot(&mut self, key: Option<&[u8]>) {
        self.in_flight = self.in_flight.saturating_sub(1);
        if let Some(key) = key {
            self.keys.remove(key);
        }
        self.report();
    }

    // `kafka_in_flight` pinned at `IN_FLIGHT` under lag means the slot count
    // is the limit; `kafka_parked` climbing while `kafka_in_flight` sits low
    // means the stream has fewer keys than slots, or one partition's fetch is
    // crowding the others out.
    fn report(&self) {
        tracing::trace!(
            gauge.kafka_in_flight = self.in_flight as u64,
            gauge.kafka_parked = self.parked_count as u64,
        );
    }
}

pub struct Shared {
    gate: Mutex<Gate>,
    tracker: Mutex<Tracker>,
    notify: Notify,
    outbound: Mutex<Option<mpsc::Sender<Message>>>,
    sends: AtomicU64,
    stop: AtomicBool,
    consumer: OnceLock<Weak<KafkaConsumer>>,
}

impl Shared {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            gate: Mutex::new(Gate::default()),
            tracker: Mutex::new(Tracker::default()),
            notify: Notify::new(),
            outbound: Mutex::new(None),
            sends: AtomicU64::new(0),
            stop: AtomicBool::new(false),
            consumer: OnceLock::new(),
        })
    }

    pub fn set_consumer(&self, consumer: &Arc<KafkaConsumer>) {
        let _ = self.consumer.set(Arc::downgrade(consumer));
    }

    pub fn install_sender(&self, sender: mpsc::Sender<Message>) {
        *self.outbound.lock().expect("outbound") = Some(sender);
    }

    pub fn next_send_id(&self) -> u64 {
        self.sends.fetch_add(1, Ordering::AcqRel)
    }

    // `false` when the forwarder should stop: the subscription ended, or the
    // client is being dropped.
    pub async fn wait_for_capacity(self: &Arc<Self>) -> bool {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            if self.stop.load(Ordering::Acquire) {
                return false;
            }
            {
                let gate = self.gate.lock().expect("gate");
                if gate.ended {
                    return false;
                }
                if gate.can_pull() {
                    return true;
                }
            }
            notified.await;
        }
    }

    pub fn note_delivered(&self, id: u64) {
        let stored = self.tracker.lock().expect("tracker").note_delivered(id);
        self.store_offsets(&stored);
    }

    fn admit(self: &Arc<Self>, record: Incoming) {
        if self.stop.load(Ordering::Acquire) || self.gate.lock().expect("gate").ended {
            return;
        }
        // before the gate parks it, so a later offset cannot resolve past it
        self.tracker.lock().expect("tracker").insert(
            &record.topic,
            record.partition,
            record.offset,
        );
        let yielded = {
            let mut gate = self.gate.lock().expect("gate");
            if gate.ended || self.stop.load(Ordering::Acquire) {
                return;
            }
            gate.admit(record)
        };
        if let Some(record) = yielded {
            self.hand_off(record);
        }
    }

    pub fn stop(&self) {
        self.stop.store(true, Ordering::Release);
        self.notify.notify_one();
    }

    fn on_ack(self: &Arc<Self>, topic: &str, partition: i32, offset: i64, key: Option<&[u8]>) {
        let sends_before = self.sends.load(Ordering::Acquire);
        let next = {
            let mut gate = self.gate.lock().expect("gate");
            if gate.ended {
                gate.release_slot(key);
                None
            } else {
                gate.release(key)
            }
        };
        let stored =
            self.tracker.lock().expect("tracker").ack(topic, partition, offset, sends_before);
        self.store_offsets(&stored);
        if let Some(record) = next {
            self.hand_off(record);
        }
        self.notify.notify_one();
    }

    fn on_unacked(&self, topic: &str, partition: i32, offset: i64, key: Option<&[u8]>) {
        // shutdown drops every outstanding token; that is not a host failure
        if self.stop.load(Ordering::Acquire) {
            self.gate.lock().expect("gate").release_slot(key);
            return;
        }
        let mut gate = self.gate.lock().expect("gate");
        gate.release_slot(key);
        // the channel is already closed, so this token is one of its leftovers
        if gate.ended {
            return;
        }
        tracing::info!(monotonic_counter.unacked_messages = 1, topic = %topic, partition, offset);
        tracing::error!(
            "host dropped record {topic}[{partition}]@{offset} without running a guest; ending \
             the subscription"
        );
        gate.ended = true;
        drop(gate);
        self.outbound.lock().expect("outbound").take();
        self.notify.notify_one();
    }

    fn hand_off(self: &Arc<Self>, mut record: Incoming) {
        record.message.ack = Some(Arc::new(KafkaAck {
            shared: Arc::clone(self),
            topic: record.topic.clone(),
            partition: record.partition,
            offset: record.offset,
            key: record.key.clone(),
            called: AtomicBool::new(false),
        }));
        let Some(sender) = self.outbound.lock().expect("outbound").clone() else {
            return;
        };
        match sender.try_send(record.message) {
            Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => {}
            Err(error) => {
                debug_assert!(false, "in-flight bound keeps the subscribe channel from filling");
                tracing::error!("failed to send message to subscriber: {error}");
            }
        }
    }

    fn store_offsets(&self, stored: &[(String, i32, i64)]) {
        if stored.is_empty() {
            return;
        }
        let Some(consumer) = self.consumer.get().and_then(Weak::upgrade) else {
            return;
        };
        for (topic, partition, offset) in stored {
            // `store_offset` records `offset + 1` itself
            if let Err(error) = consumer.store_offset(topic, *partition, *offset) {
                tracing::warn!("storing offset {topic}[{partition}]@{offset}: {error}");
            }
        }
    }

    fn end(&self) {
        self.gate.lock().expect("gate").ended = true;
        self.outbound.lock().expect("outbound").take();
        self.notify.notify_one();
    }
}

struct KafkaAck {
    shared: Arc<Shared>,
    topic: String,
    partition: i32,
    offset: i64,
    key: Option<Vec<u8>>,
    called: AtomicBool,
}

impl std::fmt::Debug for KafkaAck {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaAck")
            .field("topic", &self.topic)
            .field("partition", &self.partition)
            .field("offset", &self.offset)
            .finish_non_exhaustive()
    }
}

impl Ack for KafkaAck {
    fn ack(&self) {
        if self.called.swap(true, Ordering::AcqRel) {
            return;
        }
        self.shared.on_ack(&self.topic, self.partition, self.offset, self.key.as_deref());
    }
}

impl Drop for KafkaAck {
    fn drop(&mut self) {
        if !self.called.load(Ordering::Acquire) {
            self.shared.on_unacked(&self.topic, self.partition, self.offset, self.key.as_deref());
        }
    }
}

pub fn spawn(
    shared: Arc<Shared>, consumer: Arc<KafkaConsumer>, registry: Option<Registry>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            if !shared.wait_for_capacity().await {
                break;
            }
            let notified = shared.notify.notified();
            tokio::pin!(notified);
            tokio::select! {
                () = &mut notified => {
                    if shared.stop.load(Ordering::Acquire) || shared.gate.lock().expect("gate").ended {
                        break;
                    }
                }
                result = consumer.recv() => {
                    if !handle(&shared, &consumer, registry.as_ref(), result).await {
                        break;
                    }
                }
            }
        }
    })
}

async fn handle(
    shared: &Arc<Shared>, consumer: &KafkaConsumer, registry: Option<&Registry>,
    result: Result<rdkafka::message::BorrowedMessage<'_>, KafkaError>,
) -> bool {
    match result {
        Ok(message) => {
            let raw = message.payload().unwrap_or_default().to_vec();
            let topic = message.topic().to_owned();
            let payload = match registry {
                Some(registry) => registry.decode(&topic, &raw).await,
                None => raw,
            };
            let owned = message.detach();
            shared.admit(incoming(&owned, payload));
            true
        }
        Err(KafkaError::MessageConsumptionFatal(code)) => {
            // librdkafka will not deliver another record on this consumer
            match consumer.client().fatal_error() {
                Some((fatal, text)) => {
                    tracing::error!(
                        "fatal kafka consumer error ({code}): {fatal}: {text}; ending the \
                         subscription"
                    );
                }
                None => {
                    tracing::error!("fatal kafka consumer error ({code}); ending the subscription");
                }
            }
            shared.end();
            false
        }
        Err(error) => {
            tracing::warn!("kafka consumer error: {error}");
            true
        }
    }
}

fn incoming(message: &OwnedMessage, payload: Vec<u8>) -> Incoming {
    let mut inner = HashMap::new();
    if let Some(headers) = message.headers() {
        for header in headers.iter() {
            let bytes = header.value.unwrap_or_default();
            inner.insert(header.key.to_owned(), String::from_utf8_lossy(bytes).into_owned());
        }
    }
    let key = message.key().map(<[u8]>::to_vec);
    if let Some(key) = &key {
        inner.insert("key".to_owned(), String::from_utf8_lossy(key).into_owned());
    }
    let partition = message.partition();
    let offset = message.offset();
    inner.insert("partition".to_owned(), partition.to_string());
    inner.insert("offset".to_owned(), offset.to_string());
    if let Some(millis) = message.timestamp().to_millis() {
        inner.insert("timestamp".to_owned(), millis.to_string());
    }

    let mut out = Message::new(payload);
    message.topic().clone_into(&mut out.topic);
    out.metadata = Some(Metadata { inner });
    Incoming {
        message: out,
        key,
        topic: message.topic().to_owned(),
        partition,
        offset,
    }
}

// The gate and the ack token, without a broker. A parked record holding the
// resolved offset is tested here with the tracker; a real group commit is
// `tests/live.rs::at_least_once`.
#[cfg(test)]
mod tests {
    use tokio::sync::mpsc::error::TryRecvError;

    use super::*;

    fn sample(key: Option<&[u8]>, offset: i64) -> Incoming {
        Incoming {
            message: Message::new(vec![]),
            key: key.map(<[u8]>::to_vec),
            topic: "t".to_owned(),
            partition: 0,
            offset,
        }
    }

    #[test]
    fn keys_wait_only_on_themselves() {
        let mut gate = Gate::default();
        assert_eq!(gate.admit(sample(Some(b"a"), 0)).expect("yield a").offset, 0);
        assert_eq!(gate.admit(sample(Some(b"b"), 0)).expect("yield b").offset, 0);
        assert!(gate.admit(sample(Some(b"a"), 1)).is_none());
        assert!(gate.release(Some(b"b")).is_none());
        assert_eq!(gate.release(Some(b"a")).expect("parked a").offset, 1);
    }

    #[test]
    fn a_record_without_a_key_is_never_parked() {
        let mut gate = Gate::default();
        assert!(gate.admit(sample(None, 0)).is_some());
        assert!(gate.admit(sample(None, 1)).is_some());
        assert_eq!(gate.parked_count, 0);
        assert!(gate.release(None).is_none());
    }

    #[test]
    fn the_record_past_the_bound_waits_for_an_ack() {
        let mut gate = Gate::default();
        for index in 0..IN_FLIGHT {
            let key = format!("k{index}");
            let offset = i64::try_from(index).expect("index");
            assert!(gate.admit(sample(Some(key.as_bytes()), offset)).is_some());
        }
        assert!(!gate.can_pull());
        assert!(gate.release(Some(b"k0")).is_none());
        assert!(gate.can_pull());
        assert!(gate.admit(sample(Some(b"extra"), 1_000)).is_some());
        assert!(!gate.can_pull());
    }

    // One partition's fetch when a topic has few distinct keys: a dozen keys
    // repeated across hundreds of records. The repeats park; they must not
    // stop the pull while most in-flight slots are still free.
    #[test]
    fn repeated_keys_do_not_stop_the_pull() {
        let mut gate = Gate::default();
        for offset in 0..1_200i64 {
            let key = format!("v{}", offset % 12);
            gate.admit(sample(Some(key.as_bytes()), offset));
        }
        assert_eq!(gate.in_flight, 12);
        assert_eq!(gate.parked_count, 1_188);
        assert!(gate.can_pull());
    }

    #[test]
    fn dropped_token_ends_the_subscription() {
        let shared = Shared::new();
        let (sender, mut receiver) = mpsc::channel(8);
        shared.install_sender(sender);
        shared.admit(sample(Some(b"k"), 0));

        let mut message = receiver.try_recv().expect("yielded");
        let ack = message.ack.take().expect("token");
        drop(message);
        drop(ack);

        assert!(matches!(receiver.try_recv(), Err(TryRecvError::Disconnected)));
        assert!(shared.gate.lock().expect("gate").ended);
    }

    #[test]
    fn a_parked_record_holds_the_resolved_offset() {
        let shared = Shared::new();
        let (sender, mut receiver) = mpsc::channel(8);
        shared.install_sender(sender);

        shared.admit(marked(b"a", 3));
        shared.admit(marked(b"a", 5));
        shared.admit(marked(b"b", 6));

        let mut first = receiver.try_recv().expect("yielded");
        let mut second = receiver.try_recv().expect("yielded");
        assert!(matches!(receiver.try_recv(), Err(TryRecvError::Empty)));

        // 6 is acked while 5 is still parked behind 3
        ack_offset(&mut first, &mut second, 6);
        assert_eq!(shared.tracker.lock().expect("tracker").stored_offset("t", 0), None);

        ack_offset(&mut first, &mut second, 3);
        assert_eq!(shared.tracker.lock().expect("tracker").stored_offset("t", 0), Some(3));

        let parked = receiver.try_recv().expect("parked 5");
        assert_eq!(parked.payload, b"5");
        assert!(parked.ack.is_some());
    }

    fn marked(key: &[u8], offset: i64) -> Incoming {
        let mut record = sample(Some(key), offset);
        record.message.payload = offset.to_string().into_bytes();
        record
    }

    fn ack_offset(first: &mut Message, second: &mut Message, offset: i64) {
        let message = if first.payload == offset.to_string().into_bytes() { first } else { second };
        message.ack.take().expect("token").ack();
    }

    #[test]
    fn revoking_a_partition_drops_its_parked_records() {
        let mut gate = Gate::default();
        assert!(gate.admit(sample(Some(b"a"), 1)).is_some());
        assert!(gate.admit(sample(Some(b"a"), 2)).is_none());
        gate.drop_parked("t", 0);
        assert_eq!(gate.parked_count, 0);
        assert!(gate.release(Some(b"a")).is_none());
    }
}
