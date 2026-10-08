use std::collections::HashMap;
use std::pin::pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use anyhow::{Context as _, Result, anyhow};
use futures::{Stream, StreamExt as _};
use omnia_wasi_messaging::{HandleError, Handler, Message, Metadata};
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{Headers, OwnedMessage};
use rdkafka::{ClientContext, Message as _};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};
use tokio::task::JoinSet;

use crate::registry::Registry;
use crate::tracker::Tracker;

// The most records this backend hands the host at once. The runtime's
// instance pool is `POOL_TOTAL_CORE_INSTANCES` (1000); 64 stays well under it.
const IN_FLIGHT: usize = 64;

// Total records pulled and not yet done, in flight or waiting behind their
// key. A memory ceiling for a stream with fewer distinct keys than slots,
// where `IN_FLIGHT` is never reached and nothing else would stop the pull.
// It must sit well above a few partition fetches (64 KiB each), or it trips
// on an ordinary stream and the pull stops with most slots idle.
const BACKLOG: usize = 4096;

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
    // librdkafka commits stored offsets on revoke itself. A record of a
    // revoked partition still waiting its turn is skipped when it comes.
    fn pre_rebalance(&self, _base: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        let mut tracker = self.shared.tracker.lock().expect("tracker");
        match rebalance {
            Rebalance::Revoke(partitions) => {
                for element in partitions.elements() {
                    tracker.revoke(element.topic(), element.partition());
                }
            }
            Rebalance::Assign(partitions) => {
                for element in partitions.elements() {
                    tracker.assign(element.topic(), element.partition());
                }
            }
            Rebalance::Error(_) => {}
        }
    }
}

// What the producer's delivery reports and the consumer's rebalances share
// with the delivery loop.
pub struct Shared {
    tracker: Mutex<Tracker>,
    sends: AtomicU64,
    consumer: OnceLock<Weak<KafkaConsumer>>,
}

impl Shared {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            tracker: Mutex::new(Tracker::default()),
            sends: AtomicU64::new(0),
            consumer: OnceLock::new(),
        })
    }

    pub fn set_consumer(&self, consumer: &Arc<KafkaConsumer>) {
        let _ = self.consumer.set(Arc::downgrade(consumer));
    }

    pub fn next_send_id(&self) -> u64 {
        self.sends.fetch_add(1, Ordering::AcqRel)
    }

    pub fn note_delivered(&self, id: u64) {
        let stored = self.tracker.lock().expect("tracker").note_delivered(id);
        self.store_offsets(&stored);
    }

    fn insert(&self, record: &OwnedMessage) {
        self.tracker.lock().expect("tracker").insert(
            record.topic(),
            record.partition(),
            record.offset(),
        );
    }

    fn holds(&self, record: &OwnedMessage) -> bool {
        self.tracker.lock().expect("tracker").holds(
            record.topic(),
            record.partition(),
            record.offset(),
        )
    }

    fn done(&self, record: &OwnedMessage, sends_before: u64) {
        let stored = self.tracker.lock().expect("tracker").done(
            record.topic(),
            record.partition(),
            record.offset(),
            sends_before,
        );
        self.store_offsets(&stored);
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
}

// The consumer's records, detached from librdkafka's buffer. A fatal error,
// after which librdkafka delivers nothing more, ends the stream with it; a
// transient one librdkafka retries itself is logged and skipped.
pub fn records(consumer: &KafkaConsumer) -> impl Stream<Item = Result<OwnedMessage>> + '_ {
    consumer.stream().filter_map(move |pulled| async move {
        match pulled {
            Ok(message) => Some(Ok(message.detach())),
            Err(KafkaError::MessageConsumptionFatal(code)) => {
                let detail = consumer
                    .client()
                    .fatal_error()
                    .map_or_else(|| code.to_string(), |(fatal, text)| format!("{fatal}: {text}"));
                Some(Err(anyhow!("fatal kafka consumer error: {detail}")))
            }
            Err(error) => {
                tracing::warn!("kafka consumer error: {error}");
                None
            }
        }
    })
}

// Deliver `records` to `handler` until the stream ends or a record fails.
//
// At most `IN_FLIGHT` records are with the handler at once, and records
// sharing a key go one at a time in pull order. A record is done, and its
// offset eligible to commit, once the handler returns `Ok` and every send
// issued before that is delivered. A failed outcome ends the loop: the
// record was never done, so a restart redelivers it from the committed
// offset.
pub async fn run(
    records: impl Stream<Item = Result<OwnedMessage>>, shared: Arc<Shared>,
    registry: Option<Registry>, handler: Arc<dyn Handler>,
) -> Result<()> {
    let delivery = Arc::new(Delivery {
        shared,
        registry,
        handler,
        slots: Arc::new(Semaphore::new(IN_FLIGHT)),
        backlog: Arc::new(Semaphore::new(BACKLOG)),
        lanes: Mutex::new(Lanes::default()),
    });
    let mut records = pin!(records);
    let mut tasks = JoinSet::new();
    loop {
        // a place and a slot before the pull, so nothing is pulled that
        // cannot wait or run
        let place = take(&delivery.backlog).await;
        let slot = take(&delivery.slots).await;
        // a finished record is reaped before the next pull, so a failure
        // ends the loop without one more record going out
        let record = tokio::select! {
            biased;
            Some(finished) = tasks.join_next() => {
                finished.context("delivery task panicked")??;
                continue;
            }
            pulled = records.next() => match pulled {
                Some(record) => record?,
                None => break,
            },
        };
        delivery.admit(record, slot, place, &mut tasks);
    }
    while let Some(finished) = tasks.join_next().await {
        finished.context("delivery task panicked")??;
    }
    Ok(())
}

async fn take(semaphore: &Arc<Semaphore>) -> OwnedSemaphorePermit {
    Arc::clone(semaphore).acquire_owned().await.expect("the semaphore is never closed")
}

// The delivery loop's state: what every record's task reads, takes, and
// releases.
struct Delivery {
    shared: Arc<Shared>,
    registry: Option<Registry>,
    handler: Arc<dyn Handler>,
    slots: Arc<Semaphore>,
    backlog: Arc<Semaphore>,
    lanes: Mutex<Lanes>,
}

impl Delivery {
    fn admit(
        self: &Arc<Self>, record: OwnedMessage, slot: OwnedSemaphorePermit,
        place: OwnedSemaphorePermit, tasks: &mut JoinSet<Result<()>>,
    ) {
        // the tracker holds the offset's place before anything can wait on
        // it, so a later offset cannot resolve past it
        self.shared.insert(&record);
        let lane = record.key().map(|key| self.lanes.lock().expect("lanes").enqueue(key.to_vec()));
        // a record behind its key waits without a slot and takes one when
        // its turn comes
        let slot = lane.as_ref().is_none_or(Lane::is_head).then_some(slot);
        tasks.spawn(Arc::clone(self).deliver(record, lane, slot, place));
    }

    async fn deliver(
        self: Arc<Self>, record: OwnedMessage, mut lane: Option<Lane>,
        slot: Option<OwnedSemaphorePermit>, place: OwnedSemaphorePermit,
    ) -> Result<()> {
        if let Some(lane) = &mut lane {
            lane.turn().await;
        }
        let slot = match slot {
            Some(slot) => slot,
            None => take(&self.slots).await,
        };
        self.report();

        let outcome = self.handle(&record).await;

        // the slot goes before the lane, so the record behind can take it
        drop(slot);
        if let Some(lane) = lane {
            self.lanes.lock().expect("lanes").release(&lane);
        }
        drop(place);
        self.report();

        outcome.with_context(|| {
            format!(
                "record {}[{}]@{} failed; ending the consumer",
                record.topic(),
                record.partition(),
                record.offset()
            )
        })
    }

    // `Ok` for a record of a partition revoked while it waited: it is
    // redelivered from the committed offset, to whichever consumer holds
    // the partition now.
    async fn handle(&self, record: &OwnedMessage) -> Result<(), HandleError> {
        if !self.shared.holds(record) {
            return Ok(());
        }
        let raw = record.payload().unwrap_or_default();
        let payload = match &self.registry {
            Some(registry) => registry.decode(record.topic(), raw).await,
            None => raw.to_vec(),
        };
        self.handler.handle(message(record, payload)).await?;
        // every send numbered below this went out before the guest returned;
        // the offset waits on them all
        let sends_before = self.shared.sends.load(Ordering::Acquire);
        self.shared.done(record, sends_before);
        Ok(())
    }

    // `kafka_in_flight` pinned at `IN_FLIGHT` under lag means the slot count
    // is the limit; `kafka_parked` climbing while `kafka_in_flight` sits low
    // means the stream has fewer keys than slots, or one partition's fetch is
    // crowding the others out.
    fn report(&self) {
        let in_flight = IN_FLIGHT - self.slots.available_permits();
        let pulled = BACKLOG - self.backlog.available_permits();
        tracing::trace!(
            gauge.kafka_in_flight = in_flight as u64,
            gauge.kafka_parked = pulled.saturating_sub(in_flight) as u64,
        );
    }
}

// Per-key FIFO. A record waits on the record for its key pulled before it,
// and on nothing else; the order is fixed at the pull, under one lock, so
// two records for a key can never swap. A lane is forgotten once its last
// record is done.
#[derive(Default)]
struct Lanes {
    tails: HashMap<Vec<u8>, (u64, oneshot::Receiver<()>)>,
    next: u64,
}

struct Lane {
    key: Vec<u8>,
    seq: u64,
    // the predecessor's completion; `None` at the head of the lane
    behind: Option<oneshot::Receiver<()>>,
    // dropped when this record is done, waking the record behind
    _done: oneshot::Sender<()>,
}

impl Lanes {
    fn enqueue(&mut self, key: Vec<u8>) -> Lane {
        let seq = self.next;
        self.next += 1;
        let (done, wakes_next) = oneshot::channel();
        let behind = self.tails.insert(key.clone(), (seq, wakes_next)).map(|(_, behind)| behind);
        Lane {
            key,
            seq,
            behind,
            _done: done,
        }
    }

    fn release(&mut self, lane: &Lane) {
        if self.tails.get(&lane.key).is_some_and(|(tail, _)| *tail == lane.seq) {
            self.tails.remove(&lane.key);
        }
    }
}

impl Lane {
    const fn is_head(&self) -> bool {
        self.behind.is_none()
    }

    // Resolves once the record ahead is done, however it finished.
    async fn turn(&mut self) {
        if let Some(behind) = self.behind.take() {
            let _ = behind.await;
        }
    }
}

fn message(record: &OwnedMessage, payload: Vec<u8>) -> Message {
    let mut metadata = Metadata::new();
    if let Some(headers) = record.headers() {
        for header in headers.iter() {
            let bytes = header.value.unwrap_or_default();
            metadata.insert(header.key.to_owned(), String::from_utf8_lossy(bytes).into_owned());
        }
    }
    if let Some(key) = record.key() {
        metadata.insert("key".to_owned(), String::from_utf8_lossy(key).into_owned());
    }
    metadata.insert("partition".to_owned(), record.partition().to_string());
    metadata.insert("offset".to_owned(), record.offset().to_string());
    if let Some(millis) = record.timestamp().to_millis() {
        metadata.insert("timestamp".to_owned(), millis.to_string());
    }

    let mut message = Message::new(payload);
    record.topic().clone_into(&mut message.topic);
    message.metadata = Some(metadata);
    message
}

// The loop and the lanes over scripted records and a scripted handler,
// without a broker; a real group commit is `tests/live.rs::at_least_once`.
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::FutureExt as _;
    use futures::future::BoxFuture;
    use rdkafka::Timestamp;
    use tokio::sync::mpsc;
    use tokio::task::JoinHandle;

    use super::*;

    // Hands every delivery to the test with the sender its outcome goes back
    // through; a delivery the test never decides stays with the handler.
    struct Recorder(mpsc::UnboundedSender<Delivered>);

    #[derive(Debug)]
    struct Delivered {
        message: Message,
        outcome: oneshot::Sender<Result<(), HandleError>>,
    }

    impl Handler for Recorder {
        fn handle(&self, message: Message) -> BoxFuture<'static, Result<(), HandleError>> {
            let (outcome, decided) = oneshot::channel();
            let handed = self.0.send(Delivered { message, outcome });
            async move {
                handed.expect("the test is receiving");
                match decided.await {
                    Ok(outcome) => outcome,
                    Err(_undecided) => std::future::pending().await,
                }
            }
            .boxed()
        }
    }

    impl Delivered {
        fn offset(&self) -> i64 {
            let metadata = self.message.metadata.as_ref().expect("metadata");
            metadata.get("offset").expect("offset").parse().expect("offset")
        }

        fn complete(self, outcome: Result<(), HandleError>) {
            self.outcome.send(outcome).expect("the loop is waiting");
        }
    }

    struct Scripted {
        records: mpsc::UnboundedSender<Result<OwnedMessage>>,
        delivered: mpsc::UnboundedReceiver<Delivered>,
        shared: Arc<Shared>,
        run: JoinHandle<Result<()>>,
    }

    impl Scripted {
        fn start() -> Self {
            let (records, pulled) = mpsc::unbounded_channel();
            let (handler, delivered) = mpsc::unbounded_channel();
            let shared = Shared::new();
            let pulled = futures::stream::unfold(pulled, |mut pulled| async move {
                pulled.recv().await.map(|record| (record, pulled))
            });
            let run =
                tokio::spawn(run(pulled, Arc::clone(&shared), None, Arc::new(Recorder(handler))));
            Self {
                records,
                delivered,
                shared,
                run,
            }
        }

        fn pull(&self, key: Option<&[u8]>, partition: i32, offset: i64) {
            let record = OwnedMessage::new(
                Some(offset.to_string().into_bytes()),
                key.map(<[u8]>::to_vec),
                "t".to_owned(),
                Timestamp::NotAvailable,
                partition,
                offset,
                None,
            );
            self.records.send(Ok(record)).expect("the loop is pulling");
        }

        async fn delivered(&mut self) -> Delivered {
            tokio::time::timeout(Duration::from_secs(5), self.delivered.recv())
                .await
                .expect("a record is delivered")
                .expect("the handler is alive")
        }

        async fn nothing_delivered(&mut self) {
            let waited =
                tokio::time::timeout(Duration::from_millis(100), self.delivered.recv()).await;
            let delivered = waited.ok().flatten().map(|delivered| delivered.message);
            assert!(delivered.is_none(), "unexpected delivery: {delivered:?}");
        }

        // A record stores its offset in the turn after its outcome is
        // decided; the yield gives it that turn.
        async fn stored_offset(&self, partition: i32) -> Option<i64> {
            tokio::task::yield_now().await;
            self.shared.tracker.lock().expect("tracker").stored_offset("t", partition)
        }

        fn revoke(&self, partition: i32) {
            self.shared.tracker.lock().expect("tracker").revoke("t", partition);
        }
    }

    #[tokio::test]
    async fn keys_wait_only_on_themselves() {
        let mut scripted = Scripted::start();
        scripted.pull(Some(b"a"), 0, 0);
        scripted.pull(Some(b"b"), 0, 1);
        scripted.pull(Some(b"a"), 0, 2);

        let first = scripted.delivered().await;
        let second = scripted.delivered().await;
        assert_eq!([first.offset(), second.offset()], [0, 1]);
        scripted.nothing_delivered().await;

        second.complete(Ok(()));
        scripted.nothing_delivered().await;

        first.complete(Ok(()));
        assert_eq!(scripted.delivered().await.offset(), 2);
    }

    #[tokio::test]
    async fn a_record_without_a_key_never_waits() {
        let mut scripted = Scripted::start();
        scripted.pull(None, 0, 0);
        scripted.pull(None, 0, 1);

        let first = scripted.delivered().await;
        let second = scripted.delivered().await;
        assert_eq!([first.offset(), second.offset()], [0, 1]);
    }

    #[tokio::test]
    async fn the_record_past_the_bound_waits() {
        let bound = i64::try_from(IN_FLIGHT).expect("bound");
        let mut scripted = Scripted::start();
        for offset in 0..=bound {
            scripted.pull(Some(format!("k{offset}").as_bytes()), 0, offset);
        }

        let mut held = Vec::new();
        for _ in 0..IN_FLIGHT {
            held.push(scripted.delivered().await);
        }
        scripted.nothing_delivered().await;

        held.pop().expect("a held record").complete(Ok(()));
        assert_eq!(scripted.delivered().await.offset(), bound);
    }

    #[tokio::test]
    async fn a_waiting_record_holds_the_resolved_offset() {
        let mut scripted = Scripted::start();
        scripted.pull(Some(b"a"), 0, 3);
        scripted.pull(Some(b"a"), 0, 5);
        scripted.pull(Some(b"b"), 0, 6);

        let first = scripted.delivered().await;
        let third = scripted.delivered().await;
        assert_eq!([first.offset(), third.offset()], [3, 6]);

        // 6 is done while 5 still waits behind 3
        third.complete(Ok(()));
        assert_eq!(scripted.stored_offset(0).await, None);

        first.complete(Ok(()));
        let second = scripted.delivered().await;
        assert_eq!(second.offset(), 5);
        assert_eq!(scripted.stored_offset(0).await, Some(3));

        second.complete(Ok(()));
        assert_eq!(scripted.stored_offset(0).await, Some(6));
    }

    #[tokio::test]
    async fn a_failed_outcome_ends_the_loop() {
        let mut scripted = Scripted::start();
        scripted.pull(Some(b"a"), 0, 7);

        scripted.delivered().await.complete(Err(HandleError::Trapped("boom".to_owned())));

        let ended = (&mut scripted.run).await.expect("the loop task joins");
        let error = ended.expect_err("a failed record ends the loop");
        assert!(format!("{error:#}").contains("t[0]@7"), "unexpected error: {error:#}");
        assert_eq!(scripted.stored_offset(0).await, None);
    }

    #[tokio::test]
    async fn a_revoked_partition_is_skipped_but_not_the_rest() {
        let mut scripted = Scripted::start();
        scripted.pull(Some(b"a"), 0, 1);
        scripted.pull(Some(b"a"), 0, 2);
        scripted.pull(Some(b"b"), 1, 3);

        let first = scripted.delivered().await;
        let other = scripted.delivered().await;
        assert_eq!([first.offset(), other.offset()], [1, 3]);

        scripted.revoke(0);
        first.complete(Ok(()));
        scripted.nothing_delivered().await;

        other.complete(Ok(()));
        scripted.pull(Some(b"c"), 1, 4);
        assert_eq!(scripted.delivered().await.offset(), 4);
        assert_eq!(scripted.stored_offset(1).await, Some(3));
        assert_eq!(scripted.stored_offset(0).await, None);
    }

    #[test]
    fn a_lane_is_forgotten_with_its_last_record() {
        let mut lanes = Lanes::default();
        let first = lanes.enqueue(b"a".to_vec());
        let second = lanes.enqueue(b"a".to_vec());
        assert!(first.is_head());
        assert!(!second.is_head());

        lanes.release(&first);
        assert_eq!(lanes.tails.len(), 1, "the lane still has a record behind");
        lanes.release(&second);
        assert!(lanes.tails.is_empty());
    }
}
