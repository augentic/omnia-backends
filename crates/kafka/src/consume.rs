mod lanes;

use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use anyhow::{Context as _, Result, anyhow};
use futures::{Stream, StreamExt as _};
use omnia_wasi_messaging::{HandleError, Handler, Message, Metadata};
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{Headers, OwnedMessage};
use rdkafka::{ClientContext, Message as _};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;

use self::lanes::{Lane, Lanes};
use crate::registry::Registry;
use crate::tracker::{Stored, Ticket, Tracker};

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
    sends: AtomicUsize,
    consumer: OnceLock<Weak<KafkaConsumer>>,
}

impl Shared {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            tracker: Mutex::new(Tracker::default()),
            sends: AtomicUsize::new(0),
            consumer: OnceLock::new(),
        })
    }

    pub fn set_consumer(&self, consumer: &Arc<KafkaConsumer>) {
        let _ = self.consumer.set(Arc::downgrade(consumer));
    }

    pub fn next_send_id(&self) -> SendId<'_> {
        SendId {
            shared: self,
            id: self.sends.fetch_add(1, Ordering::AcqRel),
            queued: false,
        }
    }

    pub fn note_delivered(&self, id: usize) {
        let stored = self.tracker.lock().expect("tracker").note_delivered(id);
        self.store_offsets(&stored);
    }

    fn insert(&self, record: &OwnedMessage) -> Ticket {
        self.tracker.lock().expect("tracker").insert(
            record.topic(),
            record.partition(),
            record.offset(),
        )
    }

    fn holds(&self, record: &OwnedMessage, ticket: Ticket) -> bool {
        self.tracker.lock().expect("tracker").holds(
            record.topic(),
            record.partition(),
            record.offset(),
            ticket,
        )
    }

    fn done(&self, record: &OwnedMessage, ticket: Ticket, sends_before: usize) {
        let stored = self.tracker.lock().expect("tracker").done(
            record.topic(),
            record.partition(),
            record.offset(),
            ticket,
            sends_before,
        );
        self.store_offsets(stored.as_slice());
    }

    fn store_offsets(&self, stored: &[Stored]) {
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

// A send's number, reported to the tracker exactly once: by the delivery
// report once the record is queued, or on drop when it was not. A `send`
// dropped in its retry sleep would otherwise skip its number, and the
// watermark would stop below it with no offset able to commit past.
pub struct SendId<'a> {
    shared: &'a Shared,
    id: usize,
    queued: bool,
}

impl SendId<'_> {
    pub const fn get(&self) -> usize {
        self.id
    }

    // The record is queued; the delivery report takes the number over.
    pub fn queued(mut self) {
        self.queued = true;
    }
}

impl Drop for SendId<'_> {
    fn drop(&mut self) {
        if !self.queued {
            self.shared.note_delivered(self.id);
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
    registry: Option<Registry>, topic_prefix: String, handler: Arc<dyn Handler>,
) -> Result<()> {
    let delivery = Arc::new(Delivery {
        shared,
        registry,
        topic_prefix,
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
        tasks.spawn(Arc::clone(&delivery).admit(record, slot, place));
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
    topic_prefix: String,
    handler: Arc<dyn Handler>,
    slots: Arc<Semaphore>,
    backlog: Arc<Semaphore>,
    lanes: Mutex<Lanes>,
}

impl Delivery {
    // The record's delivery, to be spawned: its offset and lane are taken
    // here, before the caller can wait on anything.
    fn admit(
        self: Arc<Self>, record: OwnedMessage, slot: OwnedSemaphorePermit,
        place: OwnedSemaphorePermit,
    ) -> impl Future<Output = Result<()>> + Send {
        // the tracker holds the offset's place before anything can wait on
        // it, so a later offset cannot resolve past it
        let ticket = self.shared.insert(&record);
        let lane =
            record.key().map(|key| self.lanes.lock().expect("lanes").enqueue(Arc::from(key)));
        // a record behind its key waits without a slot and takes one when
        // its turn comes
        let slot = lane.as_ref().is_none_or(Lane::is_head).then_some(slot);
        self.deliver(record, ticket, lane, slot, place)
    }

    async fn deliver(
        self: Arc<Self>, record: OwnedMessage, ticket: Ticket, mut lane: Option<Lane>,
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

        let outcome = self.handle(&record, ticket).await;

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
    // the partition now. `Ok` too for a record no guest could read: it is
    // dropped, logged, counted, and committed past.
    async fn handle(&self, record: &OwnedMessage, ticket: Ticket) -> Result<(), HandleError> {
        if !self.shared.holds(record, ticket) {
            return Ok(());
        }
        let Some(topic) = record.topic().strip_prefix(&self.topic_prefix) else {
            self.done(record, ticket);
            return Ok(());
        };
        let raw = record.payload().unwrap_or_default();
        let payload = match &self.registry {
            Some(registry) => match registry.decode(record.topic(), raw).await {
                Ok(payload) => payload,
                Err(rejection) => {
                    tracing::error!(
                        "record {}[{}]@{} dropped: {rejection}",
                        record.topic(),
                        record.partition(),
                        record.offset()
                    );
                    tracing::info!(
                        monotonic_counter.discarded_messages = 1,
                        topic = record.topic(),
                        reason = rejection.reason()
                    );
                    self.done(record, ticket);
                    return Ok(());
                }
            },
            None => raw.to_vec(),
        };
        self.handler.handle(message(record, topic, payload)).await?;
        self.done(record, ticket);
        Ok(())
    }

    // Every send numbered below the counter went out before the guest
    // returned; the offset waits on them all.
    fn done(&self, record: &OwnedMessage, ticket: Ticket) {
        let sends_before = self.shared.sends.load(Ordering::Acquire);
        self.shared.done(record, ticket, sends_before);
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

// The partition the record was read from travels as `source-partition`:
// `partition` is a send's destination, and a guest forwarding a record's
// metadata must not pin its output to where the record came from.
pub fn message(record: &OwnedMessage, topic: &str, payload: Vec<u8>) -> Message {
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
    metadata.insert("source-partition".to_owned(), record.partition().to_string());
    metadata.insert("offset".to_owned(), record.offset().to_string());
    if let Some(millis) = record.timestamp().to_millis() {
        metadata.insert("timestamp".to_owned(), millis.to_string());
    }

    let mut message = Message::new(payload);
    topic.clone_into(&mut message.topic);
    message.metadata = Some(metadata);
    message
}
