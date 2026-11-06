// Keyed sends land on the partitions the other publishers on the cluster use.
// `KafkaJs` reproduces KafkaJS's murmur2, which runs on JS Numbers — f64
// arithmetic with 32-bit bitwise operators — rather than librdkafka's.
// `Java` is Kafka's `Utils.murmur2`, wrapping 32-bit integers throughout.
#[derive(Clone, Copy)]
pub struct Partitioner {
    count: i32,
    scheme: PartitionerScheme,
}

impl Partitioner {
    #[must_use]
    pub const fn new(count: i32, scheme: PartitionerScheme) -> Self {
        Self { count, scheme }
    }

    #[must_use]
    pub fn partition(self, key: &[u8]) -> i32 {
        match self.scheme {
            PartitionerScheme::KafkaJs => self.kafkajs(key),
            PartitionerScheme::Java => self.java(key),
        }
    }

    // kafkajs/src/producer/partitioners/default/partitioner.js (v1.15.0)
    fn kafkajs(self, key: &[u8]) -> i32 {
        to_positive(murmur2(key)) % self.count
    }

    // Kafka's `DefaultPartitioner`: `Utils.toPositive(murmur2(key)) % count`
    fn java(self, key: &[u8]) -> i32 {
        (java_murmur2(key) & i32::MAX) % self.count
    }
}

/// Key-to-partition scheme for sends that do not set `metadata["partition"]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PartitionerScheme {
    /// `KafkaJS` murmur2, matching the Node and .NET producers.
    #[default]
    KafkaJs,
    /// Kafka's `Utils.murmur2`, matching the Java client.
    Java,
}

// Kafka's `Utils.murmur2`: wrapping `u32` arithmetic throughout.
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    reason = "Java Utils.murmur2 wraps the byte length and the hash through i32"
)]
fn java_murmur2(data: &[u8]) -> i32 {
    const SEED: u32 = 0x9747_b28c;
    const M: u32 = 0x5bd1_e995;
    const R: u32 = 24;

    let length = data.len() as u32;
    let mut hash = SEED ^ length;
    let mut index = 0;
    while index + 4 <= data.len() {
        let mut chunk =
            u32::from_le_bytes([data[index], data[index + 1], data[index + 2], data[index + 3]]);
        chunk = chunk.wrapping_mul(M);
        chunk ^= chunk >> R;
        chunk = chunk.wrapping_mul(M);
        hash = hash.wrapping_mul(M);
        hash ^= chunk;
        index += 4;
    }

    let rest = data.len() - index;
    if rest >= 3 {
        hash ^= u32::from(data[index + 2]) << 16;
    }
    if rest >= 2 {
        hash ^= u32::from(data[index + 1]) << 8;
    }
    if rest >= 1 {
        hash ^= u32::from(data[index]);
        hash = hash.wrapping_mul(M);
    }

    hash ^= hash >> 13;
    hash = hash.wrapping_mul(M);
    hash ^= hash >> 15;
    hash as i32
}

// kafkajs/src/producer/partitioners/default/murmur2.js (v1.15.0)
fn murmur2(key: &[u8]) -> f64 {
    const SEED: f64 = 0x9747_B28C_u32 as f64;
    const M: f64 = 0x5BD1_E995_u32 as f64;
    const R: u32 = 24;

    let len = key.len();

    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_possible_wrap,
        reason = "JS ToInt32 of the key length"
    )]
    let length = len as i32;
    let mut h = f(i(SEED) ^ length);

    // every chunk, the short last one zero-padded: KafkaJS's loop bound
    // `i < length / 4` is not floored
    for chunk in key.chunks(4) {
        let mut bytes = [0u8; 4];
        bytes[..chunk.len()].copy_from_slice(chunk);

        let mut k = f(i32::from_le_bytes(bytes));
        k *= M;
        k = js_xor(k, js_rshift(k, R));
        k *= M;
        h *= M;
        h = js_xor(h, k);
    }

    // the trailing one to three bytes, a second time
    let tail = len & !3;
    if len % 4 >= 3 {
        h = js_xor(h, f(i32::from(key[tail + 2]) << 16));
    }
    if len % 4 >= 2 {
        h = js_xor(h, f(i32::from(key[tail + 1]) << 8));
    }
    if len % 4 >= 1 {
        h = js_xor(h, f(i32::from(key[tail])));
        h *= M;
    }

    h = js_xor(h, js_rshift(h, 13));
    h *= M;
    h = js_xor(h, js_rshift(h, 15));

    h
}

// KafkaJS's `toPositive`: `x & 0x7fffffff`.
const fn to_positive(x: f64) -> i32 {
    i(x) & i32::MAX
}

// JS's ToInt32: what a Number becomes on either side of a bitwise operator.
#[expect(clippy::cast_possible_truncation, reason = "ToInt32 truncates, then wraps modulo 2^32")]
const fn i(f: f64) -> i32 {
    (f as i64) as i32
}

// The i32 a bitwise operator produced, back to a Number.
fn f(i: i32) -> f64 {
    f64::from(i)
}

// JS's `>>>`: the shift runs on the Number's ToUint32.
fn js_rshift(f: f64, shift: u32) -> f64 {
    f64::from(i(f).cast_unsigned() >> shift)
}

// JS's `^`.
fn js_xor(a: f64, b: f64) -> f64 {
    f(i(a) ^ i(b))
}
