// KafkaJS's default partitioner, so keyed sends land on the partitions the
// Node publishers on the same cluster use. KafkaJS hashes with a murmur2 run
// on JS Numbers — f64 arithmetic with 32-bit bitwise operators — which is not
// librdkafka's murmur2, so the arithmetic below reproduces JS rather than
// calling a murmur2 crate.
#[derive(Clone)]
pub struct Partitioner {
    count: i32,
}

impl Partitioner {
    #[must_use]
    pub const fn new(count: i32) -> Self {
        Self { count }
    }

    // kafkajs/src/producer/partitioners/default/partitioner.js (v1.15.0)
    #[must_use]
    pub fn partition(&self, key: &[u8]) -> i32 {
        to_positive(murmur2(key)) % self.count
    }
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

// KafkaJS's own murmur2 vectors and partitions observed on the cluster. A
// real broker landing keyed sends on them is `tests/live.rs::keyed_sends`.
#[cfg(test)]
mod tests {

    use serde::Deserialize;

    use super::*;

    struct TestCase {
        key: Vec<u8>,
        expected: i32,
    }

    #[test]
    fn murmur2_vectors() {
        let cases = vec![
            // kafkajs/src/producer/partitioners/default/murmur2.spec.js (v1.15.0)
            TestCase {
                key: b"0".to_vec(),
                expected: 272_173_970,
            },
            TestCase {
                key: b"1".to_vec(),
                expected: 1_311_020_360,
            },
            TestCase {
                key: b"128".to_vec(),
                expected: 2_053_105_854,
            },
            TestCase {
                key: b"2187".to_vec(),
                expected: -2_081_355_488,
            },
            TestCase {
                key: b"16384".to_vec(),
                expected: 204_404_061,
            },
            TestCase {
                key: b"78125".to_vec(),
                expected: -677_491_393,
            },
            TestCase {
                key: b"279936".to_vec(),
                expected: -622_460_209,
            },
            TestCase {
                key: b"823543".to_vec(),
                expected: 651_276_451,
            },
            TestCase {
                key: b"2097152".to_vec(),
                expected: 944_683_677,
            },
            TestCase {
                key: b"4782969".to_vec(),
                expected: -892_695_770,
            },
            TestCase {
                key: b"10000000".to_vec(),
                expected: -1_778_616_326,
            },
            TestCase {
                key: b"19487171".to_vec(),
                expected: -518_311_627,
            },
            TestCase {
                key: b"35831808".to_vec(),
                expected: 556_972_389,
            },
            TestCase {
                key: b"62748517".to_vec(),
                expected: -233_806_557,
            },
            TestCase {
                key: b"105413504".to_vec(),
                expected: -109_398_538,
            },
            TestCase {
                key: b"170859375".to_vec(),
                expected: 102_939_717,
            },
            // a trip id, as published
            TestCase {
                key: b"1039-36302-36840-2-9f138052".to_vec(),
                expected: 1_289_839_555,
            },
        ];

        for case in cases {
            let got = i(murmur2(&case.key));
            assert_eq!(
                got,
                case.expected,
                "murmur2({}) = {}, expected {}",
                String::from_utf8_lossy(&case.key),
                got,
                case.expected
            );
        }
    }

    #[test]
    fn partitioning() {
        let cases = vec![
            // keys and partitions observed on the cluster
            TestCase {
                key: b"1039-36302-36840-2-9f138052".to_vec(),
                expected: 7,
            },
            TestCase {
                key: b"1388-20023-36900-2-c9985f66".to_vec(),
                expected: 7,
            },
            TestCase {
                key: b"1175-03505-36600-2-126dd9ca".to_vec(),
                expected: 7,
            },
            TestCase {
                key: b"1011-98208-36480-2-9eef750a".to_vec(),
                expected: 7,
            },
            TestCase {
                key: b"233-75504-36900-2-609151bd".to_vec(),
                expected: 7,
            },
            TestCase {
                key: b"1182-07205-22440-2-0ad4507d".to_vec(),
                expected: 3,
            },
            TestCase {
                key: b"1010-98110-23700-2-295bfb4c".to_vec(),
                expected: 3,
            },
            TestCase {
                key: b"1137-01302-23820-2-d560d49a".to_vec(),
                expected: 3,
            },
            // Rich's example
            TestCase {
                key: b"599999".to_vec(),
                expected: 6,
            },
        ];

        for case in cases {
            let got = Partitioner::new(12).partition(&case.key);
            assert_eq!(
                got,
                case.expected,
                "partition({}) = {}, expected {}",
                String::from_utf8_lossy(&case.key),
                got,
                case.expected
            );
        }
    }

    #[derive(Deserialize, Debug)]
    struct Partition {
        key: String,
        partition: i32,
    }

    #[test]
    fn load_partitions() {
        let partitioner = Partitioner::new(12);
        let data = include_bytes!("../data/partitions.csv");
        let mut reader = csv::ReaderBuilder::new().from_reader(data.as_slice());

        for result in reader.deserialize() {
            let record: Partition = result.expect("should deserialize");
            let found = partitioner.partition(record.key.as_bytes());
            assert_eq!(found, record.partition);
        }
    }
}
