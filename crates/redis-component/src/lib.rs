#![doc = include_str!("../README.md")]

pub mod resp;
pub mod script;

mod keyvalue;

pub use self::keyvalue::Redis;
pub use self::script::CAS_SCRIPT;

#[cfg(target_arch = "wasm32")]
mod package {
    use omnia_wasi_keyvalue::backend as kv;

    use crate::Redis;
    use crate::resp::Resp;

    static POOL: kv::Slot<kv::Pool<Resp>> = kv::Slot::new();

    kv::lifecycle::export!(POOL);
    kv::export!(Redis, POOL);
}
