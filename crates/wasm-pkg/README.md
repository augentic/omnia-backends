# omnia-wasm-pkg

Registry acquirer for omnia's plugin loader: an `Acquire` implementation
over [wasm-pkg-client](https://github.com/bytecodealliance/wasm-pkg-tools)
that serves `Location::Registry` requests from OCI and well-known wasm-pkg
registries. It is a composition-root value for the `runtime!` macro's
`plugins: { acquire: ... }` key (or `DeploymentBuilder::acquirer`), not a
`Backend`/`WasiXxxCtx` host backend.

- Packages must pin an exact version (`namespace:name@1.2.3`); remote lookup
  never resolves "latest".
- `RegistryAcquire::new(default)` starts from an empty client configuration —
  no user-global `wasm-pkg` config file, no hard-coded fallback registries —
  so the compiled binary alone attests which endpoints a deployment may
  reach. A per-load `Location::Registry(Some(...))` override replaces the
  default endpoint, exactly the authority a run input is meant to carry.
- Acquired bytes are verified against the registry's content digest before
  they are returned; the operator's own sha256 pin is verified host-side by
  the loader after acquisition.

## Caching

The acquirer is cacheless until `.cached(...)` attaches a `ContentStore` — a
digest-keyed content-addressed store that backs wasm-pkg-client's
`CachingClient`. A cacheless acquirer is a valid deployment posture (for
example an immutable container with no writable volume).

- Content entries are keyed by their sha256 digest and are
  verify-before-persist: bytes that do not hash to their key are refused,
  and accepted entries land by temp-file plus atomic rename.
- Reads are self-verifying end to end: the acquirer re-hashes whatever the
  store or network produced against the registry's release digest, so a
  poisoned or truncated entry fails its hash instead of becoming code.
- Release records (exact version → digest) are cached per registry, which is
  what lets a warm cache serve offline runs.
- Only raw wasm ever enters the store — compiled (`Component::serialize`)
  artifacts are never cached, and the loader refuses them regardless.

## Composition

`AcquireExt::or` composes acquirers by location kind: the first acquirer
that serves the location wins, and real failures never fall through.

```rust,ignore
use omnia::MountAcquire;
use omnia_wasm_pkg::{AcquireExt as _, RegistryAcquire};

omnia::runtime!({
    mode: command,
    plugins: {
        interfaces: ["example:adapter/source@0.1.0"],
        // Paths first, then the registry: preopen-relative reads stay
        // fresh and uncached; registry loads go through the CAS.
        acquire: MountAcquire.or(
            RegistryAcquire::new("omnia.host").cached_at(".omnia/storage/plugins"),
        ),
    },
    // ...
});
```

## Configuration

`RegistryAcquire::with_config` accepts a full `wasm_pkg_client::Config` for
per-registry backend and credential settings (for example, forcing a plain
HTTP OCI backend, or supplying auth). The acquirer's default registry and
per-load overrides always take precedence over the configuration's own
default. OCI credentials otherwise resolve through ambient Docker credential
helpers.

## Live tests

CI-runnable tests drive the acquirer against wasm-pkg-client's `local`
filesystem backend. The `#[ignore]`-gated live test needs a reachable OCI
registry:

```sh
docker run --rm -p 5000:5000 distribution/distribution:edge
cargo nextest run -p omnia-wasm-pkg --run-ignored all
```
