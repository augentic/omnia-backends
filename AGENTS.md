# Agents

## Overview

`omnia-backends` provides production backend implementations of the Omnia WASI host
interfaces (Azure Blob/Table/Vault/Identity, Postgres, Redis, NATS, Kafka,
MongoDB, OpenTelemetry, and the `genai`/`cursor` model backends). Each crate
implements the corresponding `omnia` `WasiXxxCtx` trait against a real service.
`omnia-filesystem` and `omnia-azure-blob` additionally implement
`omnia_plugin::ContentStore` and `omnia_plugin::ReleaseStore` (the
store bound behind the `RegistryClient` acquirer);
registry acquisition itself lives in `omnia-plugin`. The `omnia` runtime is consumed
as published crates.io dependencies (currently 0.36.0), declared once under
`[workspace.dependencies]` in the root `Cargo.toml`.

## Key commands

| Task | Command |
|------|---------|
| Build | `cargo build --all-features` |
| Lint | `cargo clippy --workspace --all-targets --all-features -- -D warnings` |
| Format check | `cargo +nightly fmt --all --check` |
| Format fix | `cargo +nightly fmt --all` |
| Test a crate | `cargo nextest run -p <crate> --all-features` (the local verification step) |
| Test (CI-runnable) | `cargo nextest run --all --all-features --no-tests=pass` (`cargo make test`) |
| Live tests (local) | `cargo nextest run -p <crate> --all-features --run-ignored all` (needs the service + credentials) |
| Supply chain | `cargo make vet` after any dependency change |
| Task runner | `cargo make <task>` (see `Makefile.toml`; `cargo make ci` is the full gate) |

## Verifying a change

- Run the suite of the crate you changed, `cargo clippy --workspace
  --all-targets --all-features -- -D warnings`, and `cargo +nightly fmt --all
  --check`. `cargo make test` is the full run; leave it to CI.
- Never build or run the `examples` to check a change. They are demos a
  human runs by hand against a real service; nothing they do is a test.
- A question of the form "does this work from a real guest?" is answered by
  a `test-programs` guest run through the e2e suite (below), never by
  driving `Client::complete` from a test and never by instantiating a WASM
  guest ad hoc inside one.

## Testing policy (integration-first, service-free CI)

These crates talk to real services, so the boundary that matters is
`backend ⇄ real service ⇄ omnia WasiXxxCtx`. CI cannot stand those services up,
so the policy splits into three tiers:

- **Guest-driven e2e over a protocol-faithful fake is the primary tier**
  wherever the service's protocol is small enough to fake honestly — today
  the `cursor` and `genai` model backends. A real guest component from
  [crates/test-programs](crates/test-programs) (compiled for `wasm32-wasip2`
  by that crate's own build script) runs through
  `omnia_test::host::Deployment` over the backend's `Client`, whose service
  is a fake the test owns: `fake-cursor-sdk-bridge` for cursor (a binary
  linked onto the test process's `PATH` as `cursor-sdk-bridge`, which the
  client spawns as one process per lease exactly as it spawns the real
  one), an `OpenAI`-compatible chat-completions endpoint behind
  `GENAI_ENDPOINT` for genai. The guest asserts what it observes across
  the boundary and traps on failure; the test asserts what reached the
  fake (per-agent RPC sequences, recorded request bodies) and that the
  backend is whole again afterwards (every spawned process gone, on
  cursor). Nothing public exists for the tests' sake — no `#[doc(hidden)]`
  probes, no options the fake alone needs; the fake is reached the way the
  real service is. One
  flat file per boundary in the crate's `tests/`: `tests/model.rs` is the
  `omnia:model` contract, `tests/worker.rs` / `tests/provider.rs` the
  lifecycle and fault matrix (hung or parked RPCs, process death before and
  after the ready line, a completion dropped at every point it can be
  waiting, a `429`/`503`/truncated body). Exemplars:
  [crates/cursor/tests/worker.rs](crates/cursor/tests/worker.rs) and
  [crates/genai/tests/model.rs](crates/genai/tests/model.rs).
  - **Every guest program pairs with a row in every model backend's
    `tests/model.rs`.** Each suite invokes `test_programs::foreach_model!()`,
    which fails to compile until a `model_<scenario>` test exists for each
    `programs/model/<scenario>.rs` — the shared scenarios are the contract
    both backends must meet. A scenario that needs to fail takes the
    expected needle as an operator argument (`expect_error`) rather than
    becoming one program per failure.
  - **Adding a scenario**: write `crates/test-programs/programs/<capability>/<scenario>.rs`
    (`#![cfg(target_arch = "wasm32")]`, entered through
    `omnia_sdk::command!(scenario)`, asserting what the guest observes and
    panicking on failure; shared helpers live in `test-programs/src/helpers.rs`),
    then the paired test in each suite. The next `cargo nextest run -p <crate>
    --all-features` compiles the guest, regenerates the `[[example]]` list in
    `crates/test-programs/Cargo.toml` from the `programs/` tree, and emits the
    `<CAPABILITY>_<SCENARIO>` path constant. No separate build, no `--target`
    flag.
  - **Other backends join this tier through `omnia_test::host::Backends`'
    per-host setters** (`Backends::defaults().await.keyvalue(client)`,
    `.blobstore(..)`, `.sql(..)`, …): a redis/postgres/nats suite runs the
    same shape over the real client behind a service-up env gate, since no
    honest in-process fake of those services exists. The setters shipped in
    `omnia-test` 0.36.0; the redis/postgres/nats suites themselves are not
    built yet.
- **Unit tests for deterministic, service-free logic, wherever it lives**:
  OData filter building (`azure-table/store/filter.rs`), Postgres type
  mapping, the Kafka partitioner, cursor's ready-line and Connect-frame
  parsing (`cursor/src/worker/discovery.rs`, `sdk/rpc.rs`), genai's
  request translation (`genai/src/model/options.rs`). A behaviour a guest
  boundary reaches is an e2e row, not a unit test: the scripted-server unit
  tests the model backends once carried inside `src/` were retired for
  exactly that reason, and a new in-crate loopback server is the signal to
  extend the fake instead.
- **Real-service tests are `#[ignore]`-gated live tests** in `tests/live.rs`,
  env/credential-gated, driving the backend's `WasiXxxCtx` against a real
  service. They never run (or spawn a process) in CI. Exemplar:
  [crates/cursor/tests/live.rs](crates/cursor/tests/live.rs). Document the run
  recipe (`cargo nextest run --all-features --run-ignored all` plus required
  env) in each crate's README. A live test may be red on purpose when it
  pins an upstream defect: its `#[ignore]` reason names the env it needs
  and its README entry says what red and green mean.
- **Delete tautological mapping tests.** A unit test that mirrors the
  implementation's output against a hand-copied literal, or asserts against
  mocked SDK types, earns its keep only if a live test — or an e2e row over
  the fake — now proves the real service accepts the mapping. Prefer that
  and drop the mock.
- **Names identify, comments explain.** A test name is the scenario
  (`abandon_during_create`), not a restated expectation.

## Gotchas

- `rust-toolchain.toml` auto-installs the `wasm32-wasip2` target; the
  `test-programs` build script needs it, and it runs on every build of a
  crate that dev-depends on `test-programs`.
- The `[[example]]` list in `crates/test-programs/Cargo.toml` is generated:
  a guest program is added by adding its file, never by editing the manifest.
- The e2e suites JIT-compile each guest through Cranelift on every run, so
  the root `Cargo.toml` builds `cranelift-codegen`, `regalloc2`, and
  `wasmtime-internal-cranelift` optimized under the dev profile (mirroring
  omnia). Without that the suites' in-test waits drift toward their bounds
  under a full-workspace run.
- The fake bridge's spawned processes append to one `flock`-serialised JSONL
  log the test folds back into per-process histories; faults can target one
  spawned process by ordinal, so one guest run can see a healthy process and
  a faulted one side by side.
- The fake bridge is a `[[bin]]` of `omnia-cursor`, so it compiles against
  the crate's `[dependencies]` alone — never a dev-dependency. The suites'
  side of its module (`Spawnable`, the liveness probe and its `libc`) is
  `cfg(test)`, which the binary never is (`test = false`, `bench = false`).
- The omnia crates come from crates.io, pinned by the single
  `[workspace.dependencies]` declarations (`omnia = "0.36.0"` and friends);
  every `omnia-*` must stay on the same line, so bump them all together and
  never add a `[patch.crates-io]` git or path override to chase an
  unreleased change. `.cargo/config.toml` sets
  `registry.global-min-publish-age = "7 days"`; stable cargo ignores it
  (it is enforced only under nightly `-Zmin-publish-age`), but where it is
  enforced, re-locking onto a freshly published omnia line needs an explicit
  bypass (`CARGO_REGISTRY_GLOBAL_MIN_PUBLISH_AGE="0 days" cargo fetch`).
  Locked versions are exempt, so `--locked` builds are unaffected.
