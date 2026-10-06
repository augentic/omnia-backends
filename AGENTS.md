# Agents

## Overview

`omnia-backends` provides production backend implementations of the Omnia WASI host
interfaces (Azure Blob/Table/Vault/Identity, Postgres, Redis, NATS, Kafka,
MongoDB, OpenTelemetry, and the `genai`/`cursor` model backends). Each crate
implements the corresponding `omnia` `WasiXxxCtx` trait against a real service.
`omnia-azure-blob` additionally implements `omnia_plugin::PackageStore` (the
store the `RegistryClient` acquirer reads before any registry and writes
what it fetches to); registry acquisition itself, and the local `FsStore`,
live in `omnia-plugin`. The `omnia` runtime is consumed
as published crates.io dependencies (currently 0.36.0), declared once under
`[workspace.dependencies]` in the root `Cargo.toml`.

## Key commands

| Task               | Command                                                                                                                |
| ------------------ | ---------------------------------------------------------------------------------------------------------------------- |
| Build              | `cargo build --all-features`                                                                                           |
| Lint               | `mise run lint` (native workspace clippy, then `test-programs` and `examples` for `wasm32-wasip2`)                     |
| Format check       | `mise run fmt-check` (`cargo +nightly fmt --all --check`)                                                              |
| Format fix         | `cargo +nightly fmt --all`                                                                                             |
| Test a crate       | `cargo nextest run -p <crate> --all-features` (the local verification step)                                            |
| Test (CI-runnable) | `mise run test` (`cargo nextest run --workspace --all-features --locked --no-tests=pass`)                              |
| Live tests (local) | `cargo nextest run -p <crate> --all-features --run-ignored all` (needs the service + credentials)                      |
| Supply chain       | `mise run vet-regen` after any dependency change (`mise run vet` only checks, as CI does)                              |
| Task runner        | `mise run <task>` (`mise.toml` includes the shared Rust tasks from `augentic/toolkit`; `mise run ci` is the full gate) |

## Verifying a change

- Run the suite of the crate you changed, `mise run lint`, and
  `mise run fmt-check`. `mise run lint` is native workspace clippy over
  all targets, then clippy of `test-programs` and `examples` (their libs,
  bins and examples, never tests) for `wasm32-wasip2`. `mise run test` is
  the full run; leave it to CI.
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
  parsing (`cursor/src/worker/discovery.rs`, `protocol.rs`), genai's
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

- Tasks run through [mise](https://mise.jdx.dev/getting-started.html), which
  must be installed by hand. `make <task>` only forwards to `mise run <task>`
  and fails, rather than installing mise, when it is missing.
- `rust-toolchain.toml` auto-installs the `wasm32-wasip2` target; the
  `test-programs` build script needs it, and it runs on every build of a
  crate that dev-depends on `test-programs`.
- The `wasm32-wasip2` clippy pass covers only `test-programs` and `examples`
  (`WASM32_PACKAGES` in `mise.toml`, mirrored by `wasm-packages` in
  `.github/workflows/ci.yaml` and `publish.yaml`); the backend crates link
  host-only libraries and never build for it. Within that scope every lib,
  bin and example is built, so a host-only example in `examples` must be an
  empty `main` on wasm32: keep the program in a sibling module and gate the
  entry point as `examples/azure_blob/main.rs` does.
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

## Code style

clippy (`make lint`) and nightly rustfmt (`make fmt`) are the style gate; beyond them and the rules below, match the surrounding code.

- Suppress a lint with `#[expect(lint, reason = "…")]` at the smallest scope, never `#[allow]`.
- `<module>.rs` plus `<module>/<child>.rs`; `mod.rs` only under `tests/support/`.
- A fn over a type is that type's method, not a free fn taking it as its first argument, where the type's module declares the fn or the fn is a plain lookup or predicate on the type. A constructor is an associated fn. A policy `const` sits beside the type whose method reads it. Values several fns thread through every call become one struct whose methods they are. A fn stays free when it is pure over primitives and iterators, or when it is one module's rule applied to another module's type.

Comments follow the conventions `std`, `serde`, and `tokio` converge on: docs state the observable contract for the crate's user, never the body's mechanics.

- `///` goes on the public API only — the `pub` types, fns, fields, variants, and re-exports a user of the crate can reach — never on a private or `pub(crate)` item, an `impl` block, or a trait-impl method. A clap field's `///` is its `--help` text. A doc opens with one summary sentence (about fifteen words, full stop), then a blank line, then short sentences and bullet lists. `# Examples` holds compiled doctests, for non-obvious usage only; `# Errors` names each class the caller matches on, linked; `# Panics` the rest. Every item mentioned is an intra-doc link. No mechanics, history, or migration notes. A `//!` says what a module is for, in the same shape.
- A private item takes a `//` only for what a senior developer would not see from its name and signature: a constraint, a why, an invariant. Most carry nothing. No restatements, match-arm labels, or body paraphrases.
- Inside a body, a `//` is a section header: lowercase, no full stop, above a blank-line-separated block, naming what the block achieves, so the headers read together outline the fn. A fn readable at a glance carries none, and a header never narrates the line beneath it. The one in-body explanation is `// HACK: …`, for a trick a senior would not see through.
- A test fn takes `//`, never `///`, and only for rationale its scenario name and assertions do not expose.
- No commented-out code.
- Every sentence earns its place and reads once: short plain sentences, one idea each; three or more things are a bullet list, not a colon-and-dash clause; no chained em-dashes, nested parentheticals, or semicolon runs; each fact has one home across `//!`, `///`, and `//`. A comment is as long as its why takes and no longer — concise is not dense, and readable is not verbose.

In this repository the register is the one `crates/cursor/src/{worker,pool,failure}.rs` is written in. Rustdoc renders crate-root items, `pub use` re-exports, and the one `pub mod` (`azure-table::store`) — including the variants of a re-exported `pub enum` and their named fields, which render and are linted however private they look; `missing_docs` under `-D warnings` is the guard, so a `///` that survives `cargo clippy` being removed was not reachable. Section dividers in a long file are `// --- Name ---`, never box-drawing characters. A test module opens with a `//` above `#[cfg(test)]` saying what the unit tier covers versus the live or e2e tier; `pub` items in a suite's `tests/support` are that suite's shared API and keep a one-line `///`, with no `# Errors` or `# Panics` sections since nothing there is linted.

## Git

Never `git commit`, `git push`, open or close a pull request, or delete a branch — in this repository or in any sibling checkout — unless the maintainer lifts this for the session, explicitly and for named work. Leave every change uncommitted in the working tree; the maintainer reviews and commits. No plan or to-do list carries a commit, push, or PR step, and an instruction to complete every step does not override this.

## Testing

Tests drive the public boundary: a behaviour is asserted through what a user of the product or crate can reach, over scripted doubles rather than a live filesystem, network, or model, never through private internals. A suite below the root survives only for an independent library contract; a unit test only for a branch no public boundary reaches. A test fn names the scenario (`gen_spec`, `no_sources`), never the outcome. Scripted doubles are strict: script exactly the exchanges a run consumes.

## Commands

All from the repository root through `make` ([`Makefile`](Makefile) → mise). The tasks are the shared `mise/rust.toml` of [`augentic/toolkit`](https://github.com/augentic/toolkit), pinned in [`mise.toml`](mise.toml) to the tag every `uses:` under `.github/workflows/` names; bump both together in one pull request.

```bash
make ci # exactly the CI jobs: fmt-check + lint + test + test-docs + docs + vet + deny  — run before handing over
make check # local advisories: audit + fmt (rewrites) + lint + outdated + deps
make test # cargo nextest run --locked --workspace --all-features, under -Dwarnings
make lint # lint-host (cargo clippy --workspace --all-targets --all-features, then cargo hack --each-feature), then lint-wasm (the same over every lib, bin and example for wasm32-wasip2 — never tests)
make fmt # cargo +nightly fmt --all
make vet-regen # regenerate cargo-vet imports/exemptions/unpublished, then vet
make cov # cargo llvm-cov nextest --workspace --all-features --summary-only
make sweep # drop target/ artifacts untouched for a week
```

If `make ci` cannot run, say exactly why and which checks ran instead.
