# Agents

## Overview

`omnia-backends` provides production backend implementations of the Omnia WASI host
interfaces (Azure Blob/Table/Vault/Identity, Postgres, Redis, NATS, Kafka,
MongoDB, OpenTelemetry, and the `genai`/`cursor` model backends). Each crate
implements the corresponding `omnia` `WasiXxxCtx` trait against a real service.
`omnia-filesystem` and `omnia-azure-blob` additionally implement
`omnia_plugin::ContentStore` and `omnia_plugin::ReleaseStore` (the
`PluginStore` bound behind the `RegistryClient` acquirer);
registry acquisition itself lives in `omnia-plugin`. The `omnia` runtime is consumed
via `[patch.crates-io]` entries pointing at the omnia git repository (or a
sibling `../omnia/crates/*` checkout by switching the commented path patches).

## Key commands

| Task | Command |
|------|---------|
| Build | `cargo build --all-features` |
| Lint | `cargo clippy --workspace --all-targets --all-features -- -D warnings` |
| Format fix | `cargo +nightly fmt --all` |
| Test (CI-runnable) | `cargo nextest run --all --all-features --no-tests=pass` |
| Live tests (local) | `cargo nextest run -p <crate> --run-ignored all` (needs the service + credentials) |

## Testing policy (integration-first, service-free CI)

These crates talk to real services, so the boundary that matters is
`backend ⇄ real service ⇄ omnia WasiXxxCtx`. CI cannot stand those services up,
so the policy splits accordingly:

- **CI floor = pure translation unit tests.** Keep unit tests for deterministic,
  service-free logic: OData filter building (`azure-table/store/filter.rs`),
  Postgres type mapping, the Kafka partitioner, `cursor` prompt build/parse.
  These are the CI-enforced coverage.
- **Real-service tests are `#[ignore]`-gated live tests** in `tests/live.rs`,
  env/credential-gated, driving the backend's `WasiXxxCtx` against a real
  service. They never run (or spawn a process) in CI. Exemplar:
  [crates/cursor/tests/live.rs](crates/cursor/tests/live.rs). Document the run
  recipe (`cargo nextest run --run-ignored all` plus required env) in each
  crate's README.
- **Delete tautological mapping tests.** A unit test that mirrors the
  implementation's output against a hand-copied literal, or asserts against
  mocked SDK types, earns its keep only if a live test now proves the real
  service accepts the mapping. Prefer the live test and drop the mock.
- **Names identify, comments explain.** A test name is the scenario, not a
  restated expectation.
