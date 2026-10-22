## 0.31.0

Released 2026-10-22

Pairs with omnia 0.37.x.

### Added

- Add `omnia-git`, the `git` backend for `omnia:vcs` (store, workspace, transport) with host-side command policy (#106).
- Add `omnia-sandbox` for session-scoped worker isolation, egress proxy, and grant enforcement on Linux and macOS (#112).
- Run `omnia-cursor` bridges inside the sandbox; add an IPv6 loopback API relay so Bun can stream request bodies (#114, #112).
- Expose worker reach through completion `grants.worker` on `omnia-cursor` and `omnia-genai` (#117, #118).
- Bring `omnia-genai` to cursor parity: shell tool under sandbox roots, budgets, logging, JSON/schema nudge, and `BudgetExhausted` mapping (#145, #112).
- Add genai knobs `GENAI_MAX_CANDIDATES`, `GENAI_MAX_REPEATS`, and a finishing round when the round budget is spent (#116).
- Answer undeclared tool calls in-band on genai instead of failing the completion (#116).
- Support Qwen Coder models on vLLM, including `qwen3-coder-next-fp8` (#146, #156, #157).
- Expand cursor completion telemetry: full usage, phase timing, tool-call logging, and configurable `max_tool_calls` (#142, #92).

### Changed

- Refuse guest mutation of lent trees in cursor: read-only tool allowlist by default (#142).
- Bound cursor inactivity only while waiting on the bridge or tools; long model composition follows the absolute cap (#111, #142).
- Tear down cursor agents after the `completion` line; drop attach-to-external-bridge options (#142).
- Implement plugin releases as one `PackageStore` on `omnia-azure-blob`; refuse the `omnia-plugins` container on guest blobstore (#105).
- Read OpenTelemetry export URL from `OTEL_EXPORTER_OTLP_ENDPOINT` (#139).
- Harden git worktree addressing and merge-rule application (#107, #113, #144, #109).
- Update wasmtime to 50.0.0 (#158).

### Fixed

- Apply git merge rules only where both sides changed; follow renames and stage removals correctly (#107, #113).
- Close cursor tool calls left open when the model resumes reasoning or speaking (#142).
- Decode integral protobuf JSON numbers as integers for guest tool arguments (#142).

### Removed

- Remove `omnia-filesystem`'s plugin store; use omnia's local `FsStore` and azure `PackageStore` (#105).

---

Release notes for previous releases can be found on the respective release
branches of the repository.

<!-- ARCHIVE_START -->
* [0.30.x](https://github.com/augentic/omnia-backends/blob/release-0.30.0/RELEASES.md)
* [0.28.x](https://github.com/augentic/omnia-backends/blob/release-0.28.0/RELEASES.md)
* [0.27.x](https://github.com/augentic/omnia-backends/blob/release-0.27.0/RELEASES.md)
* [0.26.x](https://github.com/augentic/omnia-backends/blob/release-0.26.0/RELEASES.md)

- [0.25.x](https://github.com/augentic/omnia-backends/blob/release-0.25.0/RELEASES.md)
- [0.24.x](https://github.com/augentic/omnia-backends/blob/release-0.24.0/RELEASES.md)
- [0.23.x](https://github.com/augentic/omnia-backends/blob/release-0.23.0/RELEASES.md)
