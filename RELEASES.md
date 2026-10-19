## 0.31.0

Unreleased

### Added

- `omnia-git`, the backend for omnia 0.37's `omnia:vcs` interface
  (`omnia-wasi-vcs`), the one that interface has: every `store`,
  `workspace`, and `transport` operation is one `git` process over the
  operator's own binary (`GIT_BINARY`, `git` on `PATH` by default; a git
  older than 2.26 is refused at connect), run in the directory the runtime
  opened for the guest's lent location — the child enters the open handle
  before it executes, so no operation walks a path again — with `GIT_DIR`
  and its siblings scrubbed from the environment, each process a session
  leader with no controlling terminal. A lent repository's own
  configuration is a guest's to write, so each operation is held to host
  policy on its command line: hooks, the file-system monitor, the
  alternate-refs command, commit and push signing, and submodule
  recursion are forced off; the work tree is the place itself, whatever
  `core.worktree` the repository set, and git's search for the repository
  stops there (`GIT_CEILING_DIRECTORIES`), so a place with no `.git` of
  its own is `not-a-repository` rather than a subdirectory of an
  enclosing checkout; a repository-scoped SSH command,
  credential prompt, signing program, credential helper, or clean,
  smudge, or merge driver gives way to the operator's own value or to
  none, and a repository key in bytes that are not UTF-8, which no pin
  can name, refuses the operation; every guest string follows `--`, one
  that is empty or option-shaped is `not-found` before any process runs;
  every process runs under `GIT_ALLOW_PROTOCOL` and `GIT_NO_LAZY_FETCH`,
  no protocol and no lazy fetch outside a transport operation, so an
  object the repository lacks is missing rather than fetched from a
  promisor remote it names, and a transport under the list for its kind,
  read from the URL as the host's configuration expands it, and under
  `LC_ALL=C`, so what git says is the English the typed errors are read
  from whatever locale the operator's shell exports; the pack command is
  named on every transport,
  a local one's holding the far side to no hooks, alternate-refs command,
  or file-system monitor and to no update of a checked-out branch, and a
  remote one's git's own, so a `remote.<name>.uploadPack`
  or `receivePack` the repository set is never what the operator's ssh
  carries to a host; and a repository that sets how a transport runs — a
  `url.<base>.insteadOf`, an `http.*` setting beyond the transfer-tuning
  keys, a `remote.<name>.proxy` — is refused on every transport, the key
  named. A merge is held before its commit and the policy
  applied by the backend from the three commits, byte for byte — over
  whatever the repository's own attributes made of a path — so nothing of
  it is written into the repository; a `union` over a binary fails the
  merge; a conflict no rule resolves comes back as data with the merge
  aborted and the tree on its head, and no failed merge stays in
  progress; a merge the policy sealed carries the message as given, its
  own `#` lines included and the `# Conflicts:` hint git appends never
  part of it. `log` is `git log
  --first-parent` over `<base>..<revision>`, both resolved first and
  either unknown `not-found`: the chain newest first, the base left out,
  a merge one entry and the side it brought in none, each entry the
  commit's id and its message as sealed. `descends` is `merge-base
  --is-ancestor` over the two resolved commits, read by exit status. A
  `label` on the branch the working copy sits on detaches the copy onto
  its commit first, and `labelled` reads it back as the exact ref
  `refs/heads/<name>` alone, so a tag or a remote-tracking ref of the
  same spelling, which git resolves a bare name to first, is never the
  label, and nor is a commit a `~`, `^`, or `@{}` in the name would walk
  to; `fetched` reads a remote's label as the exact ref
  `refs/remotes/<remote>/<name>` the same way, so a guest never spells
  `origin/<name>` itself. A `push` is never forced: a rejection the remote's branch has
  moved past is the typed `diverged`, naming the label. A `clone` with a
  depth passes `--no-single-branch`, so a shallow clone still answers
  every label. What git reports is read into the typed `omnia:vcs`
  error. Wired as `WasiVcs: omnia_git::Client`.
- `omnia-cursor`'s `complete` span carries the request's schema name as
  `label` when the format is a schema — the guest's own name for the
  question — so a host log tells one completion from another by what was
  asked, not by sequence number alone.
- `omnia-cursor` reads `cursor-sdk-bridge`'s own tool-call frames — the
  tool's `name` and `args` at the top of the payload, a custom tool as
  `mcp` with its name and arguments beneath — beside the CLI stream's
  nested `<name>ToolCall` shape it already read. `tool_calls` on the
  `in progress` line and `tool_turns` on `send answered` count again, and
  the `Transcript` a completion returns is populated. Each call is logged
  at DEBUG as it starts (`tool call`, with `tool` and a compact `subject`:
  a path, a pattern, or the first line of a command) and completes
  (`tool call completed`, with `result_bytes`).
- The `in progress` heartbeat and `Failure::Inactive` name what a silent
  run is `waiting` on — `the opening frame`, ``tool `<name>` ``, or `the
  model`; `Failure::Inactive` gains a `waiting` field.
- A run cut short — at a deadline, aborted, or dropped — is cancelled and
  its bill asked back through `SdkAgentService/GetUsage` before the
  `completion` event, so the event's token counts cover it whenever the
  bridge can report them; when it cannot, a DEBUG line says why.
- `omnia-cursor`'s `completion` line carries the bill in full and where
  the wait went. `cache_read_tokens`, `cache_write_tokens`, and
  `total_tokens` sit beside `input_tokens` and `output_tokens` — the
  bridge's `usage` frame had carried all three unread. The bridge's total
  is input plus output plus cache reads, so the cache reads are context
  beyond `input_tokens` rather than a part of it, and a completion's
  context is `input_tokens + cache_read_tokens`; `reasoning_tokens` is
  among `output_tokens`. `send answered` and the settled-usage line carry
  the same counts, and `TokenUsage` gains the three fields, each absent
  when the bridge reports none. `opening_ms`, `tool_ms`, `model_ms`, and
  `check_ms` split `duration_ms` by what the completion waited on — the
  bridge before a round's first frame, the agent's creation included;
  tool calls outstanding; the model composing; the guest's check of each
  candidate — every round together, so whether a slow completion read too
  much or reasoned too long is read off one INFO line rather than a TRACE
  log. The four sum to `duration_ms` less the moments between them and,
  on a run cut short, the cancel and usage read-back before the line.
- A reasoning block's closing frame is a DEBUG `thinking` event carrying
  its `duration_ms` — read as the bridge spells an `int64`, a number or a
  string — so a `-v` run shows the long silences between tool calls for
  what they are; the text deltas before it stay TRACE `frame`s. The
  `completion` line sums them as `thinking_ms`, beside `model_ms` — the
  part of the model's wait it spent reasoning by its own report, every
  round together — so whether a slow completion thought or wrote is read
  off the one INFO line.
- Each tool call's arguments are logged whole at TRACE (`tool call args`,
  with `tool` and `args`) as the call starts, beneath the DEBUG `tool
  call` line's one-argument `subject`, so a `-vv` run reads what the
  agent asked of each tool without the payload head of every frame. The
  line goes out under the target `omnia_cursor::tools`, so
  `RUST_LOG=omnia_cursor=debug,omnia_cursor::tools=trace` reads the
  arguments without the stream's frames.
- The `subject` on `tool call` / `tool call completed` spells every
  `<cwd>/` in the argument as `./`, in a command's words as in a path, so
  the eighty characters go on the part that tells one file from another
  rather than on the lent tree's absolute path, and `cd <cwd>/x && npm
  test` reads `cd ./x && npm test`; the transcript keeps the text as the
  agent spelled it.
- `omnia-cursor` owns the guest tool-call budget: `ConnectOptions`
  gains `max_tool_calls` (`CURSOR_MAX_TOOL_CALLS`, default `128`; zero
  refused at connect), answered to the host through
  `WasiModelCtx::limits` in place of `wasi-model`'s default of 32, with
  the result-size cap and the per-call timeout left the host's. An agent
  that lays a tree one file a tool call ran past 32 before its wall-clock
  cap; the cap on the run is `CURSOR_TIMEOUT_SECS`, and the call budget
  now sits beside it as a knob of the same backend.
- `omnia-sandbox`, the crate that owns a host process in the model's
  trust domain: `Sandbox::connect` holds the backend's `Options` to the
  host once — `shell_roots`, `inherit`, `refused` — reads the
  operator's `HOME` as what a `~/` spells, and probes the kernel
  boundary, refusing to connect where none runs; `Sandbox::prepare` puts
  one process at a workspace, or in a private directory when nothing is
  lent, and prepares a `Session` under the process's own `Grant` —
  `egress`, the hosts its commands may reach; `read`, the roots beneath
  the home it may read, each spelled `~/<path>`, one the home lacks
  logged at WARN and granted nothing, and the stores the operator's
  credentials live in (`~/.ssh`, `~/.gnupg`, `~/.aws`, `~/.azure`,
  `~/.kube`, `~/.docker`, `~/.config/gh`, `~/.config/gcloud`,
  `~/.password-store`, `~/Library/Keychains`) no grant's by the crate's
  own rule, a root at, beneath, or above one, as spelled or as either
  resolves, refused on every machine, so no deployment setting bounds
  what a process reads of the home and none is needed to; `env`, the
  variables set for it, a value's leading `~/` the home — binding the
  egress proxy for that session alone, and answering a grant that breaks
  a rule of the sandbox (a root spelled otherwise, stepping through `.`
  or `..`, overlapping a credential store, resolving outside the home,
  or a file; a variable the sandbox sets, one the dynamic loader or a
  runtime reads at startup, one inherited from the host, or no name at
  all) with the typed `Denied`, which a backend tells from a host
  failure. The `Session` is a handle — a clone names the same session,
  and its scratch and proxy live as long as any handle or any process a
  command ran — whose `access` says what its working directory admits —
  `Private`, `Read`, or `Shell` for a workspace under a shell root — and
  whose `command` borrows it for a `Command` scoped to it, as many times
  as a completion's commands ask, so a build's `cargo check` and its
  `cargo test` run in one `HOME` with one cache: the bare program
  resolved against the host's `PATH` rather than the grant's, run in the
  workspace's open directory handle, as a session leader, with an
  environment built from nothing (`PATH`, `LANG`, `LC_*`, `TZ`, `TERM`,
  a private `HOME` and `TMPDIR`, the proxy variables, the `inherit`, and
  the grant's `env`), with `arg`, `args`, `env` and `ready_prefix` the
  only knobs. `Command::spawn` supervises the process as a `Child` — the
  supervisor lifted from `omnia-cursor`'s worker, which holds the session
  until the process is gone — and `Command::output` runs it once under a
  bound, each stream capped at sixteen mebibytes and the rest drained,
  killing what the process forked once it has exited so a fork left
  holding the pipes neither delays the exit nor outlives it, and failing
  with the typed `TimedOut` where the process outlived the bound, so a
  caller tells a run cut short from one that never started. `Exit` is an
  `Error`. The kernel holds the
  process to its workspace, its scratch, the system, and the grant's read
  roots, always, with no setting to turn it off: on macOS a Seatbelt
  profile rendered from Codex's base policy and applied through
  `/usr/bin/sandbox-exec`, every path a `-D` parameter, the directories
  above the workspace listable (a Bun program reads every one on the way
  to its working directory before it reads its environment), `.git` and
  `.emery` beneath the workspace denied, `process-info` closed to outside
  processes, and the network closed to loopback; on Linux a Landlock
  ruleset (ABI 4, a hard requirement) and two seccomp filters built in
  the parent and applied in the child, the host set non-dumpable at
  connect, and a connect refusal where the kernel lacks Landlock ABI 4
  or Yama. The proxy is an HTTP `CONNECT` proxy on loopback, one per
  session, admitting the grant's `egress` — a name, a `*.suffix`, or an
  IP literal — and vetting every address, resolved or literal, against
  the unspecified, loopback, link-local, multicast, broadcast and
  cloud-metadata ranges, IPv4 carried in IPv6, and the host's own
  addresses, a loopback literal alone connected as written; it stops
  serving with its session, the tunnels it carried closed with it. The
  kernel boundary files start from heel 0.3.0 (MIT OR Apache-2.0), with
  the notice kept in each header. The crate's `tests/toolchain.rs` runs a
  build's toolchain inside the kernel boundary — a Node server on port 0,
  `node --test` with a worker thread, `npm install` of a `file:`
  dependency with the cache in the scratch `HOME` — and, in an ignored
  row, `npm install typescript` and `tsc --noEmit` through the proxy.
- `omnia-cursor` runs every `cursor-sdk-bridge` under `omnia-sandbox`:
  `ConnectOptions` gains `shell_roots` (`CURSOR_SHELL_ROOTS`), and what
  a worker reaches beyond its workspace is the completion's own grant —
  `grants.worker` of `omnia:model@0.2.0`, the `egress` hosts, `~/` `read`
  roots, and `env` the guest spells per request — prepared as the
  session's `Grant`: a grant over a workspace under no shell root, or one
  the sandbox refuses (`CURSOR_API_KEY` or `CURSOR_BACKEND_URL` as an
  `env`, a root outside the home or overlapping a credential store), is
  `invalid-request` before anything is spawned, and no setting of the
  deployment widens or narrows a worker's reach beyond what its
  completion asked: the adapter that spells the grant is trusted by its
  digest, and what no grant reads is the sandbox's own rule. A workspace
  at or beneath a shell root runs with the `shell` tool beside the
  read-only set, so an agent can run the tree's own build and tests; the
  inactivity window stands down while a shell command runs, since the
  bridge bounds it by the call's own timeout, and `in progress` says the
  run is waiting on `the shell`.
  The bridge's own API goes through a relay of the client's, not the
  proxy: a plain-HTTP server on the IPv6 loopback, bound beside the
  callback endpoint, that the worker is pointed at through
  `CURSOR_BACKEND_URL` and that forwards each call to the backend
  `ConnectOptions::backend_url` names (`https://api2.cursor.sh` under
  `connect`, set in code alone; a host's `CURSOR_BACKEND_URL` is Cursor's
  own variable, which the client does not read) over TLS under the host's own trust
  store and proxy settings, bodies streamed both ways, calls under `/v1/`
  to `api.cursor.com`, an unreachable backend `502`. The bridge's runtime
  (Bun 1.3.9) cannot stream a request body through a `CONNECT` proxy, so
  a bridge behind the proxy never completed a turn; behind the relay it
  does, and no Cursor host is on the egress list any more. The relay is
  addressed as `[::1]` and never `localhost` or `127.0.0.1`, which the
  bridge reads as a cue to turn TLS verification off for itself and every
  command it runs; `omnia-sandbox`'s `NO_PROXY` lists the IPv6 loopback
  bracketed as well as bare, since Bun matches an entry against the host
  as the URL spells it. `tests/relay.rs` holds the relay to a fake
  backend, and `tests/live.rs` gains two ignored rows: a turn completed
  inside the kernel boundary with the bridge's API through the relay, and
  an `npm install` through the proxy under a request's egress grant. The
  fake bridge the suites spawn runs inside the sandbox as the real one
  does, its script and log in a home every guest lends.
- `omnia-genai` runs a shell under `omnia-sandbox`, so a model served at
  `GENAI_ENDPOINT` — a local Ollama under a bare model name, a vLLM or
  any `OpenAI`-compatible server under `openai::<name>` — builds and
  tests a tree as the Cursor backend's agent does: `ConnectOptions` gains
  `shell_roots`
  (`GENAI_SHELL_ROOTS`, `:`-separated, none by default), and a workspace
  lent at or beneath one is advertised a `shell` tool beside `read` and
  `list` — `sh -c <command>`, resolved on the host's `PATH`, in the lend
  as its working directory and as `$WORKSPACE`, inside one sandbox
  session per completion, so every command of a turn shares one scratch
  `HOME` and one egress proxy — its result what the command wrote, its
  stderr after `--- stderr ---` when any, its exit after `--- exit ---`
  when not clean (`exit 3`, `killed by signal 9`; a silent clean command
  reads `exit 0`), cut to the host's result cap keeping its head and
  tail with the omitted count between them, and a command past
  `shell_timeout_secs` (`GENAI_SHELL_TIMEOUT_SECS`, default `600`)
  killed with everything it started and the model told so as a result
  it can act on. What the shell reaches beyond the lend is the
  completion's `grants.worker`, prepared as the session's `Grant` under
  the sandbox's rules and refused as the Cursor backend refuses it — a
  grant over a lend under no shell root, over no lend, or under a
  deployment with no shell root at all is `invalid-request` before any
  request, and a `Denied` grant is `invalid-request` in the sandbox's
  words — with the provider keys the SDK reads (`OPENAI_API_KEY` and its
  siblings) refused as a grant's `env`, and `USER`, `LOGNAME`,
  `SSL_CERT_FILE`, and `SSL_CERT_DIR` the shell's inheritance beyond
  `PATH`; with no shell root configured no kernel boundary is set up and
  the backend runs as before. A guest tool named `shell` is
  `invalid-request`, as one named `read` is the host's. The budgets are
  two, as the Cursor backend's are: `max_rounds` (`GENAI_MAX_ROUNDS`,
  default `128`) bounds the provider round-trips one completion makes,
  a tool round, a candidate, or a correction each one, and a rejected
  `check` candidate is corrected once, the second rejection the typed
  `budget-exhausted` carrying the correction, so a correction counts the
  same under either backend; `max_tool_calls` (`GENAI_MAX_TOOL_CALLS`,
  default `128`) is answered to the host through `WasiModelCtx::limits`,
  as the Cursor backend's is. The log carries each command as the Cursor
  backend's stream does — `tool call tool=shell subject=…` before it and
  `tool call completed tool=shell subject=… result_bytes=… exit=…
  duration_ms=…` after — and the `completion` line gains `model_ms` and
  `tool_ms`, the time waited on the provider and the time spent in tool
  rounds, so a scorecard over the log tells a slow model from a slow
  build. The `model` suite's fake provider scripts a list of tool calls
  and a scripted final answer, and its shell rows run real commands
  inside the sandbox: the grant's variable, the proxy, and `$WORKSPACE`
  seen from inside, nothing of the host's environment, a failing exit
  and a signal rendered, a command past the bound killed with its fork,
  two commands in one session, and the refusals; `tests/live.rs` gains
  an ignored row running `cargo --version` under the Rust toolchain's
  grant.

### Changed

- `omnia-cursor`'s workers inherit nothing of the host's environment:
  `PATH`, `LANG`, `LC_*`, `TZ` and `TERM`, a private `HOME` and `TMPDIR`,
  the proxy variables naming the session's egress proxy,
  `CURSOR_BACKEND_URL` naming the client's relay, the bridge's own
  settings and the trust-store variables it honours, and the completion's
  `grants.worker` `env`, with `CURSOR_API_KEY` riding the handshake
  alone and `NODE_USE_SYSTEM_CA`, `CURSOR_BACKEND_URL`, the proxy
  variables, the dynamic loader's `LD_*` and `DYLD_*`, `BUN_OPTIONS`,
  `NODE_OPTIONS`, and every `GIT_DIR` sibling refused as an inherit
  or a grant, and a grant that names one the bridge inherits
  refused as well. A grant's `PATH` reaches the commands the worker
  runs, while `cursor-sdk-bridge` is resolved against the host's.
  Workers are session leaders
  rather than process-group leaders, so a worker has no controlling
  terminal; the kill at shutdown reaches the whole session.

- `omnia-azure-blob` implements omnia's one `PackageStore` in place of the
  retired `ContentStore` and `ReleaseStore` pair: one blob per release in
  the `omnia-plugins` container, named as omnia's local `FsStore` files it
  (`namespace_name@version.wasm`), read before any registry and written once
  — a blob already there is never replaced, so a stored release is final
  until it is deleted. The release-record tier, and with it the per-registry
  scoping of a stored release, is gone with omnia's.
- `omnia-azure-blob`'s `wasi:blobstore` view refuses the store's
  `omnia-plugins` container by name — create, get, delete, and exists alike,
  which copy and move go through — so a guest lent blobstore on the same
  storage account can no longer plant, replace, or delete a release the
  acquirer would serve ahead of every registry. The name was documented as
  reserved; now the backend holds it so.

- `omnia-cursor`'s nudge for a reply that is not JSON names what the parser
  said of it — `Your last reply is not one well-formed JSON value (EOF while
  parsing an object at line 1 column 34), so it is not the answer. …` —
  where it told every such reply it "held no JSON value", so a truncated
  document is told where it broke. A truncated document is nudged at all
  only once omnia's `Format::candidate` passes over a bracketed block that
  does not parse whole rather than reading the values inside it; before
  that, `{"findings":[],"verdict":"pass"` reached the check as `[]`.
- `omnia-cursor` returns the answer before the teardown. `CloseAgent` and
  `DeleteAgent` run on a task of their own once the `completion` line is
  written, where the completion had awaited them — half a second to two
  per agent, paid on the guest's critical path — so a guest waits on the
  model and never on the bridge putting the agent away. The teardown
  still runs whole, bounded as before, and the slot the agent held
  reopens only once its process is gone.
- `omnia-cursor` reports the absolute cap (`CURSOR_TIMEOUT_SECS`) as the
  typed `omnia_wasi_model::Error::BudgetExhausted` — `Failure::into_error`
  wraps the failure in it, and the failure still downcasts beneath — so a
  guest sees `bad_request` and a retry-on-`bad_gateway` policy does not
  put once more a request that takes as long the second time. The
  inactivity window stays a backend failure: a run that fell silent may
  answer when put again. The message and `Outcome::Timeout` are unchanged.
- `omnia-cursor` closes a tool call the stream never terminated when the
  model next reasons or speaks: the model resumes only once every call it
  issued has answered, so a call still pending then was answered off the
  stream. The phase leaves `Tool` for `Model`, so the inactivity window
  bounds a thinking model as a thinking model again and `tool_ms` stops
  booking model time; the unterminated call's turn is not counted.
- `omnia-cursor` connects without a `CURSOR_API_KEY` and fails the first
  completion instead, with the same message the connect once gave — so a
  process whose verbs never complete runs keyless. Nothing spawns at
  connect either way.
- `omnia-cursor` grants a lent workspace a read-only tool allowlist —
  `read`, `glob`, `grep`, `ls`, and `mcp`, the channel custom tools
  arrive over — where it granted the agent's full default toolset. No
  shell, write, or delete: a lent tree is inspected, never changed. The
  guest's function tools are unaffected, since they arrive as `mcp`
  frames.
- `omnia-cursor`'s inactivity window (`CURSOR_INACTIVITY_SECS`) bounds a
  run only while it waits on the bridge — for the stream's opening frame,
  or for a tool call the agent started to complete. Once the agent has
  every tool result and the model is composing, the window stands down and
  the absolute cap (`CURSOR_TIMEOUT_SECS`) alone ends the wait: the stream
  carries no model text while it reasons, so a long answer was being cut
  at the window for taking longer than 120s to think.

- `omnia-opentelemetry` reads its collector endpoint from
  `OTEL_EXPORTER_OTLP_ENDPOINT`, OpenTelemetry's own variable and the one
  the omnia host exports its spans to, where it read `OTEL_GRPC_URL`; one
  setting names the collector for host and guest telemetry alike. The
  `http://localhost:4317` default is unchanged.
- `omnia-cursor` no longer attaches to a bridge another process manages.
  `ConnectOptions` drops `bridge_bin`, `bridge_url`, and `bridge_token`;
  the executable is `cursor-sdk-bridge` on `PATH`, and a ready line naming
  a non-loopback URL is rejected before any RPC, so the bearer token and
  API key never leave the host. Struct literals name `max_agents` only.
- The cursor end-to-end fake is linked onto the test process's `PATH` as
  `cursor-sdk-bridge` and spawned per lease. A lease fully released is
  observed as its process gone; `Client::idle_slots` and `upstream_tripwire`
  are gone.
- `omnia-cursor` spawns each bridge as the leader of its own process group
  (via `process-wrap`), so the kill after an unanswered `Shutdown` reaches
  the agent processes the bridge forks, and whatever a bridge left in its
  group when it exited is swept as the exit is seen. A graceful exit is
  bounded once — 5s for the `Shutdown` RPC and the exit together — rather
  than once each.
- `omnia-cursor` has no `fake-bridge` feature. The fake bridge binary and
  the `model` and `bridge` suites build on every `cargo nextest run -p
  omnia-cursor`; `libc` is a dev-dependency, and `http-body-util`'s `channel`
  (the fake's run stream, no crate of its own) is always on.
- `omnia-cursor` decodes an integral `google.protobuf.Struct` number (proto3
  JSON prints `3.0` as `3`) as a JSON integer, so a guest tool's arguments
  arrive as the tool declared them rather than as `3.0`. The fake bridge
  shares the crate's callback codec instead of carrying a copy.
- The `omnia-cursor` bridge-exit WARN carries `pid`, `uptime_ms`, and
  `status`; `run_in_flight` and `silent_ms` are gone from it. The completion
  that lost its run logs `run lost with its process` at INFO with the `pid`
  and its own `silent_ms`.
- A rejected candidate is logged at DEBUG by its `round` and
  `candidate_bytes`, in `omnia-cursor` and `omnia-genai` alike; the
  correction the guest's check minted — the rejected answer echoed whole
  with the findings after it — is the TRACE `correction turn`, where it had
  been the DEBUG line's `correction` field, so `-v` no longer carries
  every rejected candidate in full and the findings are read where the
  guest logs them.
- `omnia-git` addresses a working copy it laid through the gitdir git
  recorded for it: `add` reads it back (`rev-parse --absolute-git-dir`)
  once `worktree add` returns, and every later process at the copy — the
  pins' read, `head`, `pending`, `commit`, `remove` — runs under `--git-dir`
  naming it rather than searching from the copy's root. A guest lent the
  copy writable, a model's shell among them, can rewrite the `.git` file at
  its root to point at a repository it laid beneath, which the search
  would have followed and read the copy's head and changes from; now that
  file is never read, and what the copy seals is the repository's it was
  cut from, the planted repository one untracked directory. A copy another
  process laid is addressed as any place is.

### Fixed

- `omnia-git` applies a merge rule only to a path both sides changed
  since their common ancestor. It had applied one to every path the two
  sides differ on, so a file one side alone added under an `ours` or
  `theirs` rule — a lockfile the merged-in slice laid for the first time
  — was removed from the merge as the side that lacked it, and the
  removal itself refused, since git had staged the addition cleanly and
  `rm` drops staged content only under `--force`, which it now carries.
  A rename one side made is followed as the merge follows it: the other
  side's change to the old path counts as a change to the new, so a rule
  over both names keeps one side whole — `ours` the old name with its
  bytes, `theirs` the new with its — where the new name had looked like a
  one-sided addition and stayed as git blended it, beside the old name a
  rule restored. Without a common ancestor every differing path is still
  ruled.

### Removed

- `omnia-filesystem`'s plugin store. omnia's `FsStore`, named by a
  deployment's `plugins.store`, is the local package store; the backend
  serves `wasi:blobstore` and `wasi:keyvalue` alone, and depends on
  `omnia-core` and `omnia-plugin` no longer.

---

Release notes for previous releases can be found on the respective release
branches of the repository.

<!-- ARCHIVE_START -->
* [0.31.x](https://github.com/augentic/omnia-backends/blob/release-0.31.0/RELEASES.md)
* [0.30.x](https://github.com/augentic/omnia-backends/blob/release-0.30.0/RELEASES.md)
* [0.28.x](https://github.com/augentic/omnia-backends/blob/release-0.28.0/RELEASES.md)
* [0.27.x](https://github.com/augentic/omnia-backends/blob/release-0.27.0/RELEASES.md)
* [0.26.x](https://github.com/augentic/omnia-backends/blob/release-0.26.0/RELEASES.md)

- [0.25.x](https://github.com/augentic/omnia-backends/blob/release-0.25.0/RELEASES.md)
- [0.24.x](https://github.com/augentic/omnia-backends/blob/release-0.24.0/RELEASES.md)
- [0.23.x](https://github.com/augentic/omnia-backends/blob/release-0.23.0/RELEASES.md)
