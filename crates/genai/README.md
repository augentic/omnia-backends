# omnia-genai

[![crates.io](https://img.shields.io/crates/v/omnia-genai.svg)](https://crates.io/crates/omnia-genai)
[![docs.rs](https://docs.rs/omnia-genai/badge.svg)](https://docs.rs/omnia-genai)

Multi-provider generative-AI model backend for the Omnia WASI runtime,
implementing the `omnia:model/completion` boundary (`wasi-model`).

Wraps the [`genai`](https://crates.io/crates/genai) SDK (`OpenAI`, Anthropic,
Gemini, Groq, Ollama, …). The backend maps the host-validated `Request`
(`system` / `messages` channels) to a provider chat request, advertising the
request's declared function tools — plus the host-injected `read`/`list`
workspace tools when the guest lent a workspace through `grants.workspace`,
and the `shell` tool when that workspace sits under one of the configured
shell roots. The in-process tool loop is driven to completion: `read`/`list`
execute host-side through the lent `ToolHost` workspace capability (bounded
by the host; results must be UTF-8 text under the per-result byte cap, and
failures such as a missing file are fed back to the model as repairable
text), `shell` runs a command inside the completion's sandbox session (see
[The shell](#the-shell)), while every other model tool call is forwarded
through `ToolHost::call_tool` to the guest's session handler. The request's
`format` rides as the provider `response_format` — steering only; nothing
here validates the answer. When the request sets `check`, each final text
the model produces is offered to the guest through `ToolHost::check`: `Ok`
ends the completion with that candidate, `Err(correction)` appends the
candidate and the guest's correction to the conversation verbatim and the
loop goes round; where JSON was asked for, a reply that is not one
well-formed JSON value, or that is JSON but does not fit the request's
schema, is nudged once per candidate, with what the parser or the schema
said of it (each fault at its path), before any candidate reaches the guest
— in closing rounds that ask for the provider's JSON mode in place of the
schema, since a provider that ignores the schema answers prose to it, with
the tools still declared so a provider that caches the prompt by its prefix
keeps the cache, and a tool call made there refused rather than run, the
schema returning on a correction round. A call to a tool the request never
declared is answered as the model's slip — a tool failure saying the name
is not declared and listing the tools that are, one round lost — in place
of the host's refusal, which would end the completion. Every tool's result
reaches the model cut to 64 kibibytes, its head and its tail kept with the
omitted count between them, so one result cannot fill the context window
where the host's cap of a mebibyte would let it. Four budgets bound
a completion,
and any spent is the typed `budget-exhausted`: every provider round-trip —
a tool round, a candidate, a nudge, or a correction — draws on `max_rounds`
(default 128), the last round of which forbids tool calls with the tools
still declared (`tool_choice` `none`), so a model that would act once more
answers the question instead where the provider honours it and a call made
there is refused as in a closing round; the guest's `check` is offered at most `max_candidates`
candidates (default 2, the count the `omnia-cursor` backend keeps), every
one but the last corrected once and the rejection of the last failing the
completion with that correction as the detail, and where a `check` stands
a quarter of the rounds are kept for its corrections: once the rest are
spent with no candidate offered (round 96 of 128, 48 of 64; a budget too
small to share is due at its cap), every round forbids tool calls the same
way until a candidate reaches the check, a call made there refused as
kept for the corrections, so the correction it earns has rounds to repair
in where a candidate forced at the cap would be refused with none; a tool call that fails the
same way with the same arguments is counted, the model told the count from
the third, and `max_repeats` such failures (default 5) end the completion
naming the call, since a model repeating one refused call is not going to
answer, while a call that succeeds with the same result as the identical
calls before it is counted the same way and `max_repeats` such results make
the next round a finishing one, its tool calls forbidden and refused, since
a model re-running one call whose result does not change has what it needs
to answer, the count starting over at a correction; each reply of the
model, the tool rounds before it included, runs
under the `timeout_secs` cap (default 600), a nudge or a correction
starting a fresh one, so a provider that never answers or a build that
never ends holds no completion for longer; the host's per-completion
tool-call limit is answered through `WasiModelCtx::limits()` as
`max_tool_calls`. Every round carries a `max_tokens`, the request's own or
`GENAI_MAX_TOKENS` (default 32768), so a reply that runs on is cut short of
the context window rather than filling it; a reply the provider cut there
— by its finish reason, or by a bill reaching the bound where it reports
none — has its tool calls refused rather than run, each answered with the
cap and told to send smaller calls, since the arguments a server hands
over from a cut reply are not the ones the model wrote. A request that fails at the
transport — the connection, before any reply: a server closing an idle
keep-alive connection as the request is written on it, a tunnel between
dropping — is put again unchanged after a wait of 1, 5, then 15 seconds,
since a chat request applies nothing until its reply is read, and only
past the third is the failure the completion's (`transport`); the
client's pooled connections are dropped after 2 seconds idle, inside the
5 seconds uvicorn (vLLM's server) keeps one, so the quick rounds reuse a
connection and a tool round of a few seconds never hands a request to a
socket the server is closing. Without a `check` the guest
sees the model's final text as-is. Whatever the request gets wrong — a
reserved tool name, a worker grant with no shell to open — is refused
before any of this starts, and before anything is billed.

MSRV: Rust 1.99

## Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `GENAI_MODEL` | no | `gpt-5.5` | Default model id when a request leaves `model` unset |
| `GENAI_ENDPOINT` | no | | Base URL every request goes to in place of the provider's own (a self-hosted gateway or proxy) |
| `GENAI_SHELL_ROOTS` | no | | Trees a lent workspace at or beneath which runs with the `shell` tool, `:`-separated, relative to the invocation directory; none configures no shell |
| `GENAI_MAX_ROUNDS` | no | `128` | Provider round-trips one completion may make before it is ended as `budget-exhausted`; under a `check`, the first candidate is due once three quarters of them are spent, the rest kept for the corrections |
| `GENAI_MAX_TOOL_CALLS` | no | `128` | Guest tool calls one completion may make before `budget-exhausted` (the host's default is 32) |
| `GENAI_MAX_CANDIDATES` | no | `2` | Candidates one completion may offer the guest's `check`; the rejection of the last is `budget-exhausted` carrying the correction |
| `GENAI_MAX_REPEATS` | no | `5` | Failures one tool call may earn with the same arguments before the completion is `budget-exhausted` naming the call, and identical results one succeeding call may return before the next round is the model's to answer in; the model is told the count from the third |
| `GENAI_MAX_TOKENS` | no | `32768` | Tokens one reply of the model may decode in one round where the request names no `max_tokens` of its own; the tool calls of a reply cut there are refused, not run |
| `GENAI_TIMEOUT_SECS` | no | `600` | Absolute cap in seconds on one reply of the model, tool rounds included, before the completion is ended as `budget-exhausted` |
| `GENAI_SHELL_TIMEOUT_SECS` | no | `600` | Seconds one `shell` command may run before it is killed |
| `OPENAI_API_KEY` | per provider | | `OpenAI` API key, read by genai from the ambient environment |
| `ANTHROPIC_API_KEY` | per provider | | Anthropic API key |
| `GEMINI_API_KEY` | per provider | | Gemini API key |
| (other provider keys) | per provider | | Any key the [`genai`](https://crates.io/crates/genai) SDK supports (Groq, Ollama, …) |

The provider model id is taken from each request (`request.model`); an unset
value falls back to `GENAI_MODEL`, else `gpt-5.5`. genai routes to the
provider by the model id's prefix. Only the key for the provider a request
routes to is required, and keys are never logged or recorded.

`GENAI_ENDPOINT` is the self-hosted-gateway option: when set, every request
goes to that base URL (`<endpoint>/chat/completions`; a missing trailing
slash is supplied) instead of the provider's own. The request keeps the
shape of the provider the model id routes to, and auth is still that
provider's key from the environment — so an `OpenAI`-compatible gateway is
reached with an `OpenAI` model id and `OPENAI_API_KEY` holding whatever the
gateway expects. A local model is reached the same way, under the adapter
that speaks the server's protocol: a name genai knows no provider for routes
to its Ollama adapter, which speaks Ollama's own `api/chat` at the endpoint
and needs no key (`GENAI_MODEL=qwen3-coder
GENAI_ENDPOINT=http://127.0.0.1:11434/`), and sends no answer schema, so
the prompt alone steers the shape; an `OpenAI`-compatible server (vLLM,
llama.cpp's) is named under the adapter's namespace
(`GENAI_MODEL=openai::qwen3-coder GENAI_ENDPOINT=http://127.0.0.1:8000/v1/`)
with `OPENAI_API_KEY` holding whatever the server expects, any value where
it checks none, and receives the schema. A local server's context length is
its own setting, since no request carries one: Ollama's
`OLLAMA_CONTEXT_LENGTH` or a Modelfile's `num_ctx`, vLLM's
`--max-model-len`; a server left at its default silently truncates a long
prompt. The URL must be `http://` or `https://`; `connect` rejects anything
else before a request is made.

`Client::connect()` / `FromEnv` reads the optional variables above; callers
that need a different default model, endpoint, shell root, or budget pass
`ConnectOptions` to `connect_with`. A zero budget or timeout is refused at
connect.

## The shell

A workspace lent at or beneath one of `shell_roots` is advertised the
`shell` tool beside `read` and `list`, so a model can run the tree's own
install, build, and tests. Each call is `sh -c <command>` — `sh` resolved on
the host's `PATH`, never on one a grant set — run in the lend as its working
directory and as `$WORKSPACE`, inside the sandbox session
[`omnia-sandbox`](../sandbox) prepares for the completion: one session per
completion, shared by every command of it, so a `cargo check` and the
`cargo test` after it run in one scratch `HOME` with one cache, behind one
egress proxy. The result the model reads is what the command wrote, its
stderr after `--- stderr ---` when any, and its exit after `--- exit ---`
when not clean (`exit 3`, `killed by signal 9`; a silent clean command reads
`exit 0`); a result past the cap keeps its head and its
tail with the omitted count between them, since a log's start names the
step and its end holds the verdict; a command past `shell_timeout_secs` is
killed with everything it started and the model told so as a result it can
act on, and one still running as the completion's `timeout_secs` cap passes
is killed the same way, the completion ended. A `shell` call where no shell
was advertised is answered as the model's slip, not the completion's
failure.

What the shell reaches beyond the lend is the completion's `grants.worker`
— the `egress` hosts, `~/` `read` roots, and `env` the guest spells per
request, as the `omnia-cursor` backend reads them — prepared as the
session's `Grant` under the sandbox's own rules: a grant over a lend under
no shell root, over no lend, or under a deployment with no shell root
configured is `invalid-request` before any request is made, and a grant the
sandbox denies (a root overlapping a credential store, a variable the
sandbox reserves) is `invalid-request` in the sandbox's words. The provider
keys the SDK reads (`OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, `GEMINI_API_KEY`,
`GROQ_API_KEY`, `XAI_API_KEY`, `DEEPSEEK_API_KEY`, `COHERE_API_KEY`,
`MISTRAL_API_KEY`, `OLLAMA_API_KEY`) are refused as a grant's `env`, and the
shell inherits `USER`, `LOGNAME`, `SSL_CERT_FILE`, and `SSL_CERT_DIR` of the
host's environment beyond what the sandbox sets. A guest tool named `shell`
is `invalid-request`, as one named `read` is the host's. With no shell root
configured no kernel boundary is set up and the backend runs as before.

## Observability

Everything a completion logs sits under its `complete` span (`n`, `model`,
`format`, and the schema name as `label` where there is one), in the shape
the `omnia-cursor` backend's log has.

- `completion started` (DEBUG) / `completion` (INFO on an answer, WARN
  otherwise), with the `outcome` — `ok`, `corrected`, `timeout`, `abort`,
  `exhausted`, `provider`, `transport`, or `error`, read off the failure's
  type, never its text — the `attempts` (the model's replies), the
  `retries` (requests put again after failing at the transport),
  `forced_at` (the round the first candidate was made due at, the rounds
  after it kept for the corrections; absent where it was offered before),
  `cut` (replies the provider cut at the token bound), the phase
  buckets `model_ms`, `tool_ms` (the shell included), and `check_ms`, and
  the bill every round of the completion together: `input_tokens`,
  `cache_read_tokens`, `cache_write_tokens`, `output_tokens`,
  `reasoning_tokens`, and `total_tokens` as the provider reports them.
  `round answered` (DEBUG) carries one round's share, and whether the
  provider `cut` it.
- `in progress` (INFO) every 15s while a completion is open: `elapsed_s`,
  `round`, `tool_calls`, `silent_s`, and what it is `waiting` on — the
  model, a tool by name, the shell, or the guest's check.
- `tool call` / `tool call completed` (DEBUG) with a `subject`: a `read` or
  `list` path, or a command's first line with the lend spelled `./`; the
  completed line adds `result_bytes`, `duration_ms`, and `exit` on a `shell`
  command. The arguments whole at TRACE under the target
  `omnia_genai::tools`, so `RUST_LOG=omnia_genai=debug,omnia_genai::tools=trace`
  reads them without the rest of the trace.

## Usage

Bind the backend in your host's `runtime!` map — the guest `.wasm` is untouched
(see the [Production Backends guide](https://github.com/augentic/omnia/blob/main/docs/guides/production-backends.md)):

```rust,ignore
use omnia_genai::Client as GenAi;
use omnia_wasi_model::WasiModel;

omnia::runtime!({
    hosts: {
        WasiModel: GenAi,
    }
});
```

For direct or embedded use, connect it yourself:

```rust,ignore
use omnia::{Backend, FromEnv};
use omnia_genai::{Client, ConnectOptions};

// GENAI_MODEL and GENAI_ENDPOINT when set; else `gpt-5.5` at the
// provider's own endpoint.
let client = Client::connect().await?;

// Explicit default model for requests that leave `model` unset, through a
// self-hosted gateway.
let client = Client::connect_with(ConnectOptions {
    model: "claude-fable-5".into(),
    endpoint: Some("https://gateway.internal/anthropic/v1/".into()),
    ..ConnectOptions::default()
}).await?;

// A local model behind an OpenAI-compatible server, with a shell on the
// trees a build lends.
let client = Client::connect_with(ConnectOptions {
    model: "openai::qwen3-coder".into(),
    endpoint: Some("http://127.0.0.1:8000/v1/".into()),
    shell_roots: vec![".emery/vcs/worktrees".into(), ".emery/vcs/integration".into()],
    ..ConnectOptions::load_env()?
}).await?;
```

## Tests

Two tiers run on every `cargo nextest run -p omnia-genai --all-features`,
with no provider and no real key; the third is a real provider, by hand.

**The fake provider.** [`tests/support/fake_openai`](tests/support/fake_openai)
is a scripted `OpenAI`-compatible chat-completions endpoint served
in-process on loopback, reached through `ConnectOptions::endpoint` — the
same seam a self-hosted gateway uses, so the SDK's own request shaping and
auth are what is under test. It records every request (path, bearer, body),
answers by a script (`Echo`, `Replies`, `Rounds` — a text, a call, or a
call cut at the request's `max_tokens` per round — `ToolCalls` — a list of
calls answered one per round, then a scripted answer or the last result), can
gate requests until a fan-out is fully in flight, park replies until the
test releases them, or fail every request one way (`Status` with an
optional `Retry-After`, `TruncatedBody`), and bills every reply with one
fixed usage block when a row opts in (`billed()`), so the bill a run
reports is that block times the requests answered.

**Guests through the runtime.** Every scenario is a guest component from
[`crates/test-programs`](../test-programs) run through
`omnia_test::host::Deployment` over an `omnia_genai::Client`; the test then
asserts what reached the provider, and, where the row installs a capture
of the backend's own log (`tests/support/capture.rs`), what the one
`completion` line said of the run. [`tests/model.rs`](tests/model.rs) is one
row per guest program (`foreach_model!` fails to compile when a program has
no row): echo, the three `check` outcomes (the rejected candidate and the
correction become the next round's turns, the completion `corrected` and
both rounds on its bill; the second rejection exhausts), a narration reply
nudged before the JSON that passes (two attempts, `ok`, the closing round
under JSON mode with the tools still declared), a JSON reply off the schema
nudged with the fault at its path, a tool call in a closing round refused
and the JSON after it accepted, a second narration after a correction
nudged again (the schema back on the correction round), the nudge spending
the last round, the round budget spent on a model that only calls tools
(`exhausted`), the first candidate made due at three quarters of the
rounds under a check (the reserved round forbidding tool calls and refusing
one made, the correction's rounds open to tools again, `forced_at` on the
completion line) and never under none, a call cut at the token bound
refused rather than run with the round after it open (`cut=1`), a tool
round-trip through `OpenAI` `tool_calls`, a repairable
tool failure, an undeclared tool answered with the declared names and the
completion going on, one call failing the same way until the repeat budget
ends it (`exhausted`, the count named from the third), the repeat budget
lowered and calls differing in their arguments counted apart, one call
returning the same result until the repeat budget makes the next round the
model's to answer in (`ok`, the count named from the third) and that count
starting over at a correction, a result past the backend's cap cut to its
head and tail, the default `max_tokens` on every round and the knob behind
it, a four-way
fan-out
seen in flight at once, tool fan-out with each conversation carrying its
own result, a fan-out whose losers are dropped mid-request (their requests
abandoned, no further ones made), a provider failure reaching the guest,
and a guest tool named `shell` refused. The shell rows run real commands
inside the sandbox over a lend under a shell root: the grant's variable,
the proxy, `$WORKSPACE`, and the working directory seen from inside;
nothing of the host's environment, the provider key among it; a failing
exit and a signal rendered; a command past the bound killed with the
process it forked; a command the completion's cap outlives killed the same
way (`timeout`, one request); two commands sharing one session's `HOME`; a
shell call with no shell advertised answered as the model's slip; and the
refusals — a lend outside the roots, no root configured, and a grant the
sandbox denies. [`tests/provider.rs`](tests/provider.rs) is the fault and
seam matrix: a 429 with `Retry-After` and a 503 (neither retried; the
status and the provider's message reach the guest; `provider`), a
truncated JSON body (`transport`), a connection the server closed as the
request was written on it (put again unchanged after a wait, `ok` with
`retries=1`, one request read by the fake), a reply the cap outlives
(abandoned at the fake, `timeout`), an endpoint without its trailing
slash, the URLs
`connect` rejects, and the zero budgets it rejects. Guests are compiled by
the `test-programs` build script; there is no separate `--target` build to
run.

**The real provider.** [`tests/live.rs`](tests/live.rs) drives real
completions through the `wasi-model` boundary: the in-process tool loop
with function-tool dispatch, the host-injected `read`/`list` workspace
tools (the model discovers and reads a file the prompt never names), the
shell (the model runs `cargo --version` under the Rust toolchain's grant
and answers with its first line), and the guest `check` loop (a stand-in
check rejects the first candidate with a correction the model must follow,
and one that rejects every candidate proves the typed `budget-exhausted`).
They are `#[ignore]`d so they never touch the network in CI; run them with
a provider key:

```bash
OPENAI_API_KEY=... \
  cargo nextest run -p omnia-genai --all-features --run-ignored all
```

## License

MIT OR Apache-2.0
