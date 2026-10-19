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
loop goes round. Two budgets bound a completion, and either spent is the
typed `budget-exhausted`: every provider round-trip — a tool round, a
candidate, or a correction — draws on `max_rounds` (default 128), and a
rejected `check` candidate is corrected once, the second rejection failing
the completion with that correction as the detail, the same count the
`omnia-cursor` backend keeps; the host's per-completion tool-call limit is
answered through `WasiModelCtx::limits()` as `max_tool_calls`. Without a
`check` the guest sees the model's final text as-is.

MSRV: Rust 1.99

## Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `GENAI_MODEL` | no | `gpt-5.5` | Default model id when a request leaves `model` unset |
| `GENAI_ENDPOINT` | no | | Base URL every request goes to in place of the provider's own (a self-hosted gateway or proxy) |
| `GENAI_SHELL_ROOTS` | no | | Trees a lent workspace at or beneath which runs with the `shell` tool, `:`-separated, relative to the invocation directory; none configures no shell |
| `GENAI_MAX_ROUNDS` | no | `128` | Provider round-trips one completion may make before it is ended as `budget-exhausted` |
| `GENAI_MAX_TOOL_CALLS` | no | `128` | Guest tool calls one completion may make before `budget-exhausted` (the host's default is 32) |
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
it checks none, and receives the schema. The URL must be `http://` or
`https://`; `connect` rejects anything else before a request is made.

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
`exit 0`); a result past the host's per-result cap keeps its head and its
tail with the omitted count between them, since a log's start names the
step and its end holds the verdict; a command past `shell_timeout_secs` is
killed with everything it started and the model told so as a result it can
act on. A `shell` call where no shell was advertised is answered as the
model's slip, not the completion's failure.

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

The log carries every tool call as the `omnia-cursor` backend's does — `tool
call tool=… subject=…` before it, `tool call completed tool=… subject=…
result_bytes=… duration_ms=…` after, with `exit=…` on a `shell` command; the
subject is a `read` or `list` path, or the command's first line with the
lend spelled `./` — and the `completion` line carries `model_ms` and
`tool_ms`, the time waited on the provider and the time spent in tool
rounds, beside the token counts.

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
use omnia::Backend;
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
answers by a script (`Echo`, `Replies`, `ToolCalls` — a list of calls
answered one per round, then a scripted answer or the last result), and can
gate requests until a fan-out is fully in flight, park replies until the
test releases them, or fail every request one way (`Status` with an
optional `Retry-After`, `TruncatedBody`).

**Guests through the runtime.** Every scenario is a guest component from
[`crates/test-programs`](../test-programs) run through
`omnia_test::host::Deployment` over an `omnia_genai::Client`; the test then
asserts what reached the provider. [`tests/model.rs`](tests/model.rs) is one
row per guest program (`foreach_model!` fails to compile when a program has
no row): echo, the three `check` outcomes (the rejected candidate and the
correction become the next round's turns; the second rejection exhausts),
the round budget spent on a model that only calls tools, a tool round-trip
through `OpenAI` `tool_calls`, a repairable tool failure, an undeclared tool
refused by the host, a four-way fan-out seen in flight at once, tool fan-out
with each conversation carrying its own result, a fan-out whose losers are
dropped mid-request (their requests abandoned, no further ones made), a
provider failure reaching the guest, and a guest tool named `shell` refused.
The shell rows run real commands inside the sandbox over a lend under a
shell root: the grant's variable, the proxy, `$WORKSPACE`, and the working
directory seen from inside; nothing of the host's environment, the provider
key among it; a failing exit and a signal rendered; a command past the
bound killed with the process it forked; two commands sharing one session's
`HOME`; a shell call with no shell advertised answered as the model's slip;
and the refusals — a lend outside the roots, no root configured, and a
grant the sandbox denies. [`tests/provider.rs`](tests/provider.rs) is the
fault and seam matrix: a 429 with `Retry-After` and a 503 (neither retried;
the status and the provider's message reach the guest), a truncated JSON
body, an endpoint without its trailing slash, the URLs `connect` rejects,
and the zero budgets it rejects. Guests are compiled by the `test-programs`
build script; there is no separate `--target` build to run.

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
