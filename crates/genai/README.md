# omnia-genai

[![crates.io](https://img.shields.io/crates/v/omnia-genai.svg)](https://crates.io/crates/omnia-genai)
[![docs.rs](https://docs.rs/omnia-genai/badge.svg)](https://docs.rs/omnia-genai)

Multi-provider generative-AI model backend for the Omnia WASI runtime,
implementing the `omnia:model/completion` boundary (`wasi-model`).

Wraps the [`genai`](https://crates.io/crates/genai) SDK (`OpenAI`, Anthropic,
Gemini, Groq, Ollama, …). The backend maps the host-validated `Request`
(`system` / `messages` channels) to a provider chat request, advertising the
request's declared function tools — plus the host-injected `read`/`list`
workspace tools when the guest lent a workspace through `grants.workspace`.
The in-process tool loop is driven to completion: `read`/`list` execute
host-side through the lent `ToolHost` workspace capability (bounded by the
host; results must be UTF-8 text under the per-result byte cap, and failures
such as a missing file are fed back to the model as repairable text), while
every other model tool call is forwarded through `ToolHost::call_tool` to the
guest's session handler. The request's `format` rides as the provider
`response_format` — steering only; nothing here validates the answer. When
the request sets `check`, each final text the model produces is offered to
the guest through `ToolHost::check`: `Ok` ends the completion with that
candidate, `Err(correction)` appends the candidate and the guest's correction
to the conversation verbatim and the loop goes round. Workspace reads, tool
calls, and check rounds share one bounded round budget (eight provider
round-trips); a rejection on the last round fails the completion with the
typed `budget-exhausted` carrying that correction. Without a `check` the
guest sees the model's final text as-is.

MSRV: Rust 1.97

## Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `GENAI_MODEL` | no | `gpt-5.5` | Default model id when a request leaves `model` unset |
| `GENAI_ENDPOINT` | no | | Base URL every request goes to in place of the provider's own (a self-hosted gateway or proxy) |
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
gateway expects. The URL must be `http://` or `https://`; `connect` rejects
anything else before a request is made.

`Client::connect()` / `FromEnv` reads the optional `GENAI_MODEL` and
`GENAI_ENDPOINT`; callers that need a different default model or endpoint
pass `ConnectOptions` to `connect_with`.

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
answers by a script (`Echo`, `Replies`, `ToolCalls`), and can gate requests
until a fan-out is fully in flight, park replies until the test releases
them, or fail every request one way (`Status` with an optional
`Retry-After`, `TruncatedBody`).

**Guests through the runtime.** Every scenario is a guest component from
[`crates/test-programs`](../test-programs) run through
`omnia_test::host::Deployment` over an `omnia_genai::Client`; the test then
asserts what reached the provider. [`tests/model.rs`](tests/model.rs) is one
row per guest program (`foreach_model!` fails to compile when a program has
no row): echo, the three `check` outcomes (the rejected candidate and the
correction become the next round's turns; eight rounds exhaust), a tool
round-trip through `OpenAI` `tool_calls`, a repairable tool failure, an
undeclared tool refused by the host, a four-way fan-out seen in flight at
once, tool fan-out with each conversation carrying its own result, a
fan-out whose losers are dropped mid-request (their requests abandoned, no
further ones made), and a provider failure reaching the guest.
[`tests/provider.rs`](tests/provider.rs) is the fault and seam matrix: a
429 with `Retry-After` and a 503 (neither retried; the status and the
provider's message reach the guest), a truncated JSON body, an endpoint
without its trailing slash, and the URLs `connect` rejects. Guests are
compiled by the `test-programs` build script; there is no separate
`--target` build to run.

**The real provider.** [`tests/live.rs`](tests/live.rs) drives real
completions through the `wasi-model` boundary: the in-process tool loop
with function-tool dispatch, the host-injected `read`/`list` workspace
tools (the model discovers and reads a file the prompt never names), and
the guest `check` loop (a stand-in check rejects the first candidate with a
correction the model must follow, and one that rejects every candidate
proves the typed `budget-exhausted`). They are `#[ignore]`d so they never
touch the network in CI; run them with a provider key:

```bash
OPENAI_API_KEY=... \
  cargo nextest run -p omnia-genai --all-features --run-ignored all
```

## License

MIT OR Apache-2.0
