# omnia-genai

[![crates.io](https://img.shields.io/crates/v/omnia-genai.svg)](https://crates.io/crates/omnia-genai)
[![docs.rs](https://docs.rs/omnia-genai/badge.svg)](https://docs.rs/omnia-genai)

Multi-provider generative-AI model backend for the Omnia WASI runtime,
implementing the `omnia:model/completion` boundary (`wasi-model`).

Wraps the [`genai`](https://crates.io/crates/genai) SDK (`OpenAI`, Anthropic,
Gemini, Groq, Ollama, …). The backend maps the gate-validated `Request`
(`system` / `messages` channels) to a provider chat request, advertising the
request's declared function tools — plus the host-injected `read`/`list`
workspace tools when the guest lent a workspace through `grants.workspace`.
The in-process tool loop is driven to completion: `read`/`list` execute
host-side through the lent `ToolHost` workspace capability (bounded by the
host; results must be UTF-8 text under the per-result byte cap, and failures
such as a missing file are fed back to the model as repairable text), while
every other model tool call is forwarded through `ToolHost::call_tool` to the
guest's session handler. Workspace reads share the completion's bounded turn
budget with tool calls and answer repair. The guest only ever sees the
validated answer string.

MSRV: Rust 1.97

## Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `OPENAI_API_KEY` | per provider | | `OpenAI` API key, read by genai from the ambient environment |
| `ANTHROPIC_API_KEY` | per provider | | Anthropic API key |
| `GEMINI_API_KEY` | per provider | | Gemini API key |
| (other provider keys) | per provider | | Any key the [`genai`](https://crates.io/crates/genai) SDK supports (Groq, Ollama, …) |

The provider model id is carried per-request (`request.model`); when a request
leaves it unset the backend falls back to `gpt-5.5`. genai routes to the
provider by the model id's prefix. Only the key for the provider a request
routes to is required, and keys are never logged or recorded.

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
use omnia_genai::Client;

let client = Client::connect().await?;
```

## Live tests

[`tests/live.rs`](tests/live.rs) drives real completions through the `wasi-model`
boundary: the in-process tool loop with function-tool dispatch, and the
host-injected `read`/`list` workspace tools (the model discovers and reads a
file the prompt never names). They are `#[ignore]`d so they never touch the
network in CI; run them with a provider key:

```bash
OPENAI_API_KEY=... \
  cargo nextest run -p omnia-genai --run-ignored all
```

## License

MIT OR Apache-2.0
