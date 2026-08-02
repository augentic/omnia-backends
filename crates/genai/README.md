# omnia-genai

[![crates.io](https://img.shields.io/crates/v/omnia-genai.svg)](https://crates.io/crates/omnia-genai)
[![docs.rs](https://docs.rs/omnia-genai/badge.svg)](https://docs.rs/omnia-genai)

Multi-provider generative-AI model backend for the Omnia WASI runtime,
implementing the `omnia:model/completion` boundary (`wasi-model`).

Wraps the [`genai`](https://crates.io/crates/genai) SDK (`OpenAI`, Anthropic,
Gemini, Groq, Ollama, …). The backend maps the gate-validated `Request`
(`system` / `messages` channels) to a provider chat request; the
in-process tool loop is driven to completion, and the runtime core's
`resolve` tool is dispatched into the guest's `references` shelf via the lent
`ToolHost`. The guest only ever sees the validated answer string.

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

[`tests/live.rs`](tests/live.rs) drives a real completion through the `wasi-model`
boundary, exercising the in-process tool loop and `resolve` dispatch. It is
`#[ignore]`d so it never touches the network in CI; run it with a provider key:

```bash
OPENAI_API_KEY=... \
  cargo nextest run -p omnia-genai --run-ignored all
```

## License

MIT OR Apache-2.0
