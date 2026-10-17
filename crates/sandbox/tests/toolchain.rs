//! What a build's toolchain does inside the kernel boundary:
//!
//! - `node` serving and fetching on loopback
//! - `node --test` with a worker thread
//! - `npm` installing from the tree with its cache in the scratch
//!
//! The rows need `node` and `npm` on `PATH`, as omnia-git's need `git`. A
//! toolchain under `$HOME` is asked for as a `~/` read root, as a target
//! adapter spells one.

#![cfg(any(target_os = "linux", target_os = "macos"))]

mod support;

use std::path::{Path, PathBuf};
use std::process::Output;
use std::time::Duration;

use omnia_sandbox::{Grant, Options, Sandbox};

const BOUND: Duration = Duration::from_secs(120);
const LIVE_BOUND: Duration = Duration::from_secs(300);
// Pinned, so the live row installs the same bytes run to run.
const TYPESCRIPT: &str = "5.9.3";

const SERVE: &str = r#"import { createServer } from "node:http";
const server = createServer((request, response) => response.end(`served ${request.url}`));
server.listen(0, "127.0.0.1", async () => {
  const { port } = server.address();
  const response = await fetch(`http://127.0.0.1:${port}/ping`);
  console.log(response.status, await response.text());
  server.close();
});
"#;

const WORKER_TEST: &str = r#"import { test } from "node:test";
import assert from "node:assert/strict";
import { Worker } from "node:worker_threads";

test("a worker thread answers", async () => {
  const worker = new Worker(
    "import { parentPort } from 'node:worker_threads'; parentPort.postMessage(21 * 2);",
    { eval: true },
  );
  const answer = await new Promise((resolve, reject) => {
    worker.once("message", resolve);
    worker.once("error", reject);
  });
  assert.equal(answer, 42);
});
"#;

const INDEX_TS: &str = r#"export function greet(name: string): string {
  return `hello ${name}`;
}

const answer: number = greet("tsc").length;
console.log(answer);
"#;

const TSCONFIG: &str = r#"{
  "compilerOptions": {
    "strict": true,
    "target": "ES2022",
    "module": "NodeNext",
    "moduleResolution": "NodeNext",
    "verbatimModuleSyntax": true,
    "noEmit": true
  },
  "include": ["src"]
}
"#;

// A workspace holding `files`, each at its path beneath the tree.
fn tree(files: &[(&str, &str)]) -> (tempfile::TempDir, PathBuf) {
    let workspace = tempfile::tempdir().expect("a workspace");
    let tree = workspace.path().join("tree");
    for (path, text) in files {
        let path = tree.join(path);
        std::fs::create_dir_all(path.parent().expect("a parent")).expect("the directory");
        std::fs::write(path, text).expect("the file");
    }
    (workspace, tree)
}

// Where `program` on the host's PATH lives.
fn on_path(program: &str) -> PathBuf {
    let path = std::env::var_os("PATH").expect("the host has a PATH");
    std::env::split_paths(&path)
        .map(|dir| dir.join(program))
        .find(|candidate| candidate.is_file())
        .unwrap_or_else(|| {
            panic!("`{program}` is not on PATH; the toolchain rows need node and npm")
        })
}

// The host's home, canonical: what the toolchain's `~/` spells.
fn home() -> PathBuf {
    let home = std::env::var_os("HOME").map(PathBuf::from).expect("the host has a home");
    std::fs::canonicalize(home).expect("the home resolves")
}

async fn connect() -> Sandbox {
    support::connect(Options::default()).await
}

// The install prefix of each tool under the host's `$HOME`, spelled
// `~/`-relative. The scratch `HOME` hides it, and `<prefix>/bin/<tool>` is
// how nvm, volta, and fnm lay a tool.
fn toolchain_roots() -> Vec<String> {
    let home = home();
    let mut roots = Vec::new();
    for tool in ["node", "npm"] {
        let bin = on_path(tool).parent().expect("a directory").to_owned();
        let bin = std::fs::canonicalize(&bin).expect("the directory resolves");
        let prefix = bin.parent().expect("a prefix");
        if let Ok(beneath) = prefix.strip_prefix(&home) {
            let root = format!("~/{}", beneath.display());
            if !roots.contains(&root) {
                roots.push(root);
            }
        }
    }
    roots
}

// The toolchain's grant: its prefixes under the home, and `egress`.
fn toolchain(egress: Vec<String>) -> Grant {
    Grant {
        egress,
        read: toolchain_roots(),
        ..Grant::default()
    }
}

async fn run(
    sandbox: &Sandbox, grant: &Grant, tree: &Path, script: &str, bound: Duration,
) -> Output {
    let session = support::granted(sandbox, Some(tree), grant).await;
    support::sh(session, script).output(bound).await.expect("the shell answers")
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).trim().to_owned()
}

#[track_caller]
fn passed(output: &Output, what: &str) {
    assert!(
        output.status.success(),
        "{what}: {}\n--- stdout ---\n{}\n--- stderr ---\n{}",
        output.status,
        text(&output.stdout),
        text(&output.stderr)
    );
}

#[tokio::test]
async fn node_loopback() {
    let (_workspace, tree) = tree(&[("serve.mjs", SERVE)]);
    let sandbox = connect().await;

    let output = run(&sandbox, &toolchain(vec![]), &tree, "node serve.mjs", BOUND).await;
    passed(&output, "a server on port 0, fetched from inside");
    assert_eq!(text(&output.stdout), "200 served /ping");
}

#[tokio::test]
async fn node_test() {
    let (_workspace, tree) = tree(&[("test/worker.test.mjs", WORKER_TEST)]);
    let sandbox = connect().await;

    let script = "node --test --test-force-exit --test-timeout=5000 --test-reporter=tap \
                  test/worker.test.mjs";
    let output = run(&sandbox, &toolchain(vec![]), &tree, script, BOUND).await;
    passed(&output, "node --test with a worker thread");
    let report = text(&output.stdout);
    assert!(report.contains("ok 1 - a worker thread answers"), "{report}");
    assert!(report.contains("# pass 1"), "{report}");
    assert!(report.contains("# fail 0"), "{report}");
}

#[tokio::test]
async fn npm_local_install() {
    let (_workspace, tree) = tree(&[
        (
            "package.json",
            r#"{ "name": "app", "private": true, "version": "0.0.0", "dependencies": { "dep": "file:./dep" } }"#,
        ),
        ("dep/package.json", r#"{ "name": "dep", "version": "1.0.0", "main": "index.js" }"#),
        ("dep/index.js", "module.exports = \"dep answers\";\n"),
        ("index.js", "console.log(require(\"dep\"));\n"),
    ]);
    let sandbox = connect().await;
    let session = support::granted(&sandbox, Some(&tree), &toolchain(vec![])).await;
    let cache = session.scratch().join(".npm");

    let script = "npm install --no-audit --no-fund >/dev/null && node index.js && npm config get \
                  cache && test -d \"$HOME/.npm\" && echo cached";
    let output = support::sh(session, script).output(BOUND).await.expect("the shell answers");
    passed(&output, "npm install of a file: dependency");
    assert_eq!(
        text(&output.stdout),
        format!("dep answers\n{}\ncached", cache.display()),
        "the cache is the scratch's, not the host's"
    );
    assert!(tree.join("node_modules/dep/package.json").exists(), "node_modules in the tree");
    assert!(tree.join("package-lock.json").exists(), "package-lock.json in the tree");
}

// The registry through the proxy: `npm install` of a pinned `typescript` and
// `tsc --noEmit` over a source, the toolchain's own checks as a build turn
// runs them. Needs the network, so run by hand, reading `log stream` for
// denials on macOS.
#[tokio::test]
#[ignore = "reaches registry.npmjs.org through the proxy"]
async fn npm_registry_install() {
    let (_workspace, tree) = tree(&[
        (
            "package.json",
            r#"{ "name": "app", "private": true, "version": "0.0.0", "type": "module" }"#,
        ),
        ("tsconfig.json", TSCONFIG),
        ("src/index.ts", INDEX_TS),
    ]);
    let sandbox = connect().await;
    let grant = toolchain(vec!["registry.npmjs.org".to_owned()]);

    let script = format!(
        "npm install --no-audit --no-fund typescript@{TYPESCRIPT} >/dev/null && node -p \
         'require(\"./node_modules/typescript/package.json\").version' && npx tsc --noEmit && echo \
         checked"
    );
    let output = run(&sandbox, &grant, &tree, &script, LIVE_BOUND).await;
    passed(&output, "npm install typescript and tsc through the proxy");
    assert_eq!(text(&output.stdout), format!("{TYPESCRIPT}\nchecked"));
}
