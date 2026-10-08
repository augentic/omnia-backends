# Omnia Git

Git backend for the `omnia:vcs` interface ([`omnia-wasi-vcs`](https://github.com/augentic/omnia/tree/main/crates/wasi-vcs)): every operation is one `git` process, spawned over the operator's own `git` binary in the directory the runtime opened for the guest's location. The child enters that open handle before it executes (`fchdir`), so git works where the runtime resolved the location and never walks a path a guest could redirect meanwhile; the one path git is handed is a working copy's destination for `worktree add`, which git records in the repository.

## Operations

| Interface | Operation | What runs |
| --------- | --------- | --------- |
| `store` | `resolve` | `rev-parse --verify <revision>^{commit}` |
| `store` | `head` | `rev-parse --verify HEAD^{commit}`; an unborn branch is `not-found` |
| `store` | `commit` | `status --porcelain`, then `add -A` and `commit -m`; nothing pending answers no commit |
| `store` | `merge` | `merge --no-ff -m` under the policy, the conflicts no rule resolved read back and the merge aborted |
| `workspace` | `init` | `init` in the place; a repository already there is `exists` |
| `workspace` | `add` | `worktree add --detach <at> <revision>` in the repository, `<at>` the destination's absolute path |
| `workspace` | `remove` | `worktree remove --force .` in the working copy; pending changes go with it |
| `workspace` | `pending` | `status --porcelain -z --no-renames --untracked-files=all`, each path once |
| `transport` | `clone` | `clone [--depth <n> --no-single-branch] <url> .` in the place; a depth cuts history, never the labels |
| `transport` | `fetch` | `fetch <remote>` |
| `transport` | `label` | `branch -f <name> <revision>` |
| `transport` | `push` | `push <remote> <label>` |

A merge policy reaches git as attribute lines in a file beside the tree, named through `core.attributesFile` on the merge command line alone, with `union` as git's own driver and `ours` / `theirs` as drivers declared the same way: nothing is written into the repository, so a policy never shows up in `pending` and never outlives its merge.

Each process runs with `GIT_DIR`, `GIT_WORK_TREE`, `GIT_COMMON_DIR`, and `GIT_INDEX_FILE` removed from the inherited environment, so the operator's shell cannot point an operation at another repository; their configuration, credential helpers, and SSH agent apply as they would at a prompt. What git reports is read into the typed `omnia:vcs` error — `not-a-repository` (a place holding no repository), `exists`, `not-found`, `access` — and anything else reaches the guest as `other` with git's own words. A location that does not exist is the runtime's refusal, before any git runs.

## Configuration

| Variable | Default | Purpose |
| -------- | ------- | ------- |
| `GIT_BINARY` | `git` | The git to run, a name on `PATH` or a path |

Connecting runs `git --version` once and refuses a git older than 2.5, the first with `worktree`, or one that does not run, so a deployment fails at startup rather than at its first operation.

## Usage

```rust,ignore
use omnia_git::Client as Git;
use omnia_wasi_vcs::WasiVcs;

omnia::runtime!({
    hosts: {
        WasiVcs: Git,
    }
});
```

## Tests

`cargo nextest run -p omnia-git` runs every suite against the `git` on `PATH`, over scratch repositories and a bare origin of their own, under a scratch global configuration, so nothing of the developer's git configuration or repositories is read or written.

## License

MIT OR Apache-2.0
