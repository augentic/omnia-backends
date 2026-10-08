# Omnia Git

Git backend for the `omnia:vcs` interface ([`omnia-wasi-vcs`](https://github.com/augentic/omnia/tree/main/crates/wasi-vcs)): every operation is one `git` process, spawned over the operator's own `git` binary in the directory the runtime opened for the guest's location. The child enters that open handle before it executes (`fchdir`), so git works where the runtime resolved the location and never walks a path a guest could redirect meanwhile; the one path git is handed is a working copy's destination for `worktree add`, which git records in the repository.

## Operations

| Interface | Operation | What runs |
| --------- | --------- | --------- |
| `store` | `resolve` | `rev-parse --verify --end-of-options <revision>^{commit}` |
| `store` | `head` | `rev-parse --verify HEAD^{commit}`; an unborn branch is `not-found` |
| `store` | `commit` | `status --porcelain`, then `add -A` and `commit -m`; nothing pending answers no commit |
| `store` | `merge` | `merge --no-ff --no-commit`, the policy applied from the three commits, then `commit --no-edit`; a conflict no rule resolves is read back and the merge aborted |
| `workspace` | `init` | `init` in the place; a repository already there is `exists` |
| `workspace` | `add` | `worktree add --detach -- <at> <revision>` in the repository, `<at>` the destination's absolute path |
| `workspace` | `remove` | `worktree remove --force .` in the working copy; pending changes go with it |
| `workspace` | `pending` | `status --porcelain -z --no-renames --untracked-files=all`, each path once |
| `transport` | `clone` | `clone [--depth <n> --no-single-branch] -- <url> .` in the place; a depth cuts history, never the labels |
| `transport` | `fetch` | `fetch -- <remote>` |
| `transport` | `label` | `branch -f -- <name> <revision>`, the working copy detached onto its commit first when it sits on that branch |
| `transport` | `push` | `push -- <remote> refs/heads/<label>:refs/heads/<label>` |

A merge is held before its commit, and the policy is the backend's to apply rather than git's: every path the two sides differ on that a rule matches is resolved from the commits themselves, byte for byte — `ours` the working copy's side, `theirs` the merged-in side, `union` both sides' lines each once through `merge-file` over files in the host's temporary directory — and staged over whatever git's own attributes made of it, so a repository's `.gitattributes` or `info/attributes` never decides a path the policy names. A `union` over a path git cannot merge as lines, a binary, fails the merge rather than seal a guess. A conflict no rule matched abandons the merge: the paths come back as data, `merge --abort` puts the working copy on its head, and nothing of a failed merge stays in progress for a later commit to finish. Nothing of a policy touches the working tree but the resolved paths, so it never shows up in `pending`.

What git reports is read into the typed `omnia:vcs` error — `not-a-repository` (a place holding no repository), `exists`, `not-found`, `access` — and anything else reaches the guest as `other` with git's own words. A location that does not exist is the runtime's refusal, before any git runs.

## Trust

A lent repository is the guest's to write, so its configuration is not the operator's: `.git/config`, `.git/hooks`, and `.gitattributes` are files a guest may have laid, and git reads them by default. Every operation is held to host policy instead:

- Hooks, the file-system monitor, the alternate-refs command, commit signing, signature verification, and submodule recursion are off on every command line (`core.hooksPath=/dev/null`, `core.fsmonitor=false`, `core.alternateRefsCommand=`, `commit.gpgsign=false`, `merge.verifySignatures=false`, `submodule.recurse=false`, `fetch.recurseSubmodules=no`), whichever scope set them.
- The work tree is the place itself (`--work-tree=.`) on every command but `clone` and `init`, which lay it, so a `core.worktree` the repository set points git at no other directory on the host.
- A repository-scoped `core.sshCommand`, `core.askPass`, `credential.helper`, `credential.<url>.helper`, `filter.<name>.clean` / `smudge` / `process`, or `merge.<name>.driver` is read from `config --show-scope` before the operation and pinned to the operator's own value — the system, global, or environment one — or to nothing where the operator set none, so the SSH, prompt, and credential programs that run are the host's as at a prompt, never a lent repository's.
- Every guest string — a revision, a label, a remote, a URL — reaches git after `--`, and one that is empty or that git would read as an option (a leading `-`) is `not-found` before any process runs.
- Every process runs under `GIT_ALLOW_PROTOCOL`, so no address reaches an `ext::` command or a remote helper whatever `protocol.<name>.allow` the repository set: `http:https:ssh` by default, which is what a lazy fetch from a promisor remote the repository names may speak, and for a transport operation the list for its kind, read from the URL as the host's configuration expands it. A local address (`file://`, or a path with no `scheme:` before its first `/`) runs under `file` with the far side held through the pack command (`--upload-pack` / `--receive-pack` naming `git -c core.hooksPath=/dev/null -c core.alternateRefsCommand= -c core.fsmonitor=false -c receive.denyCurrentBranch=refuse`), since the near side's `-c` pins reach no process the far side starts: no hooks, no alternate-refs command, no file-system monitor, and no update of a branch a working copy has checked out, which `receive.denyCurrentBranch=updateInstead` would write through the copy's own filters; any other under `http:https:ssh` with the pack command git's own (`--upload-pack=git-upload-pack` / `--receive-pack=git-receive-pack`), so a `remote.<name>.uploadPack` or `receivePack` the repository set — which a `-c` does not outrank — is never the program the operator's ssh carries to a host.
- How a transport runs is the host's configuration alone: a URL rewrite (`url.<base>.insteadOf`, `pushInsteadOf`), an `http.*` or `http.<url>.*` setting other than the transfer-tuning keys (`postBuffer`, `lowSpeedLimit`, `lowSpeedTime`, `maxRequests`, `minSessions`, `userAgent`, `version`), or a `remote.<name>.proxy` / `proxyAuthMethod`. A repository that sets one is refused on every transport operation (`other`, naming the key): a rewrite is additive configuration no command line can cancel, a proxy or TLS setting pinned empty disables rather than restores (`http.sslVerify=` reads as false), and either would carry the operator's credentials for the real remote anywhere or over anything.
- `GIT_DIR`, `GIT_WORK_TREE`, `GIT_COMMON_DIR`, and `GIT_INDEX_FILE` are removed from the inherited environment, so the operator's shell cannot point an operation at another repository; terminal prompts, editors, and merge-message editing are off; and every process runs under `LC_ALL=C`, over whatever locale the shell exports, so what git says is the English the typed errors are read from.

What stays with the operator: a repository's `.gitattributes` still selects a filter or diff driver by name where the operator declared one in their own configuration, so such a driver runs over the lent tree as it would at a prompt; a local address names any repository the host's git can read, as the runtime's lend rule bounds the place an operation runs in but not where a clone or fetch reads from; and a working copy's `.git` file names the repository it belongs to, which git follows where it points. A deployment that lends a working copy it does not trust controls what it lays as `.git`, under it, and as `.gitattributes`.

## Configuration

| Variable | Default | Purpose |
| -------- | ------- | ------- |
| `GIT_BINARY` | `git` | The git to run, a name on `PATH` or a path |

Connecting runs `git --version` once and refuses a git older than 2.26, the first with `config --show-scope`, or one that does not run, so a deployment fails at startup rather than at its first operation.

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
