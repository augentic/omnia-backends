# Contribution Guide

Augentic welcomes community contributions to omnia-backends.

Since the project is still unstable, there are specific priorities for development. Pull requests
that do not address these priorities will not be accepted until Augentic is production ready.

Please familiarize yourself with the Contribution Guidelines and Project Roadmap before
contributing.

There are many ways to help Augentic besides contributing code:

- Fix bugs or file issues
- Improve the documentation

## Table of Contents

- [Contribution Guide](#contribution-guide)
  - [Table of Contents](#table-of-contents)
  - [Contributing Code](#contributing-code)
  - [Code Style](#code-style)
  - [Before you open a pull request](#before-you-open-a-pull-request)
  - [Developer's Certificate of Origin](#developers-certificate-of-origin)
  - [Pull request procedure](#pull-request-procedure)
  - [Conduct](#conduct)

## Contributing Code

Unless you are fixing a known bug, we **strongly** recommend discussing it with the core team via
a GitHub issue before getting started to ensure your work is consistent with Augentic's roadmap
and architecture.

Reviewers should leave a "LGTM" comment once they are satisfied with the patch. If the patch was
submitted by a maintainer with write access, the pull request should be merged by the submitter
after review (the procedure is [below](#pull-request-procedure)).

## Code Style

Please follow these guidelines when formatting source code:

- Rust code should match the output of `cargo +nightly fmt --all`

## Before you open a pull request

Run the full CI check locally and make sure it passes:

```shell
mise run ci
```

This runs the format check (`cargo +nightly fmt --all --check`), clippy
(warnings denied, natively and for `wasm32-wasip2`), the test suite, doc
tests, rustdoc, and the supply-chain checks (`cargo vet`, `cargo deny`), exactly as CI does. Tasks run through
[mise](https://mise.jdx.dev/getting-started.html), which is installed by
hand; `mise tasks` lists them, and [mise.toml](mise.toml) includes the shared
Rust tasks from [augentic/toolkit](https://github.com/augentic/toolkit).

## Developer's Certificate of Origin

All contributions must include acceptance of the [DCO](https://developercertificate.org/):

```text
Developer Certificate of Origin
Version 1.1

Copyright (C) 2004, 2006 The Linux Foundation and its contributors.
660 York Street, Suite 102,
San Francisco, CA 94110 USA

Everyone is permitted to copy and distribute verbatim copies of this
license document, but changing it is not allowed.


Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the best
    of my knowledge, is covered under an appropriate open source
    license and I have the right under that license to submit that
    work with modifications, whether created in whole or in part
    by me, under the same open source license (unless I am
    permitted to submit under a different license), as indicated
    in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it.

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including all
    personal information I submit with it, including my sign-off) is
    maintained indefinitely and may be redistributed consistent with
    this project or the open source license(s) involved.
```

To accept the DCO, add this line to each commit message with your name and email address (`git commit -s` will do this for you):

```text
Signed-off-by: Jane Example <jane@example.com>
```

For legal reasons, no anonymous or pseudonymous contributions are accepted; open a GitHub issue if this is a problem for you.

## Pull request procedure

Pull requests should be targeted at the `main` branch. Before creating a pull request, go through this checklist:

1. Create a feature branch off of `main`.
2. [Rebase](https://git-scm.com/book/en/Git-Branching-Rebasing) your local changes against `main`.
3. Run `make ci` and confirm that it passes: exactly the CI jobs, in order.
4. Accept the Developer's Certificate of Origin on all commits (see above).

All contributions are made via pull request. All patches from all contributors get reviewed. At least one review from a maintainer is required for all patches (even patches from maintainers). When CI fails, authors are expected to update the pull request until it passes.

Normally, all pull requests must include tests that cover your change. Occasionally, a change will be very difficult to test for; in those cases, include a note in your commit message explaining why.

Each commit has a subsystem prefix (`cursor:`, `genai:`, `postgres:`, ...).

## Conduct

Whether you are a regular contributor or a newcomer, we care about making this community a safe place for you and we've got your back.

- We are committed to providing a friendly, safe and welcoming environment for all, regardless of gender, sexual orientation, disability, ethnicity, religion, or similar personal characteristic.
- Be kind and courteous. There is no need to be mean or rude.
- We will exclude you from interaction if you insult, demean or harass anyone. In particular, we do not tolerate behavior that excludes people in socially marginalized groups.
- Private harassment is also unacceptable. If you feel you have been or are being harassed or made uncomfortable by a community member, please contact a member of the core team immediately.
- Likewise any spamming, trolling, flaming, baiting or other attention-stealing behaviour is not welcome.

We welcome discussion about creating a welcoming, safe, and productive environment for the community. If you have any questions, feedback, or concerns please let us know with a GitHub issue. The [Code of Conduct](CODE_OF_CONDUCT.md) applies throughout.
