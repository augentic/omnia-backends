## 0.33.0

Unreleased

Pairs with omnia 0.39.x.

### Breaking

- Every `omnia-*` dependency moves to omnia 0.39.0.
- `omnia-git` holds each address a `clone`, `fetch`, or `push` dials to the
  guest's `hosts` grant
  ([#190](https://github.com/augentic/omnia-backends/issues/190)). Under a
  `hosts` list:
  - an address whose host the list does not reach, or one the backend
    cannot read a host from, is `denied` before anything connects;
  - a `push` checks every URL its remote pushes to;
  - an http(s) redirect is `denied`, even to the same host.

  A guest without a `hosts` list dials as before. A local address names no
  host, so a list does not hold it, and `omnia-git` does not yet hold it to
  the guest's mounts
  ([omnia#477](https://github.com/augentic/omnia/issues/477)).
- `omnia-git` takes a `fetch` or `push` remote as a name the repository's
  configuration records. An address, or a name only `.git/remotes` or
  `.git/branches` defines, is `not-found`. A repository that sets
  `remote.<name>.vcs`, or a remote URL across lines, is refused on every
  transport operation, as one that rewrites a URL or sets a proxy already
  was.
- `omnia-kafka` hands the host no more records at once than the handler's
  `max_in_flight`, where that is below its own 64.

---

Release notes for previous releases can be found on the respective release
branches of the repository.

<!-- ARCHIVE_START -->
* [0.32.x](https://github.com/augentic/omnia-backends/blob/release-0.32.0/RELEASES.md)
* [0.31.x](https://github.com/augentic/omnia-backends/blob/release-0.31.0/RELEASES.md)
* [0.30.x](https://github.com/augentic/omnia-backends/blob/release-0.30.0/RELEASES.md)
* [0.28.x](https://github.com/augentic/omnia-backends/blob/release-0.28.0/RELEASES.md)
* [0.27.x](https://github.com/augentic/omnia-backends/blob/release-0.27.0/RELEASES.md)
* [0.26.x](https://github.com/augentic/omnia-backends/blob/release-0.26.0/RELEASES.md)

- [0.25.x](https://github.com/augentic/omnia-backends/blob/release-0.25.0/RELEASES.md)
- [0.24.x](https://github.com/augentic/omnia-backends/blob/release-0.24.0/RELEASES.md)
- [0.23.x](https://github.com/augentic/omnia-backends/blob/release-0.23.0/RELEASES.md)
