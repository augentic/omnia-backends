# Cargo Vet

Following a Cargo dependency update, run:

```bash
mise run vet
```

That regenerates imports, exemptions, and unpublished, then runs
`cargo vet --locked`, updating the vetted dependencies based on trusted
authors.

See the [Cargo Vet book](https://mozilla.github.io/cargo-vet/commands.html) for
more information.
