# ma-irc - Agent Notes

## Repository role

`ma-irc` is an IRC-focused runtime prototype. It derives a runtime `did:ma:`
identity and may publish its own DID document directly through Kubo/IPNS.

## Agent rules

- Use British English for project-owned names and prose.
- Write DRY, KISS code: avoid duplicated logic and prefer the simplest
  implementation that meets the requirement.
- Keep `ma-core` at `^0.14.4` or newer from crates.io. Never commit a local path
  dependency.
- Do not start, stop, or restart services unless the user explicitly asks.
- Use `ma_core::config::Config`, `MaArgs`, and `SecretBundle` for configuration,
  XDG paths, secure persistence, and runtime identity keys. Never add a raw key
  file or derive secret storage from `--config`; on Linux ma-core stores the
  encrypted bundle at `~/.config/ma/irc.bin` by default.

## DID document publication

- Generate timestamps through `ma-core`; do not rewrite RFC 3339 timestamps with
  ad hoc string manipulation.
- After changing the `ma` extension, call `Document::touch()`, re-sign the
  document, then require both `Document::validate()` and `Document::verify()`
  before encoding or publishing it.
- Published `createdAt` and `updatedAt` values must use RFC 3339 UTC
  whole-second form such as `2026-08-08T12:34:56Z`.

## Validation

```sh
cargo fmt --check
cargo test
```
