# Clarity engineering guide

Write Rust as a seasoned staff engineer would: simple, explicit, and shaped
around the domain rather than frameworks.

- Prefer a functional core with actor or message-passing shells.
- Prefer immutable state transitions where practical.
- Model lifecycle and protocol choices with enums; make invalid states hard to
  represent.
- Keep modules cohesive. Do not create type-dumping-ground modules.
- Choose the smallest elegant abstraction that solves the current problem.
- Do not add speculative generality or distributed-systems machinery before
  the local product proof needs it.
- Use current stable Rust, edition 2024, `cargo add` for dependencies,
  `cargo nextest` for tests, rustfmt, and strict Clippy.
- Keep the Nix flake and Cargo workflows working together.
- Use `jj` for version control. Describe each small, understandable chunk of
  work with a meaningful commit message, then start a new change before taking
  on a separate concern. Do not bundle unrelated cleanup, behavior changes,
  and refactors into one revision.

The product boundary is in `README.md`. Linear is declared intent; Clarity is
live coordination state. A dashboard, transcript store, and autonomous agent
fleet are not the MVP.

## Dogfood sessions

When `CLARITY_PARTICIPANT_ID` is present, the process is already attached to
a Clarity workstream. Do not wait for the user to explain the integration:

- Run `clarity observe` before planning to identify the active Linear issue,
  then retrieve that issue through the available Linear integration and treat
  it as the task source of truth.
- Claim the coherent files or subsystem before editing. If the claim conflicts,
  inspect the existing work and choose a non-overlapping contribution instead
  of proceeding silently.
- Publish meaningful findings, blockers, checkpoints, and completion evidence
  through the `clarity` CLI as the work develops. Do not stream routine tool
  activity or narration.
- Check workspace events at natural planning boundaries and consume new human
  direction before continuing.
- Publish a final typed outcome with changed files, revision when available,
  and the narrow verification receipt before exiting.

These are temporary repository-level dogfood instructions. The productized
harness adapter must inject equivalent behavior so other repositories do not
need custom agent guidance.
