# Clarity

Clarity is an open-source, editor-native attention runtime for agentic work.
One command takes a developer to the agent work that needs judgment, restores
the exact context needed to act, routes the decision to the correct execution,
and returns the developer to prior work.

Clarity owns workstreams, attention semantics, navigation targets, response
routing, and outcome-linked closure. Editors, terminals, agent harnesses, Git,
CI, and browsers remain execution and rendering surfaces. It is not another
inbox, terminal emulator, task manager, dashboard, transcript store, or
autonomous agent fleet.

The current Rust code is the retained coordination foundation from an early
prototype. It provides a SQLite WAL event log, a single-writer runtime, typed
events and IDs, deterministic attention projection, targeted responses, and
delivery and consumption receipts. Its Linear-bound workspace model and
internal command API are transitional.

The first product slice is deliberately narrow:

- one repository with multiple real workstreams;
- one managed Amp execution path;
- one typed file/range or diff navigation target;
- one Doom Emacs client;
- one request → response → delivery → consumption → outcome loop; and
- exact return to the developer's prior editor context.

Ghostel may be an optional Emacs terminal surface. It is not a core dependency.

## Development

```sh
nix develop
cargo nextest run
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --check
```

Install the current checkout on `PATH` once with:

```sh
cargo install --path .
```

## Current prototype CLI

Run `up` once anywhere inside the Git checkout to bring that repository's local
coordination service online:

```sh
cd src
clarity up
```

The command discovers the repository root, starts or reuses only a daemon that
belongs to this checkout and runs the same build, prints its state, and returns
to the shell. Other entry commands start it implicitly when needed.

To resolve or create a Linear workspace and attach Amp to that task for the
life of the process, expose a Linear personal API key and run:

```sh
export LINEAR_API_KEY=lin_api_...
clarity run amp TG-182
```

If the workspace already exists in the repository daemon, the launch does not
need the API key. Arguments after `--` are passed to Amp unchanged.

Switching issue identifiers changes the task used by that agent process; it
does not restart or recreate the repository service. `clarity up TG-*` remains
as a deprecated compatibility alias for scripts that still rely on checkout
selection. The API key is used only for the direct Linear request. Clarity
does not write it to the repository, daemon database, events, logs, or status
output.

The launcher exports the stable `CLARITY_SCOPE` variable to the harness
process (for example, `linear:TG-187`). Scope resolution uses an explicit
command `--scope` first, then `CLARITY_SCOPE`, and only then the checkout's
legacy active-workspace selection. The process-local value is inherited by
child commands without changing repository-global state, so concurrent agents
can work on different issues in one checkout.

The harness can receive only its relevant human direction with
`clarity directions`; that read durably records delivery. After applying a
direction, it acknowledges the exact record with
`clarity consume-direction <DIRECTION_ID>`. Targeted responses retain the
source request, participant, and work identities. Workspace-level interventions
are explicit broadcasts and receive independent delivery receipts for each
participant.

Review readiness is explicit rather than inferred from activity. `working`,
`checkpoint`, and `done` remain quiet durable state; an agent publishes
`review-requested` with bounded artifacts when human review is actually useful:

```sh
clarity signal --work "$WORK_ID" review-requested \
  --request-key direction-routing-review \
  --summary "Direction routing is ready for review" \
  --requested-action "Inspect the routing change and mark it reviewed or request changes" \
  --known-risk "Delivery after participant reassignment remains intentionally unsupported" \
  --artifact revision:"$(git rev-parse HEAD)" \
  --artifact test_receipt:cargo-test/targeted-direction
```

Distinct unresolved review requests for a work item coexist in **Ready to
review**. Repeating a stable request key replaces only the prior request from
the same participant and work context. Responding acknowledges that exact
request without turning historical checkpoints and outcomes into an inbox.

Decision requests accept up to nine `--choice` values plus an optional
`--recommendation`; blocked and help requests require `--requested-action`.
Decision and review requests require supporting artifacts, and review evidence
must include both a revision or patch and a test receipt.

Every attention request has a generated `request_id` returned by the signal
command and a caller-supplied stable `--request-key`. Repeating a key within the
same participant and work context replaces the prior duplicate;
`--supersedes-request <REQUEST_ID>` explicitly replaces a different request in
that context. Checkpoints and completion never resolve requests. After a harness
has applied an answer or no longer needs one, it closes that exact request
explicitly:

```sh
clarity resolve-request "$REQUEST_ID" \
  --summary "Applied the selected direction and verified the result"
```
