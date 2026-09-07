# StateForge Agent repository instructions

Read `.claude/MVP_PLAN.md` before planning or changing implementation. It is
the delivery ledger and defines the next reviewable unit of work. Read
`AGENTS.md` and use the applicable repository skills:

- `stateforge-python-development` for Python, packaging, tests, and docs;
- `stateforge-protocol-development` for SFP contracts or integrations; and
- `stateforge-pr-review` when reviewing or drafting a pull request.

## Delivery discipline

- Work on one plan item at a time; do not implement later procedures
  opportunistically.
- Target `mvp/implementation`. Use `mvp/<issue-slug>` for each issue branch and
  `[TASK][MVP] Title` for issue and pull-request titles.
- The user owns GitHub mutations. Do not create issues, open pull requests,
  push, or merge unless explicitly instructed.
- Finish by drafting a human-readable pull-request description with purpose,
  resulting behavior, verification, and non-goals. Stop for human review.
- Update the plan when implementation evidence changes. A written
  specification alone never makes an item implemented.

## MVP boundary

The active phase is the local SFP path between the Agent and standalone
Provider processes. SF-AOP and the StateForge Control Plane are mandatory MVP
phases, but SF-AOP is not yet designed. The completed SFP path will inform that
design. Never describe either phase as optional, post-MVP, or out of scope.

Do not invent SF-AOP or Control Plane behavior during an SFP pull request.
Preserve internal boundaries that can later connect SFP to SF-AOP cleanly.

## Repository boundary

This repository owns the Agent daemon, Agent-side SFP coordination, staging
workspaces, and BackupEngine tooling. It stays independent of specific
third-party systems. Protobuf sources belong in `stateforge-protocol`; Provider
server behavior belongs in `stateforge-provider-implemntation`.

The Provider and its Developer RPC Library share a process that is separate
from the Agent. They communicate over one multiplexed local gRPC channel.
Backup engines are Agent tools, not Providers or SFP peers.

Operations are namespaced string identifiers, not a fixed enumeration. Backup
and disaster recovery are predefined compliance profiles built from operations.

## Review and verification

- Review only against the active plan item, its boundary, and acceptance
  criteria; report scope drift separately.
- Tests are automated behavioral gates. Do not substitute ad hoc manual tests
  or qualitatively review test files unless requested.
- Run all applicable configured checks: Ruff, strict mypy, pytest, package
  build, and strict MkDocs build.
- Preserve unrelated files. Do not touch the untracked `tools/` directory
  unless explicitly asked.
