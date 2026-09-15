# StateForge Agent repository instructions

Read root `STATEFORGE_DPT.md` for the shared milestone declaration and
generated delivery snapshot, not an editable local implementation ledger.
For every milestone assignment, read and follow
`.agents/skills/stateforge-dpt-milestone/SKILL.md`. It applies equally to
agents reached through `AGENTS.md` or `CLAUDE.md`.

Use `stateforge-python-development` for Python, packaging, tests, and docs;
`stateforge-protocol-development` for SFP contracts or integrations; and
`stateforge-pr-review` when drafting a PR description or assisting a review.
Those engineering skills live under `.claude/skills/`.

## Delivery discipline

- Work only on the human-selected Issue and its mapped DPT step. Use the
  Issue's acceptance criteria for implementation and DPT for ownership,
  prerequisites, and the declared result. Ask about material disagreements.
- Follow `stateforge-dpt-milestone` for dependency checks, branch context,
  verification, scoped local commits, and the human handoff. Never infer the
  assignment from queue order or start a later milestone opportunistically.
- Humans own pushing, Issue/PR mutations, PR creation, formal review, and
  merging. Local implementation commits are allowed; they do not complete a
  step. End with verification evidence and a human-readable PR draft.
- Do not edit `STATEFORGE_DPT.md`, its manifest/revision/statuses, or SQLite to
  record implementation progress. Plan changes are managed through DPT in a
  separate human-requested workflow.
- A step is delivered only after its correctly associated PR has been
  reviewed by a human and merged into the intended integration branch.
  DPT reconciliation records the GitHub-derived state. A local commit,
  passing tests, an approval, or a closed Issue alone is not completion.
- This project's current source/integration branch is `mvp/implementation`.
  Prefer the human-selected Issue branch and DPT's declared branch; never use
  descriptive placeholders such as "Existing work" as branch names.
  Issue and PR titles currently use `[TASK][MVP] Title`.

## MVP boundary

The active phase is the local SFP path between the Agent and standalone
Provider processes. SF-AOP and the StateForge Control Plane are mandatory MVP
phases, but SF-AOP is not yet designed. The completed SFP path will inform that
design. Never describe either phase as optional, post-MVP, or out of scope.

Do not invent SF-AOP or Control Plane behavior during an SFP pull request.
Preserve internal boundaries that can later connect SFP to SF-AOP cleanly.

The local SFP phase must work across Protocol, Provider, and Agent, including
backup-engine-backed backup and disaster recovery. Only afterward are the
SF-AOP contracts and Control Plane milestones defined from that proven
behavior. The end-to-end MVP must let a trigger create a coordinated snapshot
and use that snapshot for disaster recovery. These are product-level exit
conditions, not permission to implement additional milestones.

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
