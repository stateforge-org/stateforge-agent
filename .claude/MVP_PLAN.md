# StateForge Agent MVP plan

This ledger tracks code implementation in `stateforge-agent`. All eleven SFP
procedures are designed; the statuses below concern implementation only.

## Product-level MVP sequence

| Phase | Status | Exit condition |
| --- | --- | --- |
| Local SFP Agent-to-Provider foundation | `IN PROGRESS` | All SFP procedures operate across Protocol, Provider, and Agent, including backup-engine-backed backup and disaster recovery. |
| SF-AOP design and contracts | `ON HOLD` | Agent-to-Control-Plane procedures and wire contracts are defined from proven SFP behavior. |
| StateForge Control Plane | `ON HOLD` | Authorization, inventory, triggers, policies, operations, snapshots, and recovery coordination are implemented. |
| End-to-end MVP integration | `ON HOLD` | A trigger can create a coordinated snapshot and that snapshot can drive disaster recovery. |

SF-AOP and the Control Plane are mandatory MVP work. They are on hold only
because SF-AOP has not been designed. Add their detailed PR queues after the
local SFP phase; do not invent them inside an SFP PR.

## Current SFP implementation

| # | Procedure | Status | Evidence or remaining work |
| ---: | --- | --- | --- |
| 1 | Endpoint Discovery and Channel Establishment | `IMPLEMENTED` | Descriptor monitoring and validation, channel establishment/reuse, endpoint replacement, and bounded retry exist. |
| 2 | Handshake and Version Negotiation | `IMPLEMENTED` | The provisional channel is authenticated to Provider identity and generation before session activation. |
| 3 | Provider Manifest Exchange | `BLOCKED` | Awaiting the Protocol contract and compatible Provider runtime. |
| 4 | Health and Availability Monitoring | `BLOCKED` | Requires manifest integration and Provider health support. |
| 5 | Capability Assessment | `BLOCKED` | Requires its Protocol contract and Provider runtime. |
| 6 | Trigger Publication and Acknowledgement | `BLOCKED` | Requires its contract and Provider runtime; SF-AOP forwarding comes later. |
| 7 | Operation Admission and Idempotency | `BLOCKED` | Requires the generic operation contract and Provider runtime. |
| 8 | Backup Preparation | `BLOCKED` | Requires operations, compliance contract, and staging lifecycle. |
| 9 | Disaster-Recovery Execution | `BLOCKED` | Requires backup-engine restore and a compliant Provider. |
| 10 | Progress, Results, and Cancellation | `BLOCKED` | Requires admitted operations and streaming contracts. |
| 11 | Disconnection and Recovery | `PARTIAL` | Channel retry/replacement exists; manifest, operation, and trigger reconciliation do not. |

`IMPLEMENTED` means code and automated verification exist. `READY` identifies
the next local PR. `BLOCKED` requires a predecessor. `PARTIAL` is incomplete.
Statuses do not claim anything about GitHub merge state.

## Ordered pull-request queue

All PRs target `mvp/implementation`. Only the first unblocked item is `READY`.

| ID | Status | Branch | Pull-request title | Required result |
| --- | --- | --- | --- | --- |
| A0 | `IMPLEMENTED` | Existing work | `[TASK][MVP] Implement SFP Discovery and Handshake Foundation` | Procedures 1 and 2, daemon lifecycle, channel reuse, and reconnection foundation. |
| A1 | `BLOCKED` | `mvp/provider-manifest-exchange` | `[TASK][MVP] Integrate Provider Manifest Exchange` | Retrieve and validate the effective manifest after handshake and bind it to the active Provider generation. Preserve namespaced string operation identifiers. |
| A2 | `BLOCKED` | `mvp/provider-health-monitoring` | `[TASK][MVP] Integrate Provider Health and Availability Monitoring` | Reuse the active channel's standard gRPC health service and maintain one local availability state. Control Plane reporting is not part of this PR. |
| A3 | `BLOCKED` | `mvp/capability-assessment` | `[TASK][MVP] Integrate Provider Capability Assessment` | Assess a named operation against a target using only normalized SFP data and the active manifest. |
| A4 | `BLOCKED` | `mvp/trigger-publication` | `[TASK][MVP] Integrate Trigger Publication and Acknowledgement` | Validate, durably record, deduplicate, and acknowledge Provider triggers behind an internal boundary for later SF-AOP forwarding. |
| A5 | `BLOCKED` | `mvp/operation-admission` | `[TASK][MVP] Integrate Operation Admission and Idempotency` | Submit immutable envelopes with string operation identifiers and handle new, replayed, conflicting, rejected, and terminal admissions. |
| A6 | `BLOCKED` | `mvp/operation-lifecycle` | `[TASK][MVP] Integrate Operation Progress Results and Cancellation` | Watch progress, accept one immutable terminal result, and request cancellation with explicit race-safe outcomes. |
| A7 | `BLOCKED` | `mvp/backup-preparation` | `[TASK][MVP] Integrate Backup Preparation Workspaces` | Own a protected staging workspace and accept validated staged-source metadata or a Provider reference from a compliant Provider. |
| A8 | `BLOCKED` | `mvp/backup-engine-layer` | `[TASK][MVP] Implement the BackupEngine Extension Layer` | Discover Python entry-point engines, validate capabilities, and expose a stable in-process engine contract. No BackupEngine RPC. |
| A9 | `BLOCKED` | `mvp/restic-backup-engine` | `[TASK][MVP] Implement ResticBackupEngine` | Safely execute restic without a shell, protect credentials, parse machine-readable results, and normalize backup, restore, verify, and removal behavior. |
| A10 | `BLOCKED` | `mvp/disaster-recovery` | `[TASK][MVP] Integrate Provider Disaster Recovery Execution` | Restore through BackupEngine into staging, then ask a compliant Provider to reconstruct and verify. Never send a generic Provider restore command. |
| A11 | `BLOCKED` | `mvp/sfp-reconciliation` | `[TASK][MVP] Complete SFP Disconnection and Recovery` | Revalidate and reconcile identity, generation, manifest, health, triggers, and operations after interruption. |
| A12 | `BLOCKED` | `mvp/sfp-integration` | `[TASK][MVP] Verify the Complete Local SFP Runtime` | Exercise the full local lifecycle between real standalone Provider and Agent processes and verify docs and distributions. |

## Cross-repository gates

- A1, A3-A7, A10, and A11 require their versioned schemas to land first in
  `stateforge-protocol`, then a compatible server implementation in
  `stateforge-provider-implemntation`.
- A2 uses the standard gRPC health service and needs no StateForge protobuf.
- A8 and A9 are Agent-only tooling; they are neither SFP services nor Provider
  responsibilities.

## Definition of done

For each PR: satisfy only the stated result; add behavioral and failure tests;
update docs and docstrings; pass Ruff, strict mypy, pytest, package build, and
`mkdocs build --strict`; update this ledger; draft the PR for human review; and
stop before the next item.
