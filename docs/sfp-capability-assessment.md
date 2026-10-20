# Capability Assessment

The Agent uses SFP Capability Assessment to collect point-in-time evidence
about whether a connected Provider can support declared operations in its
current runtime. Assessment becomes eligible only after bootstrap, manifest
acceptance, and Provider-wide Health establish the exact current Provider
session as available.

Capability Assessment is evidence, not authorization. It does not dispatch an
operation, mutate the target, create a logical component, or decide whether a
later operation may run. Policy and operation admission remain separate
responsibilities.

## Agent API

`ProviderSessionManager.assess_capabilities` accepts a Provider installation
identity, an ordered tuple of operation identifiers, and an optional bounded
execution-context mapping. Every requested operation must be present in the
accepted manifest for the active session.

The result is immutable Agent-owned data that preserves all canonical SFP
states:

- support is `supported`, `supported_with_requirements`, `unsupported`, or
  `indeterminate`;
- each requirement remains `satisfied`, `unsatisfied`, or `unknown`; and
- structured reasons and constraints are retained without reducing them to an
  eligibility boolean.

The current Capability Assessment contract intentionally has no generic
target or resource field. Target-specific input belongs to the relevant
operation payload when operation admission and execution are implemented.

The Capability Assessment service coordinator owns the complete procedure:
context conversion, request construction, manifest-declaration checks,
current-scope construction, retry policy, response normalization, and bounded
evidence reuse. The generic Provider session manager exposes only a narrow
lease containing the exact session and availability fences, verifies that the
lease remains current, and invalidates evidence on lifecycle transitions.
This keeps procedure semantics out of generic connection lifecycle code.

## Safety boundaries

Before sending a request, the Agent validates it with the public
`stateforge-protocol` validator and applies Agent-local session and manifest
checks. Responses must pass the canonical exchange validator before the Agent
normalizes them.

Each RPC has a finite deadline. Constructor configuration rejects boolean,
non-finite, and out-of-range deadline or retry values. Only `UNAVAILABLE` is
retried, with bounded attempts and delay. A retry retains the same assessment
request ID and revalidates the active session, manifest, availability
generation, and declared operations before it sends again. Other gRPC
failures become separate sanitized Agent errors; Provider diagnostic text is
not copied into logs or public exceptions.

## Evidence reuse

Validated results may be retained as bounded evidence. A cache key includes
the Provider installation ID, endpoint generation, negotiated session ID,
manifest revision, local availability generation, ordered operation set, and
SHA-256 digest of the deterministically serialized execution context. The
cache has both an entry-count limit and an aggregate serialized-evidence byte
budget; an individual result larger than that budget is not retained.

Any session, manifest, endpoint, availability, operation-set, or context
change therefore misses or invalidates the old entry. Cached evidence is
available only through the explicit evidence lookup API and is never used by
`assess_capabilities` as an authorization shortcut.

## Installed verification

The RPM installed-system scenario makes the separate Provider process return
one transient `UNAVAILABLE` result from its real gRPC service boundary. It
requires the installed Agent client to translate that status, retry, preserve
the exact serialized request, and then complete against the production
Provider service. It also changes a target permission observed by a Provider
requirement check and requires fresh operation-specific evidence while
Provider-wide availability remains `AVAILABLE`.

Before the first valid call, the same installed Agent rejects malformed and
undeclared operation identifiers locally and proves that neither request
reaches Provider code.

The same scenario then transitions the Provider from `SERVING` to
`NOT_SERVING` and back. The Agent must block assessment while unavailable,
discard the old evidence across the availability transition, and obtain a new
assessment only after recovery. Target content must remain unchanged and no
operation handler may run during any assessment.

## Installed-system verification

The installed-runtime CI scenario builds the Agent RPM and the exact Protocol
and Provider wheels through their repository-owned build actions. It installs
those artifacts in a clean Linux systemd container, starts the packaged Agent
and a real Provider Python process separately, and assesses that Provider over
its Unix-domain SFP socket.

The scenario verifies every support outcome, unmet and unknown requirements,
exact-scope evidence reuse, target immutability, and absence of operation
dispatch. Its JSON result, dependency provenance, journals, package ownership,
and RPM verification are retained as CI evidence.

Separate clean-container jobs use the same RPM-installed client to verify a
finite timeout, malformed protobuf rejection, mismatched session/generation/
manifest response fences, and both canonical message-size limits. These
adversarial peers deliberately violate the Provider side of the contract;
the normal scenario remains the authoritative real Provider Python
integration.
