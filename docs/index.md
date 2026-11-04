# StateForge Agent

The StateForge Agent is the long-running execution daemon inside a customer
environment. It discovers local Provider processes, maintains StateForge
Protocol (SFP) sessions, and coordinates operations through the Agent's
internal SDK and Library layers.

The Agent does not contain third-party-specific integration logic. A Provider
runs as an independent process and exposes that logic through SFP.

## Documentation scope

This site documents the Agent executable, its local configuration, and its
implementation. The SFP wire contract and Provider authoring interface belong
to their own repositories and documentation sites.

## Current implementation

The current runtime can:

- load and validate the main configuration and ordered `conf.d` drop-ins;
- run as a long-lived daemon;
- monitor the Provider discovery directory;
- validate Provider endpoint descriptors;
- establish and reuse local gRPC channels; and
- perform the SFP bootstrap handshake and version negotiation;
- retrieve and validate the Provider manifest on the negotiated session; and
- watch Provider-wide standard gRPC Health on that same channel; and
- cache accepted, immutable declarations independently of channel lifetime;
  and
- collect bounded, read-only Capability Assessment evidence for declared
  operations without converting that evidence into authorization; and
- consume manifest-declared Provider triggers, validate their payloads, and
  durably assume bounded local delivery responsibility before acknowledging
  them; and
- admit manifest-declared operations only after current capability evidence,
  persist their immutable identities before transport, and recover uncertain
  responses without dispatching duplicate work; and
- watch admitted operations, retain their latest progress and terminal
  outcomes, and request cooperative operation-scoped cancellation.

The runtime reports a Provider as connected only after both session setup and
manifest validation succeed. Operational eligibility is stricter: the exact
current session and generation must also have a live Provider-wide Health
Watch reporting `SERVING`. Health is process-wide; target-specific support
remains the responsibility of Capability Assessment.

Capability Assessment preserves supported, supported-with-requirements,
unsupported, and indeterminate outcomes together with their requirement
states, reasons, and constraints. Assessment evidence is fenced to the exact
session, manifest, availability generation, operation set, and execution
context that produced it.

Trigger Publication begins only for the exact current session while Provider
Health reports `SERVING`. The Agent validates each event against the canonical
P3 contract and accepted manifest schema, then commits its idempotency record
before returning an acknowledgement. Accepted events remain pending in the
Agent journal until the later SF-AOP forwarding boundary is implemented.

Operation Admission is similarly fenced to the exact available session,
manifest, declaration, and capability evidence. A lost response is retried
with the same durable operation identity; the Provider resolves the existing
record rather than running the operation twice.

Operation Lifecycle resumes Watch from the last durable sequence after an
interruption. A cancellation request is recorded separately from the final
outcome; only a terminal Provider snapshot confirms that execution stopped.
