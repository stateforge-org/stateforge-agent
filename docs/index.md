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
  operations without converting that evidence into authorization.

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
