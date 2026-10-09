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
- cache accepted, immutable declarations independently of channel lifetime.

The runtime reports a Provider as connected only after both session setup and
manifest validation succeed. This connection state is intentionally narrower
than operational availability: health and capability assessment are separate
procedures and are not inferred from an open channel or accepted manifest.
