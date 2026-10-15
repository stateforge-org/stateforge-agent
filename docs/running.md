# Running the Agent

Use the installed console command to inspect the available commands:

```console
stateforge-agent --help
```

Validate the effective configuration before starting the daemon:

```console
stateforge-agent check-config
```

Start the foreground daemon process:

```console
stateforge-agent run
```

The packaged system service invokes the same `run` command. Process
supervision, restart policy, runtime directory creation, and reload signaling
belong to the service manager rather than the Python process itself.

## Provider activation

For each discovered Provider endpoint, the Agent opens the local gRPC channel,
negotiates an SFP version, and retrieves the complete Provider manifest. The
candidate endpoint becomes the connected session only after the manifest has
passed its negotiated schema, protocol limits, canonical-revision, embedded
JSON Schema, and secret-safety checks.

The channel send and receive ceilings are derived from the complete procedure
inventory published by `stateforge-protocol`. They allow the largest valid SFP
message in either direction, while every procedure—including Bootstrap and
Manifest Exchange—continues to enforce its smaller serialized-message limits
at its own RPC boundary. The standard gRPC Health identifiers and limits are
likewise owned by `stateforge_protocol.sfp.health_contract`; Agent health
monitoring will consume that contract rather than define a local copy.

A transport failure is retried with bounded backoff. An incompatible SFP
version or rejected manifest is instead reported as a declaration problem and
does not enter the transport reconnect loop. When an existing valid endpoint
is being replaced, a failed candidate does not displace the active session.

Provider session identifiers are opaque and are used only while that session
is active. Validated manifest declarations are immutable and may be reused
from a bounded cache keyed by Provider installation, negotiated SFP version,
manifest schema version, and manifest revision.

`connected` means that the local SFP transport, handshake, and declaration are
accepted. It does not claim that the Provider or its third-party component is
healthy or currently able to perform an operation.

The packaged service owns the Agent filesystem layout. Persistent state is
stored below `/var/lib/stateforge-agent`, and volatile runtime files are stored
below `/run/stateforge-agent`. These locations are deliberately not
configurable through `config.toml`.
