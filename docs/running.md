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

## Process exit status

The `stateforge-agent` executable has three stable exit statuses:

| Status | Meaning |
| ---: | --- |
| `0` | The command succeeded, or the daemon completed a graceful shutdown. |
| `1` | A fatal Agent runtime or operational failure prevented continued service. |
| `2` | The command invocation or effective configuration was invalid. |

Help and version output, a successful `check-config`, and an orderly daemon
shutdown return status `0`. During normal daemon operation, `SIGINT` and
`SIGTERM` request that orderly shutdown: the Agent stops its Provider sessions
and watchers before returning `0`.

A failure that affects only one Provider connection, manifest, or Health
Watch is isolated to that Provider and does not terminate the Agent. A fatal
Agent-wide failure, such as an unusable Provider discovery directory, returns
status `1`. An unexpected exception remains visible with its traceback and the
Python executable terminates unsuccessfully with status `1`.

Invalid command-line syntax and configuration loading or validation failures
return status `2`. The RPM systemd unit uses `Restart=on-failure`, but declares
`RestartPreventExitStatus=2`: runtime failures are restarted, while a
persistent configuration error stops the service until an operator corrects
the configuration and starts it again. A requested `systemctl stop` is handled
through graceful `SIGTERM` shutdown.

These values are StateForge Agent exit statuses. Shell statuses derived from
an external fatal signal, such as `137` after `SIGKILL`, are operating-system
conventions and are not emitted by the Agent. Likewise, gRPC status codes
describe individual remote procedure calls and are unrelated to process exit
status.

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
likewise owned by `stateforge_protocol.sfp.health_contract`; the Agent
consumes that contract rather than defining a local copy.

A transport failure is retried with bounded backoff. An incompatible SFP
version or rejected manifest is instead reported as a declaration problem and
does not enter the transport reconnect loop. When an existing valid endpoint
is being replaced, a failed candidate does not displace the active session.

Provider session identifiers are opaque and are used only while that session
is active. Validated manifest declarations are immutable and may be reused
from a bounded cache keyed by Provider installation, negotiated SFP version,
manifest schema version, and manifest revision.

`connected` means that the local SFP transport, handshake, and declaration are
accepted. It remains distinct from operational eligibility. The Agent keeps a
long-lived `grpc.health.v1.Health.Watch(service="")` on the same channel and
allows new work only after the current session reports `SERVING`. `UNKNOWN`,
`NOT_SERVING`, `SERVICE_UNKNOWN`, Watch failure, Watch completion, and an
unimplemented Watch all fail closed. Retriable failures reopen Watch with
bounded backoff; `UNIMPLEMENTED` remains terminal for that endpoint
generation.

Every Health update is fenced by Provider installation identity, endpoint
generation, negotiated session, and accepted manifest revision. A stale Watch
cannot reactivate a replaced or rejected session. Health loss changes only
new-work eligibility; it neither cancels nor invents outcomes for work already
admitted. Provider availability transitions and sanitized reasons are written
to the local Agent journal for diagnostics and later internal consumers.

## Trigger acceptance

Once a Provider is operationally eligible, the Agent opens its Trigger
Publication stream on the existing SFP channel. Incoming events must satisfy
the canonical Protocol contract, belong to the accepted manifest, and match
the declared payload schema. The Agent commits accepted content to its durable
journal before acknowledging the Provider. Exact replays receive a duplicate
acceptance; publication-ID reuse with different content is rejected.

Health loss, endpoint replacement, descriptor removal, and channel failure
cancel the current trigger consumer. A stale stream cannot commit against a
new session or availability generation. Accepted records survive Agent restart
and remain bounded by `journal.maximum_pending_trigger_publications` while
they await the future SF-AOP forwarding implementation.

The packaged service owns the Agent filesystem layout. Persistent state is
stored below `/var/lib/stateforge-agent`, and volatile runtime files are stored
below `/run/stateforge-agent`. These locations are deliberately not
configurable through `config.toml`.
