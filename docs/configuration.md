# Agent configuration file

The StateForge Agent uses TOML configuration. The packaged primary file is:

```text
/etc/stateforge-agent/config.toml
```

The configuration is strict: every documented top-level section and field is
required in the final merged configuration, unknown fields are rejected, and
values are not silently coerced to another type.

!!! important

    The packaged file intentionally contains an empty `control_plane.url`.
    Configure that value before starting the Agent. An empty URL does not pass
    validation.

## Complete example

```toml
schema_version = 1

[control_plane]
url = "https://control.stateforge.example"
ca_certificate = "/etc/pki/tls/certs/ca-bundle.crt"
connect_timeout_seconds = 10
request_timeout_seconds = 30

[providers]
health_interval_seconds = 15
reconnect_initial_delay_seconds = 1
reconnect_max_delay_seconds = 30
trigger_max_in_flight_publications = 32

[journal]
backend = "sqlite"
maximum_pending_trigger_publications = 10000

[logging]
level = "info"
format = "text"
destination = "stderr"
```

## Top-level fields

| Field | Type | Required | Accepted value | Purpose |
| --- | --- | --- | --- | --- |
| `schema_version` | Integer | Yes | `1` | Selects the configuration schema understood by this Agent release. |
| `control_plane` | Table | Yes | See below | Defines the Control Plane connection. |
| `providers` | Table | Yes | See below | Defines local Provider discovery and connection behavior. |
| `journal` | Table | Yes | See below | Defines durable local journal storage. |
| `logging` | Table | Yes | See below | Defines process logging. |

## Runtime filesystem layout

Agent-owned filesystem locations are part of the platform packaging contract,
not operator configuration. This prevents `config.toml` from selecting paths
that the service manager did not create or grant to the Agent process.

| Purpose | Packaged path | Owner |
| --- | --- | --- |
| Persistent Agent state | `/var/lib/stateforge-agent` | systemd `StateDirectory` |
| Operation staging | `/var/lib/stateforge-agent/staging` | StateForge Agent |
| Durable journal | `/var/lib/stateforge-agent/journal/agent.db` | StateForge Agent |
| Volatile runtime state | `/run/stateforge-agent` | RPM `tmpfiles.d` policy |
| Local control socket | `/run/stateforge-agent/control.sock` | StateForge Agent |
| Provider discovery | `/run/stateforge-agent/providers` | RPM `tmpfiles.d` policy |

Tests and alternative packaging may supply a different internal runtime
layout. The TOML schema intentionally provides no filesystem-location
overrides.

## `[control_plane]`

| Field | Type | Packaged value | Validation and purpose |
| --- | --- | --- | --- |
| `url` | String | Empty; must be configured | Absolute HTTPS base URL for the StateForge Control Plane. It must contain a hostname and must not contain embedded credentials, a query string, or a fragment. |
| `ca_certificate` | String path | `/etc/pki/tls/certs/ca-bundle.crt` | Absolute path to the CA certificate bundle used to authenticate the remote endpoint. |
| `connect_timeout_seconds` | Integer | `10` | Connection establishment timeout. Must be greater than zero. |
| `request_timeout_seconds` | Integer | `30` | Control Plane request timeout. Must be greater than zero. |

A path component in `url` is allowed. Credentials must never be embedded in
the URL:

```toml
# Valid
url = "https://control.example.com/stateforge"

# Invalid: HTTP is not accepted
url = "http://control.example.com"

# Invalid: embedded credentials are not accepted
url = "https://user:password@control.example.com"

# Invalid: query strings and fragments are not accepted
url = "https://control.example.com?token=value"
```

The loader validates the CA bundle path syntactically as an absolute path. It
does not check whether the referenced file exists during `check-config`.

## `[providers]`

| Field | Type | Packaged value | Validation and purpose |
| --- | --- | --- | --- |
| `health_interval_seconds` | Integer | `15` | Maximum bounded delay before reopening a failed Provider Health Watch. Must be greater than zero. The Agent does not poll healthy Providers. |
| `reconnect_initial_delay_seconds` | Integer | `1` | Initial bounded-backoff delay after a failed Provider connection. Must be greater than zero. |
| `reconnect_max_delay_seconds` | Integer | `30` | Maximum reconnect delay. Must be greater than zero and greater than or equal to the initial delay. |
| `trigger_max_in_flight_publications` | Integer | `32` | Requested SFP Trigger Publication receive window. Must be between 1 and the Protocol-owned maximum of 1,024. |

The RPM creates the Provider discovery directory through `systemd-tmpfiles`.
Provider processes publish their endpoint descriptors and Unix domain sockets
there; the Agent does not use this directory for Provider-specific data.

## `[journal]`

| Field | Type | Packaged value | Validation and purpose |
| --- | --- | --- | --- |
| `backend` | String | `sqlite` | Selects the durable journal implementation. `sqlite` selects the bundled SQLite implementation and is the only backend shipped in schema version 1. |
| `maximum_pending_trigger_publications` | Integer | `10000` | Maximum accepted trigger publications retained for later SF-AOP forwarding. Must be greater than zero. When full, new publications receive a retryable capacity rejection. |

The journal database is stored at the package-owned path
`/var/lib/stateforge-agent/journal/agent.db`.

Journal implementations conform to storage-neutral Agent contracts. Runtime
services do not depend on SQLite classes or SQL; another backend can supply its
own journal and durable service stores under a different backend name.

The selected Backup Engine does not belong in this section. Backup Engines are
operation tools selected through Control Plane policy rather than global Agent
configuration.

## `[logging]`

| Field | Type | Packaged value | Accepted values and purpose |
| --- | --- | --- | --- |
| `level` | String | `info` | `debug`, `info`, `warning`, `error`, or `critical`. |
| `format` | String | `text` | `text` or `json`. |
| `destination` | String | `stderr` | `stderr` is the only destination supported by schema version 1. |

When the Agent runs under systemd, output written to standard error is captured
by the service manager and is available through `journalctl`.

## Drop-in configuration

By default, the Agent loads partial overrides from:

```text
/etc/stateforge-agent/conf.d/*.toml
```

Sources are applied in this order:

1. The primary `config.toml` file.
2. Files ending in `.toml` from `conf.d`, sorted lexicographically by filename.

Tables are merged recursively. When multiple sources define the same final
field, the value from the later source wins. Drop-ins may contain only the
tables and fields they override; the final merged result must still contain
the complete schema.

For example:

```text
/etc/stateforge-agent/config.toml
/etc/stateforge-agent/conf.d/10-logging.toml
/etc/stateforge-agent/conf.d/90-local.toml
```

If both drop-ins define `logging.level`, `90-local.toml` wins:

```toml
# /etc/stateforge-agent/conf.d/10-logging.toml
[logging]
level = "warning"
```

```toml
# /etc/stateforge-agent/conf.d/90-local.toml
[logging]
level = "debug"
```

Only the filename controls precedence. A file's creation time, modification
time, and package origin have no effect.

!!! warning

    Do not put credentials or other secrets directly in the configuration.
    Use protected credential files or the credential mechanism defined for the
    relevant subsystem.

## File ownership and permissions

The RPM installs the primary file with this ownership and mode:

```text
root:stateforge-agent 0640
```

It installs `/etc/stateforge-agent` and `conf.d` as
`root:stateforge-agent 0750`. Administrators should give drop-ins the same
`root:stateforge-agent 0640` ownership and mode unless a stricter deployment
policy applies.

The primary file is packaged with RPM `%config(noreplace)` semantics, so a
package upgrade does not silently replace an administrator-modified file.

## Validation

Validate the effective configuration before starting or restarting the Agent:

```console
stateforge-agent check-config
```

A successful result includes:

```text
Configuration is valid.
Fingerprint: sha256:...
Sources:
- /etc/stateforge-agent/config.toml
- /etc/stateforge-agent/conf.d/90-local.toml
```

The fingerprint identifies the validated, effective configuration after all
drop-ins have been merged. It is not the checksum of any individual source
file. Two different source layouts that produce the same effective values
produce the same fingerprint.

Validation errors identify both the dotted field name and the source that
defined the invalid value. `check-config` returns exit status `0` when valid
and `2` when loading or validation fails. See
[Process exit status](running.md#process-exit-status) for the complete
executable contract and packaged-service restart behavior.

You can validate another primary file:

```console
stateforge-agent check-config --config /path/to/config.toml
```

You can also override the drop-in directory:

```console
stateforge-agent check-config \
    --config /path/to/config.toml \
    --drop-in-directory /path/to/conf.d
```

If `--drop-in-directory` is omitted, the Agent always looks for `conf.d`
beside the selected primary file.

## Reload behavior

The current Agent implementation loads configuration once during process
startup. It does not currently reload configuration through SIGHUP, a control
socket, or filesystem monitoring. Apply a validated change by restarting the
service:

```console
sudo stateforge-agent check-config
sudo systemctl restart stateforge-agent
```

Runtime reload will require a separately implemented and tested command and a
clear field-by-field reloadability contract. Until that exists, documentation
must not imply that `systemctl reload stateforge-agent` applies changes.

## Schema-version behavior

Schema version `1` is the only supported configuration schema. The Agent
rejects a missing, non-integer, older, or newer schema version instead of
guessing compatibility.

When a later schema is introduced, migrations and compatibility behavior must
be documented before that version is accepted by the loader.
