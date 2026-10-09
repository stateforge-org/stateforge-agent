# StateForge Agent

StateForge Agent is the secure execution daemon for the StateForge platform.
It runs inside a customer environment, discovers local Provider processes,
maintains StateForge Protocol (SFP) sessions, and coordinates authorized
operations without embedding third-party-specific logic in the Agent.

The project is under active development and is not yet ready for production
use.

## Current scope

The current implementation provides:

- strict TOML configuration with ordered `conf.d` overrides;
- a long-running foreground daemon;
- protected local Provider endpoint discovery;
- validated and reusable local gRPC channels;
- SFP handshake and version negotiation;
- bounded Provider manifest retrieval, validation, and declaration caching; and
- bounded reconnection and endpoint replacement.

Providers run as separate processes. They expose vendor-specific behavior over
SFP, while the Agent remains vendor-independent.

After a successful handshake, the Agent retrieves the Provider's complete
manifest over the same SFP channel. A Provider is marked connected only after
the manifest passes the negotiated schema, canonical-revision, size, embedded
JSON Schema, and secret-safety checks. Connected describes the local SFP
transport and accepted declaration; it does not assert that the Provider or
its third-party component is operationally healthy.

## Requirements

- Python 3.11 or later
- Linux for the packaged daemon and Unix-domain-socket integration

## Development installation

Create and activate a virtual environment, then install the project and its
development and documentation dependency groups with a current version of
`pip`:

```console
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install --editable . --group dev --group docs
```

The StateForge Protocol distribution must be available to the selected package
installer. During coordinated local development, install the sibling
`stateforge-protocol` project first.

## Commands

Inspect the command-line interface:

```console
stateforge-agent --help
```

Validate an effective configuration:

```console
stateforge-agent check-config --config /path/to/config.toml
```

Run the foreground daemon:

```console
stateforge-agent run --config /path/to/config.toml
```

The RPM package supplies the system configuration, service account, runtime
directories, and systemd unit required for a managed installation.

Build and verify that RPM through the same reusable commands used by GitHub
Actions:

```console
packaging/rpm/build-rpm.sh
packaging/rpm/test-installed-rpm.sh \
  --rpm dist/rpm/stateforge-agent-0.1.0-1.fc42.noarch.rpm
```

See [RPM CI/CD building blocks](docs/ci-cd.md) for the composite actions,
reusable workflows, exact-artifact handoff, and release-candidate gate.

## Quality checks

```console
ruff format --check .
ruff check .
mypy src
pytest
python -m build
mkdocs build --strict
```

The test suite includes integration coverage for Provider discovery, channel
establishment, SFP negotiation, manifest exchange, endpoint replacement,
connection loss, and recovery.

## Documentation

The source documentation is in [`docs/`](docs/index.md). Build and serve it
locally with:

```console
mkdocs serve
```

## Source and license

Source code and issue tracking are hosted in the
[StateForge Agent repository][source].

Copyright StateForge. Licensed under the
[Apache License 2.0](LICENSE).

[source]: https://github.com/stateforge-org/stateforge-agent
