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

The packaged service owns the Agent filesystem layout. Persistent state is
stored below `/var/lib/stateforge-agent`, and volatile runtime files are stored
below `/run/stateforge-agent`. These locations are deliberately not
configurable through `config.toml`.
