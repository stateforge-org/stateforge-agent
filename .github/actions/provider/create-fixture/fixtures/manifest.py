"""Run a minimal Provider that declares a valid manifest."""

from __future__ import annotations

import os
from pathlib import Path

from stateforge_rpc import Platform, Provider


class ManifestFixtureProvider(Provider):
    """Expose a small valid manifest through the public Provider API."""

    identity = os.environ["STATEFORGE_TEST_PROVIDER_IDENTITY"]
    display_name = os.environ.get(
        "STATEFORGE_TEST_PROVIDER_DISPLAY_NAME",
        "StateForge manifest fixture",
    )
    version = "1.0.0"
    platforms = (Platform("linux", os.uname().machine),)


def main() -> None:
    """Run the Provider fixture until its service manager stops it."""
    ManifestFixtureProvider(
        discovery_directory=Path(
            os.environ["STATEFORGE_TEST_DISCOVERY_DIRECTORY"]
        ),
        state_directory=Path(os.environ["STATEFORGE_TEST_STATE_DIRECTORY"]),
    ).run()


if __name__ == "__main__":
    main()
