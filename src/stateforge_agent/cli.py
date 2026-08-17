"""Command-line interface for the StateForge Agent."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Iterable, Sequence
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

from stateforge_agent._config import ConfigurationError, load_configuration
from stateforge_agent._runtime import run_agent

_DEFAULT_CONFIG_PATH = Path("/etc/stateforge-agent/config.toml")
_DISTRIBUTION_NAME = "stateforge-agent"


class _StateForgeHelpFormatter(argparse.HelpFormatter):
    """Format command help using the StateForge CLI presentation."""

    def add_usage(
        self,
        usage: str | None,
        actions: Iterable[argparse.Action],
        groups: Iterable[argparse._MutuallyExclusiveGroup],
        prefix: str | None = None,
    ) -> None:
        """Render the usage heading with command-line convention casing."""
        super().add_usage(usage, actions, groups, prefix or "Usage: ")

    def _format_action(self, action: argparse.Action) -> str:
        """Render subcommands directly without an argparse metavar row."""
        if isinstance(action, argparse._SubParsersAction):
            return "".join(
                self._format_action(subaction)
                for subaction in action._get_subactions()
            )
        return super()._format_action(action)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the StateForge Agent command-line interface."""
    parser = _create_parser()
    arguments = parser.parse_args(argv)

    if arguments.command == "run":
        return _run(
            config_path=Path(arguments.config),
            drop_in_directory=(
                Path(arguments.drop_in_directory)
                if arguments.drop_in_directory is not None
                else None
            ),
        )

    if arguments.command == "check-config":
        return _check_config(
            config_path=Path(arguments.config),
            drop_in_directory=(
                Path(arguments.drop_in_directory)
                if arguments.drop_in_directory is not None
                else None
            ),
        )

    parser.error(f"unsupported command: {arguments.command}")


def _create_parser() -> argparse.ArgumentParser:
    """Create the top-level argument parser."""
    parser = argparse.ArgumentParser(
        prog="stateforge-agent",
        usage="%(prog)s [OPTIONS] COMMAND",
        description="A secure execution runtime for StateForge operations.",
        epilog=(
            "Run 'stateforge-agent COMMAND --help' for more information "
            "on a command."
        ),
        formatter_class=_StateForgeHelpFormatter,
        add_help=False,
    )
    subcommands = parser.add_subparsers(
        dest="command",
        title="Commands",
        metavar="COMMAND",
        required=True,
    )

    run = subcommands.add_parser(
        "run",
        prog="stateforge-agent run",
        usage="%(prog)s [OPTIONS]",
        help="Run the StateForge Agent daemon",
        description="Run the long-lived StateForge Agent process.",
        formatter_class=_StateForgeHelpFormatter,
        add_help=False,
    )
    _add_configuration_arguments(run)

    check_config = subcommands.add_parser(
        "check-config",
        prog="stateforge-agent check-config",
        usage="%(prog)s [OPTIONS]",
        help="Validate the effective Agent configuration",
        description="Validate the effective StateForge Agent configuration.",
        formatter_class=_StateForgeHelpFormatter,
        add_help=False,
    )
    _add_configuration_arguments(check_config)

    global_options = parser.add_argument_group("Global Options")
    global_options.add_argument(
        "-h",
        "--help",
        action="help",
        help="Show this help message and exit",
    )
    global_options.add_argument(
        "-v",
        "--version",
        action="version",
        version=f"%(prog)s {_distribution_version()}",
        help="Print version information and quit",
    )
    return parser


def _add_configuration_arguments(parser: argparse.ArgumentParser) -> None:
    """Add shared configuration-source arguments to a command."""
    options = parser.add_argument_group("Options")
    options.add_argument(
        "-h",
        "--help",
        action="help",
        help="Show this help message and exit",
    )
    options.add_argument(
        "--config",
        default=_DEFAULT_CONFIG_PATH,
        metavar="PATH",
        help=f"primary configuration file (default: {_DEFAULT_CONFIG_PATH})",
    )
    options.add_argument(
        "--drop-in-directory",
        metavar="PATH",
        help="override the default conf.d directory",
    )


def _distribution_version() -> str:
    """Return the installed package version for CLI reporting."""
    try:
        return version(_DISTRIBUTION_NAME)
    except PackageNotFoundError:
        return "unknown"


def _run(
    *,
    config_path: Path,
    drop_in_directory: Path | None,
) -> int:
    """Load configuration and start the long-running Agent runtime."""
    try:
        loaded = load_configuration(
            config_path,
            drop_in_directory=drop_in_directory,
        )
    except ConfigurationError as error:
        sys.stderr.write(f"{error}\n")
        return 2
    return run_agent(loaded.config)


def _check_config(
    *,
    config_path: Path,
    drop_in_directory: Path | None,
) -> int:
    """Validate configuration and report its effective source identity."""
    try:
        loaded = load_configuration(
            config_path,
            drop_in_directory=drop_in_directory,
        )
    except ConfigurationError as error:
        sys.stderr.write(f"{error}\n")
        return 2

    source_list = "\n".join(f"- {source}" for source in loaded.sources)
    sys.stdout.write(
        "Configuration is valid.\n"
        f"Fingerprint: {loaded.fingerprint}\n"
        f"Sources:\n{source_list}\n"
    )
    return 0
