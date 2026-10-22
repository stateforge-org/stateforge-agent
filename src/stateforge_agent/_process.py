"""Define StateForge Agent-owned process exit statuses."""

from enum import IntEnum


class ProcessExitCode(IntEnum):
    """Describe stable exit statuses returned by the Agent executable."""

    SUCCESS = 0
    RUNTIME_FAILURE = 1
    INVALID_INPUT = 2
