"""Configuration-specific exceptions."""

from collections.abc import Sequence


class ConfigurationError(Exception):
    """Base exception for configuration loading failures."""


class ConfigurationFileError(ConfigurationError):
    """Report a configuration source that cannot be loaded."""


class ConfigurationValidationError(ConfigurationError):
    """Report one or more invalid effective configuration values."""

    def __init__(self, issues: Sequence[str]) -> None:
        """Create an error from ordered validation issues."""
        self.issues = tuple(issues)
        details = "\n".join(f"- {issue}" for issue in self.issues)
        super().__init__(f"Configuration validation failed:\n{details}")
