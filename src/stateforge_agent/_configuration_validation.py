"""Validate numeric configuration shared across Agent subsystems."""

from __future__ import annotations

import math


def require_finite_positive(value: object, *, name: str) -> float:
    """Return a finite positive numeric configuration value."""
    number = _require_finite_number(value, name=name)
    if number <= 0:
        raise ValueError(f"{name} must be a finite positive number")
    return number


def require_finite_non_negative(value: object, *, name: str) -> float:
    """Return a finite non-negative numeric configuration value."""
    number = _require_finite_number(value, name=name)
    if number < 0:
        raise ValueError(f"{name} must be a finite non-negative number")
    return number


def require_bounded_integer(
    value: object,
    *,
    name: str,
    minimum: int,
    maximum: int | None = None,
) -> int:
    """Return a non-boolean integer within an inclusive range."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    if value < minimum or (maximum is not None and value > maximum):
        if maximum is None:
            raise ValueError(
                f"{name} must be greater than or equal to {minimum}"
            )
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return value


def _require_finite_number(value: object, *, name: str) -> float:
    """Return a finite float while excluding boolean pseudo-numbers."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a finite number")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"{name} must be a finite number")
    return number
