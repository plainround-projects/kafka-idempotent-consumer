"""Argument validation shared by the command-line programs."""

from __future__ import annotations

import argparse


def positive_int(text: str) -> int:
    """Parse an integer of at least 1, for argparse."""
    value = int(text)
    if value < 1:
        raise argparse.ArgumentTypeError(f"must be at least 1, got {value}")
    return value


def non_negative_int(text: str) -> int:
    """Parse an integer of at least 0, for argparse."""
    value = int(text)
    if value < 0:
        raise argparse.ArgumentTypeError(f"must not be negative, got {value}")
    return value


def non_negative_float(text: str) -> float:
    """Parse a float of at least 0, for argparse."""
    value = float(text)
    if value < 0:
        raise argparse.ArgumentTypeError(f"must not be negative, got {value}")
    return value
