"""Where the crash test kills the consumer."""

from __future__ import annotations


def kill_points(runs: int, payments: int) -> list[int]:
    """`runs` different record numbers spread over 1..payments, the same every time."""
    if runs < 1 or payments < 1:
        raise ValueError("runs and payments must be at least 1")
    step = max(payments // (runs + 1), 1)
    return [min(payments, step * run + (run * 37) % max(step // 2, 1)) for run in range(1, runs + 1)]
