"""Graceful shutdown: SIGINT and SIGTERM set a flag the work loops check between units of work."""

from __future__ import annotations

import signal
import threading
from types import FrameType


class ShutdownSignal:
    """A flag that SIGINT and SIGTERM set, which work loops check between units of work."""

    def __init__(self) -> None:
        self._requested = threading.Event()

    def install(self) -> None:
        """Route SIGINT and SIGTERM to `request`, so in-flight work finishes and connections close."""
        for number in (signal.SIGINT, signal.SIGTERM):
            signal.signal(number, self._handle)

    def _handle(self, _number: int, _frame: FrameType | None) -> None:
        self.request()

    def request(self) -> None:
        self._requested.set()

    @property
    def requested(self) -> bool:
        return self._requested.is_set()
