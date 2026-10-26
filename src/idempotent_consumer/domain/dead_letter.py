"""A record parked in the dead-letter topic, with everything needed to find and replay it."""

from __future__ import annotations

import base64
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class DeadLetter:
    """A record parked in the dead-letter topic, with where it came from and why."""

    original_topic: str
    original_partition: int
    original_offset: int
    error: str
    raw: bytes

    def payload(self) -> dict[str, Any]:
        return {
            "originalTopic": self.original_topic,
            "originalPartition": self.original_partition,
            "originalOffset": self.original_offset,
            "error": self.error,
            "raw": self.raw.decode("utf-8", errors="replace"),
            # The exact bytes, so a replay is byte-faithful even when they were not valid text.
            "rawBase64": base64.b64encode(self.raw).decode("ascii"),
        }
