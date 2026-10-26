"""Use case: ask every dependency whether it is usable."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from .ports import HealthProbe


class ProbeFailedError(Exception):
    """A dependency is not usable; the message says why."""


@dataclass(frozen=True, slots=True)
class ProbeResult:
    name: str
    ok: bool
    detail: str


@dataclass(frozen=True, slots=True)
class HealthReport:
    results: tuple[ProbeResult, ...]

    @property
    def healthy(self) -> bool:
        return all(result.ok for result in self.results)


def check_health(probes: Iterable[HealthProbe]) -> HealthReport:
    results = []
    for probe in probes:
        try:
            results.append(ProbeResult(probe.name, True, probe.check()))
        except ProbeFailedError as error:
            results.append(ProbeResult(probe.name, False, str(error)))
    return HealthReport(tuple(results))
