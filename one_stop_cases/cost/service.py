from __future__ import annotations

from typing import Any, Mapping, Sequence

from ..storage import Database
from .analyzer import CostAnalyzer
from .events import events_from_database, events_from_records
from .reporting import GROUP_VERSION_BATCH, CostReporter


class CostAnalysisService:
    """Read-only entry point for actual handling cost analysis.

    It reuses the existing step dependencies (via ``workflows``), execution
    results and compensation records. It never creates steps, writes execution
    results, changes compensation plans, or inflates intake volume.
    """

    def __init__(self, database: Database,
                 workflows: Mapping[str, Sequence[Mapping[str, Any]]] | None = None):
        self.database = database
        self.analyzer = CostAnalyzer(workflows)
        self.reporter = CostReporter()

    def collect_events(self) -> tuple[tuple, tuple[str, ...]]:
        store_events, store_problems = events_from_database(self.database)
        record_events, record_problems = events_from_records(
            self.database.list_records('execution'),
            self.database.list_records('compensation'),
        )
        # De-duplicate identical evidence (event store + record adapter may
        # describe the same occurrence) by event id, keeping first source.
        seen: dict[str, Any] = {}
        for event in (*store_events, *record_events):
            seen.setdefault(event.event_id, event)
        problems = tuple(sorted(set(store_problems) | set(record_problems)))
        return tuple(seen.values()), problems

    def analyze(self) -> tuple:
        events, self.last_evidence_problems = self.collect_events()
        cases = self.analyzer.analyze(events, self.last_evidence_problems)
        return cases

    def report(self, dimension: str = GROUP_VERSION_BATCH) -> dict[str, Any]:
        events, problems = self.collect_events()
        cases = self.analyzer.analyze(events, problems)
        payload = self.reporter.report(cases, dimension)
        # Global problems (e.g. unparseable event-store rows) are not tied to a
        # case; surface them explicitly rather than treating them as no data.
        payload['evidence_problems'] = sorted(self.analyzer.global_evidence_problems)
        return payload
