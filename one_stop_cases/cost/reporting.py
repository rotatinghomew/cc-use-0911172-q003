from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from .analyzer import (
    CLOSURE_CLOSED,
    CLOSURE_INSUFFICIENT,
    CLOSURE_UNCLOSED,
    WORK_CATEGORIES,
    WORK_COMPENSATION,
    WORK_FIRST,
    WORK_REEXECUTION,
    CaseCost,
    WorkItem,
    union_seconds,
)

# Group dimensions. A case always belongs to exactly one intake batch and was
# handled under one service version; unknown values are reported, not guessed.
GROUP_VERSION = 'service_version'
GROUP_BATCH = 'intake_batch_id'
GROUP_VERSION_BATCH = 'service_version|intake_batch_id'
GROUP_CLOSURE = 'closure'

UNKNOWN_DIMENSION = 'unknown'


class CostReportError(ValueError):
    pass


@dataclass(frozen=True)
class GroupSummary:
    group_key: str
    service_version: str | None
    intake_batch_id: str | None
    cases: int
    closed_cases: int
    unclosed_cases: int
    insufficient_cases: int
    intake_count: int
    replay_count: int
    first_execution_items: int
    compensation_items: int
    re_execution_items: int
    effort_seconds: int
    active_seconds: int | None
    wait_seconds: int
    work_item_ids: tuple[str, ...]
    case_ids: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            'group_key': self.group_key,
            'service_version': self.service_version,
            'intake_batch_id': self.intake_batch_id,
            'cases': self.cases,
            'closed_cases': self.closed_cases,
            'unclosed_cases': self.unclosed_cases,
            'insufficient_evidence_cases': self.insufficient_cases,
            'intake_count': self.intake_count,
            'idempotent_replay_count': self.replay_count,
            'work_items': {
                WORK_FIRST: self.first_execution_items,
                WORK_COMPENSATION: self.compensation_items,
                WORK_REEXECUTION: self.re_execution_items,
            },
            'effort_seconds': self.effort_seconds,
            'active_seconds': self.active_seconds,
            'wait_seconds': self.wait_seconds,
            'work_item_ids': list(self.work_item_ids),
            'case_ids': list(self.case_ids),
        }


class CostReporter:
    """Builds traceable details and reconcilable grouped summaries."""

    def detail_rows(self, cases: Sequence[CaseCost]) -> tuple[dict[str, Any], ...]:
        rows: list[dict[str, Any]] = []
        for case in cases:
            for item in case.work_items:
                rows.append(self._detail_row(case, item))
        return tuple(sorted(rows, key=lambda row: (row['case_id'], row['started_at'] or '', row['work_id'])))

    def _detail_row(self, case: CaseCost, item: WorkItem) -> dict[str, Any]:
        return {
            'work_id': item.work_id,
            'case_id': item.case_id,
            'service_version': case.service_version,
            'intake_batch_id': case.intake_batch_id,
            'step_id': item.step_id,
            'category': item.category,
            'attempt': item.attempt,
            'status': item.status,
            'started_at': item.started_at.isoformat() if item.started_at else None,
            'finished_at': item.finished_at.isoformat() if item.finished_at else None,
            'duration_seconds': item.duration_seconds,
            'wait_seconds': item.wait_seconds,
            'closure': case.closure,
            'evidence_event_ids': list(item.evidence),
        }

    def case_rows(self, cases: Sequence[CaseCost]) -> tuple[dict[str, Any], ...]:
        rows = []
        for case in cases:
            active_seconds = case.active_seconds()
            rows.append({
                'case_id': case.case_id,
                'service_version': case.service_version,
                'intake_batch_id': case.intake_batch_id,
                'closure': case.closure,
                'closed_normally': case.closed_normally,
                'intake_count': case.intake_count,
                'idempotent_replay_count': case.replay_count,
                'work_item_counts': case.category_counts(),
                'effort_seconds': case.effort_seconds(),
                'active_seconds': active_seconds,
                'active_seconds_complete': active_seconds is not None,
                'wait_seconds': sum(item.wait_seconds for item in case.work_items),
                'first_seen_at': case.first_seen_at.isoformat() if case.first_seen_at else None,
                'last_event_at': case.last_event_at.isoformat() if case.last_event_at else None,
                'closed_at': case.closed_at.isoformat() if case.closed_at else None,
                'evidence_problems': list(case.evidence_problems),
                'evidence_event_ids': list(case.event_ids),
            })
        return tuple(sorted(rows, key=lambda row: row['case_id']))

    def grouped_summary(self, cases: Sequence[CaseCost],
                        dimension: str = GROUP_VERSION_BATCH) -> tuple[GroupSummary, ...]:
        if dimension not in {GROUP_VERSION, GROUP_BATCH, GROUP_VERSION_BATCH, GROUP_CLOSURE}:
            raise CostReportError(f'unsupported group dimension: {dimension}')
        buckets: dict[tuple[Any, ...], list[CaseCost]] = defaultdict(list)
        for case in cases:
            buckets[self._bucket_key(case, dimension)].append(case)
        summaries = [self._summarize(key, dimension, grouped)
                     for key, grouped in sorted(buckets.items(),
                                                key=lambda kv: tuple(str(part) for part in kv[0]))]
        return tuple(summaries)

    def _bucket_key(self, case: CaseCost, dimension: str) -> tuple[Any, ...]:
        version = case.service_version or UNKNOWN_DIMENSION
        batch = case.intake_batch_id or UNKNOWN_DIMENSION
        if dimension == GROUP_VERSION:
            return (version,)
        if dimension == GROUP_BATCH:
            return (batch,)
        if dimension == GROUP_CLOSURE:
            return (case.closure,)
        return (version, batch)

    def _summarize(self, key: tuple[Any, ...], dimension: str,
                   grouped: Sequence[CaseCost]) -> GroupSummary:
        if dimension == GROUP_VERSION:
            version, batch = key[0], None
        elif dimension == GROUP_BATCH:
            version, batch = None, key[0]
        elif dimension == GROUP_CLOSURE:
            version, batch = None, None
        else:
            version, batch = key
        work_ids = tuple(item.work_id for case in grouped for item in case.work_items)
        case_ids = tuple(case.case_id for case in grouped)
        items = [item for case in grouped for item in case.work_items]
        # Parallel-aware active time: union intervals within each case first,
        # then sum across cases — never by adding parallel step durations.
        per_case_active = [case.active_seconds() for case in grouped]
        active_seconds = sum(value for value in per_case_active if value is not None)
        if any(value is None for value in per_case_active):
            active_seconds = None
        counts = {category: 0 for category in WORK_CATEGORIES}
        for item in items:
            counts[item.category] += 1
        return GroupSummary(
            group_key='|'.join(str(part) for part in key),
            service_version=version,
            intake_batch_id=batch,
            cases=len(grouped),
            closed_cases=sum(case.closure == CLOSURE_CLOSED for case in grouped),
            unclosed_cases=sum(case.closure == CLOSURE_UNCLOSED for case in grouped),
            insufficient_cases=sum(case.closure == CLOSURE_INSUFFICIENT for case in grouped),
            intake_count=sum(case.intake_count for case in grouped),
            replay_count=sum(case.replay_count for case in grouped),
            first_execution_items=counts[WORK_FIRST],
            compensation_items=counts[WORK_COMPENSATION],
            re_execution_items=counts[WORK_REEXECUTION],
            effort_seconds=sum(item.duration_seconds or 0 for item in items),
            active_seconds=active_seconds,
            wait_seconds=sum(item.wait_seconds for item in items),
            work_item_ids=tuple(sorted(work_ids)),
            case_ids=tuple(sorted(case_ids)),
        )

    def reconciliation(self, cases: Sequence[CaseCost],
                       summaries: Sequence[GroupSummary]) -> dict[str, Any]:
        """Cross-check that groups sum back to the whole population."""
        detail_ids = sorted(item.work_id for case in cases for item in case.work_items)
        grouped_ids = sorted(work_id for summary in summaries for work_id in summary.work_item_ids)
        case_ids = sorted(case.case_id for case in cases)
        grouped_case_ids = sorted(case_id for summary in summaries for case_id in summary.case_ids)
        checks = {
            'cases_match': case_ids == grouped_case_ids,
            'work_items_match': detail_ids == grouped_ids,
            'intake_match': sum(case.intake_count for case in cases)
                             == sum(summary.intake_count for summary in summaries),
            'replay_match': sum(case.replay_count for case in cases)
                            == sum(summary.replay_count for summary in summaries),
            'effort_match': sum(case.effort_seconds() for case in cases)
                            == sum(summary.effort_seconds for summary in summaries),
            'wait_match': sum(item.wait_seconds for case in cases for item in case.work_items)
                          == sum(summary.wait_seconds for summary in summaries),
            'category_counts_match': all(
                sum(case.category_counts()[category] for case in cases)
                == sum(getattr(summary, f'{category}_items') for summary in summaries)
                for category in WORK_CATEGORIES),
        }
        return {
            'balanced': all(checks.values()),
            'checks': checks,
            'totals': {
                'cases': len(cases),
                'work_items': len(detail_ids),
                'intake_count': sum(case.intake_count for case in cases),
                'idempotent_replay_count': sum(case.replay_count for case in cases),
                'effort_seconds': sum(case.effort_seconds() for case in cases),
                'wait_seconds': sum(item.wait_seconds for case in cases for item in case.work_items),
            },
        }

    def report(self, cases: Sequence[CaseCost],
               dimension: str = GROUP_VERSION_BATCH) -> dict[str, Any]:
        summaries = self.grouped_summary(cases, dimension)
        closure_counts = {
            CLOSURE_CLOSED: sum(case.closure == CLOSURE_CLOSED for case in cases),
            CLOSURE_UNCLOSED: sum(case.closure == CLOSURE_UNCLOSED for case in cases),
            CLOSURE_INSUFFICIENT: sum(case.closure == CLOSURE_INSUFFICIENT for case in cases),
        }
        return {
            'generated_for': 'actual_handling_cost',
            'group_dimension': dimension,
            'case_summary': {
                'total_cases': len(cases),
                'closure_counts': closure_counts,
                'closed_normally': sum(case.closed_normally for case in cases),
                'intake_count': sum(case.intake_count for case in cases),
                'idempotent_replay_count': sum(case.replay_count for case in cases),
            },
            'case_details': list(self.case_rows(cases)),
            'work_details': list(self.detail_rows(cases)),
            'grouped_summary': [summary.as_dict() for summary in summaries],
            'reconciliation': self.reconciliation(cases, summaries),
        }
