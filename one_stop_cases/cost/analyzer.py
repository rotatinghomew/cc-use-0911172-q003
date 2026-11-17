from __future__ import annotations

import datetime as dt
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from ..graph_runtime.orchestrator import CaseOrchestrator
from .events import (
    CANONICAL_KINDS,
    KIND_CASE_CANCELLED,
    KIND_CASE_COMPLETED,
    KIND_CASE_INTAKE,
    KIND_CASE_VERSION,
    KIND_COMPENSATION_COMPLETED,
    KIND_COMPENSATION_STARTED,
    KIND_STEP_FAILED,
    KIND_STEP_RESTARTED,
    KIND_STEP_STARTED,
    KIND_STEP_SUCCEEDED,
    CostEvent,
    _ATTEMPT_KEYS,
    _BATCH_KEYS,
    _REQUEST_KEYS,
    _STEP_KEYS,
    _VERSION_KEYS,
    _first_value,
)

# Work categories. They deliberately separate rework from first-time work so
# failed retries can never be counted as new intake volume.
WORK_FIRST = 'first_execution'
WORK_COMPENSATION = 'compensation'
WORK_REEXECUTION = 're_execution'
WORK_CATEGORIES = (WORK_FIRST, WORK_COMPENSATION, WORK_REEXECUTION)

# Closure verdicts for a case.
CLOSURE_CLOSED = 'closed'
CLOSURE_UNCLOSED = 'unclosed'
CLOSURE_INSUFFICIENT = 'insufficient_evidence'

_TERMINAL_KINDS = frozenset({KIND_CASE_COMPLETED, KIND_CASE_CANCELLED})
_STEP_TERMINAL = frozenset({KIND_STEP_SUCCEEDED, KIND_STEP_FAILED})
_OPEN_STEP_KINDS = frozenset({KIND_STEP_STARTED, KIND_STEP_RESTARTED})
_OPEN_COMPENSATION = frozenset({KIND_COMPENSATION_STARTED})


class CostAnalysisError(ValueError):
    pass


@dataclass(frozen=True)
class WorkItem:
    """One unit of effective work, always traceable to case + step + events."""
    work_id: str
    case_id: str
    step_id: str
    category: str
    attempt: int
    started_at: dt.datetime | None
    finished_at: dt.datetime | None
    duration_seconds: int | None
    wait_seconds: int
    evidence: tuple[str, ...]
    status: str

    def as_dict(self) -> dict[str, Any]:
        return {
            'work_id': self.work_id,
            'case_id': self.case_id,
            'step_id': self.step_id,
            'category': self.category,
            'attempt': self.attempt,
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'finished_at': self.finished_at.isoformat() if self.finished_at else None,
            'duration_seconds': self.duration_seconds,
            'wait_seconds': self.wait_seconds,
            'status': self.status,
            'evidence': list(self.evidence),
        }


@dataclass
class CaseCost:
    case_id: str
    service_version: str | None
    intake_batch_id: str | None
    closure: str
    closed_normally: bool
    intake_count: int
    replay_count: int
    work_items: list[WorkItem] = field(default_factory=list)
    evidence_problems: list[str] = field(default_factory=list)
    event_ids: list[str] = field(default_factory=list)
    first_seen_at: dt.datetime | None = None
    last_event_at: dt.datetime | None = None
    closed_at: dt.datetime | None = None

    def category_counts(self) -> dict[str, int]:
        counts = {category: 0 for category in WORK_CATEGORIES}
        for item in self.work_items:
            counts[item.category] += 1
        return counts

    def effort_seconds(self) -> int:
        """Sum of per-step effective work durations.

        Each step attempt is measured independently from its own start/finish;
        this is the effort actually consumed, not a wall-clock wait.
        """
        return sum(item.duration_seconds or 0 for item in self.work_items)

    def active_seconds(self) -> int | None:
        """Case-level active time with overlapping parallel work merged.

        Parallel steps overlap in wall-clock time, so their intervals are
        unioned before measuring; adding them would double-count shared waits.
        Returns ``None`` when any work item lacks a start/finish boundary.
        """
        intervals: list[tuple[dt.datetime, dt.datetime]] = []
        for item in self.work_items:
            if item.started_at is None or item.finished_at is None:
                return None
            intervals.append((item.started_at, item.finished_at))
        return union_seconds(intervals)


def union_seconds(intervals: Sequence[tuple[dt.datetime, dt.datetime]]) -> int:
    """Total covered seconds after merging overlapping/adjacent intervals."""
    if not intervals:
        return 0
    ordered = sorted(intervals, key=lambda pair: pair[0])
    total = 0
    current_start, current_end = ordered[0]
    for start, end in ordered[1:]:
        if start <= current_end:
            if end > current_end:
                current_end = end
        else:
            total += int((current_end - current_start).total_seconds())
            current_start, current_end = start, end
    total += int((current_end - current_start).total_seconds())
    return total


@dataclass
class _StepState:
    step_id: str
    open_event: CostEvent | None = None
    open_category: str = WORK_FIRST
    open_attempt: int = 1
    open_wait_seconds: int = 0
    last_finished_at: dt.datetime | None = None
    last_ready_at: dt.datetime | None = None
    finished_count: int = 0
    ever_failed: bool = False
    compensation_open: CostEvent | None = None


class CostAnalyzer:
    """Reconstructs actual handling cost from normalized cost events.

    The analyzer is strictly read-only: it never creates steps, changes
    execution results, or alters compensation plans. It only interprets the
    events those existing mechanisms already produced.
    """

    def __init__(self, workflows: Mapping[str, Sequence[Mapping[str, Any]]] | None = None):
        # Keyed by service version; each value is a step list with `requires`.
        self.workflows = dict(workflows or {})
        self._orchestrator = CaseOrchestrator()
        self.global_evidence_problems: tuple[str, ...] = ()

    # ------------------------------------------------------------------
    def analyze(self, events: Sequence[CostEvent],
                evidence_problems: Sequence[str] = ()) -> tuple[CaseCost, ...]:
        """Reconstruct cases from normalized events.

        ``evidence_problems`` are global parse-level problems (e.g. an event
        store row that could not be normalized). They are returned to callers
        but never attached to an individual case, since they are not evidence
        about that case and must not flip a well-formed case's closure verdict.
        """
        self.global_evidence_problems = tuple(evidence_problems)
        by_case: dict[str, list[CostEvent]] = defaultdict(list)
        for event in events:
            by_case[event.case_id].append(event)
        result: list[CaseCost] = []
        for case_id in sorted(by_case):
            result.append(self._analyze_case(case_id, by_case[case_id]))
        return tuple(result)

    # ------------------------------------------------------------------
    def _analyze_case(self, case_id: str, events: Sequence[CostEvent]) -> CaseCost:
        ordered = sorted(events, key=lambda event: (event.occurred_at, event.sequence, event.event_id))
        problems: list[str] = []
        event_ids = [event.event_id for event in ordered]

        intake_events = [event for event in ordered if event.kind == KIND_CASE_INTAKE]
        version_events = [event for event in ordered if event.kind == KIND_CASE_VERSION]
        terminal_events = [event for event in ordered if event.kind in _TERMINAL_KINDS]

        service_version = self._resolve_version(ordered, version_events, problems)
        intake_batch_id = self._resolve_batch(intake_events, problems)
        intake_count, replay_count = self._count_intake(intake_events, problems)

        self._validate_workflow(service_version, ordered, problems)

        case = CaseCost(
            case_id=case_id,
            service_version=service_version,
            intake_batch_id=intake_batch_id,
            closure=CLOSURE_INSUFFICIENT,
            closed_normally=False,
            intake_count=intake_count,
            replay_count=replay_count,
            event_ids=event_ids,
            first_seen_at=ordered[0].occurred_at if ordered else None,
            last_event_at=ordered[-1].occurred_at if ordered else None,
        )

        if not intake_events:
            problems.append(f'{case_id}:missing case_intake event')
        missing_intake = not intake_events

        step_states: dict[str, _StepState] = {}
        work_counter = 0

        for event in ordered:
            if event.kind in (KIND_CASE_INTAKE, KIND_CASE_VERSION):
                continue
            if event.kind in _TERMINAL_KINDS:
                case.closed_at = event.occurred_at
                continue
            step_id = _first_value(event.payload, _STEP_KEYS)
            if not step_id:
                problems.append(f'{case_id}:{event.event_id}:{event.kind} missing step_id')
                continue
            step_id = str(step_id)
            state = step_states.setdefault(step_id, _StepState(step_id))

            if event.kind in _OPEN_STEP_KINDS:
                if state.open_event is not None:
                    problems.append(
                        f'{case_id}:{step_id}:{event.event_id}:step restarted/started before previous attempt closed')
                category = self._open_category(event, state)
                attempt = self._attempt_number(event, state)
                state.open_event = event
                state.open_category = category
                state.open_attempt = attempt
                state.open_wait_seconds = self._wait_seconds(state, event)
            elif event.kind in _STEP_TERMINAL:
                work_counter += 1
                item = self._close_step(case_id, state, event, work_counter, problems)
                case.work_items.append(item)
                if event.kind == KIND_STEP_FAILED:
                    state.ever_failed = True
                state.last_finished_at = event.occurred_at
                state.last_ready_at = event.occurred_at
                state.finished_count += 1
                state.open_event = None
            elif event.kind == KIND_COMPENSATION_STARTED:
                if state.compensation_open is not None:
                    problems.append(f'{case_id}:{step_id}:{event.event_id}:nested compensation start')
                state.compensation_open = event
            elif event.kind == KIND_COMPENSATION_COMPLETED:
                work_counter += 1
                item = self._close_compensation(case_id, state, event, work_counter, problems)
                case.work_items.append(item)
                state.compensation_open = None
                # Compensation restores readiness; the next attempt's wait is
                # measured from this boundary, not the original failure.
                state.last_ready_at = event.occurred_at
            else:  # pragma: no cover - vocabulary is closed
                problems.append(f'{case_id}:{event.event_id}:unhandled kind {event.kind}')

        # Anything still open at the end means the case is not fully closed.
        for state in step_states.values():
            if state.open_event is not None:
                work_counter += 1
                case.work_items.append(self._open_step_item(case_id, state, work_counter))
                problems.append(f'{case_id}:{state.step_id}:step has no terminal event')
            if state.compensation_open is not None:
                work_counter += 1
                case.work_items.append(self._open_compensation_item(case_id, state, work_counter))
                problems.append(f'{case_id}:{state.step_id}:compensation has no completion event')

        case.closure = self._verdict(case, terminal_events, step_states, problems, missing_intake)
        case.closed_normally = (
            case.closure == CLOSURE_CLOSED
            and any(event.kind == KIND_CASE_COMPLETED for event in terminal_events)
        )
        case.evidence_problems = problems
        return case

    # ------------------------------------------------------------------
    def _resolve_version(self, ordered: Sequence[CostEvent],
                         version_events: Sequence[CostEvent], problems: list[str]) -> str | None:
        candidates = [str(value) for event in ordered
                      if (value := _first_value(event.payload, _VERSION_KEYS)) is not None]
        if version_events:
            candidates = [str(_first_value(event.payload, _VERSION_KEYS) or event.payload.get('to_version') or 'unknown')
                          for event in version_events] + candidates
        if not candidates:
            return None
        distinct = tuple(sorted(set(candidates)))
        if len(distinct) > 1:
            problems.append(f'{ordered[0].case_id}:conflicting service versions {distinct}')
        return distinct[0]

    def _resolve_batch(self, intake_events: Sequence[CostEvent], problems: list[str]) -> str | None:
        batches = []
        for event in intake_events:
            value = _first_value(event.payload, _BATCH_KEYS)
            if value is not None:
                batches.append(str(value))
        if not batches:
            return None
        distinct = tuple(sorted(set(batches)))
        if len(distinct) > 1:
            problems.append(f'{intake_events[0].case_id}:intake spans multiple batches {distinct}')
        return distinct[0]

    def _count_intake(self, intake_events: Sequence[CostEvent], problems: list[str]) -> tuple[int, int]:
        """One real intake per case; idempotent replays are counted separately.

        Replays share a request key with an earlier intake. They never inflate
        intake volume, which stays at one per accepted case.
        """
        if not intake_events:
            return 0, 0
        seen_keys: set[str] = set()
        intake_count = 0
        replay_count = 0
        for event in intake_events:
            key = _first_value(event.payload, _REQUEST_KEYS)
            key = str(key) if key is not None else f'@{event.event_id}'
            if key in seen_keys:
                replay_count += 1
                continue
            seen_keys.add(key)
            intake_count += 1
        if intake_count > 1:
            problems.append(f'{intake_events[0].case_id}:multiple distinct intakes for one case')
        return intake_count, replay_count

    def _validate_workflow(self, service_version: str | None,
                           events: Sequence[CostEvent], problems: list[str]) -> None:
        if service_version is None or service_version not in self.workflows:
            return
        steps = self.workflows[service_version]
        known = {str(step['id']) for step in steps}
        try:
            self._orchestrator.topological(steps)
        except ValueError as exc:
            problems.append(f'{events[0].case_id}:workflow {service_version} invalid: {exc}')
        seen_steps = {str(_first_value(event.payload, _STEP_KEYS))
                      for event in events if _first_value(event.payload, _STEP_KEYS) is not None}
        unknown = sorted(step_id for step_id in seen_steps if step_id not in known)
        for step_id in unknown:
            problems.append(f'{events[0].case_id}:step {step_id} not defined in workflow {service_version}')

    def _open_category(self, event: CostEvent, state: _StepState) -> str:
        if event.kind == KIND_STEP_RESTARTED:
            return WORK_REEXECUTION
        # A plain start after a prior failure/compensation is still rework.
        if state.finished_count > 0 or state.ever_failed:
            return WORK_REEXECUTION
        return WORK_FIRST

    def _attempt_number(self, event: CostEvent, state: _StepState) -> int:
        raw = _first_value(event.payload, _ATTEMPT_KEYS)
        if raw is not None:
            try:
                return max(1, int(raw))
            except (TypeError, ValueError):
                pass
        # Compensation actions are not step attempts; only finished step runs
        # advance the attempt counter.
        return state.finished_count + 1

    def _wait_seconds(self, state: _StepState, event: CostEvent) -> int:
        """Idle time before this (re)start since the step last became ready.

        The first attempt on a step has no predecessor and therefore zero wait.
        After a failure, the readiness boundary moves forward to compensation
        completion, so the re-execution wait excludes compensation work itself.
        """
        if state.last_ready_at is None:
            return 0
        delta = (event.occurred_at - state.last_ready_at).total_seconds()
        return max(0, int(delta))

    def _close_step(self, case_id: str, state: _StepState, event: CostEvent,
                    ordinal: int, problems: list[str]) -> WorkItem:
        if state.open_event is None:
            problems.append(
                f'{case_id}:{state.step_id}:{event.event_id}:terminal event without a matching start')
            started_at = None
            category = WORK_REEXECUTION if state.finished_count else WORK_FIRST
            attempt = state.finished_count + 1
            wait_seconds = 0
            evidence = (event.event_id,)
        else:
            started_at = state.open_event.occurred_at
            category = state.open_category
            attempt = state.open_attempt
            wait_seconds = state.open_wait_seconds
            evidence = (state.open_event.event_id, event.event_id)
        finished_at = event.occurred_at
        duration = int((finished_at - started_at).total_seconds()) if started_at is not None else None
        if duration is not None and duration < 0:
            problems.append(f'{case_id}:{state.step_id}:{event.event_id}:finish precedes start')
            duration = None
        status = 'succeeded' if event.kind == KIND_STEP_SUCCEEDED else 'failed'
        return WorkItem(
            work_id=f'{case_id}:{state.step_id}:w{ordinal}',
            case_id=case_id,
            step_id=state.step_id,
            category=category,
            attempt=attempt,
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=duration,
            wait_seconds=wait_seconds,
            evidence=evidence,
            status=status,
        )

    def _close_compensation(self, case_id: str, state: _StepState, event: CostEvent,
                            ordinal: int, problems: list[str]) -> WorkItem:
        if state.compensation_open is None:
            problems.append(
                f'{case_id}:{state.step_id}:{event.event_id}:compensation completed without start')
            started_at = None
            evidence = (event.event_id,)
        else:
            started_at = state.compensation_open.occurred_at
            evidence = (state.compensation_open.event_id, event.event_id)
        finished_at = event.occurred_at
        duration = int((finished_at - started_at).total_seconds()) if started_at is not None else None
        if duration is not None and duration < 0:
            problems.append(f'{case_id}:{state.step_id}:{event.event_id}:compensation finish precedes start')
            duration = None
        return WorkItem(
            work_id=f'{case_id}:{state.step_id}:c{ordinal}',
            case_id=case_id,
            step_id=state.step_id,
            category=WORK_COMPENSATION,
            attempt=0,
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=duration,
            wait_seconds=0,
            evidence=evidence,
            status='compensated',
        )

    def _open_step_item(self, case_id: str, state: _StepState, ordinal: int) -> WorkItem:
        event = state.open_event
        return WorkItem(
            work_id=f'{case_id}:{state.step_id}:o{ordinal}',
            case_id=case_id,
            step_id=state.step_id,
            category=state.open_category,
            attempt=state.open_attempt,
            started_at=event.occurred_at,
            finished_at=None,
            duration_seconds=None,
            wait_seconds=0,
            evidence=(event.event_id,),
            status='open',
        )

    def _open_compensation_item(self, case_id: str, state: _StepState, ordinal: int) -> WorkItem:
        event = state.compensation_open
        return WorkItem(
            work_id=f'{case_id}:{state.step_id}:oc{ordinal}',
            case_id=case_id,
            step_id=state.step_id,
            category=WORK_COMPENSATION,
            attempt=0,
            started_at=event.occurred_at,
            finished_at=None,
            duration_seconds=None,
            wait_seconds=0,
            evidence=(event.event_id,),
            status='compensation_open',
        )

    def _verdict(self, case: CaseCost, terminal_events: Sequence[CostEvent],
                 step_states: Mapping[str, _StepState], problems: list[str],
                 missing_intake: bool) -> str:
        has_open = any(state.open_event is not None or state.compensation_open is not None
                       for state in step_states.values())
        missing_boundary = any(item.duration_seconds is None and item.status not in {'open', 'compensation_open'}
                               for item in case.work_items)
        if has_open or not terminal_events:
            return CLOSURE_UNCLOSED
        if missing_boundary or missing_intake or problems:
            return CLOSURE_INSUFFICIENT
        return CLOSURE_CLOSED
