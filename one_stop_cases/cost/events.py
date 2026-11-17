from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

from ..contracts import require_datetime
from ..storage import Database


class CostEventError(ValueError):
    pass


# Canonical event vocabulary used by the actual-work-cost analysis.
# Existing producers keep their own event kinds; aliases map them onto this
# vocabulary without changing any executable step or compensation plan.
KIND_CASE_INTAKE = 'case_intake'
KIND_CASE_VERSION = 'case_version'
KIND_STEP_STARTED = 'step_started'
KIND_STEP_SUCCEEDED = 'step_succeeded'
KIND_STEP_FAILED = 'step_failed'
KIND_COMPENSATION_STARTED = 'compensation_started'
KIND_COMPENSATION_COMPLETED = 'compensation_completed'
KIND_STEP_RESTARTED = 'step_restarted'
KIND_CASE_COMPLETED = 'case_completed'
KIND_CASE_CANCELLED = 'case_cancelled'

CANONICAL_KINDS = frozenset({
    KIND_CASE_INTAKE,
    KIND_CASE_VERSION,
    KIND_STEP_STARTED,
    KIND_STEP_SUCCEEDED,
    KIND_STEP_FAILED,
    KIND_COMPENSATION_STARTED,
    KIND_COMPENSATION_COMPLETED,
    KIND_STEP_RESTARTED,
    KIND_CASE_COMPLETED,
    KIND_CASE_CANCELLED,
})

_KIND_ALIASES = {
    'case_received': KIND_CASE_INTAKE,
    'case_accepted': KIND_CASE_INTAKE,
    'received': KIND_CASE_INTAKE,
    'intake': KIND_CASE_INTAKE,
    'step_start': KIND_STEP_STARTED,
    'started': KIND_STEP_STARTED,
    'execution_started': KIND_STEP_STARTED,
    'step_success': KIND_STEP_SUCCEEDED,
    'step_completed': KIND_STEP_SUCCEEDED,
    'succeeded': KIND_STEP_SUCCEEDED,
    'execution_succeeded': KIND_STEP_SUCCEEDED,
    'step_failure': KIND_STEP_FAILED,
    'failed': KIND_STEP_FAILED,
    'execution_failed': KIND_STEP_FAILED,
    'compensation_start': KIND_COMPENSATION_STARTED,
    'compensation_complete': KIND_COMPENSATION_COMPLETED,
    'compensation_done': KIND_COMPENSATION_COMPLETED,
    'step_retry': KIND_STEP_RESTARTED,
    'retry': KIND_STEP_RESTARTED,
    're_execution': KIND_STEP_RESTARTED,
    'reexecution': KIND_STEP_RESTARTED,
    'case_closed': KIND_CASE_COMPLETED,
    'completed': KIND_CASE_COMPLETED,
    'case_canceled': KIND_CASE_CANCELLED,
    'cancelled': KIND_CASE_CANCELLED,
}

_STEP_KINDS = frozenset({
    KIND_STEP_STARTED,
    KIND_STEP_SUCCEEDED,
    KIND_STEP_FAILED,
    KIND_STEP_RESTARTED,
    KIND_COMPENSATION_STARTED,
    KIND_COMPENSATION_COMPLETED,
})

# Payload keys that may carry the same meaning; checked in order.
_CASE_KEYS = ('case_id', 'caseId', 'aggregate_id', 'case')
_BATCH_KEYS = ('intake_batch_id', 'batch_id', 'batchId', 'batch', 'intake_batch')
_VERSION_KEYS = ('service_version', 'item_version', 'version', 'service_version_id')
_STEP_KEYS = ('step_id', 'stepId', 'step', 'execution_step', 'node_id')
_ATTEMPT_KEYS = ('attempt', 'attempt_no', 'attempt_number', 'retry_count')
_OCCURRED_KEYS = ('occurred_at', 'timestamp', 'event_time', 'at', 'time')
_REQUEST_KEYS = ('request_key', 'requestKey', 'idempotency_key', 'request_id')


@dataclass(frozen=True)
class CostEvent:
    event_id: str
    case_id: str
    kind: str
    occurred_at: dt.datetime
    payload: Mapping[str, Any]
    sequence: int
    source: str

    def as_dict(self) -> dict[str, Any]:
        return {
            'event_id': self.event_id,
            'case_id': self.case_id,
            'kind': self.kind,
            'occurred_at': self.occurred_at.isoformat(),
            'sequence': self.sequence,
            'source': self.source,
            'payload': dict(self.payload),
        }


def canonical_kind(kind: Any) -> str | None:
    text = str(kind).strip().lower()
    if text in CANONICAL_KINDS:
        return text
    return _KIND_ALIASES.get(text)


def _first_value(payload: Mapping[str, Any], keys: Sequence[str]) -> Any:
    for key in keys:
        if key in payload and payload[key] not in (None, ''):
            return payload[key]
    return None


def _extract_payload(raw: Mapping[str, Any]) -> Mapping[str, Any]:
    payload = raw.get('payload')
    if isinstance(payload, Mapping):
        merged = {key: value for key, value in raw.items() if key != 'payload'}
        merged.update(payload)
        return merged
    return raw


def _extract_timestamp(payload: Mapping[str, Any], fallback: Any = None) -> dt.datetime:
    value = _first_value(payload, _OCCURRED_KEYS)
    if value is None:
        value = fallback
    if value is None:
        raise CostEventError('event timestamp is required')
    return require_datetime(value, 'occurred_at')


def event_from_mapping(raw: Mapping[str, Any], sequence_fallback: int, source: str = 'mapping') -> CostEvent:
    """Normalize one raw event-like mapping into a :class:`CostEvent`.

    Raises :class:`CostEventError` when the event cannot be tied to a case,
    its kind is unknown, or the timestamp is missing/unparseable. Callers
    collect these as evidence problems instead of silently dropping data.
    """
    payload = _extract_payload(raw)
    kind = canonical_kind(raw.get('kind'))
    if kind is None:
        raise CostEventError(f"unknown event kind: {raw.get('kind')!r}")
    case_id = _first_value(payload, _CASE_KEYS)
    if case_id is None or not str(case_id).strip():
        raise CostEventError('case_id is required')
    event_id = str(raw.get('event_id') or f'{source}:{sequence_fallback}')
    sequence = int(raw.get('sequence') or sequence_fallback)
    occurred_at = _extract_timestamp(payload, raw.get('occurred_at'))
    return CostEvent(event_id, str(case_id), kind, occurred_at, dict(payload), sequence, source)


def events_from_database(database: Database, aggregate_type: str | None = None) -> tuple[tuple[CostEvent, ...], tuple[str, ...]]:
    """Read the event store and normalize recognized cost events.

    Unrecognized rows are returned as evidence problems so that a case with
    missing key events is never mistaken for a zero-cost, normally closed case.
    """
    events: list[CostEvent] = []
    problems: list[str] = []
    for index, row in enumerate(database.all_events(aggregate_type), 1):
        try:
            events.append(event_from_mapping(row, index, source='event_store'))
        except CostEventError as exc:
            problems.append(f"{row.get('event_id', f'row-{index}')}:{exc}")
    return tuple(events), tuple(problems)


def _record_case_id(record: Mapping[str, Any]) -> str | None:
    nested = record.get('attributes')
    if isinstance(nested, Mapping):
        candidate = _first_value(nested, _CASE_KEYS)
        if candidate:
            return str(candidate)
    candidate = _first_value(record, _CASE_KEYS)
    return str(candidate) if candidate else None


def _record_timestamp(record: Mapping[str, Any], payload: Mapping[str, Any]) -> dt.datetime:
    value = _first_value(payload, _OCCURRED_KEYS)
    if value is None:
        value = record.get('effective_to') or record.get('effective_from')
    return require_datetime(value, 'occurred_at')


def _record_identifier(record: Mapping[str, Any], kind: str) -> str:
    return str(record.get(f'{kind}_id') or record.get('record_id') or '')


def events_from_records(executions: Iterable[Mapping[str, Any]],
                        compensations: Iterable[Mapping[str, Any]] = ()) -> tuple[tuple[CostEvent, ...], tuple[str, ...]]:
    """Adapt existing execution and compensation records into cost events.

    Two record shapes are supported:

    * detailed rows with an ``events`` list in ``attributes`` (each entry
      carrying its own kind/timestamp);
    * flat repository rows, where status plus effective dates produce a
      start/terminal pair.

    Rows that cannot be tied to a case or parsed become evidence problems.
    """
    events: list[CostEvent] = []
    problems: list[str] = []
    sequence = 0

    def emit(raw: Mapping[str, Any], source: str) -> None:
        nonlocal sequence
        sequence += 1
        try:
            events.append(event_from_mapping(raw, sequence, source=source))
        except CostEventError as exc:
            problems.append(f"{source}:{raw.get('event_id', sequence)}:{exc}")

    for record in executions:
        identifier = _record_identifier(record, 'execution')
        case_id = _record_case_id(record)
        if not case_id:
            problems.append(f'execution:{identifier}:case_id is required')
            continue
        attributes = record.get('attributes') if isinstance(record.get('attributes'), Mapping) else {}
        detailed = attributes.get('events')
        if isinstance(detailed, Sequence) and not isinstance(detailed, (str, bytes)):
            for entry in detailed:
                if not isinstance(entry, Mapping):
                    problems.append(f'execution:{identifier}:event entry must be a mapping')
                    continue
                merged = dict(entry)
                merged.setdefault('case_id', case_id)
                merged.setdefault('event_id', f'{identifier}:{entry.get("kind", "event")}')
                emit(merged, 'execution_record')
            continue
        status = str(record.get('status', '')).lower()
        payload = {**dict(attributes), 'case_id': case_id, 'step_id': attributes.get('step_id', identifier)}
        started_kind = KIND_STEP_RESTARTED if attributes.get('retry_of') or int(attributes.get('attempt', 1) or 1) > 1 else KIND_STEP_STARTED
        if record.get('effective_from'):
            emit({'event_id': f'{identifier}:started', 'kind': started_kind,
                  'occurred_at': record['effective_from'], **payload}, 'execution_record')
        if status in {'completed', 'active'} and record.get('effective_to'):
            emit({'event_id': f'{identifier}:terminal', 'kind': KIND_STEP_SUCCEEDED,
                  'occurred_at': record['effective_to'], **payload}, 'execution_record')
        elif status == 'cancelled' and record.get('effective_to'):
            emit({'event_id': f'{identifier}:terminal', 'kind': KIND_STEP_FAILED,
                  'occurred_at': record['effective_to'], **payload}, 'execution_record')
        elif status not in {'completed', 'active', 'cancelled', 'draft'}:
            problems.append(f'execution:{identifier}:unsupported status {status}')

    for record in compensations:
        identifier = _record_identifier(record, 'compensation')
        case_id = _record_case_id(record)
        if not case_id:
            problems.append(f'compensation:{identifier}:case_id is required')
            continue
        attributes = record.get('attributes') if isinstance(record.get('attributes'), Mapping) else {}
        detailed = attributes.get('events')
        if isinstance(detailed, Sequence) and not isinstance(detailed, (str, bytes)):
            for entry in detailed:
                if not isinstance(entry, Mapping):
                    problems.append(f'compensation:{identifier}:event entry must be a mapping')
                    continue
                merged = dict(entry)
                merged.setdefault('case_id', case_id)
                merged.setdefault('event_id', f'{identifier}:{entry.get("kind", "event")}')
                emit(merged, 'compensation_record')
            continue
        payload = {**dict(attributes), 'case_id': case_id,
                   'step_id': attributes.get('step_id') or attributes.get('step')}
        status = str(record.get('status', '')).lower()
        if record.get('effective_from'):
            emit({'event_id': f'{identifier}:started', 'kind': KIND_COMPENSATION_STARTED,
                  'occurred_at': record['effective_from'], **payload}, 'compensation_record')
        if status == 'completed' and record.get('effective_to'):
            emit({'event_id': f'{identifier}:completed', 'kind': KIND_COMPENSATION_COMPLETED,
                  'occurred_at': record['effective_to'], **payload}, 'compensation_record')
        elif status not in {'completed', 'active', 'draft', 'cancelled'}:
            problems.append(f'compensation:{identifier}:unsupported status {status}')

    return tuple(events), tuple(problems)
