import datetime as dt
import json
import tempfile
import unittest
from pathlib import Path

from one_stop_cases.cost import (
    CLOSURE_CLOSED,
    CLOSURE_INSUFFICIENT,
    CLOSURE_UNCLOSED,
    GROUP_BATCH,
    GROUP_CLOSURE,
    GROUP_VERSION,
    GROUP_VERSION_BATCH,
    WORK_COMPENSATION,
    WORK_FIRST,
    WORK_REEXECUTION,
    CostAnalysisService,
    CostAnalyzer,
    CostEvent,
    CostReporter,
    event_from_mapping,
    events_from_records,
    union_seconds,
)
from one_stop_cases.storage import Database

UTC = dt.timezone.utc


def at(day, hour=0, minute=0):
    return dt.datetime(2026, 11, day, hour, minute, tzinfo=UTC)


def ev(event_id, case_id, kind, moment, **payload):
    return {'event_id': event_id, 'case_id': case_id, 'kind': kind,
            'occurred_at': moment.isoformat(), 'sequence': 1, 'payload': payload}


def workflows_v1():
    return {
        'svc-v1': [
            {'id': 'intake', 'requires': []},
            {'id': 'review', 'requires': ['intake']},
            {'id': 'deliver', 'requires': ['review']},
        ],
        'svc-v2': [
            {'id': 'intake', 'requires': []},
            {'id': 'review', 'requires': ['intake']},
            {'id': 'deliver', 'requires': ['review']},
        ],
    }


def rework_case_events(case_id='case-A', version='svc-v1', batch='batch-1'):
    """First pass fails at review, gets compensated, then re-executed and closed."""
    base = {'service_version': version, 'intake_batch_id': batch}
    return [
        ev('e1', case_id, 'case_intake', at(1, 9), request_key='req-1', **base),
        ev('e2', case_id, 'step_started', at(1, 9), step_id='intake', attempt=1, **base),
        ev('e3', case_id, 'step_succeeded', at(1, 10), step_id='intake', attempt=1, **base),
        ev('e4', case_id, 'step_started', at(1, 10), step_id='review', attempt=1, **base),
        ev('e5', case_id, 'step_failed', at(1, 11), step_id='review', attempt=1, error='bad', **base),
        ev('e6', case_id, 'compensation_started', at(1, 11, 30), step_id='review', **base),
        ev('e7', case_id, 'compensation_completed', at(1, 12), step_id='review', **base),
        ev('e8', case_id, 'step_restarted', at(1, 13), step_id='review', attempt=2, **base),
        ev('e9', case_id, 'step_succeeded', at(1, 14), step_id='review', attempt=2, **base),
        ev('e10', case_id, 'step_started', at(1, 14), step_id='deliver', attempt=1, **base),
        ev('e11', case_id, 'step_succeeded', at(1, 15), step_id='deliver', attempt=1, **base),
        ev('e12', case_id, 'case_completed', at(1, 15, 30), **base),
    ]


class EventNormalizationTests(unittest.TestCase):
    def test_canonical_aliases(self):
        from one_stop_cases.cost.events import canonical_kind
        self.assertEqual(canonical_kind('case_received'), 'case_intake')
        self.assertEqual(canonical_kind('STEP_STARTED'), 'step_started')
        self.assertEqual(canonical_kind('compensation_done'), 'compensation_completed')
        self.assertIsNone(canonical_kind('not_a_real_kind'))

    def test_event_from_mapping_aliases(self):
        normalized = event_from_mapping(
            {'event_id': 'x', 'caseId': 'c1', 'kind': 'retry', 'timestamp': at(2).isoformat(),
             'stepId': 'review', 'attempt_no': 3}, 1)
        self.assertEqual(normalized.kind, 'step_restarted')
        self.assertEqual(normalized.case_id, 'c1')
        self.assertEqual(normalized.step_id if hasattr(normalized, 'step_id') else normalized.payload['stepId'], 'review')

    def test_missing_case_rejected(self):
        with self.assertRaises(ValueError):
            event_from_mapping({'event_id': 'x', 'kind': 'step_started',
                                'occurred_at': at(1).isoformat(), 'payload': {}}, 1)

    def test_unknown_kind_rejected(self):
        with self.assertRaises(ValueError):
            event_from_mapping({'event_id': 'x', 'case_id': 'c1', 'kind': 'mystery',
                                'occurred_at': at(1).isoformat()}, 1)

    def test_missing_timestamp_rejected(self):
        with self.assertRaises(ValueError):
            event_from_mapping({'event_id': 'x', 'case_id': 'c1', 'kind': 'step_started'}, 1)


class RecordAdapterTests(unittest.TestCase):
    def test_flat_execution_records_become_start_and_terminal(self):
        executions = [{
            'execution_id': 'ex-1', 'owner_id': 'o1', 'category': 'workflow', 'status': 'completed',
            'effective_from': '2026-11-01', 'effective_to': '2026-11-02', 'quantity': 1,
            'amount': '0.00', 'version': 1,
            'attributes': {'case_id': 'case-A', 'step_id': 'intake', 'attempt': 1},
        }]
        events, problems = events_from_records(executions, [])
        kinds = sorted(event.kind for event in events)
        self.assertEqual(kinds, ['step_started', 'step_succeeded'])
        self.assertEqual(problems, ())

    def test_retry_execution_is_restarted(self):
        executions = [{
            'execution_id': 'ex-2', 'owner_id': 'o1', 'category': 'workflow', 'status': 'completed',
            'effective_from': '2026-11-03', 'effective_to': '2026-11-03', 'quantity': 1,
            'amount': '0.00', 'version': 1,
            'attributes': {'case_id': 'case-A', 'step_id': 'review', 'attempt': 2},
        }]
        events, _ = events_from_records(executions, [])
        self.assertEqual(events[0].kind, 'step_restarted')

    def test_failed_flat_execution(self):
        executions = [{
            'execution_id': 'ex-3', 'owner_id': 'o1', 'category': 'workflow', 'status': 'cancelled',
            'effective_from': '2026-11-03', 'effective_to': '2026-11-03', 'quantity': 1,
            'amount': '0.00', 'version': 1,
            'attributes': {'case_id': 'case-A', 'step_id': 'review'},
        }]
        events, _ = events_from_records(executions, [])
        self.assertTrue(any(event.kind == 'step_failed' for event in events))

    def test_detailed_event_list_adapted(self):
        executions = [{
            'execution_id': 'ex-4', 'owner_id': 'o1', 'category': 'workflow', 'status': 'active',
            'effective_from': '2026-11-03', 'effective_to': None, 'quantity': 1,
            'amount': '0.00', 'version': 1,
            'attributes': {'case_id': 'case-B', 'events': [
                {'kind': 'step_started', 'timestamp': at(3, 9).isoformat(), 'step_id': 'intake'},
                {'kind': 'step_succeeded', 'timestamp': at(3, 10).isoformat(), 'step_id': 'intake'},
            ]},
        }]
        events, problems = events_from_records(executions, [])
        self.assertEqual([event.kind for event in events], ['step_started', 'step_succeeded'])
        self.assertEqual(problems, ())

    def test_compensation_record_adapted(self):
        compensations = [{
            'compensation_id': 'cp-1', 'owner_id': 'o1', 'category': 'workflow', 'status': 'completed',
            'effective_from': '2026-11-01', 'effective_to': '2026-11-01', 'quantity': 1,
            'amount': '0.00', 'version': 1,
            'attributes': {'case_id': 'case-A', 'step_id': 'review'},
        }]
        events, problems = events_from_records([], compensations)
        self.assertEqual(sorted(event.kind for event in events),
                         ['compensation_completed', 'compensation_started'])
        self.assertEqual(problems, ())

    def test_record_without_case_is_evidence_problem(self):
        executions = [{
            'execution_id': 'ex-9', 'owner_id': 'o1', 'category': 'workflow', 'status': 'active',
            'effective_from': '2026-11-01', 'effective_to': None, 'quantity': 1,
            'amount': '0.00', 'version': 1, 'attributes': {},
        }]
        events, problems = events_from_records(executions, [])
        self.assertEqual(events, ())
        self.assertTrue(any('case_id is required' for problem in problems))


class UnionIntervalTests(unittest.TestCase):
    def test_disjoint_intervals_add(self):
        total = union_seconds([(at(1, 9), at(1, 10)), (at(1, 11), at(1, 12))])
        self.assertEqual(total, 7200)

    def test_overlapping_parallel_intervals_merged(self):
        # Two parallel steps running 09:00-11:00 and 10:00-12:00 cover three
        # wall-clock hours (09:00-12:00), not four.
        total = union_seconds([(at(1, 9), at(1, 11)), (at(1, 10), at(1, 12))])
        self.assertEqual(total, 3 * 3600)

    def test_adjacent_intervals_merged(self):
        total = union_seconds([(at(1, 9), at(1, 10)), (at(1, 10), at(1, 11))])
        self.assertEqual(total, 7200)

    def test_empty(self):
        self.assertEqual(union_seconds([]), 0)


class ReworkClassificationTests(unittest.TestCase):
    def setUp(self):
        self.analyzer = CostAnalyzer(workflows_v1())
        self.events = [event_from_mapping(row, index + 1) for index, row in enumerate(rework_case_events())]

    def test_categories_split_first_compensation_reexecution(self):
        cases = self.analyzer.analyze(self.events)
        self.assertEqual(len(cases), 1)
        case = cases[0]
        categories = {item.step_id: item.category for item in case.work_items}
        self.assertEqual(categories['intake'], WORK_FIRST)
        # First review attempt failed: still first-execution work (it consumed effort).
        review_attempts = [item for item in case.work_items if item.step_id == 'review']
        self.assertEqual(review_attempts[0].category, WORK_FIRST)
        self.assertEqual(review_attempts[0].status, 'failed')
        # Compensation is its own category.
        compensation = [item for item in case.work_items if item.category == WORK_COMPENSATION]
        self.assertEqual(len(compensation), 1)
        self.assertEqual(compensation[0].step_id, 'review')
        # The post-compensation review run is re-execution.
        self.assertEqual(review_attempts[-1].category, WORK_REEXECUTION)
        self.assertEqual(review_attempts[-1].status, 'succeeded')
        self.assertEqual(categories['deliver'], WORK_FIRST)

    def test_every_work_item_points_back_to_case_step_and_evidence(self):
        cases = self.analyzer.analyze(self.events)
        for item in cases[0].work_items:
            self.assertEqual(item.case_id, 'case-A')
            self.assertTrue(item.step_id)
            self.assertTrue(item.evidence)
            for event_id in item.evidence:
                self.assertIn(event_id, cases[0].event_ids)

    def test_durations_are_measured_per_attempt(self):
        case = self.analyzer.analyze(self.events)[0]
        review = [item for item in case.work_items if item.step_id == 'review' and item.status == 'failed'][0]
        self.assertEqual(review.duration_seconds, 3600)
        compensation = next(item for item in case.work_items if item.category == WORK_COMPENSATION)
        self.assertEqual(compensation.duration_seconds, 1800)
        re_run = [item for item in case.work_items if item.step_id == 'review' and item.category == WORK_REEXECUTION][0]
        self.assertEqual(re_run.duration_seconds, 3600)

    def test_wait_time_is_not_added_to_effort(self):
        case = self.analyzer.analyze(self.events)[0]
        re_run = next(item for item in case.work_items
                      if item.step_id == 'review' and item.category == WORK_REEXECUTION)
        # Re-run started at 13:00; previous review terminal was at 12:00.
        self.assertEqual(re_run.wait_seconds, 3600)

    def test_case_closed_normally(self):
        case = self.analyzer.analyze(self.events)[0]
        self.assertEqual(case.closure, CLOSURE_CLOSED)
        self.assertTrue(case.closed_normally)
        self.assertEqual(case.evidence_problems, [])

    def test_intake_is_one_despite_rework(self):
        case = self.analyzer.analyze(self.events)[0]
        self.assertEqual(case.intake_count, 1)
        self.assertEqual(case.replay_count, 0)

    def test_plain_start_after_failure_is_rework_and_attempt_excludes_compensation(self):
        rows = [
            ev('i', 'case-C', 'case_intake', at(1, 9), service_version='svc-v1', intake_batch_id='b1'),
            ev('s1', 'case-C', 'step_started', at(1, 9), step_id='review'),
            ev('f1', 'case-C', 'step_failed', at(1, 10), step_id='review'),
            ev('cs', 'case-C', 'compensation_started', at(1, 10), step_id='review'),
            ev('cc', 'case-C', 'compensation_completed', at(1, 11), step_id='review'),
            # No explicit step_restarted: a plain start after compensation is rework.
            ev('s2', 'case-C', 'step_started', at(1, 12), step_id='review'),
            ev('e2', 'case-C', 'step_succeeded', at(1, 13), step_id='review'),
            ev('x', 'case-C', 'case_completed', at(1, 14)),
        ]
        analyzer = CostAnalyzer({'svc-v1': [{'id': 'review', 'requires': []}]})
        case = analyzer.analyze([event_from_mapping(row, i + 1) for i, row in enumerate(rows)])[0]
        step_items = [item for item in case.work_items if item.step_id == 'review']
        self.assertEqual([(item.category, item.attempt) for item in step_items],
                         [(WORK_FIRST, 1), (WORK_COMPENSATION, 0), (WORK_REEXECUTION, 2)])
        self.assertEqual(case.closure, CLOSURE_CLOSED)

    def test_effort_vs_parallel_active_time(self):
        case = self.analyzer.analyze(self.events)[0]
        # Sequential chain: active wall time equals summed effort here.
        self.assertIsNotNone(case.active_seconds())
        self.assertEqual(case.active_seconds(), case.effort_seconds())


class ParallelStepTests(unittest.TestCase):
    def test_parallel_active_time_is_union_not_sum(self):
        analyzer = CostAnalyzer({
            'svc-v1': [
                {'id': 'a', 'requires': []},
                {'id': 'b', 'requires': []},
                {'id': 'join', 'requires': ['a', 'b']},
            ],
        })
        rows = [
            ev('i', 'case-P', 'case_intake', at(1, 9), service_version='svc-v1', intake_batch_id='b1'),
            ev('s1', 'case-P', 'step_started', at(1, 9), step_id='a'),
            ev('s2', 'case-P', 'step_started', at(1, 10), step_id='b'),
            ev('e1', 'case-P', 'step_succeeded', at(1, 11), step_id='a'),
            ev('e2', 'case-P', 'step_succeeded', at(1, 12), step_id='b'),
            ev('s3', 'case-P', 'step_started', at(1, 12), step_id='join'),
            ev('e3', 'case-P', 'step_succeeded', at(1, 13), step_id='join'),
            ev('c', 'case-P', 'case_completed', at(1, 13)),
        ]
        events = [event_from_mapping(row, i + 1) for i, row in enumerate(rows)]
        case = analyzer.analyze(events)[0]
        # Summed effort: a=2h, b=2h, join=1h -> 5h.
        self.assertEqual(case.effort_seconds(), 5 * 3600)
        # Union active span: 09:00-13:00 -> 4h (parallel a/b overlap merged).
        self.assertEqual(case.active_seconds(), 4 * 3600)
        self.assertEqual(case.closure, CLOSURE_CLOSED)


class ClosureAndEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.analyzer = CostAnalyzer(workflows_v1())

    def _normalize(self, rows):
        return [event_from_mapping(row, i + 1) for i, row in enumerate(rows)]

    def test_open_step_is_unclosed_not_zero_cost(self):
        rows = [
            ev('i', 'case-U', 'case_intake', at(1, 9), service_version='svc-v1', intake_batch_id='b1'),
            ev('s1', 'case-U', 'step_started', at(1, 9), step_id='intake'),
            ev('e1', 'case-U', 'step_succeeded', at(1, 10), step_id='intake'),
            ev('s2', 'case-U', 'step_started', at(1, 10), step_id='review'),
        ]
        case = self.analyzer.analyze(self._normalize(rows))[0]
        self.assertEqual(case.closure, CLOSURE_UNCLOSED)
        self.assertFalse(case.closed_normally)
        # The open review still carries observed effort evidence, never zero.
        open_items = [item for item in case.work_items if item.status == 'open']
        self.assertEqual(len(open_items), 1)
        self.assertIsNone(open_items[0].duration_seconds)
        self.assertIsNone(case.active_seconds())
        self.assertTrue(any('no terminal event' for problem in case.evidence_problems))

    def test_missing_intake_is_insufficient_evidence(self):
        rows = [
            ev('s1', 'case-M', 'step_started', at(1, 9), step_id='intake'),
            ev('e1', 'case-M', 'step_succeeded', at(1, 10), step_id='intake'),
            ev('c', 'case-M', 'case_completed', at(1, 10)),
        ]
        case = self.analyzer.analyze(self._normalize(rows))[0]
        self.assertEqual(case.closure, CLOSURE_INSUFFICIENT)
        self.assertFalse(case.closed_normally)
        self.assertTrue(any('missing case_intake' for problem in case.evidence_problems))

    def test_terminal_without_start_is_insufficient(self):
        rows = [
            ev('i', 'case-T', 'case_intake', at(1, 9), service_version='svc-v1', intake_batch_id='b1'),
            ev('e1', 'case-T', 'step_succeeded', at(1, 10), step_id='intake'),
            ev('c', 'case-T', 'case_completed', at(1, 11)),
        ]
        case = self.analyzer.analyze(self._normalize(rows))[0]
        self.assertEqual(case.closure, CLOSURE_INSUFFICIENT)
        self.assertTrue(any('without a matching start' for problem in case.evidence_problems))

    def test_cancelled_case_is_closed_but_not_normal(self):
        rows = [
            ev('i', 'case-X', 'case_intake', at(1, 9), service_version='svc-v1', intake_batch_id='b1'),
            ev('s1', 'case-X', 'step_started', at(1, 9), step_id='intake'),
            ev('e1', 'case-X', 'step_failed', at(1, 10), step_id='intake'),
            ev('x', 'case-X', 'case_cancelled', at(1, 11)),
        ]
        case = self.analyzer.analyze(self._normalize(rows))[0]
        self.assertEqual(case.closure, CLOSURE_CLOSED)
        self.assertFalse(case.closed_normally)

    def test_unknown_step_is_evidence_problem(self):
        rows = rework_case_events('case-Z')
        for row in rows:
            row['payload'] = {**row['payload'], 'step_id': 'ghost'} if row['kind'].startswith('step_') or 'compensation' in row['kind'] else row['payload']
        case = self.analyzer.analyze(self._normalize(rows))[0]
        self.assertTrue(any('not defined in workflow' for problem in case.evidence_problems))


class IdempotencyTests(unittest.TestCase):
    def test_replayed_intake_does_not_inflate_volume(self):
        analyzer = CostAnalyzer(workflows_v1())
        rows = [
            ev('i1', 'case-R', 'case_intake', at(1, 9), request_key='req-9',
               service_version='svc-v1', intake_batch_id='b1'),
            # Same request key retried: idempotent replay.
            ev('i2', 'case-R', 'case_intake', at(1, 9, 5), request_key='req-9',
               service_version='svc-v1', intake_batch_id='b1'),
            ev('s1', 'case-R', 'step_started', at(1, 9), step_id='intake'),
            ev('e1', 'case-R', 'step_succeeded', at(1, 10), step_id='intake'),
            ev('c', 'case-R', 'case_completed', at(1, 10)),
        ]
        case = analyzer.analyze([event_from_mapping(row, i + 1) for i, row in enumerate(rows)])[0]
        self.assertEqual(case.intake_count, 1)
        self.assertEqual(case.replay_count, 1)


class GroupingAndReconciliationTests(unittest.TestCase):
    def setUp(self):
        self.reporter = CostReporter()
        analyzer = CostAnalyzer(workflows_v1())
        events_a = [event_from_mapping(row, i + 1)
                    for i, row in enumerate(rework_case_events('case-A', 'svc-v1', 'batch-1'))]
        events_b = [event_from_mapping(row, i + 100)
                    for i, row in enumerate(rework_case_events('case-B', 'svc-v2', 'batch-2'))]
        self.cases = analyzer.analyze(events_a + events_b)

    def test_grouped_by_version_and_batch(self):
        summaries = self.reporter.grouped_summary(self.cases, GROUP_VERSION_BATCH)
        self.assertEqual(len(summaries), 2)
        keys = {summary.group_key for summary in summaries}
        self.assertEqual(keys, {'svc-v1|batch-1', 'svc-v2|batch-2'})

    def test_group_carry_rework_breakdown(self):
        summaries = self.reporter.grouped_summary(self.cases, GROUP_VERSION)
        v1 = next(summary for summary in summaries if summary.service_version == 'svc-v1')
        self.assertGreaterEqual(v1.compensation_items, 1)
        self.assertGreaterEqual(v1.re_execution_items, 1)
        self.assertGreaterEqual(v1.first_execution_items, 1)

    def test_reconciliation_balances(self):
        summaries = self.reporter.grouped_summary(self.cases, GROUP_VERSION_BATCH)
        reconciliation = self.reporter.reconciliation(self.cases, summaries)
        self.assertTrue(reconciliation['balanced'], reconciliation)
        self.assertEqual(reconciliation['totals']['cases'], 2)
        # Two real intakes, retries never inflate this number.
        self.assertEqual(reconciliation['totals']['intake_count'], 2)

    def test_batch_dimension_and_closure_dimension(self):
        by_batch = self.reporter.grouped_summary(self.cases, GROUP_BATCH)
        self.assertEqual({summary.intake_batch_id for summary in by_batch}, {'batch-1', 'batch-2'})
        by_closure = self.reporter.grouped_summary(self.cases, GROUP_CLOSURE)
        self.assertEqual({summary.group_key for summary in by_closure}, {CLOSURE_CLOSED})

    def test_detail_rows_are_traceable(self):
        details = self.reporter.detail_rows(self.cases)
        self.assertTrue(details)
        for row in details:
            self.assertIn('case_id', row)
            self.assertIn('step_id', row)
            self.assertIn('category', row)
            self.assertTrue(row['evidence_event_ids'])

    def test_report_structure(self):
        report = self.reporter.report(self.cases)
        self.assertEqual(report['case_summary']['total_cases'], 2)
        self.assertEqual(report['case_summary']['closure_counts'][CLOSURE_CLOSED], 2)
        self.assertTrue(report['reconciliation']['balanced'])
        self.assertEqual(len(report['work_details']),
                         sum(len(case.work_items) for case in self.cases))

    def test_unknown_dimension_rejected(self):
        with self.assertRaises(ValueError):
            self.reporter.grouped_summary(self.cases, 'not_a_dimension')


class MixedPopulationReportTests(unittest.TestCase):
    def test_unclosed_and_insufficient_kept_out_of_normal_buckets(self):
        analyzer = CostAnalyzer(workflows_v1())
        reporter = CostReporter()
        good = [event_from_mapping(row, i + 1)
                for i, row in enumerate(rework_case_events('case-G', 'svc-v1', 'batch-1'))]
        unclosed_rows = [
            ev('u-i', 'case-U', 'case_intake', at(2, 9), service_version='svc-v1', intake_batch_id='batch-1'),
            ev('u-s', 'case-U', 'step_started', at(2, 9), step_id='intake'),
        ]
        insufficient_rows = [
            ev('m-s', 'case-M', 'step_started', at(2, 9), step_id='intake'),
            ev('m-e', 'case-M', 'step_succeeded', at(2, 10), step_id='intake'),
            ev('m-c', 'case-M', 'case_completed', at(2, 11)),
        ]
        events = good + [event_from_mapping(row, i + 50) for i, row in enumerate(unclosed_rows)]
        events += [event_from_mapping(row, i + 80) for i, row in enumerate(insufficient_rows)]
        cases = analyzer.analyze(events)
        report = reporter.report(cases, GROUP_CLOSURE)
        counts = report['case_summary']['closure_counts']
        self.assertEqual(counts[CLOSURE_CLOSED], 1)
        self.assertEqual(counts[CLOSURE_UNCLOSED], 1)
        self.assertEqual(counts[CLOSURE_INSUFFICIENT], 1)
        self.assertEqual(report['case_summary']['closed_normally'], 1)
        # Reconciliation still balances across the non-normal cases.
        self.assertTrue(report['reconciliation']['balanced'])


class DatabaseIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.temp.name) / 'cost.sqlite3')

    def tearDown(self):
        self.temp.cleanup()

    def _seed_records(self):
        # Execution records: first attempt fails, second attempt succeeds.
        self.db.put_record  # attribute exists
        import uuid
        from one_stop_cases.contracts import Event
        for index, row in enumerate(rework_case_events(), 1):
            event = Event(str(uuid.uuid4()), 'case', row['case_id'], row['kind'],
                          dt.datetime.fromisoformat(row['occurred_at']),
                          dict(row['payload']), index)
            with self.db.transaction() as connection:
                self.db.append_event(connection, event)

    def test_service_reads_event_store(self):
        self._seed_records()
        service = CostAnalysisService(self.db, workflows=workflows_v1())
        report = service.report()
        self.assertEqual(report['case_summary']['total_cases'], 1)
        self.assertEqual(report['case_summary']['closure_counts'][CLOSURE_CLOSED], 1)
        self.assertTrue(report['reconciliation']['balanced'])

    def test_service_reads_execution_and_compensation_records(self):
        executions = [
            {'execution_id': 'ex-1', 'owner_id': 'o1', 'category': 'workflow', 'status': 'cancelled',
             'effective_from': '2026-11-01T09:00:00+00:00', 'effective_to': '2026-11-01T10:00:00+00:00',
             'quantity': 1, 'amount': '0.00', 'version': 1,
             'attributes': {'case_id': 'case-A', 'step_id': 'review', 'attempt': 1}},
            {'execution_id': 'ex-2', 'owner_id': 'o1', 'category': 'workflow', 'status': 'completed',
             'effective_from': '2026-11-01T13:00:00+00:00', 'effective_to': '2026-11-01T14:00:00+00:00',
             'quantity': 1, 'amount': '0.00', 'version': 1,
             'attributes': {'case_id': 'case-A', 'step_id': 'review', 'attempt': 2}},
        ]
        compensations = [{
            'compensation_id': 'cp-1', 'owner_id': 'o1', 'category': 'workflow', 'status': 'completed',
            'effective_from': '2026-11-01T11:30:00+00:00', 'effective_to': '2026-11-01T12:00:00+00:00',
            'quantity': 1, 'amount': '0.00', 'version': 1,
            'attributes': {'case_id': 'case-A', 'step_id': 'review'},
        }]
        with self.db.transaction() as connection:
            for record in executions:
                self.db.put_record(connection, 'execution', record)
            for record in compensations:
                self.db.put_record(connection, 'compensation', record)
        service = CostAnalysisService(self.db)
        cases = service.analyze()
        self.assertEqual(len(cases), 1)
        case = cases[0]
        categories = {item.step_id + ':' + item.status: item.category for item in case.work_items}
        self.assertIn('review:failed', categories)
        self.assertEqual(categories['review:failed'], WORK_FIRST)
        self.assertEqual(categories['review:succeeded'], WORK_REEXECUTION)
        self.assertTrue(any(item.category == WORK_COMPENSATION for item in case.work_items))

    def test_analysis_does_not_write_anything(self):
        self._seed_records()
        before = self.db.counts()
        CostAnalysisService(self.db, workflows=workflows_v1()).report()
        self.assertEqual(self.db.counts(), before)

    def test_unparseable_store_events_become_global_evidence_problems(self):
        import uuid
        from one_stop_cases.contracts import Event
        # A recognized cost event for one case, plus an unrelated unknown kind.
        good = Event(str(uuid.uuid4()), 'case', 'case-K', 'case_intake',
                     at(1, 9), {'request_key': 'r', 'service_version': 'svc-v1',
                                'intake_batch_id': 'b1'}, 1)
        unknown = Event(str(uuid.uuid4()), 'case', 'case-OTHER', 'processed',
                        at(1, 9), {'some': 'fact'}, 1)
        with self.db.transaction() as connection:
            self.db.append_event(connection, good)
            self.db.append_event(connection, unknown)
        report = CostAnalysisService(self.db, workflows=workflows_v1()).report()
        self.assertEqual(report['case_summary']['total_cases'], 1)
        self.assertTrue(any('unknown event kind' for problem in report['evidence_problems']))


class CliCostTests(unittest.TestCase):
    def test_cost_subcommand_outputs_report(self):
        from one_stop_cases.cli import main
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        db_path = Path(temp.name) / 'cli.sqlite3'
        fixture_path = Path(temp.name) / 'cost-fixture.json'
        output_path = Path(temp.name) / 'cost-output.json'
        fixture = {'workflows': workflows_v1(), 'cost_events': rework_case_events()}
        fixture_path.write_text(json.dumps(fixture), encoding='utf-8')
        rc = main(['--database', str(db_path), 'cost', '--fixture', str(fixture_path),
                   '--output', str(output_path)])
        self.assertEqual(rc, 0)
        payload = json.loads(output_path.read_text(encoding='utf-8'))
        self.assertEqual(payload['case_summary']['total_cases'], 1)
        self.assertTrue(payload['reconciliation']['balanced'])


if __name__ == '__main__':
    unittest.main()
