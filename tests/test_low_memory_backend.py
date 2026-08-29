import os
import sys
import types
import unittest
from unittest.mock import MagicMock, patch


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "attendance_sync"))
sys.path.insert(0, ROOT)

# The unit test exercises server orchestration without opening PostgreSQL. The
# optional production driver is not installed in every developer test runtime.
if "psycopg" not in sys.modules:
    psycopg = types.ModuleType("psycopg")
    psycopg.Connection = object
    psycopg.connect = lambda *args, **kwargs: None
    psycopg_rows = types.ModuleType("psycopg.rows")
    psycopg_rows.dict_row = object()
    sys.modules["psycopg"] = psycopg
    sys.modules["psycopg.rows"] = psycopg_rows

import server
from storage.postgres_event_store import PostgresEventStore


class _BatchStore:
    def __init__(self, count: int) -> None:
        self.pending = [
            {
                "id": index,
                "source_node": "node-a",
                "payload": {
                    "serialNo": str(index),
                    "employeeNoString": str(index),
                    "time": "2026-08-20T09:00:00+05:30",
                },
            }
            for index in range(count)
        ]
        self.requested_limits = []

    def get_pending_inbound_events(self, limit=None, after_id=None):
        self.requested_limits.append(limit)
        rows = [row for row in self.pending if after_id is None or row["id"] > after_id]
        return rows[:limit]

    def mark_inbound_processed(self, row_id, _result):
        self.pending = [row for row in self.pending if row["id"] != row_id]


class _Processor:
    def __init__(self):
        self.pushed = []

    def prepare_event(self, event):
        employee = event["employeeNoString"]
        return "ready", {
            "serial_no": event["serialNo"],
            "event_dt": event["time"],
            "event_date": "2026-08-20",
            "hrms_id": employee,
        }

    def push_prepared_event(self, _prepared, log_type=None):
        self.pushed.append((_prepared["serial_no"], log_type))
        return "pushed"

    def process_retries(self, force=False):
        return 0


class LowMemoryBackendTests(unittest.TestCase):
    def _handler(self, path):
        handler = object.__new__(server.EventIngestHandler)
        handler.path = path
        handler.headers = {}
        handler.store = MagicMock()
        handler.store.attendance_overview_page.return_value = {
            "overview": [], "total": 0, "has_next": False, "has_prev": False
        }
        handler.frappe = MagicMock()
        return handler

    def test_config_api_rejects_secret_and_node_key_writes(self):
        with self.assertRaisesRegex(ValueError, "cannot be changed"):
            server._save_config({"values": {"HRMS_API_SECRET": "leaked"}})
        with self.assertRaisesRegex(ValueError, "SERVER_NODE_KEYS"):
            server._save_config({"nodes": [{"node_id": "node", "secret": "secret"}]})

    def test_dashboard_summary_uses_native_aggregate_contract(self):
        store = MagicMock()
        store.dashboard_summary_aggregates.return_value = {
            "stats": {"total": 2, "late": 1, "on_time": 1, "total_employees": 1},
            "trend": [{"date": "2026-08-20", "total": 2, "late": 1, "on_time": 1}],
            "employees": [{"employee": "101", "total": 2, "late": 1, "on_time": 1}],
        }
        store.live_attendance_source_events.return_value = []
        frappe = MagicMock()
        frappe.get_employees.return_value = {"EMP-101": {"department": "Engineering"}}
        server._dashboard_summary_cache.clear()
        with patch.object(server.settings, "load_employee_map", return_value={"101": "EMP-101"}):
            payload = server._dashboard_summary_payload(
                store, frappe, "2026-08-01", "2026-08-20"
            )

        store.attendance_overview.assert_not_called()
        self.assertEqual(payload["departments"][0]["department"], "Engineering")

    def test_hr_filters_are_passed_to_store_before_pagination(self):
        handler = self._handler(
            "/api/hr-verification?search=Alice&department=Engineering"
        )
        handler.store.attendance_overview_summary.return_value = {
            "total": 0, "late": 0, "on_time": 0, "missing_map": 0
        }
        handler.frappe.get_employees.return_value = {
            "EMP-101": {"employee_name": "Alice", "department": "Engineering"},
            "EMP-102": {"employee_name": "Bob", "department": "Sales"},
        }
        with (
            patch.object(server.settings, "load_employee_map", return_value={"101": "EMP-101", "102": "EMP-102"}),
            patch.object(server, "_json_response"),
        ):
            server.EventIngestHandler._dispatch_get(handler)

        kwargs = handler.store.attendance_overview_page.call_args.kwargs
        self.assertEqual(kwargs["eligible_employee_ids"], ["101"])
        self.assertEqual(kwargs["search_employee_ids"], ["101"])
        self.assertEqual(kwargs["search"], "Alice")

    def test_postgres_overview_query_uses_safe_time_order_and_pre_page_filters(self):
        store = object.__new__(PostgresEventStore)
        connection = MagicMock()
        connection.execute.return_value.fetchall.return_value = []
        store._conn = MagicMock(return_value=connection)

        store.attendance_overview_page(
            search="Alice",
            eligible_employee_ids=["101"],
            search_employee_ids=["101"],
        )

        sql = connection.execute.call_args.args[0]
        self.assertIn("pg_input_is_valid(event_time, 'timestamp with time zone')", sql)
        self.assertIn("employee_no = ANY(%s::text[])", sql)
        self.assertIn("OR employee = ANY(%s::text[])", sql)
        self.assertLess(sql.index("filtered AS"), sql.index("LIMIT %s OFFSET %s"))

    def test_pending_queue_is_drained_in_bounded_batches(self):
        store = _BatchStore(5000)

        result = server.process_pending_events(store, _Processor())

        self.assertEqual(result["processed"], 5000)
        self.assertLessEqual(result["peak_in_memory_candidates"], 3)
        self.assertFalse(store.pending)
        self.assertTrue(store.requested_limits)
        self.assertEqual(
            set(store.requested_limits), {server.settings.PENDING_PROCESS_BATCH_SIZE}
        )

    def test_node_tracker_is_bounded_and_hides_internal_timestamps(self):
        tracker = server.NodeTracker()
        with patch.object(server.settings, "NODE_TRACKER_MAX_ENTRIES", 2):
            tracker.record("one", 1, 1, 0)
            tracker.record("two", 1, 1, 0)
            tracker.record("three", 1, 1, 0)
            snapshot = tracker.snapshot()

        self.assertEqual(len(snapshot), 2)
        self.assertTrue(all("_last_seen_monotonic" not in row for row in snapshot))

    def test_first_and_last_selection_survives_batch_boundaries(self):
        store = _BatchStore(4)
        for index, row in enumerate(store.pending):
            row["payload"]["employeeNoString"] = "same-employee"
            row["payload"]["time"] = f"2026-08-20T{9 + index:02d}:00:00+05:30"
        processor = _Processor()

        with patch.object(server.settings, "PENDING_PROCESS_BATCH_SIZE", 2):
            result = server.process_pending_events(store, processor)

        self.assertEqual(processor.pushed, [("node-a:0", "IN"), ("node-a:3", "OUT")])
        self.assertEqual(result["results"]["skipped_middle_punch"], 2)


if __name__ == "__main__":
    unittest.main()
