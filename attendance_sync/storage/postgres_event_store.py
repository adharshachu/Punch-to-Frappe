"""PostgreSQL-backed event store for the central Docker server."""
import json
import threading
from datetime import datetime, timezone
from typing import Any

import psycopg
from psycopg.rows import dict_row

from processors.punch_selection import select_daily_first_last_events


class PostgresEventStore:
    """Thread-local PostgreSQL store implementing the EventStore interface."""

    def __init__(self, dsn: str) -> None:
        if not dsn:
            raise EnvironmentError("POSTGRES_DSN is required when STORAGE_BACKEND=postgres.")
        self._dsn = dsn
        self._local = threading.local()
        self._init_db()

    def _conn(self) -> psycopg.Connection:
        if not hasattr(self._local, "conn"):
            self._local.conn = psycopg.connect(
                self._dsn,
                row_factory=dict_row,
                autocommit=True,
            )
        return self._local.conn

    def close(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            del self._local.conn

    def _init_db(self) -> None:
        conn = self._conn()
        with conn.transaction():
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS processed_events (
                    serial_no   TEXT PRIMARY KEY,
                    employee_no TEXT NOT NULL,
                    device_ip   TEXT NOT NULL,
                    event_time  TEXT NOT NULL,
                    pushed_at   TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS last_punch (
                    employee_id TEXT PRIMARY KEY,
                    punch_time  TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS retry_queue (
                    id          BIGSERIAL PRIMARY KEY,
                    employee_id TEXT    NOT NULL,
                    event_time  TEXT    NOT NULL,
                    device_ip   TEXT    NOT NULL,
                    serial_no   TEXT    NOT NULL UNIQUE,
                    log_type    TEXT,
                    attempts    INTEGER NOT NULL DEFAULT 0,
                    next_retry  TEXT    NOT NULL,
                    last_error  TEXT
                )
                """
            )
            conn.execute("ALTER TABLE retry_queue ADD COLUMN IF NOT EXISTS log_type TEXT")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS inbound_events (
                    id              BIGSERIAL PRIMARY KEY,
                    source_node     TEXT    NOT NULL,
                    source_event_id TEXT    NOT NULL,
                    payload         JSONB   NOT NULL,
                    status          TEXT    NOT NULL DEFAULT 'pending',
                    received_at     TEXT    NOT NULL,
                    processed_at    TEXT,
                    last_result     TEXT,
                    UNIQUE(source_node, source_event_id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS frappe_push_log (
                    id             BIGSERIAL PRIMARY KEY,
                    attempted_at   TEXT    NOT NULL,
                    serial_no      TEXT    NOT NULL,
                    employee_no    TEXT    NOT NULL,
                    hrms_id        TEXT    NOT NULL,
                    event_time     TEXT    NOT NULL,
                    device_ip      TEXT    NOT NULL,
                    device_id      TEXT    NOT NULL,
                    log_type       TEXT,
                    result         TEXT    NOT NULL,
                    http_status    INTEGER,
                    payload        JSONB   NOT NULL,
                    response_body  TEXT,
                    error          TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_inbound_events_pending
                ON inbound_events (status, received_at, id)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_retry_queue_due
                ON retry_queue (next_retry, attempts)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_frappe_push_log_attempted
                ON frappe_push_log (attempted_at DESC, id DESC)
                """
            )

    def is_processed(self, serial_no: str) -> bool:
        row = self._conn().execute(
            "SELECT 1 FROM processed_events WHERE serial_no = %s",
            (serial_no,),
        ).fetchone()
        return row is not None

    def mark_processed(
        self,
        serial_no: str,
        employee_no: str,
        device_ip: str,
        event_time: str,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._conn().transaction():
            self._conn().execute(
                """
                INSERT INTO processed_events
                    (serial_no, employee_no, device_ip, event_time, pushed_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (serial_no) DO NOTHING
                """,
                (serial_no, employee_no, device_ip, event_time, now),
            )

    def is_duplicate_punch(
        self, employee_id: str, punch_time: datetime, window_seconds: int
    ) -> bool:
        row = self._conn().execute(
            "SELECT punch_time FROM last_punch WHERE employee_id = %s",
            (employee_id,),
        ).fetchone()
        if row is None:
            return False

        last = datetime.fromisoformat(row["punch_time"])
        p_naive = punch_time.replace(tzinfo=None)
        l_naive = last.replace(tzinfo=None)
        return abs((p_naive - l_naive).total_seconds()) < window_seconds

    def update_last_punch(self, employee_id: str, punch_time: datetime) -> None:
        with self._conn().transaction():
            self._conn().execute(
                """
                INSERT INTO last_punch (employee_id, punch_time)
                VALUES (%s, %s)
                ON CONFLICT (employee_id)
                DO UPDATE SET punch_time = EXCLUDED.punch_time
                """,
                (employee_id, punch_time.isoformat()),
            )

    def enqueue_retry(
        self,
        employee_id: str,
        event_time: str,
        device_ip: str,
        serial_no: str,
        next_retry: datetime,
        error: str = "",
        log_type: str | None = None,
    ) -> None:
        with self._conn().transaction():
            self._conn().execute(
                """
                INSERT INTO retry_queue
                    (employee_id, event_time, device_ip, serial_no, log_type, attempts, next_retry, last_error)
                VALUES (%s, %s, %s, %s, %s, 1, %s, %s)
                ON CONFLICT (serial_no) DO UPDATE SET
                    attempts = retry_queue.attempts + 1,
                    log_type = EXCLUDED.log_type,
                    next_retry = EXCLUDED.next_retry,
                    last_error = EXCLUDED.last_error
                """,
                (
                    employee_id,
                    event_time,
                    device_ip,
                    serial_no,
                    log_type,
                    next_retry.isoformat(),
                    error,
                ),
            )

    def get_due_retries(self, max_attempts: int, force: bool = False) -> list[dict]:
        due_clause = "" if force else "next_retry <= %s AND"
        params: tuple[Any, ...] = (
            (max_attempts,) if force
            else (datetime.now(timezone.utc).isoformat(), max_attempts)
        )
        rows = self._conn().execute(
            f"""
            SELECT id, employee_id, event_time, device_ip, serial_no, log_type, attempts
            FROM retry_queue
            WHERE {due_clause} attempts < %s
            ORDER BY next_retry
            """,
            params,
        ).fetchall()
        return [dict(row) for row in rows]

    def remove_retry(self, row_id: int) -> None:
        with self._conn().transaction():
            self._conn().execute("DELETE FROM retry_queue WHERE id = %s", (row_id,))

    def purge_dead_retries(self, max_attempts: int) -> int:
        with self._conn().transaction():
            cur = self._conn().execute(
                "DELETE FROM retry_queue WHERE attempts >= %s",
                (max_attempts,),
            )
        return cur.rowcount or 0

    def record_frappe_push_attempt(
        self,
        *,
        serial_no: str,
        employee_no: str,
        hrms_id: str,
        event_time: str,
        device_ip: str,
        device_id: str,
        log_type: str | None,
        payload: dict[str, Any],
        result: str,
        http_status: int | None = None,
        response_body: str = "",
        error: str = "",
    ) -> None:
        with self._conn().transaction():
            self._conn().execute(
                """
                INSERT INTO frappe_push_log
                    (attempted_at, serial_no, employee_no, hrms_id, event_time,
                     device_ip, device_id, log_type, result, http_status, payload,
                     response_body, error)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)
                """,
                (
                    datetime.now(timezone.utc).isoformat(),
                    serial_no,
                    employee_no,
                    hrms_id,
                    event_time,
                    device_ip,
                    device_id,
                    log_type,
                    result,
                    http_status,
                    json.dumps(payload, separators=(",", ":"), sort_keys=True),
                    response_body[:4000],
                    error[:4000],
                ),
            )

    def enqueue_inbound_events(
        self,
        source_node: str,
        events: list[dict[str, Any]],
    ) -> tuple[int, int]:
        now = datetime.now(timezone.utc).isoformat()
        inserted = 0
        skipped = 0

        with self._conn().transaction():
            for event in events:
                source_event_id = self._source_event_id(event)
                row = self._conn().execute(
                    """
                    INSERT INTO inbound_events
                        (source_node, source_event_id, payload, received_at)
                    VALUES (%s, %s, %s::jsonb, %s)
                    ON CONFLICT (source_node, source_event_id) DO NOTHING
                    RETURNING id
                    """,
                    (
                        source_node,
                        source_event_id,
                        json.dumps(event, separators=(",", ":"), sort_keys=True),
                        now,
                    ),
                ).fetchone()
                if row:
                    inserted += 1
                else:
                    skipped += 1

        return inserted, skipped

    def get_pending_inbound_events(self, limit: int | None = None) -> list[dict[str, Any]]:
        query = """
            SELECT id, source_node, payload
            FROM inbound_events
            WHERE status = 'pending'
            ORDER BY received_at, id
        """
        params: tuple[Any, ...] = ()
        if limit is not None:
            query += " LIMIT %s"
            params = (limit,)
        rows = self._conn().execute(query, params).fetchall()
        return [
            {
                "id": row["id"],
                "source_node": row["source_node"],
                "payload": row["payload"]
                if isinstance(row["payload"], dict)
                else json.loads(row["payload"]),
            }
            for row in rows
        ]

    def mark_inbound_processed(self, row_id: int, result: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._conn().transaction():
            self._conn().execute(
                """
                UPDATE inbound_events
                SET status = 'done', processed_at = %s, last_result = %s
                WHERE id = %s
                """,
                (now, result, row_id),
            )

    def pending_inbound_count(self) -> int:
        row = self._conn().execute(
            "SELECT COUNT(*) AS count FROM inbound_events WHERE status = 'pending'"
        ).fetchone()
        return int(row["count"]) if row else 0

    def inbound_counts(self) -> dict[str, int]:
        rows = self._conn().execute(
            "SELECT status, COUNT(*) AS count FROM inbound_events GROUP BY status"
        ).fetchall()
        counts = {"pending": 0, "done": 0}
        for row in rows:
            counts[row["status"]] = int(row["count"])
        return counts

    def latest_inbound_by_node(self) -> dict[str, dict[str, Any]]:
        rows = self._conn().execute(
            """
            SELECT source_node, MAX(received_at) AS last_received_at, COUNT(*) AS event_count
            FROM inbound_events
            GROUP BY source_node
            """
        ).fetchall()
        return {
            row["source_node"]: {
                "last_received_at": row["last_received_at"],
                "event_count": int(row["event_count"] or 0),
            }
            for row in rows
        }

    def processed_count(self) -> int:
        row = self._conn().execute(
            "SELECT COUNT(*) AS count FROM processed_events"
        ).fetchone()
        return int(row["count"]) if row else 0

    def retry_queue_size(self) -> int:
        row = self._conn().execute(
            "SELECT COUNT(*) AS count FROM retry_queue"
        ).fetchone()
        return int(row["count"]) if row else 0

    def live_attendance_source_events(self, date: str | None = None, scan_limit: int | None = 10000) -> list[dict[str, Any]]:
        query = """
            SELECT id, source_node, payload
            FROM inbound_events
        """
        params: list[Any] = []
        if date:
            query += " WHERE substr(payload->>'time', 1, 10) = %s"
            params.append(date)
        query += " ORDER BY id DESC"
        if scan_limit is not None:
            query += " LIMIT %s"
            params.append(scan_limit)
        rows = self._conn().execute(query, tuple(params)).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            payload = row["payload"] if isinstance(row["payload"], dict) else json.loads(row["payload"])
            event_time = str(payload.get("time") or "").strip()
            employee = str(
                payload.get("employeeNoString") or payload.get("employeeNo") or ""
            ).strip()
            if not employee or not event_time:
                continue
            device_ip = str(
                payload.get("deviceIP") or payload.get("deviceIp") or ""
            ).strip()
            out.append(
                {
                    "id": row["id"],
                    "employee": employee,
                    "event_time": event_time,
                    "serial_no": payload.get("serialNo"),
                    "name": payload.get("name"),
                    "device_ip": device_ip,
                    "source_node": row["source_node"],
                }
            )
        return out

    def recent_inbound(self, limit: int = 50) -> list[dict[str, Any]]:
        rows = self._conn().execute(
            """
            SELECT id, source_node, payload, status, received_at, processed_at, last_result
            FROM inbound_events
            ORDER BY id DESC
            LIMIT %s
            """,
            (limit,),
        ).fetchall()
        out = []
        for row in rows:
            payload = row["payload"] if isinstance(row["payload"], dict) else json.loads(row["payload"])
            out.append(
                {
                    "id": row["id"],
                    "source_node": row["source_node"],
                    "status": row["status"],
                    "received_at": row["received_at"],
                    "processed_at": row["processed_at"],
                    "last_result": row["last_result"],
                    "employee": payload.get("employeeNoString") or payload.get("employeeNo"),
                    "device_ip": payload.get("deviceIP"),
                    "event_time": payload.get("time"),
                    "serial_no": payload.get("serialNo"),
                    "event_type": payload.get("eventType"),
                    "minor": payload.get("minor"),
                }
            )
        return out

    def punch_records(
        self,
        page: int = 1,
        page_size: int = 100,
        search: str = "",
        date_from: str = "",
        date_to: str = "",
        status: str = "",
    ) -> dict[str, Any]:
        rows = self._conn().execute(
            """
            SELECT id, source_node, payload, status, received_at, processed_at, last_result
            FROM inbound_events
            ORDER BY id DESC
            """
        ).fetchall()
        search_text = search.lower()
        filtered: list[dict[str, Any]] = []
        for row in rows:
            payload = row["payload"] if isinstance(row["payload"], dict) else json.loads(row["payload"])
            event_time = str(payload.get("time") or "").strip()
            event_date = event_time.split("T", 1)[0].split(" ", 1)[0] if event_time else ""
            if date_from and event_date < date_from:
                continue
            if date_to and event_date > date_to:
                continue
            if status and row["status"] != status:
                continue

            record = {
                "id": row["id"],
                "source_node": row["source_node"],
                "status": row["status"],
                "received_at": row["received_at"],
                "processed_at": row["processed_at"],
                "last_result": row["last_result"],
                "employee": payload.get("employeeNoString") or payload.get("employeeNo"),
                "name": payload.get("name"),
                "device_ip": payload.get("deviceIP"),
                "event_time": event_time,
                "serial_no": payload.get("serialNo"),
                "event_type": payload.get("eventType"),
                "minor": payload.get("minor"),
            }
            if search_text:
                haystack = " ".join(str(value or "") for value in record.values()).lower()
                if search_text not in haystack:
                    continue
            filtered.append(record)

        total = len(filtered)
        start = (page - 1) * page_size
        end = start + page_size
        return {
            "records": filtered[start:end],
            "page": page,
            "page_size": page_size,
            "total": total,
            "has_next": end < total,
            "has_prev": page > 1,
        }

    def recent_processed(self, limit: int = 50) -> list[dict[str, Any]]:
        rows = self._conn().execute(
            """
            SELECT serial_no, employee_no, device_ip, event_time, pushed_at
            FROM processed_events
            ORDER BY pushed_at DESC
            LIMIT %s
            """,
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]

    def frappe_push_logs(self, page: int = 1, page_size: int = 100) -> dict[str, Any]:
        total_row = self._conn().execute(
            "SELECT COUNT(*) AS count FROM frappe_push_log"
        ).fetchone()
        total = int(total_row["count"]) if total_row else 0
        offset = (page - 1) * page_size
        rows = self._conn().execute(
            """
            SELECT id, attempted_at, serial_no, employee_no, hrms_id, event_time,
                   device_ip, device_id, log_type, result, http_status, payload,
                   response_body, error
            FROM frappe_push_log
            ORDER BY id DESC
            LIMIT %s OFFSET %s
            """,
            (page_size, offset),
        ).fetchall()
        out = []
        for row in rows:
            item = dict(row)
            if isinstance(item.get("payload"), str):
                try:
                    item["payload"] = json.loads(item["payload"])
                except json.JSONDecodeError:
                    pass
            out.append(item)
        return {
            "logs": out,
            "page": page,
            "page_size": page_size,
            "total": total,
            "has_next": offset + page_size < total,
            "has_prev": page > 1,
        }

    def recent_frappe_push_logs(self, limit: int = 100) -> list[dict[str, Any]]:
        return self.frappe_push_logs(page=1, page_size=limit)["logs"]

    def get_retry_queue(self, limit: int = 50) -> list[dict[str, Any]]:
        rows = self._conn().execute(
            """
            SELECT id, employee_id, event_time, device_ip, serial_no, log_type, attempts,
                   next_retry, last_error
            FROM retry_queue
            ORDER BY next_retry
            LIMIT %s
            """,
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]

    def attendance_overview(self, limit: int | None = None, date_from: str | None = None, date_to: str | None = None) -> list[dict[str, Any]]:
        query = """
            SELECT source_node, payload, status, last_result
            FROM inbound_events
        """
        params: list[Any] = []
        predicates: list[str] = []
        if date_from:
            predicates.append("substr(payload->>'time', 1, 10) >= %s")
            params.append(date_from)
        if date_to:
            predicates.append("substr(payload->>'time', 1, 10) <= %s")
            params.append(date_to)
        if predicates:
            query += " WHERE " + " AND ".join(predicates)
        query += " ORDER BY id DESC"
        if limit is not None:
            query += " LIMIT %s"
            params.append(limit)
        rows = self._conn().execute(query, tuple(params)).fetchall()
        grouped: dict[tuple[str, str], dict[str, Any]] = {}
        for row in rows:
            payload = row["payload"] if isinstance(row["payload"], dict) else json.loads(row["payload"])
            employee = str(
                payload.get("employeeNoString") or payload.get("employeeNo") or ""
            ).strip()
            event_time = str(payload.get("time") or "").strip()
            if not employee or not event_time:
                continue

            event_date = event_time.split("T", 1)[0].split(" ", 1)[0]
            key = (employee, event_date)
            item = grouped.setdefault(
                key,
                {
                    "employee": employee,
                    "date": event_date,
                    "source_nodes": set(),
                    "devices": set(),
                    "punch_count": 0,
                    "first_time": None,
                    "first_result": None,
                    "last_time": None,
                    "last_result": None,
                    "_events": [],
                },
            )
            item["source_nodes"].add(row["source_node"])
            device_ip = payload.get("deviceIP")
            if device_ip:
                item["devices"].add(str(device_ip))
            item["punch_count"] += 1
            item["_events"].append(
                {
                    "time": event_time,
                    "result": row["last_result"] or row["status"],
                    "serialNo": payload.get("serialNo"),
                }
            )

        overview = []
        for item in grouped.values():
            boundaries = select_daily_first_last_events(item.pop("_events"))
            first = boundaries["first"]
            last = boundaries["last"]
            if first:
                item["first_time"] = first["time"]
                item["first_result"] = first["result"]
            if last:
                item["last_time"] = last["time"]
                item["last_result"] = last["result"]
            item["source_nodes"] = sorted(item["source_nodes"])
            item["devices"] = sorted(item["devices"])
            overview.append(item)
        overview.sort(key=lambda item: (item["date"], item["employee"]), reverse=True)
        return overview

    def dashboard_alerts(self, limit: int = 100) -> list[dict[str, Any]]:
        alerts: list[dict[str, Any]] = []

        retry_rows = self._conn().execute(
            """
            SELECT employee_id, event_time, device_ip, serial_no, log_type, attempts,
                   next_retry, last_error
            FROM retry_queue
            ORDER BY next_retry
            LIMIT %s
            """,
            (limit,),
        ).fetchall()
        for row in retry_rows:
            alerts.append(
                {
                    "severity": "warning",
                    "kind": "retry",
                    "title": "Frappe push waiting for retry",
                    "employee": row["employee_id"],
                    "device_ip": row["device_ip"],
                    "event_time": row["event_time"],
                    "detail": row["last_error"] or f"attempts: {row['attempts']}",
                    "action": "Check Frappe connectivity/credentials, then use Push Now.",
                }
            )
        retry_details = {
            row["serial_no"]: f"{row['last_error'] or 'waiting for retry'} (attempts: {row['attempts']})"
            for row in retry_rows
        }

        bad_rows = self._conn().execute(
            """
            SELECT source_node, payload, received_at, processed_at, last_result
            FROM inbound_events
            WHERE last_result LIKE '%%missing%%'
               OR last_result LIKE '%%bad%%'
               OR last_result LIKE '%%error%%'
               OR last_result LIKE '%%discarded%%'
            ORDER BY id DESC
            LIMIT %s
            """,
            (limit,),
        ).fetchall()
        for row in bad_rows:
            payload = row["payload"] if isinstance(row["payload"], dict) else json.loads(row["payload"])
            result = row["last_result"] or "unresolved"
            action = "Review the event and logs."
            if "missing_mapping" in result:
                action = "Add this employee number in Employee Map, restart server, then re-upload or reprocess the range."
            elif "bad_time" in result:
                action = "Check the device clock/time format."
            elif "missing_serial" in result:
                action = "Check the device event payload; serial number is required for dedupe."
            elif "queued_client_error" in result:
                action = "Fix the Frappe validation error shown here, then use Manual Push to Frappe."
            serial_no = str(payload.get("serialNo") or "").strip()
            namespaced_serial = f"{row['source_node']}:{serial_no}" if serial_no else ""
            detail = retry_details.get(namespaced_serial) or result
            alerts.append(
                {
                    "severity": "critical" if "missing_mapping" in result else "warning",
                    "kind": result,
                    "title": "Punch was not pushed to Frappe",
                    "employee": payload.get("employeeNoString") or payload.get("employeeNo"),
                    "device_ip": payload.get("deviceIP"),
                    "event_time": payload.get("time"),
                    "source_node": row["source_node"],
                    "detail": detail,
                    "action": action,
                    "received_at": row["received_at"],
                    "processed_at": row["processed_at"],
                }
            )

        pending_row = self._conn().execute(
            """
            SELECT COUNT(*) AS count, MIN(received_at) AS oldest
            FROM inbound_events
            WHERE status = 'pending'
            """
        ).fetchone()
        if pending_row and int(pending_row["count"] or 0) > 0:
            alerts.insert(
                0,
                {
                    "severity": "info",
                    "kind": "pending_queue",
                    "title": "Punches are waiting to be processed",
                    "employee": "",
                    "device_ip": "",
                    "event_time": pending_row["oldest"],
                    "detail": f"{pending_row['count']} pending event(s)",
                    "action": "Wait for the next interval or use Push Now.",
                },
            )

        return alerts[:limit]

    @staticmethod
    def _source_event_id(event: dict[str, Any]) -> str:
        device_ip = str(event.get("deviceIP", "")).strip()
        serial_no = str(event.get("serialNo", "")).strip()
        if serial_no:
            return "|".join([device_ip, serial_no])

        parts = [
            device_ip,
            str(event.get("employeeNoString", "")).strip(),
            str(event.get("time", "")).strip(),
        ]
        return "|".join(parts)
