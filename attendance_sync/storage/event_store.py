"""
SQLite-backed event store.

Responsibilities
────────────────
1. Track processed serialNos so the same Hikvision event is never pushed twice.
2. Track per-employee last-punch time for the 30-second de-duplication window.
3. Persist a retry queue for checkins that failed to reach Frappe.
"""
import sqlite3
import threading
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from processors.punch_selection import select_daily_first_last_events


class EventStore:
    """Thread-safe SQLite store for attendance events."""

    def __init__(self, db_path: Path) -> None:
        self._path = db_path
        self._local = threading.local()
        self._init_db()

    # ── connection management ────────────────────────────────────────────────

    def _conn(self) -> sqlite3.Connection:
        """Return a per-thread connection (created on first access)."""
        if not hasattr(self._local, "conn"):
            conn = sqlite3.connect(str(self._path), check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            self._local.conn = conn
        return self._local.conn

    def close(self) -> None:
        """Close this thread's SQLite connection if one has been opened."""
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            del self._local.conn

    def _init_db(self) -> None:
        conn = self._conn()
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS processed_events (
                serial_no   TEXT PRIMARY KEY,
                employee_no TEXT NOT NULL,
                device_ip   TEXT NOT NULL,
                event_time  TEXT NOT NULL,
                pushed_at   TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS last_punch (
                employee_id TEXT PRIMARY KEY,
                punch_time  TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS retry_queue (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                employee_id TEXT    NOT NULL,
                event_time  TEXT    NOT NULL,
                device_ip   TEXT    NOT NULL,
                serial_no   TEXT    NOT NULL UNIQUE,
                log_type    TEXT,
                attempts    INTEGER NOT NULL DEFAULT 0,
                next_retry  TEXT    NOT NULL,
                last_error  TEXT
            );

            CREATE TABLE IF NOT EXISTS inbound_events (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                source_node    TEXT    NOT NULL,
                source_event_id TEXT    NOT NULL,
                payload        TEXT    NOT NULL,
                status         TEXT    NOT NULL DEFAULT 'pending',
                received_at    TEXT    NOT NULL,
                processed_at   TEXT,
                last_result    TEXT,
                UNIQUE(source_node, source_event_id)
            );

            CREATE TABLE IF NOT EXISTS frappe_push_log (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
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
                payload        TEXT    NOT NULL,
                response_body  TEXT,
                error          TEXT
            );
            """
        )
        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(retry_queue)").fetchall()
        }
        if "log_type" not in columns:
            conn.execute("ALTER TABLE retry_queue ADD COLUMN log_type TEXT")
        conn.commit()

    # ── processed events ─────────────────────────────────────────────────────

    def is_processed(self, serial_no: str) -> bool:
        """Return True if this serialNo has already been pushed."""
        cur = self._conn().execute(
            "SELECT 1 FROM processed_events WHERE serial_no = ?", (serial_no,)
        )
        return cur.fetchone() is not None

    def mark_processed(
        self,
        serial_no: str,
        employee_no: str,
        device_ip: str,
        event_time: str,
    ) -> None:
        """Record a successfully pushed event."""
        now = datetime.now(timezone.utc).isoformat()
        self._conn().execute(
            """
            INSERT OR IGNORE INTO processed_events
                (serial_no, employee_no, device_ip, event_time, pushed_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (serial_no, employee_no, device_ip, event_time, now),
        )
        self._conn().commit()

    # ── 30-second de-duplication ──────────────────────────────────────────────

    def is_duplicate_punch(
        self, employee_id: str, punch_time: datetime, window_seconds: int
    ) -> bool:
        """
        Return True if the employee already has a punch within *window_seconds*
        of *punch_time*.
        """
        cur = self._conn().execute(
            "SELECT punch_time FROM last_punch WHERE employee_id = ?",
            (employee_id,),
        )
        row = cur.fetchone()
        if row is None:
            return False
        last = datetime.fromisoformat(row["punch_time"])
        # To avoid "TypeError: can't subtract offset-naive and offset-aware datetimes",
        # strip tzinfo if they mismatch, or just unconditionally strip it since both are local.
        p_naive = punch_time.replace(tzinfo=None)
        l_naive = last.replace(tzinfo=None)
        delta = abs((p_naive - l_naive).total_seconds())
        return delta < window_seconds

    def update_last_punch(self, employee_id: str, punch_time: datetime) -> None:
        self._conn().execute(
            """
            INSERT INTO last_punch (employee_id, punch_time)
            VALUES (?, ?)
            ON CONFLICT(employee_id) DO UPDATE SET punch_time = excluded.punch_time
            """,
            (employee_id, punch_time.isoformat()),
        )
        self._conn().commit()

    # ── retry queue ───────────────────────────────────────────────────────────

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
        """Add (or update attempt count of) a failed checkin to the retry queue."""
        self._conn().execute(
            """
            INSERT INTO retry_queue
                (employee_id, event_time, device_ip, serial_no, log_type, attempts, next_retry, last_error)
            VALUES (?, ?, ?, ?, ?, 1, ?, ?)
            ON CONFLICT(serial_no) DO UPDATE SET
                attempts   = attempts + 1,
                log_type   = excluded.log_type,
                next_retry = excluded.next_retry,
                last_error = excluded.last_error
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
        self._conn().commit()

    def get_due_retries(self, max_attempts: int, force: bool = False) -> list[dict]:
        """Return retry rows whose next_retry is due, or all live retries when forced."""
        now = datetime.now(timezone.utc).isoformat()
        due_clause = "" if force else "next_retry <= ? AND"
        params: tuple[Any, ...] = (max_attempts,) if force else (now, max_attempts)
        cur = self._conn().execute(
            f"""
            SELECT id, employee_id, event_time, device_ip, serial_no, log_type, attempts
            FROM retry_queue
            WHERE {due_clause} attempts < ?
            ORDER BY next_retry
            """,
            params,
        )
        return [dict(row) for row in cur.fetchall()]

    def remove_retry(self, row_id: int) -> None:
        self._conn().execute("DELETE FROM retry_queue WHERE id = ?", (row_id,))
        self._conn().commit()

    def purge_dead_retries(self, max_attempts: int) -> int:
        """Delete permanently-failed entries; return how many were removed."""
        cur = self._conn().execute(
            "DELETE FROM retry_queue WHERE attempts >= ?", (max_attempts,)
        )
        self._conn().commit()
        return cur.rowcount

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
        self._conn().execute(
            """
            INSERT INTO frappe_push_log
                (attempted_at, serial_no, employee_no, hrms_id, event_time,
                 device_ip, device_id, log_type, result, http_status, payload,
                 response_body, error)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        self._conn().commit()

    # ── inbound edge queue ───────────────────────────────────────────────────

    def enqueue_inbound_events(
        self,
        source_node: str,
        events: list[dict[str, Any]],
    ) -> tuple[int, int]:
        """Store raw events received from an edge node; return inserted/skipped counts."""
        now = datetime.now(timezone.utc).isoformat()
        inserted = 0
        skipped = 0

        for event in events:
            source_event_id = self._source_event_id(event)
            payload = json.dumps(event, separators=(",", ":"), sort_keys=True)
            cur = self._conn().execute(
                """
                INSERT OR IGNORE INTO inbound_events
                    (source_node, source_event_id, payload, received_at)
                VALUES (?, ?, ?, ?)
                """,
                (source_node, source_event_id, payload, now),
            )
            if cur.rowcount:
                inserted += 1
            else:
                skipped += 1

        self._conn().commit()
        return inserted, skipped

    def get_pending_inbound_events(self, limit: int | None = None) -> list[dict[str, Any]]:
        """Return queued edge events that still need to be pushed to Frappe."""
        query = """
            SELECT id, source_node, payload
            FROM inbound_events
            WHERE status = 'pending'
            ORDER BY received_at, id
        """
        params: tuple[Any, ...] = ()
        if limit is not None:
            query += " LIMIT ?"
            params = (limit,)
        cur = self._conn().execute(query, params)
        rows = []
        for row in cur.fetchall():
            rows.append(
                {
                    "id": row["id"],
                    "source_node": row["source_node"],
                    "payload": json.loads(row["payload"]),
                }
            )
        return rows

    def mark_inbound_processed(self, row_id: int, result: str) -> None:
        """Mark an inbound event as handled by the central processor."""
        now = datetime.now(timezone.utc).isoformat()
        self._conn().execute(
            """
            UPDATE inbound_events
            SET status = 'done', processed_at = ?, last_result = ?
            WHERE id = ?
            """,
            (now, result, row_id),
        )
        self._conn().commit()

    def pending_inbound_count(self) -> int:
        cur = self._conn().execute(
            "SELECT COUNT(*) AS count FROM inbound_events WHERE status = 'pending'"
        )
        row = cur.fetchone()
        return int(row["count"]) if row else 0

    # ── dashboard helpers ────────────────────────────────────────────────────

    def inbound_counts(self) -> dict[str, int]:
        cur = self._conn().execute(
            "SELECT status, COUNT(*) AS count FROM inbound_events GROUP BY status"
        )
        counts = {"pending": 0, "done": 0}
        for row in cur.fetchall():
            counts[row["status"]] = int(row["count"])
        return counts

    def latest_inbound_by_node(self) -> dict[str, dict[str, Any]]:
        cur = self._conn().execute(
            """
            SELECT source_node, MAX(received_at) AS last_received_at, COUNT(*) AS event_count
            FROM inbound_events
            GROUP BY source_node
            """
        )
        return {
            row["source_node"]: {
                "last_received_at": row["last_received_at"],
                "event_count": int(row["event_count"] or 0),
            }
            for row in cur.fetchall()
        }

    def processed_count(self) -> int:
        cur = self._conn().execute("SELECT COUNT(*) AS count FROM processed_events")
        row = cur.fetchone()
        return int(row["count"]) if row else 0

    def retry_queue_size(self) -> int:
        cur = self._conn().execute("SELECT COUNT(*) AS count FROM retry_queue")
        row = cur.fetchone()
        return int(row["count"]) if row else 0

    def live_attendance_source_events(self, date: str | None = None, scan_limit: int | None = 10000) -> list[dict[str, Any]]:
        query = """
            SELECT id, source_node, payload
            FROM inbound_events
        """
        params: list[Any] = []
        if date:
            query += " WHERE substr(json_extract(payload, '$.time'), 1, 10) = ?"
            params.append(date)
        query += " ORDER BY id DESC"
        if scan_limit is not None:
            query += " LIMIT ?"
            params.append(scan_limit)
        cur = self._conn().execute(query, tuple(params))
        rows: list[dict[str, Any]] = []
        for row in cur.fetchall():
            try:
                payload = json.loads(row["payload"])
            except Exception:
                payload = {}
            event_time = str(payload.get("time") or "").strip()
            employee = str(
                payload.get("employeeNoString") or payload.get("employeeNo") or ""
            ).strip()
            if not employee or not event_time:
                continue
            device_ip = str(
                payload.get("deviceIP") or payload.get("deviceIp") or ""
            ).strip()
            rows.append(
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
        return rows

    def recent_inbound(self, limit: int = 50) -> list[dict[str, Any]]:
        cur = self._conn().execute(
            """
            SELECT id, source_node, payload, status, received_at, processed_at, last_result
            FROM inbound_events
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = []
        for row in cur.fetchall():
            try:
                payload = json.loads(row["payload"])
            except Exception:
                payload = {}
            rows.append(
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
        return rows

    def punch_records(
        self,
        page: int = 1,
        page_size: int = 100,
        search: str = "",
        date_from: str = "",
        date_to: str = "",
        status: str = "",
    ) -> dict[str, Any]:
        cur = self._conn().execute(
            """
            SELECT id, source_node, payload, status, received_at, processed_at, last_result
            FROM inbound_events
            ORDER BY id DESC
            """
        )
        search_text = search.lower()
        filtered: list[dict[str, Any]] = []
        for row in cur.fetchall():
            try:
                payload = json.loads(row["payload"])
            except Exception:
                payload = {}
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
        cur = self._conn().execute(
            """
            SELECT serial_no, employee_no, device_ip, event_time, pushed_at
            FROM processed_events
            ORDER BY pushed_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        return [dict(row) for row in cur.fetchall()]

    def frappe_push_logs(self, page: int = 1, page_size: int = 100) -> dict[str, Any]:
        total_row = self._conn().execute(
            "SELECT COUNT(*) AS count FROM frappe_push_log"
        ).fetchone()
        total = int(total_row["count"]) if total_row else 0
        offset = (page - 1) * page_size
        cur = self._conn().execute(
            """
            SELECT id, attempted_at, serial_no, employee_no, hrms_id, event_time,
                   device_ip, device_id, log_type, result, http_status, payload,
                   response_body, error
            FROM frappe_push_log
            ORDER BY id DESC
            LIMIT ? OFFSET ?
            """,
            (page_size, offset),
        )
        out = []
        for row in cur.fetchall():
            item = dict(row)
            try:
                item["payload"] = json.loads(item["payload"])
            except Exception:
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
        cur = self._conn().execute(
            """
            SELECT id, employee_id, event_time, device_ip, serial_no, log_type, attempts,
                   next_retry, last_error
            FROM retry_queue
            ORDER BY next_retry
            LIMIT ?
            """,
            (limit,),
        )
        return [dict(row) for row in cur.fetchall()]

    def attendance_overview(self, limit: int | None = None, date_from: str | None = None, date_to: str | None = None) -> list[dict[str, Any]]:
        query = """
            SELECT source_node, payload, status, last_result
            FROM inbound_events
        """
        params: list[Any] = []
        predicates: list[str] = []
        if date_from:
            predicates.append("substr(json_extract(payload, '$.time'), 1, 10) >= ?")
            params.append(date_from)
        if date_to:
            predicates.append("substr(json_extract(payload, '$.time'), 1, 10) <= ?")
            params.append(date_to)
        if predicates:
            query += " WHERE " + " AND ".join(predicates)
        query += " ORDER BY id DESC"
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        cur = self._conn().execute(query, tuple(params))
        grouped: dict[tuple[str, str], dict[str, Any]] = {}
        for row in cur.fetchall():
            try:
                payload = json.loads(row["payload"])
            except Exception:
                continue

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
            LIMIT ?
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
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        for row in bad_rows:
            try:
                payload = json.loads(row["payload"])
            except Exception:
                payload = {}
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
        """Build a stable source id for deduping uploads from one edge node."""
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
