"""
Central attendance sync server.

Receives signed event batches from edge PCs, stores them in SQLite, and pushes
queued events to Frappe only when the dashboard/manual API asks it to.

Also serves a small dashboard at "/" with live status, recent events, retry
queue, per-node connection status, and a manual "Push now" button.
"""
import json
import logging
import errno
import pickle
import sqlite3
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta, time as datetime_time, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib import error as urlerror
from urllib import request as urlrequest
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

import os as _os
_os.chdir(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))

from config import settings
from config.env_file import read_env, update_env
from hrms.frappe_client import FrappeClient
from processors.event_processor import EventProcessor
from processors.live_attendance import build_live_attendance, live_calendar_today
from processors.punch_selection import punch_sort_key, select_daily_punches
from storage.factory import create_event_store
from transport.security import (
    NODE_HEADER,
    SIGNATURE_HEADER,
    TIMESTAMP_HEADER,
    verify_auth_headers,
)


logger = logging.getLogger(__name__)
_running = True

# Serialises pushes so the same event is never processed by two requests at once.
_push_lock = threading.Lock()

# Wakes the server loop for shutdown.
_wake_event = threading.Event()

# Caps how many HTTP requests are handled at once so dashboard polls and API
# floods cannot exhaust memory on a small server.
_MAX_CONCURRENT_REQUESTS = settings.SERVER_MAX_CONCURRENT_REQUESTS
_request_slots = threading.BoundedSemaphore(_MAX_CONCURRENT_REQUESTS)
_heavy_request_slots = threading.BoundedSemaphore(settings.SERVER_MAX_HEAVY_REQUESTS)
_HEAVY_GET_PATHS = {
    "/api/attendance-overview",
    "/api/hr-verification",
    "/api/dashboard-summary",
}

# Default window for the attendance overview when no date range is requested,
# so an unfiltered dashboard view can never load the whole event table.
_OVERVIEW_DEFAULT_DAYS = 90

# Resident-memory safety net for small servers. LOG_LEVEL can be tuned, and
# AUTO_RESTART_ON_HIGH_MEMORY=1 exits cleanly so the container manager restarts.
_MEMORY_WARN_MB = int(_os.environ.get("MEMORY_WARN_MB", "700") or "700")
_MEMORY_RESTART_MB = int(_os.environ.get("MEMORY_RESTART_MB", "1100") or "1100")
_MEMORY_RESTART_ENABLED = (_os.environ.get("AUTO_RESTART_ON_HIGH_MEMORY", "0") or "0").lower() in {"1", "true", "yes"}


def process_rss_mb() -> int:
    """Return current resident set size of this process in megabytes."""
    try:
        with open("/proc/self/status", encoding="utf-8") as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) // 1024
    except (OSError, ValueError):
        return 0
    return 0

_DASHBOARD_HTML_PATH = Path(__file__).resolve().parent / "dashboard.html"
_OPENAPI_JSON_PATH = Path(__file__).resolve().parent / "openapi.json"
_SWAGGER_HTML_PATH = Path(__file__).resolve().parent / "swagger.html"
_ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
_last_push_lock = threading.Lock()
_last_push: dict[str, Any] = {
    "started_at": None,
    "finished_at": None,
    "processed": 0,
    "retries": 0,
    "results": {},
    "error": None,
    "trigger": None,
}
_uptime_monitor_lock = threading.Lock()
_uptime_monitor: dict[str, Any] = {
    "enabled": False,
    "threshold_hours": None,
    "checked_at": None,
    "nodes": [],
}

# Keys treated as secrets in the config API: masked in GET, blank-on-PUT means keep.
_PROTECTED_CONFIG_KEYS = {
    "HRMS_API_KEY",
    "HRMS_API_SECRET",
    "POSTGRES_DSN",
    "SLACK_WEBHOOK_URL",
    "SERVER_NODE_KEYS",
    "NGINX_BASIC_AUTH_USER",
    "NGINX_BASIC_AUTH_PASSWORD",
    "EMPLOYEE_MAP_RESTART_COMMAND",
}

# Whitelist of plain key/value config fields editable via the dashboard.
_EDITABLE_KEYS = (
    "HRMS_URL",
    "POLL_INTERVAL",
    "DEDUP_WINDOW",
    "LOG_LEVEL",
    "SERVER_HOST",
    "SERVER_PORT",
    "STORAGE_BACKEND",
    "DEFAULT_LOG_TYPE",
    "LATE_AFTER_TIME",
    "FRAPPE_AUTO_PUSH_ENABLED",
    "FRAPPE_AUTO_PUSH_TIME",
    "FRAPPE_AUTO_PUSH_TIMEZONE",
    "NODE_UPTIME_MONITOR_ENABLED",
    "NODE_UPTIME_THRESHOLD_HOURS",
    "NODE_UPTIME_CHECK_INTERVAL_SECONDS",
    "NODE_UPTIME_NOTIFY_ON_RECOVERY",
)

_employee_details_lock = threading.Lock()
_employee_details_cache: dict[str, dict[str, Any]] = {}
_employee_details_cache_expires_at = 0.0
_dashboard_summary_lock = threading.Lock()
_dashboard_summary_cache: dict[tuple[str, str], tuple[float, dict[str, Any]]] = {}


def _int_query(params: dict[str, list[str]], key: str, default: int, minimum: int, maximum: int) -> int:
    raw = (params.get(key) or [""])[0]
    try:
        value = int(raw)
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(maximum, value))


class NodeTracker:
    """In-memory record of which edge nodes have called /events recently."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._nodes: dict[str, dict[str, Any]] = {}

    def record(self, node_id: str, accepted: int, inserted: int, skipped: int) -> None:
        now = datetime.now(timezone.utc).isoformat()
        monotonic_now = time.monotonic()
        with self._lock:
            self._prune(monotonic_now)
            entry = self._nodes.setdefault(
                node_id,
                {"first_seen": now, "total_accepted": 0, "total_inserted": 0, "total_skipped": 0},
            )
            entry["last_seen"] = now
            entry["last_accepted"] = accepted
            entry["last_inserted"] = inserted
            entry["last_skipped"] = skipped
            entry["total_accepted"] += accepted
            entry["total_inserted"] += inserted
            entry["total_skipped"] += skipped
            entry["_last_seen_monotonic"] = monotonic_now
            self._prune(monotonic_now)

    def record_unauthorized(self, node_id: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        monotonic_now = time.monotonic()
        with self._lock:
            self._prune(monotonic_now)
            entry = self._nodes.setdefault(node_id or "(unknown)", {"first_seen": now})
            entry["last_unauthorized_at"] = now
            entry["_last_seen_monotonic"] = monotonic_now
            self._prune(monotonic_now)

    def snapshot(self) -> list[dict[str, Any]]:
        with self._lock:
            self._prune(time.monotonic())
            return [
                {"node_id": key, **{k: value for k, value in entry.items() if not k.startswith("_")}}
                for key, entry in self._nodes.items()
            ]

    def _prune(self, now: float) -> None:
        expired = [
            key for key, entry in self._nodes.items()
            if now - float(entry.get("_last_seen_monotonic", now)) > settings.NODE_TRACKER_TTL_SECONDS
        ]
        for key in expired:
            self._nodes.pop(key, None)
        while len(self._nodes) > settings.NODE_TRACKER_MAX_ENTRIES:
            oldest = min(
                self._nodes,
                key=lambda key: float(self._nodes[key].get("_last_seen_monotonic", 0.0)),
            )
            self._nodes.pop(oldest, None)


_node_tracker = NodeTracker()


def _setup_logging() -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if settings.LOG_FILE:
        handlers.append(logging.FileHandler(settings.LOG_FILE, encoding="utf-8"))

    logging.basicConfig(
        level=getattr(logging, settings.LOG_LEVEL, logging.INFO),
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers,
    )


def _handle_signal(signum, _frame) -> None:
    global _running
    logger.info("Received signal %d; shutting down gracefully.", signum)
    _running = False
    _wake_event.set()


def _json_response(handler: BaseHTTPRequestHandler, status: int, payload: Any) -> None:
    body = json.dumps(payload, default=str).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def _html_response(handler: BaseHTTPRequestHandler, status: int, body: str) -> None:
    encoded = body.encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "text/html; charset=utf-8")
    handler.send_header("Content-Length", str(len(encoded)))
    handler.end_headers()
    handler.wfile.write(encoded)


def _namespaced_event(source_node: str, event: dict[str, Any]) -> dict[str, Any]:
    """Avoid serialNo collisions between independent edge PCs/devices."""
    normalized = dict(event)
    serial_no = str(normalized.get("serialNo", "")).strip()
    if serial_no and not serial_no.startswith(f"{source_node}:"):
        normalized["serialNo"] = f"{source_node}:{serial_no}"
    return normalized


def _parse_node_keys(raw: str) -> list[dict[str, str]]:
    nodes: list[dict[str, str]] = []
    for entry in raw.split(","):
        if ":" not in entry:
            continue
        node_id, secret = entry.split(":", 1)
        node_id = node_id.strip()
        secret = secret.strip()
        if node_id:
            nodes.append({"node_id": node_id, "secret": secret})
    return nodes


def _load_config_view() -> dict[str, Any]:
    """Return editable non-secret values and secret configured booleans."""
    env = read_env(_ENV_PATH)
    values: dict[str, Any] = {}
    for key in _EDITABLE_KEYS:
        raw = env.get(key, "")
        values[key] = {"set": bool(raw), "value": raw}
    return {
        "env_path": str(_ENV_PATH),
        "values": values,
        "configured": {
            key: bool(env.get(key, "") or getattr(settings, key, ""))
            for key in sorted(_PROTECTED_CONFIG_KEYS)
            if key != "EMPLOYEE_MAP_RESTART_COMMAND"
        },
    }


def _save_config(body: dict[str, Any]) -> list[str]:
    """
    Persist user-supplied config changes to the .env file.

    Body shape:
      {
        "values": {KEY: "new value", ...},
        "nodes":  [{node_id, secret}, ...]      # full replacement of SERVER_NODE_KEYS
      }
    Secret values and node credentials are deliberately not writable here.
    """
    incoming_values = body.get("values") or {}
    if not isinstance(incoming_values, dict):
        raise ValueError("values must be an object")

    updates: dict[str, str] = {}

    for key, new_value in incoming_values.items():
        if key in _PROTECTED_CONFIG_KEYS:
            raise ValueError(f"{key} cannot be changed through the dashboard")
        if key not in _EDITABLE_KEYS:
            continue
        new_value = "" if new_value is None else str(new_value)
        updates[key] = new_value

    if "nodes" in body:
        raise ValueError("SERVER_NODE_KEYS cannot be changed through the dashboard")

    update_env(_ENV_PATH, updates)
    return list(updates.keys())


def _load_employee_map_view() -> dict[str, Any]:
    employee_map = settings.load_employee_map()
    return {
        "path": str(settings.employee_map_path()),
        "count": len(employee_map),
        "entries": [
            {"device_employee_no": key, "frappe_employee_id": value}
            for key, value in sorted(employee_map.items(), key=lambda item: item[0].lower())
        ],
    }


def _save_employee_map(body: dict[str, Any]) -> int:
    entries = body.get("entries")
    if not isinstance(entries, list):
        raise ValueError("entries must be a list")

    employee_map: dict[str, str] = {}
    for index, entry in enumerate(entries, start=1):
        if not isinstance(entry, dict):
            raise ValueError(f"entry #{index} must be an object")
        device_employee_no = str(entry.get("device_employee_no", "")).strip()
        frappe_employee_id = str(entry.get("frappe_employee_id", "")).strip()
        if not device_employee_no and not frappe_employee_id:
            continue
        if not device_employee_no:
            raise ValueError(f"entry #{index} is missing device_employee_no")
        if not frappe_employee_id:
            raise ValueError(f"entry #{index} is missing frappe_employee_id")
        if device_employee_no in employee_map:
            raise ValueError(f"duplicate device_employee_no: {device_employee_no}")
        employee_map[device_employee_no] = frappe_employee_id

    path = settings.employee_map_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    content = json.dumps(employee_map, indent=2, sort_keys=True) + "\n"
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as fh:
        fh.write(content)
    try:
        tmp_path.replace(path)
    except OSError as exc:
        if exc.errno != errno.EBUSY:
            raise
        logger.warning(
            "Could not atomically replace employee map at %s; falling back to in-place write.",
            path,
        )
        with path.open("w", encoding="utf-8") as fh:
            fh.write(content)
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass
    return len(employee_map)


def _execute_employee_map_restart_command(command: list[str]) -> None:
    time.sleep(1.0)
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("Employee map restart command failed to start: %s", exc)
        return

    if completed.returncode == 0:
        logger.info("Employee map restart command completed successfully.")
        return

    logger.error(
        "Employee map restart command failed: returncode=%s stdout=%s stderr=%s",
        completed.returncode,
        completed.stdout[-500:],
        completed.stderr[-500:],
    )


def _schedule_employee_map_restart_command() -> dict[str, Any]:
    command = settings.EMPLOYEE_MAP_RESTART_COMMAND
    if not command:
        return {
            "attempted": False,
            "ok": False,
            "message": "EMPLOYEE_MAP_RESTART_COMMAND is not configured.",
        }
    threading.Thread(
        target=_execute_employee_map_restart_command,
        args=(command,),
        daemon=True,
    ).start()
    return {
        "attempted": True,
        "ok": True,
        "command": command,
        "scheduled": True,
        "message": "Restart command scheduled.",
    }


def _filter_attendance_overview(
    rows: list[dict[str, Any]],
    search: str = "",
    date_from: str = "",
    date_to: str = "",
) -> list[dict[str, Any]]:
    search_text = search.lower()
    filtered: list[dict[str, Any]] = []
    for row in rows:
        date = str(row.get("date") or "")
        if date_from and date < date_from:
            continue
        if date_to and date > date_to:
            continue
        if search_text:
            haystack = " ".join(
                str(part)
                for part in [
                    row.get("employee"),
                    row.get("date"),
                    row.get("first_time"),
                    row.get("last_time"),
                    *(row.get("source_nodes") or []),
                    *(row.get("devices") or []),
                ]
            ).lower()
            if search_text not in haystack:
                continue
        filtered.append(row)
    return filtered


def _time_to_minutes(value: str) -> int | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        if "T" in raw:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return parsed.hour * 60 + parsed.minute
        time_part = raw.split(" ", 1)[-1]
        hour, minute, *_ = time_part.split(":")
        return int(hour) * 60 + int(minute)
    except (TypeError, ValueError):
        return None


def _late_threshold_minutes(value: str) -> tuple[str, int]:
    raw = str(value or settings.LATE_AFTER_TIME or "09:30").strip()
    try:
        hour, minute, *_ = raw.split(":")
        threshold = int(hour) * 60 + int(minute)
        if not 0 <= threshold < 24 * 60:
            raise ValueError
        return f"{int(hour):02d}:{int(minute):02d}", threshold
    except (TypeError, ValueError):
        return "09:30", 9 * 60 + 30


def _parse_schedule_time(value: str) -> datetime_time:
    raw = str(value or "22:00").strip()
    try:
        hour, minute, *_ = raw.split(":")
        parsed = datetime_time(hour=int(hour), minute=int(minute))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"FRAPPE_AUTO_PUSH_TIME must be HH:MM, got {raw!r}") from exc
    return parsed


def _auto_push_due(now: datetime, scheduled_time: datetime_time, last_run_date: str | None) -> bool:
    today = now.date().isoformat()
    return last_run_date != today and now.time() >= scheduled_time


def _parse_utc_datetime(value: str | None) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _format_duration(seconds: float) -> str:
    total_minutes = max(0, int(seconds // 60))
    hours, minutes = divmod(total_minutes, 60)
    if hours and minutes:
        return f"{hours}h {minutes}m"
    if hours:
        return f"{hours}h"
    return f"{minutes}m"


def _slack_enabled() -> bool:
    return bool(settings.SLACK_WEBHOOK_URL.strip())


def _send_slack_message(text: str, blocks: list[dict[str, Any]] | None = None) -> bool:
    if not _slack_enabled():
        logger.warning("Slack webhook is not configured; uptime alert not sent: %s", text)
        return False

    payload: dict[str, Any] = {"text": text}
    if blocks:
        payload["blocks"] = blocks
    body = json.dumps(payload).encode("utf-8")
    req = urlrequest.Request(
        settings.SLACK_WEBHOOK_URL,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlrequest.urlopen(req, timeout=10) as resp:
            if 200 <= resp.status < 300:
                return True
            logger.warning("Slack webhook returned HTTP %s for uptime alert.", resp.status)
    except (urlerror.URLError, TimeoutError, OSError) as exc:
        logger.warning("Could not send Slack uptime alert: %s", exc)
    return False


def _slack_uptime_blocks(title: str, fields: dict[str, str]) -> list[dict[str, Any]]:
    return [
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*{title}*"}},
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*{key}*\n{value}"}
                for key, value in fields.items()
            ],
        },
    ]


def _check_node_uptime(
    store: Any,
    alert_state: dict[str, bool],
    monitor_started_at: datetime,
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    threshold_seconds = max(60.0, settings.NODE_UPTIME_THRESHOLD_HOURS * 3600.0)
    latest_by_node = store.latest_inbound_by_node()
    results: list[dict[str, Any]] = []

    for node_id in sorted(settings.SERVER_NODE_KEYS.keys()):
        latest = latest_by_node.get(node_id, {})
        last_received_at = _parse_utc_datetime(latest.get("last_received_at"))
        comparison_time = last_received_at or monitor_started_at
        silent_for = (now - comparison_time).total_seconds()
        is_down = silent_for >= threshold_seconds
        was_alerted = alert_state.get(node_id, False)

        status = {
            "node_id": node_id,
            "ok": not is_down,
            "last_received_at": latest.get("last_received_at"),
            "event_count": latest.get("event_count", 0),
            "silent_for_seconds": int(silent_for),
            "alert_sent": was_alerted,
        }
        results.append(status)

        if is_down and not was_alerted:
            last_seen = last_received_at.isoformat() if last_received_at else "No punches received since monitor start"
            text = f"Attendance edge node {node_id} may be down: no punch records for {_format_duration(silent_for)}."
            sent = _send_slack_message(
                text,
                _slack_uptime_blocks(
                    "Attendance edge node may be down",
                    {
                        "Node": node_id,
                        "Silent for": _format_duration(silent_for),
                        "Threshold": _format_duration(threshold_seconds),
                        "Last punch received": last_seen,
                    },
                ),
            )
            alert_state[node_id] = sent or not _slack_enabled()
            status["alert_sent"] = alert_state[node_id]
            status["slack_sent"] = sent
            logger.warning(text)
            continue

        if not is_down and was_alerted:
            alert_state[node_id] = False
            status["alert_sent"] = False
            if settings.NODE_UPTIME_NOTIFY_ON_RECOVERY:
                last_seen = last_received_at.isoformat() if last_received_at else "unknown"
                text = f"Attendance edge node {node_id} is sending punch records again."
                status["slack_recovery_sent"] = _send_slack_message(
                    text,
                    _slack_uptime_blocks(
                        "Attendance edge node recovered",
                        {
                            "Node": node_id,
                            "Last punch received": last_seen,
                            "Previous silent time": _format_duration(silent_for),
                        },
                    ),
                )
                logger.info(text)

    return {
        "enabled": settings.NODE_UPTIME_MONITOR_ENABLED,
        "threshold_hours": settings.NODE_UPTIME_THRESHOLD_HOURS,
        "checked_at": now.isoformat(),
        "nodes": results,
    }


def _schedule_timezone() -> ZoneInfo | None:
    name = str(settings.FRAPPE_AUTO_PUSH_TIMEZONE or "").strip()
    if not name:
        return None
    try:
        return ZoneInfo(name)
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"FRAPPE_AUTO_PUSH_TIMEZONE is invalid: {name!r}") from exc


def _employee_display_name(details: dict[str, Any]) -> str:
    if details.get("employee_name"):
        return str(details["employee_name"])
    parts = [details.get("first_name"), details.get("middle_name"), details.get("last_name")]
    return " ".join(str(part).strip() for part in parts if str(part or "").strip())


def _load_frappe_employee_details(
    frappe: FrappeClient,
    employee_ids: list[str],
    refresh: bool = False,
) -> tuple[dict[str, dict[str, Any]], str | None]:
    global _employee_details_cache_expires_at
    now = time.time()
    unique_ids = sorted({employee_id for employee_id in employee_ids if employee_id})
    with _employee_details_lock:
        missing = [
            employee_id for employee_id in unique_ids
            if refresh or now >= _employee_details_cache_expires_at or employee_id not in _employee_details_cache
        ]

    error = None
    if missing:
        try:
            fetched = frappe.get_employees(missing)
            with _employee_details_lock:
                _employee_details_cache.update(fetched)
                for employee_id in missing:
                    _employee_details_cache.setdefault(employee_id, {})
                while len(_employee_details_cache) > settings.EMPLOYEE_CACHE_MAX_ENTRIES:
                    _employee_details_cache.pop(next(iter(_employee_details_cache)))
                _employee_details_cache_expires_at = now + settings.EMPLOYEE_CACHE_TTL_SECONDS
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not load Frappe employee details: %s", exc)
            error = str(exc)

    with _employee_details_lock:
        return {employee_id: dict(_employee_details_cache.get(employee_id, {})) for employee_id in unique_ids}, error


def _live_attendance_payload(
    store: Any,
    frappe: FrappeClient,
    *,
    feed_limit: int,
    refresh_frappe: bool,
) -> dict[str, Any]:
    today = live_calendar_today()
    events = store.live_attendance_source_events(date=today)
    employee_map = settings.load_employee_map()
    device_ids = {str(event.get("employee") or "").strip() for event in events}
    mapped_ids = [
        employee_map.get(device_id, "")
        for device_id in device_ids
        if employee_map.get(device_id, "")
    ]
    details_by_id, _frappe_error = _load_frappe_employee_details(
        frappe,
        mapped_ids,
        refresh=refresh_frappe,
    )
    return build_live_attendance(
        events,
        today=today,
        employee_map=employee_map,
        details_by_id=details_by_id,
        feed_limit=feed_limit,
        display_name=_employee_display_name,
    )


def _hr_verification_rows(
    store: Any,
    frappe: FrappeClient,
    search: str,
    date_from: str,
    date_to: str,
    late_after: str,
    refresh_frappe: bool,
    overview_rows: list[dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    employee_map = settings.load_employee_map()
    overview = overview_rows
    if overview is None:
        overview = _filter_attendance_overview(
            store.attendance_overview(limit=None, date_from=date_from or None, date_to=date_to or None),
            "",
            date_from,
            date_to,
        )
    mapped_ids = [employee_map.get(str(row.get("employee") or "").strip(), "") for row in overview]
    details_by_id, frappe_error = _load_frappe_employee_details(frappe, mapped_ids, refresh=refresh_frappe)
    late_label, late_minutes = _late_threshold_minutes(late_after)

    rows: list[dict[str, Any]] = []
    for row in overview:
        device_employee_no = str(row.get("employee") or "").strip()
        frappe_employee_id = employee_map.get(device_employee_no, "")
        details = details_by_id.get(frappe_employee_id, {})
        first_minutes = _time_to_minutes(str(row.get("first_time") or ""))
        late_by = None
        late_status = "missing_first_punch"
        if first_minutes is not None:
            late_by = max(0, first_minutes - late_minutes)
            late_status = "late" if late_by > 0 else "on_time"

        enriched = {
            "date": row.get("date"),
            "device_employee_no": device_employee_no,
            "frappe_employee_id": frappe_employee_id,
            "employee_name": _employee_display_name(details),
            "department": details.get("department") or "",
            "designation": details.get("designation") or "",
            "branch": details.get("branch") or "",
            "company": details.get("company") or "",
            "employee_status": details.get("status") or "",
            "default_shift": details.get("default_shift") or "",
            "first_time": row.get("first_time"),
            "first_result": row.get("first_result"),
            "last_time": row.get("last_time"),
            "last_result": row.get("last_result"),
            "punch_count": row.get("punch_count"),
            "source_nodes": row.get("source_nodes") or [],
            "devices": row.get("devices") or [],
            "late_after": late_label,
            "late_by_minutes": late_by,
            "late_status": late_status,
            "mapping_status": "mapped" if frappe_employee_id else "missing_map",
            "frappe_details_status": "loaded" if details else ("not_found" if frappe_employee_id else "not_mapped"),
        }
        rows.append(enriched)

    search_text = search.lower()
    if search_text:
        rows = [
            row for row in rows
            if search_text in " ".join(
                str(part)
                for part in [
                    row.get("date"),
                    row.get("device_employee_no"),
                    row.get("frappe_employee_id"),
                    row.get("employee_name"),
                    row.get("department"),
                    row.get("designation"),
                    row.get("branch"),
                    row.get("employee_status"),
                    row.get("late_status"),
                    *(row.get("source_nodes") or []),
                    *(row.get("devices") or []),
                ]
            ).lower()
        ]

    summary = {
        "late_after": late_label,
        "total": len(rows),
        "late": sum(1 for row in rows if row["late_status"] == "late"),
        "on_time": sum(1 for row in rows if row["late_status"] == "on_time"),
        "missing_map": sum(1 for row in rows if row["mapping_status"] == "missing_map"),
        "frappe_error": frappe_error,
    }
    return rows, summary


def _dashboard_summary_payload(
    store: Any,
    frappe: FrappeClient,
    date_from: str,
    date_to: str,
) -> dict[str, Any]:
    cache_key = (date_from, date_to)
    now = time.monotonic()
    with _dashboard_summary_lock:
        cached = _dashboard_summary_cache.get(cache_key)
        if cached and now - cached[0] < 30:
            return dict(cached[1])

    if hasattr(store, "dashboard_summary_aggregates"):
        aggregate = store.dashboard_summary_aggregates(
            date_from=date_from,
            date_to=date_to,
            late_after=settings.LATE_AFTER_TIME,
        )
        employee_map = settings.load_employee_map()
        mapped_ids = [
            employee_map.get(str(row.get("employee") or ""), "")
            for row in aggregate["employees"]
        ]
        details_by_id, _ = _load_frappe_employee_details(frappe, mapped_ids)
        departments: dict[str, dict[str, Any]] = {}
        missing_map = 0
        for row in aggregate["employees"]:
            device_id = str(row.get("employee") or "")
            frappe_id = employee_map.get(device_id, "")
            if not frappe_id:
                missing_map += int(row["total"])
            department = str(details_by_id.get(frappe_id, {}).get("department") or "Unassigned")
            item = departments.setdefault(
                department,
                {"department": department, "total": 0, "late": 0, "on_time": 0, "missing_map": 0},
            )
            for key in ("total", "late", "on_time"):
                item[key] += int(row[key])
            if not frappe_id:
                item["missing_map"] += int(row["total"])
        stats = dict(aggregate["stats"])
        stats["missing_map"] = missing_map
        payload = {
            "stats": stats,
            "trend": aggregate["trend"],
            "departments": sorted(departments.values(), key=lambda item: (-item["total"], item["department"])),
            "live": _live_attendance_payload(store, frappe, feed_limit=50, refresh_frappe=False),
            "as_of": datetime.now(timezone.utc).isoformat(),
        }
        with _dashboard_summary_lock:
            _dashboard_summary_cache.clear()
            _dashboard_summary_cache[cache_key] = (now, payload)
        return dict(payload)

    rows, summary = _hr_verification_rows(
        store,
        frappe,
        search="",
        date_from=date_from,
        date_to=date_to,
        late_after=settings.LATE_AFTER_TIME,
        refresh_frappe=False,
    )
    trend_by_date: dict[str, dict[str, Any]] = {}
    departments: dict[str, dict[str, Any]] = {}
    for row in rows:
        date = str(row.get("date") or "")
        department = str(row.get("department") or "Unassigned")
        for bucket, key in ((trend_by_date, date), (departments, department)):
            item = bucket.setdefault(
                key,
                {
                    "date" if bucket is trend_by_date else "department": key,
                    "total": 0,
                    "late": 0,
                    "on_time": 0,
                    "missing_map": 0,
                },
            )
            item["total"] += 1
            if row["late_status"] in {"late", "on_time"}:
                item[row["late_status"]] += 1
            if row["mapping_status"] == "missing_map":
                item["missing_map"] += 1
    payload = {
        "stats": {
            **{key: summary[key] for key in ("total", "late", "on_time", "missing_map")},
            "total_employees": len({
                str(row.get("frappe_employee_id") or row.get("device_employee_no") or "")
                for row in rows
                if row.get("frappe_employee_id") or row.get("device_employee_no")
            }),
        },
        "trend": sorted(trend_by_date.values(), key=lambda item: item["date"]),
        "departments": sorted(
            departments.values(), key=lambda item: (-item["total"], item["department"])
        ),
        "live": _live_attendance_payload(
            store, frappe, feed_limit=50, refresh_frappe=False
        ),
        "as_of": datetime.now(timezone.utc).isoformat(),
    }
    with _dashboard_summary_lock:
        _dashboard_summary_cache.clear()
        _dashboard_summary_cache[cache_key] = (now, payload)
    return dict(payload)


def process_pending_events(store: Any, processor: EventProcessor) -> dict[str, Any]:
    """Drain queued inbound events and run retry queue. Thread-safe."""
    with _push_lock:
        processed = 0
        results: dict[str, int] = {}
        candidate_db = sqlite3.connect("")
        candidate_db.execute("PRAGMA temp_store=FILE")
        candidate_db.execute("PRAGMA cache_size=-1024")
        candidate_db.execute(
            "CREATE TABLE candidates (group_key TEXT PRIMARY KEY, first_item BLOB NOT NULL, last_item BLOB)"
        )
        peak_in_memory_candidates = 0
        after_id: int | None = None
        while True:
            rows = store.get_pending_inbound_events(
                limit=settings.PENDING_PROCESS_BATCH_SIZE,
                after_id=after_id,
            )
            if not rows:
                break
            after_id = int(rows[-1]["id"])
            logger.info("Processing queued inbound batch of %d event(s).", len(rows))
            for row in rows:
                event = _namespaced_event(row["source_node"], row["payload"])
                result, prepared = processor.prepare_event(event)
                if result == "ready" and prepared is not None:
                    item = {"row": row, "prepared": prepared}
                    key = (prepared["hrms_id"], prepared["event_date"])
                    group_key = json.dumps(key, separators=(",", ":"))
                    stored = candidate_db.execute(
                        "SELECT first_item, last_item FROM candidates WHERE group_key = ?",
                        (group_key,),
                    ).fetchone()
                    candidates = [item]
                    if stored:
                        candidates.append(pickle.loads(stored[0]))
                        if stored[1] is not None:
                            candidates.append(pickle.loads(stored[1]))
                    peak_in_memory_candidates = max(peak_in_memory_candidates, len(candidates))
                    candidates.sort(key=lambda candidate: punch_sort_key(candidate["prepared"]))
                    kept = candidates if len(candidates) <= 2 else [candidates[0], candidates[-1]]
                    kept_ids = {candidate["row"]["id"] for candidate in kept}
                    for displaced in candidates:
                        displaced_row = displaced["row"]
                        if displaced_row["id"] in kept_ids:
                            continue
                        store.mark_inbound_processed(displaced_row["id"], "skipped_middle_punch")
                        results["skipped_middle_punch"] = results.get("skipped_middle_punch", 0) + 1
                        processed += 1
                    candidate_db.execute(
                        "INSERT OR REPLACE INTO candidates (group_key, first_item, last_item) VALUES (?, ?, ?)",
                        (
                            group_key,
                            pickle.dumps(kept[0], protocol=pickle.HIGHEST_PROTOCOL),
                            pickle.dumps(kept[1], protocol=pickle.HIGHEST_PROTOCOL) if len(kept) > 1 else None,
                        ),
                    )
                    continue

                store.mark_inbound_processed(row["id"], result)
                results[result] = results.get(result, 0) + 1
                processed += 1

            del rows

        candidate_db.commit()
        for first_blob, last_blob in candidate_db.execute(
            "SELECT first_item, last_item FROM candidates ORDER BY group_key"
        ):
            items = [pickle.loads(first_blob)]
            if last_blob is not None:
                items.append(pickle.loads(last_blob))
            for item, log_type, label in select_daily_punches(
                items, lambda candidate: candidate["prepared"]
            ):
                row = item["row"]
                result = processor.push_prepared_event(item["prepared"], log_type=log_type)
                result_key = f"{label}_{result}"
                store.mark_inbound_processed(row["id"], result_key)
                results[result_key] = results.get(result_key, 0) + 1
                processed += 1
        candidate_db.close()

        retries = processor.process_retries(force=True)
        return {
            "processed": processed,
            "retries": int(retries or 0),
            "results": results,
            "peak_in_memory_candidates": peak_in_memory_candidates,
        }


def run_push(store: Any, processor: EventProcessor, trigger: str) -> dict[str, Any]:
    """Run a push cycle and remember the latest outcome for the dashboard."""
    started_at = datetime.now(timezone.utc).isoformat()
    try:
        result = process_pending_events(store, processor)
        snapshot = {
            "started_at": started_at,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "processed": result["processed"],
            "retries": result["retries"],
            "results": result["results"],
            "error": None,
            "trigger": trigger,
        }
        with _last_push_lock:
            _last_push.update(snapshot)
        return {"ok": True, **result}
    except Exception as exc:
        snapshot = {
            "started_at": started_at,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "processed": 0,
            "retries": 0,
            "results": {},
            "error": str(exc),
            "trigger": trigger,
        }
        with _last_push_lock:
            _last_push.update(snapshot)
        raise


def latest_push_snapshot() -> dict[str, Any]:
    with _last_push_lock:
        return dict(_last_push)


def latest_uptime_monitor_snapshot() -> dict[str, Any]:
    with _uptime_monitor_lock:
        return dict(_uptime_monitor)


class EventIngestHandler(BaseHTTPRequestHandler):
    store: Any
    processor: EventProcessor

    def log_message(self, format: str, *args: Any) -> None:
        logger.debug("HTTP: " + format, *args)

    # ── routing ──────────────────────────────────────────────────────────────

    def do_GET(self) -> None:
        if not _request_slots.acquire(blocking=False):
            self._safe_json_response(503, {"ok": False, "error": "server_at_capacity"})
            return
        path = urlparse(self.path).path
        heavy_acquired = False
        try:
            if path in _HEAVY_GET_PATHS:
                heavy_acquired = _heavy_request_slots.acquire(blocking=False)
                if not heavy_acquired:
                    self._safe_json_response(503, {"ok": False, "error": "heavy_request_capacity"})
                    return
            self._dispatch_get()
        except Exception as exc:  # noqa: BLE001
            logger.exception("Unhandled GET %s failed", self.path)
            self._safe_json_response(500, {"ok": False, "error": str(exc)})
        finally:
            if heavy_acquired:
                _heavy_request_slots.release()
            self.store.close()
            _request_slots.release()

    def _dispatch_get(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)
        if path == "/health":
            _json_response(
                self, 200,
                {"ok": True, "pending_events": self.store.pending_inbound_count()},
            )
            return
        if path in ("/", "/dashboard"):
            self._serve_dashboard()
            return
        if path in ("/api/docs", "/docs", "/swagger"):
            self._serve_swagger()
            return
        if path == "/api/openapi.json":
            self._serve_openapi_json()
            return
        if path == "/api/status":
            self._serve_status()
            return
        if path == "/api/events":
            _json_response(self, 200, {"events": self.store.recent_inbound(100)})
            return
        if path == "/api/punch-records":
            page = _int_query(query, "page", 1, 1, 100000)
            page_size = _int_query(query, "page_size", 100, 10, 1000)
            search = (query.get("search") or [""])[0].strip()
            date_from = (query.get("from") or [""])[0].strip()
            date_to = (query.get("to") or [""])[0].strip()
            status = (query.get("status") or [""])[0].strip()
            _json_response(
                self,
                200,
                self.store.punch_records(
                    page=page,
                    page_size=page_size,
                    search=search,
                    date_from=date_from,
                    date_to=date_to,
                    status=status,
                ),
            )
            return
        if path == "/api/attendance-overview":
            page = _int_query(query, "page", 1, 1, 100000)
            page_size = _int_query(query, "page_size", 100, 10, 1000)
            search = (query.get("search") or [""])[0].strip()
            date_from = (query.get("from") or [""])[0].strip()
            date_to = (query.get("to") or [""])[0].strip()
            if not date_from and not date_to:
                date_to = live_calendar_today()
                date_from = (datetime.fromisoformat(date_to) - timedelta(days=_OVERVIEW_DEFAULT_DAYS)).isoformat()[:10]
            if hasattr(self.store, "attendance_overview_page"):
                payload = self.store.attendance_overview_page(
                    page=page,
                    page_size=page_size,
                    search=search,
                    date_from=date_from,
                    date_to=date_to,
                )
                _json_response(self, 200, payload)
                return
            all_rows = self.store.attendance_overview(limit=None, date_from=date_from or None, date_to=date_to or None)
            filtered_rows = _filter_attendance_overview(all_rows, search, date_from, date_to)
            total = len(filtered_rows)
            start = (page - 1) * page_size
            end = start + page_size
            _json_response(
                self,
                200,
                {
                    "overview": filtered_rows[start:end],
                    "page": page,
                    "page_size": page_size,
                    "total": total,
                    "has_next": end < total,
                    "has_prev": page > 1,
                },
            )
            return
        if path == "/api/hr-verification":
            page = _int_query(query, "page", 1, 1, 100000)
            page_size = _int_query(query, "page_size", 100, 10, 1000)
            search = (query.get("search") or [""])[0].strip()
            date_from = (query.get("from") or [""])[0].strip()
            date_to = (query.get("to") or [""])[0].strip()
            status = (query.get("status") or [""])[0].strip()
            department = (query.get("department") or [""])[0].strip()
            late_after = (query.get("late_after") or [settings.LATE_AFTER_TIME])[0].strip()
            refresh_frappe = (query.get("refresh_frappe") or [""])[0].lower() in {"1", "true", "yes"}
            if not date_from and not date_to:
                date_to = live_calendar_today()
                date_from = (datetime.fromisoformat(date_to) - timedelta(days=_OVERVIEW_DEFAULT_DAYS)).isoformat()[:10]
            if hasattr(self.store, "attendance_overview_page"):
                employee_map = settings.load_employee_map()
                all_details, _ = _load_frappe_employee_details(
                    self.frappe,
                    list(employee_map.values()),
                    refresh=refresh_frappe,
                )
                eligible_employee_ids: list[str] | None = None
                if department:
                    department_text = department.casefold()
                    eligible_employee_ids = [
                        device_id for device_id, frappe_id in employee_map.items()
                        if str(all_details.get(frappe_id, {}).get("department") or "").casefold() == department_text
                    ]
                search_employee_ids: list[str] = []
                if search:
                    search_text = search.casefold()
                    for device_id, frappe_id in employee_map.items():
                        details = all_details.get(frappe_id, {})
                        haystack = " ".join(str(value or "") for value in (
                            device_id, frappe_id, details.get("employee_name"),
                            details.get("department"), details.get("designation"),
                            details.get("branch"), details.get("status"),
                        )).casefold()
                        if search_text in haystack:
                            search_employee_ids.append(device_id)
                source_page = self.store.attendance_overview_page(
                    page=page,
                    page_size=page_size,
                    search=search,
                    date_from=date_from,
                    date_to=date_to,
                    status=status,
                    late_after=_late_threshold_minutes(late_after)[0],
                    eligible_employee_ids=eligible_employee_ids,
                    search_employee_ids=search_employee_ids,
                )
                rows, summary = _hr_verification_rows(
                    self.store,
                    self.frappe,
                    search="",
                    date_from=date_from,
                    date_to=date_to,
                    late_after=late_after,
                    refresh_frappe=refresh_frappe,
                    overview_rows=source_page["overview"],
                )
                if hasattr(self.store, "attendance_overview_summary"):
                    summary.update(self.store.attendance_overview_summary(
                        date_from=date_from,
                        date_to=date_to,
                        late_after=_late_threshold_minutes(late_after)[0],
                        mapped_employees=list(employee_map),
                    ))
                summary["total"] = source_page["total"]
                _json_response(self, 200, {
                    "rows": rows,
                    "summary": summary,
                    "page": page,
                    "page_size": page_size,
                    "total": source_page["total"],
                    "has_next": source_page["has_next"],
                    "has_prev": source_page["has_prev"],
                })
                return
            all_rows, summary = _hr_verification_rows(
                self.store,
                self.frappe,
                search=search,
                date_from=date_from,
                date_to=date_to,
                late_after=late_after,
                refresh_frappe=refresh_frappe,
            )
            normalized_status = status.lower()
            if normalized_status:
                accepted_statuses = {
                    "late": {"late"},
                    "present": {"on_time"},
                    "on_time": {"on_time"},
                    "absent": {"missing_first_punch"},
                    "missing_first_punch": {"missing_first_punch"},
                }.get(normalized_status, {normalized_status})
                all_rows = [
                    row for row in all_rows
                    if row.get("late_status") in accepted_statuses
                ]
            if department:
                all_rows = [
                    row for row in all_rows
                    if str(row.get("department") or "").casefold() == department.casefold()
                ]
            total = len(all_rows)
            start = (page - 1) * page_size
            end = start + page_size
            _json_response(
                self,
                200,
                {
                    "rows": all_rows[start:end],
                    "summary": summary,
                    "page": page,
                    "page_size": page_size,
                    "total": total,
                    "has_next": end < total,
                    "has_prev": page > 1,
                },
            )
            return
        if path == "/api/dashboard-summary":
            date_from = (query.get("from") or [""])[0].strip()
            date_to = (query.get("to") or [""])[0].strip()
            if not date_to:
                date_to = live_calendar_today()
            if not date_from:
                date_from = (datetime.fromisoformat(date_to) - timedelta(days=30)).isoformat()[:10]
            _json_response(
                self,
                200,
                _dashboard_summary_payload(self.store, self.frappe, date_from, date_to),
            )
            return
        if path == "/api/live-attendance":
            feed_limit = _int_query(query, "limit", 50, 1, 200)
            refresh_frappe = (query.get("refresh_frappe") or [""])[0].lower() in {"1", "true", "yes"}
            _json_response(
                self,
                200,
                _live_attendance_payload(
                    self.store,
                    self.frappe,
                    feed_limit=feed_limit,
                    refresh_frappe=refresh_frappe,
                ),
            )
            return
        if path == "/api/alerts":
            _json_response(self, 200, {"alerts": self.store.dashboard_alerts()})
            return
        if path == "/api/retries":
            _json_response(self, 200, {"retries": self.store.get_retry_queue(100)})
            return
        if path == "/api/processed":
            _json_response(self, 200, {"processed": self.store.recent_processed(100)})
            return
        if path == "/api/frappe-push-logs":
            page = _int_query(query, "page", 1, 1, 100000)
            page_size = _int_query(query, "page_size", 100, 10, 500)
            _json_response(
                self,
                200,
                self.store.frappe_push_logs(page=page, page_size=page_size),
            )
            return
        if path == "/api/config":
            _json_response(self, 200, _load_config_view())
            return
        if path == "/api/employee-map":
            _json_response(self, 200, _load_employee_map_view())
            return
        _json_response(self, 404, {"error": "not_found"})

    def do_POST(self) -> None:
        if not _request_slots.acquire(blocking=False):
            self._safe_json_response(503, {"ok": False, "error": "server_at_capacity"})
            return
        try:
            self._dispatch_post()
        except Exception as exc:  # noqa: BLE001
            logger.exception("Unhandled POST %s failed", self.path)
            self._safe_json_response(500, {"ok": False, "error": str(exc)})
        finally:
            self.store.close()
            _request_slots.release()

    def _dispatch_post(self) -> None:
        if self.path == "/events":
            self._handle_events_post()
            return
        if self.path == "/api/push":
            try:
                result = run_push(self.store, self.processor, trigger="manual")
            except Exception as exc:  # noqa: BLE001
                logger.exception("Manual push failed")
                _json_response(self, 500, {"ok": False, "error": str(exc)})
                return
            _json_response(self, 200, result)
            return
        if self.path == "/api/config":
            self._handle_config_post()
            return
        if self.path == "/api/employee-map":
            self._handle_employee_map_post()
            return
        _json_response(self, 404, {"error": "not_found"})

    def _safe_json_response(self, status: int, payload: Any) -> None:
        try:
            _json_response(self, status, payload)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            logger.debug("HTTP client disconnected before error response was sent")

    def _read_request_body(self) -> bytes | None:
        raw_length = self.headers.get("Content-Length")
        try:
            length = int(raw_length or "0")
        except ValueError:
            _json_response(self, 400, {"error": "invalid_content_length"})
            return None
        if length < 0:
            _json_response(self, 400, {"error": "invalid_content_length"})
            return None
        if length > settings.SERVER_MAX_REQUEST_BODY_BYTES:
            _json_response(self, 413, {
                "error": "request_body_too_large",
                "max_bytes": settings.SERVER_MAX_REQUEST_BODY_BYTES,
            })
            return None
        return self.rfile.read(length)

    def _handle_config_post(self) -> None:
        raw = self._read_request_body()
        if raw is None:
            return
        try:
            body = json.loads(raw.decode("utf-8")) if raw else {}
        except json.JSONDecodeError:
            _json_response(self, 400, {"error": "invalid_json"})
            return

        try:
            written = _save_config(body)
        except ValueError as exc:
            _json_response(self, 400, {"error": str(exc)})
            return
        except PermissionError as exc:
            _json_response(
                self, 500,
                {"error": f"cannot write {_ENV_PATH}: {exc}. "
                          "In Docker, ensure the host file is writable by uid 10001 "
                          "(e.g. `chown 10001:10001 .env.server` or `chmod 666 .env.server`)."},
            )
            return
        except OSError as exc:
            _json_response(self, 500, {"error": f"cannot write {_ENV_PATH}: {exc}"})
            return

        _json_response(
            self, 200,
            {
                "ok": True,
                "written_keys": sorted(written),
                "restart_required": True,
                "env_path": str(_ENV_PATH),
            },
        )

    def _handle_employee_map_post(self) -> None:
        raw = self._read_request_body()
        if raw is None:
            return
        try:
            body = json.loads(raw.decode("utf-8")) if raw else {}
        except json.JSONDecodeError:
            _json_response(self, 400, {"error": "invalid_json"})
            return

        try:
            count = _save_employee_map(body)
        except ValueError as exc:
            _json_response(self, 400, {"error": str(exc)})
            return
        except PermissionError as exc:
            _json_response(
                self,
                500,
                {
                    "error": f"cannot write {settings.employee_map_path()}: {exc}. "
                    "In Docker, mount employee_map.json read-write and make it writable "
                    "by uid 10001 (e.g. `chown 10001:10001 employee_map.json`)."
                },
            )
            return
        except OSError as exc:
            _json_response(
                self,
                500,
                {"error": f"cannot write {settings.employee_map_path()}: {exc}"},
            )
            return

        restart = _schedule_employee_map_restart_command()
        _json_response(
            self,
            200,
            {
                "ok": True,
                "count": count,
                "path": str(settings.employee_map_path()),
                "restart_required": not restart.get("ok", False),
                "restart": restart,
            },
        )

    # ── handlers ─────────────────────────────────────────────────────────────

    def _handle_events_post(self) -> None:
        body = self._read_request_body()
        if body is None:
            return

        node_id = self.headers.get(NODE_HEADER, "")
        timestamp = self.headers.get(TIMESTAMP_HEADER, "")
        signature = self.headers.get(SIGNATURE_HEADER, "")
        if not verify_auth_headers(
            node_id=node_id,
            timestamp=timestamp,
            signature=signature,
            body=body,
            allowed_secrets=settings.SERVER_NODE_KEYS,
        ):
            _node_tracker.record_unauthorized(node_id)
            _json_response(self, 401, {"error": "unauthorized"})
            return

        try:
            payload = json.loads(body.decode("utf-8"))
        except json.JSONDecodeError:
            _json_response(self, 400, {"error": "invalid_json"})
            return

        events = payload.get("events")
        if not isinstance(events, list):
            _json_response(self, 400, {"error": "events_must_be_list"})
            return
        if len(events) > settings.SERVER_MAX_EVENT_BATCH_SIZE:
            _json_response(self, 413, {
                "error": "event_batch_too_large",
                "max_events": settings.SERVER_MAX_EVENT_BATCH_SIZE,
            })
            return

        safe_events = [event for event in events if isinstance(event, dict)]
        inserted, skipped = self.store.enqueue_inbound_events(node_id, safe_events)
        _node_tracker.record(node_id, len(safe_events), inserted, skipped)
        logger.info(
            "Received %d event(s) from %s: inserted=%d skipped=%d",
            len(safe_events), node_id, inserted, skipped,
        )
        _json_response(
            self, 202,
            {"accepted": len(safe_events), "inserted": inserted, "skipped": skipped},
        )

    def _serve_dashboard(self) -> None:
        try:
            html = _DASHBOARD_HTML_PATH.read_text(encoding="utf-8")
        except FileNotFoundError:
            _html_response(self, 500, "<h1>dashboard.html missing</h1>")
            return
        _html_response(self, 200, html)

    def _serve_swagger(self) -> None:
        try:
            html = _SWAGGER_HTML_PATH.read_text(encoding="utf-8")
        except FileNotFoundError:
            _html_response(self, 500, "<h1>swagger.html missing</h1>")
            return
        _html_response(self, 200, html)

    def _serve_openapi_json(self) -> None:
        try:
            payload = json.loads(_OPENAPI_JSON_PATH.read_text(encoding="utf-8"))
        except FileNotFoundError:
            _json_response(self, 500, {"error": "openapi.json missing"})
            return
        except json.JSONDecodeError as exc:
            _json_response(self, 500, {"error": f"openapi.json invalid: {exc}"})
            return
        _json_response(self, 200, payload)

    def _serve_status(self) -> None:
        counts = self.store.inbound_counts()
        payload = {
            "now": datetime.now(timezone.utc).isoformat(),
            "process": {
                "pid": _os.getpid(),
                "rss_mb": process_rss_mb(),
            },
            "server": {
                "host": settings.SERVER_HOST,
                "port": settings.SERVER_PORT,
                "poll_interval": settings.POLL_INTERVAL,
                "storage_backend": settings.STORAGE_BACKEND,
                "hrms_url": settings.HRMS_URL,
                "late_after_time": settings.LATE_AFTER_TIME,
                "auto_push_enabled": settings.FRAPPE_AUTO_PUSH_ENABLED,
                "auto_push_time": settings.FRAPPE_AUTO_PUSH_TIME,
                "auto_push_timezone": settings.FRAPPE_AUTO_PUSH_TIMEZONE or "server local",
            },
            "counts": {
                "pending": counts.get("pending", 0),
                "done": counts.get("done", 0),
                "processed_total": self.store.processed_count(),
                "retry_queue": self.store.retry_queue_size(),
            },
            "last_push": latest_push_snapshot(),
            "uptime_monitor": latest_uptime_monitor_snapshot(),
            "configured_nodes": sorted(settings.SERVER_NODE_KEYS.keys()),
            "nodes": _node_tracker.snapshot(),
        }
        _json_response(self, 200, payload)


class QuietThreadingHTTPServer(ThreadingHTTPServer):
    """HTTP server that does not print tracebacks for client disconnects."""

    def handle_error(self, request: Any, client_address: Any) -> None:
        exc = sys.exc_info()[1]
        if isinstance(exc, (BrokenPipeError, ConnectionResetError, ConnectionAbortedError)):
            logger.debug("HTTP client disconnected early: %s", client_address)
            return
        super().handle_error(request, client_address)


def create_server(store: Any, processor: EventProcessor, frappe: FrappeClient) -> ThreadingHTTPServer:
    EventIngestHandler.store = store
    EventIngestHandler.processor = processor
    EventIngestHandler.frappe = frappe
    return QuietThreadingHTTPServer((settings.SERVER_HOST, settings.SERVER_PORT), EventIngestHandler)


def create_processor(store: Any) -> tuple[FrappeClient, EventProcessor]:
    if not settings.SERVER_NODE_KEYS:
        raise EnvironmentError("SERVER_NODE_KEYS must list at least one node_id:secret pair.")
    if not settings.HRMS_URL:
        raise EnvironmentError("HRMS_URL is required on the central server.")
    if not settings.HRMS_API_KEY:
        raise EnvironmentError("HRMS_API_KEY is required on the central server.")
    if not settings.HRMS_API_SECRET:
        raise EnvironmentError("HRMS_API_SECRET is required on the central server.")

    frappe = FrappeClient(
        base_url=settings.HRMS_URL,
        api_key=settings.HRMS_API_KEY,
        api_secret=settings.HRMS_API_SECRET,
    )
    employee_map = settings.load_employee_map()
    processor = EventProcessor(
        frappe_client=frappe,
        store=store,
        employee_map=employee_map,
        dedup_window=settings.DEDUP_WINDOW,
        retry_max_attempts=settings.RETRY_MAX_ATTEMPTS,
        retry_backoff_base=settings.RETRY_BACKOFF_BASE,
    )
    return frappe, processor


def main() -> None:
    global _running
    _setup_logging()
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    store = create_event_store()
    frappe, processor = create_processor(store)
    server = create_server(store, processor, frappe)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()

    auto_push_time = _parse_schedule_time(settings.FRAPPE_AUTO_PUSH_TIME)
    auto_push_tz = _schedule_timezone()
    last_auto_push_date: str | None = None
    uptime_alert_state: dict[str, bool] = {}
    uptime_monitor_started_at = datetime.now(timezone.utc)
    last_uptime_check_monotonic = 0.0
    last_memory_check_monotonic = 0.0
    logger.info(
        "Central sync server listening on %s:%d; dashboard at http://%s:%d/ | auto Frappe push=%s at %s (%s) | uptime monitor=%s threshold=%sh | rss=%dMB",
        settings.SERVER_HOST, settings.SERVER_PORT,
        settings.SERVER_HOST, settings.SERVER_PORT,
        "enabled" if settings.FRAPPE_AUTO_PUSH_ENABLED else "disabled",
        settings.FRAPPE_AUTO_PUSH_TIME,
        settings.FRAPPE_AUTO_PUSH_TIMEZONE or "server local time",
        "enabled" if settings.NODE_UPTIME_MONITOR_ENABLED else "disabled",
        settings.NODE_UPTIME_THRESHOLD_HOURS,
        process_rss_mb(),
    )

    try:
        while _running:
            if settings.FRAPPE_AUTO_PUSH_ENABLED:
                now = datetime.now(auto_push_tz) if auto_push_tz else datetime.now()
                if _auto_push_due(now, auto_push_time, last_auto_push_date):
                    last_auto_push_date = now.date().isoformat()
                    logger.info("Running scheduled nightly Frappe push for %s.", last_auto_push_date)
                    run_push(store, processor, trigger="scheduled-nightly")
            if settings.NODE_UPTIME_MONITOR_ENABLED:
                now_monotonic = time.monotonic()
                if now_monotonic - last_uptime_check_monotonic >= max(60, settings.NODE_UPTIME_CHECK_INTERVAL_SECONDS):
                    last_uptime_check_monotonic = now_monotonic
                    snapshot = _check_node_uptime(store, uptime_alert_state, uptime_monitor_started_at)
                    with _uptime_monitor_lock:
                        _uptime_monitor.update(snapshot)
            now_monotonic = time.monotonic()
            if now_monotonic - last_memory_check_monotonic >= 60:
                last_memory_check_monotonic = now_monotonic
                rss_mb = process_rss_mb()
                if rss_mb >= _MEMORY_RESTART_MB and _MEMORY_RESTART_ENABLED:
                    logger.critical(
                        "Memory at %d MB (>= restart threshold %d MB); restarting cleanly. "
                        "Pending events will be re-processed after restart.",
                        rss_mb, _MEMORY_RESTART_MB,
                    )
                    _running = False
                    _wake_event.set()
                elif rss_mb >= _MEMORY_WARN_MB:
                    logger.warning(
                        "Memory at %d MB (>= warn threshold %d MB). Consider restarting the service "
                        "or enabling AUTO_RESTART_ON_HIGH_MEMORY=1.",
                        rss_mb, _MEMORY_WARN_MB,
                    )
            _wake_event.wait(timeout=30.0 if settings.FRAPPE_AUTO_PUSH_ENABLED else 1.0)
            _wake_event.clear()
    finally:
        logger.info("Stopping central sync server.")
        server.shutdown()
        server.server_close()
        frappe.close()
        store.close()


if __name__ == "__main__":
    main()
