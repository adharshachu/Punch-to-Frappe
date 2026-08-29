"""
Central configuration loaded from environment variables (.env file).
All values have sensible defaults; required values raise on missing.
"""
import json
import os
import shlex
from pathlib import Path

from dotenv import load_dotenv

# Load .env from the project root (two levels up from this file)
_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(_ROOT / ".env")


def _require(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise EnvironmentError(f"Required environment variable '{name}' is not set.")
    return value


def _root_relative_path(value: str) -> Path:
    """Resolve relative env paths from the project root, not process cwd."""
    path = Path(value)
    if path.is_absolute():
        return path
    return _ROOT / path


# ── Hikvision devices ────────────────────────────────────────────────────────
# List of IPs or "ip:user:pass" strings
_DEVICES_RAW = os.getenv("DEVICES", "").split(",")
DEVICE_CONFIGS: list[dict[str, str]] = []

for entry in _DEVICES_RAW:
    parts = [p.strip() for p in entry.split(":") if p.strip()]
    if not parts:
        continue
    
    config = {"ip": parts[0]}
    if len(parts) >= 3:
        config["user"] = parts[1]
        config["pass"] = parts[2]
    else:
        # Fallback to global credentials
        config["user"] = os.getenv("DEVICE_USER", "")
        config["pass"] = os.getenv("DEVICE_PASS", "")
    
    DEVICE_CONFIGS.append(config)

DEVICE_IPS = [c["ip"] for c in DEVICE_CONFIGS]
DEVICE_USER: str = os.getenv("DEVICE_USER", "")
DEVICE_PASS: str = os.getenv("DEVICE_PASS", "")
HIKVISION_USE_HTTPS: bool = os.getenv("HIKVISION_USE_HTTPS", "true").lower() == "true"
HIKVISION_VERIFY_SSL: bool = os.getenv("HIKVISION_VERIFY_SSL", "false").lower() == "true"

# ── Device Friendly Names ────────────────────────────────────────────────────
# Map IP addresses to friendly names (e.g. 10.10.10.131:BIOMETRIC-01)
_DEVICE_NAMES_RAW = os.getenv("DEVICE_NAMES", "")
DEVICE_NAMES: dict[str, str] = {}
for entry in _DEVICE_NAMES_RAW.split(","):
    if ":" in entry:
        ip, name = entry.split(":", 1)
        DEVICE_NAMES[ip.strip()] = name.strip()

# ── Polling ──────────────────────────────────────────────────────────────────
POLL_INTERVAL: int = int(os.getenv("POLL_INTERVAL", "600"))         # seconds (10 mins)
DEDUP_WINDOW: int = int(os.getenv("DEDUP_WINDOW", "30"))            # seconds
FIRST_RUN_LOOKBACK_HOURS: int = int(os.getenv("FIRST_RUN_LOOKBACK_HOURS", "24"))

# ── Hikvision event filter ────────────────────────────────────────────────────
EVENT_MAJOR: int = int(os.getenv("EVENT_MAJOR", "5"))


def _parse_event_minors() -> list[int]:
    raw = os.getenv("EVENT_MINORS")
    if raw is None:
        raw = os.getenv("EVENT_MINOR", "75,38")
    minors: list[int] = []
    for part in raw.split(","):
        value = part.strip()
        if not value:
            continue
        minors.append(int(value, 0))
    return minors


EVENT_MINORS: list[int] = _parse_event_minors()
EVENT_MINOR: int = EVENT_MINORS[0] if EVENT_MINORS else 75

# ── Frappe HRMS ───────────────────────────────────────────────────────────────
HRMS_URL: str = os.getenv("HRMS_URL", "").rstrip("/")
HRMS_API_KEY: str = os.getenv("HRMS_API_KEY", "")
HRMS_API_SECRET: str = os.getenv("HRMS_API_SECRET", "")

# ── Default Check-in Metadata ────────────────────────────────────────────────
DEFAULT_LOG_TYPE: str = os.getenv("DEFAULT_LOG_TYPE", "IN")
LATE_AFTER_TIME: str = os.getenv("LATE_AFTER_TIME", "09:30")

# ── Central server Frappe push schedule ──────────────────────────────────────
FRAPPE_AUTO_PUSH_ENABLED: bool = os.getenv("FRAPPE_AUTO_PUSH_ENABLED", "false").lower() in {
    "1",
    "true",
    "yes",
    "on",
}
FRAPPE_AUTO_PUSH_TIME: str = os.getenv("FRAPPE_AUTO_PUSH_TIME", "22:00")
FRAPPE_AUTO_PUSH_TIMEZONE: str = os.getenv("FRAPPE_AUTO_PUSH_TIMEZONE", "")

# ── Edge node uptime monitor ─────────────────────────────────────────────────
NODE_UPTIME_MONITOR_ENABLED: bool = os.getenv("NODE_UPTIME_MONITOR_ENABLED", "false").lower() in {
    "1",
    "true",
    "yes",
    "on",
}
NODE_UPTIME_THRESHOLD_HOURS: float = float(os.getenv("NODE_UPTIME_THRESHOLD_HOURS", "8"))
NODE_UPTIME_CHECK_INTERVAL_SECONDS: int = int(os.getenv("NODE_UPTIME_CHECK_INTERVAL_SECONDS", "1200"))
NODE_UPTIME_NOTIFY_ON_RECOVERY: bool = os.getenv("NODE_UPTIME_NOTIFY_ON_RECOVERY", "true").lower() in {
    "1",
    "true",
    "yes",
    "on",
}
SLACK_WEBHOOK_URL: str = os.getenv("SLACK_WEBHOOK_URL", "")

# Optional command run after dashboard Employee Map save. Leave empty by default;
# enabling this usually requires Docker CLI/socket access inside the container.
EMPLOYEE_MAP_RESTART_COMMAND: list[str] = shlex.split(
    os.getenv("EMPLOYEE_MAP_RESTART_COMMAND", "")
)


def _optional_float(name: str, minimum: float, maximum: float) -> float | None:
    raw = os.getenv(name, "").strip()
    if not raw:
        return None
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a decimal number, got {raw!r}") from exc
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}, got {raw!r}")
    return value


LATITUDE: float | None = _optional_float("LATITUDE", -90.0, 90.0)
LONGITUDE: float | None = _optional_float("LONGITUDE", -180.0, 180.0)

# ── Employee map ──────────────────────────────────────────────────────────────
_MAP_PATH = _root_relative_path(os.getenv("EMPLOYEE_MAP", "employee_map.json"))

def employee_map_path() -> Path:
    return _MAP_PATH


def load_employee_map() -> dict[str, str]:
    if not _MAP_PATH.exists():
        raise FileNotFoundError(f"employee_map.json not found at {_MAP_PATH}")
    with _MAP_PATH.open() as fh:
        return json.load(fh)

# ── Storage ───────────────────────────────────────────────────────────────────
STORAGE_BACKEND: str = os.getenv("STORAGE_BACKEND", "sqlite").lower()
STORE_PATH: Path = _root_relative_path(os.getenv("STORE_PATH", "data/events.db"))
STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
POSTGRES_DSN: str = os.getenv("POSTGRES_DSN", "")
POSTGRES_MAX_CONNECTIONS: int = max(1, int(os.getenv("POSTGRES_MAX_CONNECTIONS", "4")))

# ── Retry queue ───────────────────────────────────────────────────────────────
RETRY_MAX_ATTEMPTS: int = int(os.getenv("RETRY_MAX_ATTEMPTS", "5"))
RETRY_BACKOFF_BASE: float = float(os.getenv("RETRY_BACKOFF_BASE", "2.0"))  # seconds

# ── Edge-to-server sync ───────────────────────────────────────────────────────
# Central server bind settings.
SERVER_HOST: str = os.getenv("SERVER_HOST", "0.0.0.0")
SERVER_PORT: int = int(os.getenv("SERVER_PORT", "8080"))
SERVER_MAX_CONCURRENT_REQUESTS: int = max(
    1, int(os.getenv("SERVER_MAX_CONCURRENT_REQUESTS", "4"))
)
SERVER_MAX_HEAVY_REQUESTS: int = max(
    1, min(
        SERVER_MAX_CONCURRENT_REQUESTS,
        int(os.getenv("SERVER_MAX_HEAVY_REQUESTS", "2")),
    )
)
SERVER_MAX_REQUEST_BODY_BYTES: int = max(
    1024, int(os.getenv("SERVER_MAX_REQUEST_BODY_BYTES", str(2 * 1024 * 1024)))
)
SERVER_MAX_EVENT_BATCH_SIZE: int = max(
    1, int(os.getenv("SERVER_MAX_EVENT_BATCH_SIZE", "500"))
)
PENDING_PROCESS_BATCH_SIZE: int = max(
    1, int(os.getenv("PENDING_PROCESS_BATCH_SIZE", "250"))
)
EMPLOYEE_CACHE_TTL_SECONDS: int = max(
    1, int(os.getenv("EMPLOYEE_CACHE_TTL_SECONDS", "300"))
)
EMPLOYEE_CACHE_MAX_ENTRIES: int = max(
    1, int(os.getenv("EMPLOYEE_CACHE_MAX_ENTRIES", "1000"))
)
NODE_TRACKER_TTL_SECONDS: int = max(
    60, int(os.getenv("NODE_TRACKER_TTL_SECONDS", "86400"))
)
NODE_TRACKER_MAX_ENTRIES: int = max(
    1, int(os.getenv("NODE_TRACKER_MAX_ENTRIES", "500"))
)

# Comma-separated node_id:shared_secret values accepted by the central server.
# Example: SERVER_NODE_KEYS=pc-a:longSecretA,pc-b:longSecretB
_SERVER_NODE_KEYS_RAW = os.getenv("SERVER_NODE_KEYS", "")
SERVER_NODE_KEYS: dict[str, str] = {}
for entry in _SERVER_NODE_KEYS_RAW.split(","):
    if ":" in entry:
        node_id, secret = entry.split(":", 1)
        SERVER_NODE_KEYS[node_id.strip()] = secret.strip()

# Edge agent settings. Each PC uses its own node id and secret.
SYNC_SERVER_URL: str = os.getenv("SYNC_SERVER_URL", "").rstrip("/")
EDGE_NODE_ID: str = os.getenv("EDGE_NODE_ID", "")
EDGE_NODE_SECRET: str = os.getenv("EDGE_NODE_SECRET", "")
EDGE_BATCH_SIZE: int = int(os.getenv("EDGE_BATCH_SIZE", "200"))
EDGE_REQUEST_TIMEOUT: int = int(os.getenv("EDGE_REQUEST_TIMEOUT", "20"))

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()
_LOG_FILE_RAW = os.getenv("LOG_FILE")
LOG_FILE: Path | None = _root_relative_path(_LOG_FILE_RAW) if _LOG_FILE_RAW else None
if LOG_FILE:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
