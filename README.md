# Punch-to-Frappe

Punch-to-Frappe reads punch events from Hikvision biometric/access-control devices and sends them to Frappe HRMS as `Employee Checkin` records.

It currently supports three workflows:

- Continuous sync service: keeps polling devices and pushes only the first and last punch for each employee/date.
- Manual date-range sync: backfills a specific period into Frappe using the same first/last rule.
- CSV export: downloads punch records from devices into a CSV for checking, mapping, or audit.
- Distributed edge/server sync: PC A and PC B send signed punch batches to a central server, and only the server pushes to Frappe.

## How It Works

1. The script loads device, Frappe, and mapping settings from `.env`.
2. For each configured Hikvision device, it calls:

   ```text
   /ISAPI/AccessControl/AcsEvent?format=json
   ```

3. The device returns punch events containing values like employee number, employee name, punch time, serial number, and source device.
4. The employee number from the device is matched against `employee_map.json`.
5. In distributed server mode, the central server keeps all received raw punches for audit, but only pushes each employee's first punch as `IN` and last punch as `OUT` for each date to Frappe HRMS. The device only provides punch times; `IN`/`OUT` is derived by this server rule.
6. Successfully processed event serial numbers are stored in SQLite so the same event is not pushed again.
7. Temporary Frappe/API failures are saved into a retry queue and retried later.

## Project Layout

```text
attendance_sync/
  main.py                       Continuous service and shared sync logic
  server.py                     Central HTTP server for PC A / PC B uploads
  edge_agent.py                 Edge PC agent that polls local devices and uploads batches
  manual_sync.py                One-off date range sync into Frappe
  config/settings.py            .env loading and runtime settings
  devices/hikvision_client.py   Hikvision ISAPI client
  hrms/frappe_client.py         Frappe Employee Checkin API client
  processors/event_processor.py Employee mapping, duplicate checks, retries
  storage/event_store.py        SQLite store for processed events and retries
  transport/security.py         HMAC request signing helpers

export_punch_records.py         Export device punches to CSV
employee_map.json               Device employee number to Frappe employee ID map
.env.example                    Example configuration
service_setup.md                Windows service setup notes
data/                           CSV exports and local SQLite database
```

## Requirements

- Python 3.10 or newer is recommended.
- Network access from this machine to the Hikvision devices.
- A Frappe HRMS API key/secret with permission to create `Employee Checkin` records.

Install Python packages:

```powershell
pip install -r requirements.txt
```

Dependencies are:

- `requests`
- `python-dotenv`

## First-Time Setup

1. Create your environment file:

   ```powershell
   Copy-Item .env.example .env
   ```

2. Edit `.env` and fill in:

   ```env
   DEVICES=10.10.10.166,10.10.10.128
   DEVICE_USER=admin
   DEVICE_PASS=your_device_password

   HRMS_URL=https://your-frappe-site.example.com
   HRMS_API_KEY=your_api_key
   HRMS_API_SECRET=your_api_secret
   ```

3. Confirm `employee_map.json` maps device employee numbers to Frappe employee IDs.

4. Run a small CSV export first to confirm the devices are reachable:

   ```powershell
   python export_punch_records.py --start 2026-05-01 --end 2026-05-01 --output data\test_punch_records.csv
   ```

5. After the CSV looks correct, run a manual sync for a small period:

   ```powershell
   python attendance_sync\manual_sync.py --from "2026-05-01 00:00:00" --to "2026-05-01 23:59:59"
   ```

6. Once the manual sync is verified in Frappe, start the continuous service:

   ```powershell
   python attendance_sync\main.py
   ```

## Configuration

All runtime settings are read from `.env`.

### Required Settings

| Setting | Purpose | Example |
| --- | --- | --- |
| `DEVICES` | Comma-separated Hikvision device IPs. Each device can optionally include credentials. | `10.10.10.166,10.10.10.128` |
| `DEVICE_USER` | Default Hikvision username. | `admin` |
| `DEVICE_PASS` | Default Hikvision password. | `password` |
| `HRMS_URL` | Frappe HRMS site URL without trailing slash. | `https://hrms.example.com` |
| `HRMS_API_KEY` | Frappe API key. | `abc123` |
| `HRMS_API_SECRET` | Frappe API secret. | `secret123` |

### Optional Settings

| Setting | Default | Purpose |
| --- | --- | --- |
| `HIKVISION_USE_HTTPS` | `true` | Use HTTPS for device calls. If connection fails, the client tries the other protocol. |
| `HIKVISION_VERIFY_SSL` | `false` | Verify device SSL certificates. Usually false for local Hikvision devices. |
| `DEVICE_NAMES` | empty | Friendly names sent to Frappe instead of raw IPs. |
| `POLL_INTERVAL` | `600` | Seconds between edge/continuous poller cycles. |
| `FRAPPE_AUTO_PUSH_ENABLED` | `false` | When true, the central server automatically runs the Frappe push once per day. |
| `FRAPPE_AUTO_PUSH_TIME` | `22:00` | Local server time for the daily automatic Frappe push. |
| `FRAPPE_AUTO_PUSH_TIMEZONE` | empty | Optional IANA timezone for auto push, for example `Asia/Kolkata`. If empty, the server/container local timezone is used. |
| `NODE_UPTIME_MONITOR_ENABLED` | `false` | When true, the central server alerts Slack if a configured edge node stops sending punch records. |
| `NODE_UPTIME_THRESHOLD_HOURS` | `8` | Hours without received punch records before an edge node is treated as down. |
| `NODE_UPTIME_CHECK_INTERVAL_SECONDS` | `1200` | Seconds between uptime checks. |
| `NODE_UPTIME_NOTIFY_ON_RECOVERY` | `true` | Send a Slack recovery message when a down node starts sending records again. |
| `SLACK_WEBHOOK_URL` | empty | Slack incoming webhook URL for uptime alerts. Keep this secret. |
| `EMPLOYEE_MAP_RESTART_COMMAND` | empty | Optional command run after saving Employee Map from the dashboard, for example `docker restart punch-sync-server`. |
| `FIRST_RUN_LOOKBACK_HOURS` | `24` | On service startup, fetch this many previous hours. |
| `DEDUP_WINDOW` | `30` | Ignore another punch from the same mapped employee within this many seconds. |
| `EVENT_MAJOR` | `5` | Hikvision event major filter. |
| `EVENT_MINORS` | `75,38` | Comma-separated Hikvision event minor filters. Default fetches face authentication (`75`) and fingerprint authentication (`38`). |
| `EMPLOYEE_MAP` | `employee_map.json` | Path to the employee mapping JSON file. |
| `STORE_PATH` | `data/events.db` | SQLite database for processed events, last punch times, and retries. |
| `RETRY_MAX_ATTEMPTS` | `5` | Maximum retry attempts for transient Frappe/API errors. |
| `RETRY_BACKOFF_BASE` | `2.0` | Exponential retry delay base. |
| `DEFAULT_LOG_TYPE` | `IN` | Default log type for one-event direct processing. Distributed server push derives `IN` for the first punch and `OUT` for the last punch. |
| `LATE_AFTER_TIME` | `09:30` | HR verification late-coming threshold, using the local punch time. |
| `LATITUDE` | empty | Optional decimal latitude added to checkin records, for example `11.2545456`. |
| `LONGITUDE` | empty | Optional decimal longitude added to checkin records, for example `75.8369735`. |
| `LOG_LEVEL` | `INFO` | Logging level: `DEBUG`, `INFO`, `WARNING`, or `ERROR`. |
| `LOG_FILE` | empty | Optional log file path. If empty, logs go to the console. |

### Device Credentials

Use global credentials when all devices use the same login:

```env
DEVICES=10.10.10.166,10.10.10.128,10.10.10.165
DEVICE_USER=admin
DEVICE_PASS=common_password
```

Use per-device credentials when needed:

```env
DEVICES=10.10.10.166:admin:pass1,10.10.10.128:admin:pass2,10.10.10.165
DEVICE_USER=admin
DEVICE_PASS=fallback_password
```

In the example above, `10.10.10.165` uses the fallback `DEVICE_USER` and `DEVICE_PASS`.

### Friendly Device Names

By default, Frappe receives the device IP as `device_id`.

To send a readable name instead:

```env
DEVICE_NAMES=10.10.10.166:BIOMETRIC-01,10.10.10.128:BIOMETRIC-02
```

## Employee Mapping

`employee_map.json` maps the employee number stored in the Hikvision device to the Frappe employee ID.

Example:

```json
{
  "339": "339",
  "269": "296",
  "i36": "327",
  "00001010": "276"
}
```

Important mapping behavior:

- Keys are matched case-insensitively, so `I24` and `i24` are treated the same.
- Numeric keys are normalized by removing leading zeroes.
- If a punch employee number is not in the map, that punch is skipped and logged.
- If you want to use another mapping file, set `EMPLOYEE_MAP`:

  ```env
  EMPLOYEE_MAP=employee_map copy.json
  ```

## Running Continuous Sync

Start the normal service:

```powershell
python attendance_sync\main.py
```

What happens while it runs:

- On startup, it looks back `FIRST_RUN_LOOKBACK_HOURS` hours.
- After that, each cycle fetches events from the last poll time to the current time.
- Each configured device is polled one after another.
- At the end of each cycle, due retry items are processed.
- The service sleeps for `POLL_INTERVAL` seconds and repeats.

Stop it with `Ctrl+C`. The script handles shutdown cleanly after the current operation.

## Running Distributed Edge/Server Sync

Use this when the punch devices are split across two PCs and one central machine should be the only machine that pushes to Frappe.

For a step-by-step setup checklist, see `DISTRIBUTED_SYNC_GUIDE.md`.

For an Ubuntu server, use the included Docker files:

Install `apache2-utils` first if the host does not provide `htpasswd`.

```bash
cp examples/env.compose.example .env
cp examples/env.server.docker.example .env.server
cp examples/env.punch.secrets.example .env.punch.secrets
cp "employee_map copy.json" employee_map.json
python3 -m json.tool employee_map.json > /tmp/employee_map_checked.json
chmod 600 .env .env.punch.secrets
htpasswd -B -c .htpasswd admin
chmod 600 .htpasswd
# The dashboard's Configuration tab writes back into .env.server, so make the
# file writable by uid 10001 (the container's appuser) before starting.
# The Employee Map tab also writes back to employee_map.json:
chown 10001:10001 .env.server
chown 10001:10001 employee_map.json
docker compose up -d
```

Replace every placeholder in `.env` and `.env.punch.secrets` before startup. Keep
non-secret editable settings in `.env.server`; do not enter secrets in the dashboard
Configuration tab. The services refuse to start with missing or common placeholder
credentials. Remote access is HTTPS-only at `https://attendance.codeace.org`; port
8090 binds to `127.0.0.1` for emergency host-local access.

Use the `cp "employee_map copy.json" employee_map.json` line only if `employee_map copy.json` is the correct final mapping. The central Docker server reads `employee_map.json` and mounts it into the container read-write so the dashboard can save mapping edits.

The Docker server uses PostgreSQL by default. Set the same password in `.env`
`POSTGRES_PASSWORD` and `.env.punch.secrets` `POSTGRES_DSN`.

The Docker Hub image is:

```text
codeaceitsolutionsllp/punch-to-frappe:<release-tag-or-digest>
```

After GitHub Actions pushes an immutable image, set `PUNCH_SYNC_IMAGE` to its
release tag or digest in `.env`, then update with:

```bash
docker compose pull
docker compose up -d
```

That is the normal production update path. The compose file deliberately has no
`:latest` fallback. To build from the local checkout instead, run:

```bash
docker compose -f docker-compose.yml -f docker-compose.build.yml up -d --build
```

Recommended layout:

- PC A: can reach its own Hikvision devices.
- PC B: can reach its own Hikvision devices.
- Central server: can receive HTTP from PC A and PC B, and can reach the Frappe site.

Security model:

- Each edge PC signs every upload with HMAC-SHA256.
- The central server only accepts node IDs and secrets listed in `SERVER_NODE_KEYS`.
- Generate node keys with `python generate_sync_keys.py`.
- For best transport safety, run this over HTTPS, a VPN, Tailscale, ZeroTier, or a private LAN. HMAC proves the sender and prevents tampering, but plain HTTP does not encrypt the punch data.

### Central Server Setup

On the central machine, configure Frappe credentials, employee mapping, storage, and the accepted edge nodes:

```env
HRMS_URL=https://your-frappe-site.example.com
HRMS_API_KEY=your_api_key
HRMS_API_SECRET=your_api_secret
EMPLOYEE_MAP=employee_map.json
STORE_PATH=data/events.db

SERVER_HOST=0.0.0.0
SERVER_PORT=8080
SERVER_NODE_KEYS=pc-a:long_random_secret_for_a,pc-b:long_random_secret_for_b
POLL_INTERVAL=600
```

Before starting the central server, make sure `employee_map.json` exists beside `docker-compose.yml` or beside the Python scripts. The central server uses this file to map Hikvision employee numbers to Frappe employee IDs. PC A and PC B do not need this file when running `edge_agent.py`; they only upload raw punch events.

Start the central server:

```powershell
python attendance_sync\server.py
```

What it does:

- Receives `POST /events` batches from PC A and PC B.
- Stores incoming events in `data/events.db`.
- Stores incoming events until you press **Push now**, call the manual push API, or the configured daily auto-push time arrives.
- Uses the same employee mapping, duplicate checks, and retry queue as the existing sync service.
- Deduplicates incoming raw uploads by edge node, device IP, and device serial number so multiple devices behind one PC do not hide each other's punches.

Health check from the Docker host:

```powershell
Invoke-RestMethod http://127.0.0.1:8090/health
```

### Web Dashboard

Open `https://attendance.codeace.org/` in a browser. The dashboard shows:

- Pending / pushed / retry counters
- Alert count and **Alerts** tab for punches that were not pushed, missing employee mappings, retry items, bad timestamps, and other issues needing resolution
- Per-edge-node connection status (online / stale / offline / never connected) based on the last `/events` POST received
- A paginated **First / Last Punches** page grouped by employee and date, with filters and CSV export for the full filtered result set
- A paginated **HR Verify** page that maps device employee numbers to Frappe employee names/details and shows late-coming status for HR review
- A paginated **All Punch Records** page for inspecting every raw punch received from edge PCs
- Recent inbound events, recently pushed checkins, and the retry queue
- A **Manual Push to Frappe** button that drains the queue and runs retries. If `FRAPPE_AUTO_PUSH_ENABLED=true`, the server also pushes once per day at `FRAPPE_AUTO_PUSH_TIME` in `FRAPPE_AUTO_PUSH_TIMEZONE`.
- Every pushed `Employee Checkin` sets `skip_auto_attendance` to `0`.
- The last push run, including trigger, result breakdown, and retry count
- An **Employee Map** tab for adding, editing, searching, and removing device employee number to Frappe employee ID mappings. Restart the server after saving so the processor reloads the map.
- A **Configuration** tab for non-secret runtime settings such as `HRMS_URL`, poll/dedup intervals, log level, schedules, and storage selection. Credentials and node keys are read-only configured flags and must be rotated through the host secret files.

In Docker, Nginx protects the host-local Punch Sync administration endpoint with
the bcrypt credentials in the host-managed `.htpasswd` file. The Python API is
only exposed inside the Compose network, and signed edge uploads continue to use
their existing HMAC authentication.

Frontend API documentation is available in [`FRONTEND_API_README.md`](FRONTEND_API_README.md). The running server also serves Swagger UI at `/api/docs` and the raw OpenAPI spec at `/api/openapi.json`.

The application container cannot access the Docker socket. After saving Employee
Map or non-secret configuration, an administrator must explicitly run
`docker compose restart punch-sync-server` on the host.

### PC A / PC B Edge Setup

On each edge PC, configure only the devices reachable from that PC plus the central server URL.

PC A example:

```env
DEVICES=10.10.10.166,10.10.10.128
DEVICE_USER=admin
DEVICE_PASS=your_device_password

SYNC_SERVER_URL=https://attendance.codeace.org
EDGE_NODE_ID=pc-a
EDGE_NODE_SECRET=long_random_secret_for_a
POLL_INTERVAL=600
FIRST_RUN_LOOKBACK_HOURS=24
```

PC B example:

```env
DEVICES=10.10.20.50,10.10.20.51
DEVICE_USER=admin
DEVICE_PASS=your_device_password

SYNC_SERVER_URL=https://attendance.codeace.org
EDGE_NODE_ID=pc-b
EDGE_NODE_SECRET=long_random_secret_for_b
POLL_INTERVAL=600
FIRST_RUN_LOOKBACK_HOURS=24
```

Start the edge agent on each PC:

```powershell
python attendance_sync\edge_agent.py
```

To manually upload a specific date/time range from an edge PC to the central
server without starting the continuous loop:

```powershell
python attendance_sync\edge_agent.py --from "2026-05-01 00:00:00" --to "2026-05-10 23:59:59"
```

The dashboard includes a **Manual Range** tab that generates this command for
PC A / PC B. The current architecture is edge-to-server only: edge PCs upload
to the central server, but the server cannot directly run commands on an edge
PC or fetch from its devices. To support server-initiated jobs safely in the
future, add a restricted command queue that the edge agent polls, rather than
opening a general remote shell.

If an edge PC cannot reach the central server, it does not advance its poll window; on the next cycle it fetches that same time range again and resends. The central server de-duplicates received batches, so repeat sends are safe.

## Running Manual Sync

Use manual sync when you need to backfill a known date/time range into Frappe:

```powershell
python attendance_sync\manual_sync.py --from "2026-05-01 00:00:00" --to "2026-05-10 23:59:59"
```

Accepted date formats:

```text
2026-05-01 09:00:00
2026-05-01T09:00:00
2026-05-01T09:00:00+05:30
```

Manual sync uses the same employee map, duplicate protection, Frappe push logic, and retry queue as the continuous service.

## Exporting Punch Records To CSV

Use CSV export when you want to inspect raw device punches without pushing anything to Frappe:

```powershell
python export_punch_records.py --start 2026-05-01 --end 2026-05-10 --output data\punch_records_2026-05-01_to_2026-05-10.csv
```

The CSV contains:

- `device_ip`
- `device_name`
- `employee_no`
- `mapped_employee_id`
- `employee_name`
- `event_time`
- `serial_no`

This is useful for:

- Checking whether devices are reachable.
- Finding employee numbers that are missing from `employee_map.json`.
- Comparing raw device punches against Frappe records.
- Keeping an audit copy for a date range.

There is also a helper batch file for Q1 2026:

```powershell
.\export_q1_2026_punch_records.bat
```

## Local SQLite Store

By default, runtime state is stored in:

```text
data/events.db
```

The database has three responsibilities:

- `processed_events`: stores Hikvision `serialNo` values that have already been handled.
- `last_punch`: stores each employee's last punch time for duplicate-window checks.
- `retry_queue`: stores checkins that failed because of temporary Frappe/API problems.
- `inbound_events`: stores signed batches received from edge PCs before the central server pushes them to Frappe.

Do not delete `data/events.db` unless you intentionally want the sync to forget what it already processed. If you delete it, old device events inside the queried date range may be pushed again unless Frappe rejects them as duplicates.

## Duplicate And Retry Rules

Duplicate protection happens in two layers:

- Same Hikvision `serialNo`: never processed twice once recorded in `processed_events`.
- Same mapped employee within `DEDUP_WINDOW`: skipped as a duplicate punch.

Frappe error handling:

- Duplicate responses from Frappe, usually HTTP `409` or `417`, are marked as processed.
- Other HTTP `4xx` errors are treated as permanent and are not retried.
- Connection errors, timeouts, and server-side failures are added to the retry queue.

## Logging

Default logging goes to the console.

For more detail:

```env
LOG_LEVEL=DEBUG
```

To write a log file:

```env
LOG_FILE=data/attendance_sync.log
```

Typical useful log messages:

- Employee map loaded count.
- Device polling count.
- Missing employee mappings.
- Duplicate punch skips.
- Frappe push success/failure.
- Retry queue processing.

## Running On Windows Full-Time

For a full-time installation, use one of these:

- Windows Task Scheduler
- PM2
- NSSM as a Windows service

Detailed setup notes are in `service_setup.md`.

If you use a virtual environment, configure the service to run:

```text
venv\Scripts\python.exe
```

instead of the global Python executable.

## Troubleshooting

### No Rows In CSV Export

- Check that `DEVICES`, `DEVICE_USER`, and `DEVICE_PASS` are correct.
- Check the date range.
- Confirm the device time/date is correct.
- Try toggling `HIKVISION_USE_HTTPS`.
- Set `LOG_LEVEL=DEBUG` and rerun.

### Punches Export But Do Not Sync To Frappe

- Confirm `HRMS_URL`, `HRMS_API_KEY`, and `HRMS_API_SECRET`.
- Confirm the API user can create `Employee Checkin`.
- Check whether employee numbers are missing from `employee_map.json`.
- Check Frappe for duplicate validation errors.

### Missing Employees

Export a CSV and look for rows where `mapped_employee_id` is empty:

```powershell
python export_punch_records.py --start 2026-05-01 --end 2026-05-10 --output data\missing_map_check.csv
```

Add the missing `employee_no` values to `employee_map.json`, then rerun manual sync for the affected period.

### Duplicate Or Extra Punches

- Increase `DEDUP_WINDOW` if devices create multiple punches for the same action.
- Check whether `data/events.db` was deleted or replaced.
- Confirm the manual sync range is not being run repeatedly after clearing the database.

### Device Authentication Fails

The CSV exporter has an extra fallback that can call `curl` with digest authentication if the Python request receives `401 Unauthorized`.

Make sure `curl.exe` is available on Windows:

```powershell
curl.exe --version
```

## Quick Command Reference

Install dependencies:

```powershell
pip install -r requirements.txt
```

Export punches:

```powershell
python export_punch_records.py --start 2026-05-01 --end 2026-05-10 --output data\punch_records_2026-05-01_to_2026-05-10.csv
```

Manual sync:

```powershell
python attendance_sync\manual_sync.py --from "2026-05-01 00:00:00" --to "2026-05-10 23:59:59"
```

Continuous service:

```powershell
python attendance_sync\main.py
```
