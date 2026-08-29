# Punch-to-Frappe Frontend API

This document lists the HTTP endpoints used by the server dashboard. Give this to the frontend developer when building a separate UI.

## Base URL And Access

In Docker, the public dashboard normally runs through Nginx:

```text
http://127.0.0.1:8090
```

Nginx protects the host-local administration endpoint with its `.htpasswd`
configuration. The HRMS server-side proxy reaches the Python service directly
inside the Compose network and does not require additional Punch credentials.

For server-local testing, the Python app also listens internally on:

```text
http://127.0.0.1:8080
```

All API responses are JSON. Error responses usually look like:

```json
{ "error": "message" }
```

## Swagger UI

The server also serves interactive Swagger docs:

```text
http://127.0.0.1:8090/api/docs
```

Raw OpenAPI JSON:

```text
http://127.0.0.1:8090/api/openapi.json
```

Direct Python-server URLs, useful from inside the server:

```text
http://127.0.0.1:8080/api/docs
http://127.0.0.1:8080/api/openapi.json
```

## Dashboard Status

### `GET /health`

Light health check.

Response:

```json
{
  "ok": true,
  "pending_events": 123
}
```

### `GET /api/status`

Main dashboard summary.

Response shape:

```json
{
  "now": "2026-05-19T10:30:00+00:00",
  "server": {
    "host": "0.0.0.0",
    "port": 8080,
    "poll_interval": 600,
    "storage_backend": "postgres",
    "hrms_url": "https://hr.codeace.org",
    "late_after_time": "09:30",
    "auto_push_enabled": true,
    "auto_push_time": "22:00",
    "auto_push_timezone": "Asia/Kolkata"
  },
  "counts": {
    "pending": 10,
    "done": 1200,
    "processed_total": 300,
    "retry_queue": 2
  },
  "last_push": {
    "started_at": "2026-05-19T16:30:00+00:00",
    "finished_at": "2026-05-19T16:30:20+00:00",
    "processed": 50,
    "retries": 0,
    "results": { "first_punch_in_pushed": 25, "last_punch_out_pushed": 25 },
    "error": null,
    "trigger": "manual"
  },
  "configured_nodes": ["pc-a", "pc-b"],
  "nodes": [
    {
      "node_id": "pc-a",
      "first_seen": "2026-05-19T08:00:00+00:00",
      "last_seen": "2026-05-19T10:00:00+00:00",
      "last_accepted": 200,
      "last_inserted": 20,
      "last_skipped": 180,
      "total_accepted": 1000,
      "total_inserted": 500,
      "total_skipped": 500
    }
  ]
}
```

## Live Attendance

### `GET /api/live-attendance`

Live attendance log for the **Workforce pulse** widget: active headcount, punch-ins today, and a recent activity feed.

Uses `FRAPPE_AUTO_PUSH_TIMEZONE` (when set) to determine the calendar day for "today". Otherwise UTC is used.

Query params:

| Param | Default | Notes |
| --- | --- | --- |
| `limit` | `50` | Feed items to return, min 1, max 200 (most recent first) |
| `refresh_frappe` | false | Use `1` to refresh cached Frappe employee details |

Response:

```json
{
  "as_of": "2026-05-19T06:36:00+00:00",
  "date": "2026-05-19",
  "active_count": 42,
  "punch_ins_today": 92,
  "punch_outs_today": 50,
  "feed": [
    {
      "id": "18452",
      "employee": "James Anderson",
      "department": "Engineering",
      "action": "punch-in",
      "time": "2026-05-19T12:06:00+05:30",
      "device_employee_no": "101",
      "frappe_employee_id": "EMP-101",
      "device_ip": "10.10.80.50",
      "device_name": "First Floor IN"
    },
    {
      "id": "18451",
      "employee": "Sarah Martinez",
      "department": "Product",
      "action": "punch-out",
      "time": "2026-05-19T12:05:00+05:30",
      "device_employee_no": "102",
      "frappe_employee_id": "EMP-102"
    }
  ]
}
```

Field notes:

- `active_count`: employees with an odd punch count today (last punch treated as in).
- `punch_ins_today`: employees with at least one punch today.
- `punch_outs_today`: employees with an even punch count of 2 or more today (last punch treated as out).
- `action`: `punch-in` or `punch-out`, derived by chronological punch order per employee for the day (1st in, 2nd out, 3rd in, …).
- `employee`: device-reported name when present, otherwise Frappe employee name, otherwise device employee number.

Frontend mapping example:

```js
const { active_count, punch_ins_today, feed } = await api("/api/live-attendance?limit=6");
// activeCount: active_count, punchInsToday: punch_ins_today, feed items use action + time
```

## Punch Data

### `GET /api/attendance-overview`

Employee/date grouped first and last punch view.

Query params:

| Param | Default | Notes |
| --- | --- | --- |
| `page` | `1` | 1-based page number |
| `page_size` | `100` | Min 10, max 1000 |
| `search` | empty | Searches employee/date/time/node/device |
| `from` | empty | Date filter, `YYYY-MM-DD` |
| `to` | empty | Date filter, `YYYY-MM-DD` |

Example:

```text
GET /api/attendance-overview?from=2026-05-15&to=2026-05-19&page=1&page_size=100
```

Response:

```json
{
  "overview": [
    {
      "employee": "303",
      "date": "2026-05-19",
      "source_nodes": ["pc-a"],
      "devices": ["192.168.50.11"],
      "punch_count": 8,
      "first_time": "2026-05-19T09:02:10+05:30",
      "first_result": "first_punch_in_pushed",
      "last_time": "2026-05-19T18:15:30+05:30",
      "last_result": "last_punch_out_pushed"
    }
  ],
  "page": 1,
  "page_size": 100,
  "total": 1,
  "has_next": false,
  "has_prev": false
}
```

### `GET /api/punch-records`

Paginated raw punch records received from edge PCs.

Query params:

| Param | Default | Notes |
| --- | --- | --- |
| `page` | `1` | 1-based page number |
| `page_size` | `100` | Min 10, max 1000 |
| `search` | empty | Searches all visible fields |
| `from` | empty | Date filter, `YYYY-MM-DD` |
| `to` | empty | Date filter, `YYYY-MM-DD` |
| `status` | empty | `pending` or `done`; empty means all |

Response:

```json
{
  "records": [
    {
      "id": 123,
      "source_node": "pc-a",
      "status": "done",
      "received_at": "2026-05-19T04:30:00+00:00",
      "processed_at": "2026-05-19T16:30:00+00:00",
      "last_result": "skipped_middle_punch",
      "employee": "303",
      "name": "Karunkarthikeyan",
      "device_ip": "192.168.50.11",
      "event_time": "2026-05-19T12:30:00+05:30",
      "serial_no": "pc-a:123456",
      "event_type": "Authenticated via Fingerprint",
      "minor": 38
    }
  ],
  "page": 1,
  "page_size": 100,
  "total": 1,
  "has_next": false,
  "has_prev": false
}
```

### `GET /api/events`

Recent inbound events, fixed limit 100.

Response:

```json
{
  "events": [
    {
      "id": 123,
      "source_node": "pc-a",
      "status": "pending",
      "received_at": "2026-05-19T04:30:00+00:00",
      "processed_at": null,
      "last_result": null,
      "employee": "303",
      "device_ip": "192.168.50.11",
      "event_time": "2026-05-19T09:02:10+05:30",
      "serial_no": "123456",
      "event_type": "Authenticated via Face",
      "minor": 75
    }
  ]
}
```

## HR Verification

### `GET /api/hr-verification`

Employee/date view enriched with Frappe Employee details and late-coming status.

Query params:

| Param | Default | Notes |
| --- | --- | --- |
| `page` | `1` | 1-based page number |
| `page_size` | `100` | Min 10, max 1000 |
| `search` | empty | Searches employee no, HRMS ID, name, department, devices, status |
| `from` | empty | Date filter, `YYYY-MM-DD` |
| `to` | empty | Date filter, `YYYY-MM-DD` |
| `late_after` | server setting | Time like `09:30` |
| `refresh_frappe` | false | Use `1` to refresh cached Frappe employee details |

Response:

```json
{
  "rows": [
    {
      "date": "2026-05-19",
      "device_employee_no": "303",
      "frappe_employee_id": "EMP-303",
      "employee_name": "Karunkarthikeyan",
      "department": "Operations",
      "designation": "Executive",
      "branch": "Kozhikode",
      "company": "CodeAce",
      "employee_status": "Active",
      "default_shift": "General",
      "first_time": "2026-05-19T09:45:00+05:30",
      "first_result": "first_punch_in_pushed",
      "last_time": "2026-05-19T18:12:00+05:30",
      "last_result": "last_punch_out_pushed",
      "punch_count": 6,
      "source_nodes": ["pc-a"],
      "devices": ["192.168.50.11"],
      "late_after": "09:30",
      "late_by_minutes": 15,
      "late_status": "late",
      "mapping_status": "mapped",
      "frappe_details_status": "loaded"
    }
  ],
  "summary": {
    "late_after": "09:30",
    "total": 1,
    "late": 1,
    "on_time": 0,
    "missing_map": 0,
    "frappe_error": null
  },
  "page": 1,
  "page_size": 100,
  "total": 1,
  "has_next": false,
  "has_prev": false
}
```

## Alerts, Retries, Processed, Logs

### `GET /api/alerts`

Dashboard alerts for missing mappings, retry queue, failed pushes, and pending queue info.

Response:

```json
{
  "alerts": [
    {
      "severity": "warning",
      "kind": "frappe_push_issue",
      "title": "Punch was not pushed to Frappe",
      "employee": "303",
      "device_ip": "192.168.50.11",
      "event_time": "2026-05-19T09:02:10+05:30",
      "detail": "queued_client_error",
      "action": "Review the event and logs."
    }
  ]
}
```

### `GET /api/retries`

Retry queue, fixed limit 100.

Response:

```json
{
  "retries": [
    {
      "id": 1,
      "employee_id": "EMP-303",
      "event_time": "2026-05-19 09:02:10",
      "device_ip": "192.168.50.11",
      "serial_no": "pc-a:123456",
      "log_type": "IN",
      "attempts": 1,
      "next_retry": "2026-05-20T04:30:00+00:00",
      "last_error": "HTTP 417 from Frappe: ..."
    }
  ]
}
```

### `GET /api/processed`

Recently accepted/pushed processed records, fixed limit 100.

Response:

```json
{
  "processed": [
    {
      "serial_no": "pc-a:123456",
      "employee_no": "303",
      "device_ip": "192.168.50.11",
      "event_time": "2026-05-19T09:02:10+05:30",
      "pushed_at": "2026-05-19T16:30:00+00:00"
    }
  ]
}
```

### `GET /api/frappe-push-logs`

Recent Frappe push attempt audit logs.

Query params:

| Param | Default | Notes |
| --- | --- | --- |
| `page` | `1` | 1-based page number |
| `page_size` | `100` | Min 10, max 500 |

Response:

```json
{
  "logs": [
    {
      "id": 1,
      "attempted_at": "2026-05-19T16:30:00+00:00",
      "serial_no": "pc-a:123456",
      "employee_no": "303",
      "hrms_id": "EMP-303",
      "event_time": "2026-05-19 09:02:10",
      "device_ip": "192.168.50.11",
      "device_id": "BIOMETRIC-01",
      "log_type": "IN",
      "result": "pushed",
      "http_status": null,
      "payload": {
        "employee": "EMP-303",
        "time": "2026-05-19 09:02:10",
        "device_id": "BIOMETRIC-01",
        "log_type": "IN",
        "skip_auto_attendance": 0
      },
      "response_body": "",
      "error": ""
    }
  ],
  "page": 1,
  "page_size": 100,
  "total": 1,
  "has_next": false,
  "has_prev": false
}
```

## Push Control

### `POST /api/push`

Runs the same Frappe push as the dashboard button. It processes pending inbound events, pushes only first `IN` and last `OUT` per employee/date, skips middle punches, and runs retry queue with `force=true`.

Request body: none.

Response:

```json
{
  "ok": true,
  "processed": 50,
  "retries": 0,
  "results": {
    "first_punch_in_pushed": 25,
    "last_punch_out_pushed": 25,
    "skipped_middle_punch": 200
  }
}
```

## Config APIs

### `GET /api/config`

Returns editable `.env` values. Secret values are masked.

Response:

```json
{
  "env_path": "/app/.env.server",
  "values": {
    "HRMS_URL": { "set": true, "value": "https://hr.codeace.org" },
    "HRMS_API_KEY": { "set": true, "value": "" },
    "HRMS_API_SECRET": { "set": true, "value": "" },
    "FRAPPE_AUTO_PUSH_ENABLED": { "set": true, "value": "true" },
    "FRAPPE_AUTO_PUSH_TIME": { "set": true, "value": "22:00" },
    "FRAPPE_AUTO_PUSH_TIMEZONE": { "set": true, "value": "Asia/Kolkata" }
  },
  "nodes": [
    { "node_id": "pc-a", "secret_set": true }
  ]
}
```

### `POST /api/config`

Updates whitelisted `.env` keys and edge node secrets. Restart is required.

Request:

```json
{
  "values": {
    "HRMS_URL": "https://hr.codeace.org",
    "FRAPPE_AUTO_PUSH_ENABLED": "true",
    "FRAPPE_AUTO_PUSH_TIME": "22:00",
    "FRAPPE_AUTO_PUSH_TIMEZONE": "Asia/Kolkata"
  },
  "nodes": [
    { "node_id": "pc-a", "secret": "" },
    { "node_id": "pc-b", "secret": "new-secret-if-changing" }
  ]
}
```

Response:

```json
{
  "ok": true,
  "written_keys": ["FRAPPE_AUTO_PUSH_ENABLED", "FRAPPE_AUTO_PUSH_TIME"],
  "restart_required": true,
  "env_path": "/app/.env.server"
}
```

Notes:

- Blank secret values keep existing secrets.
- Blank `HRMS_API_KEY`, `HRMS_API_SECRET`, or `POSTGRES_DSN` keep existing values.

## Employee Map APIs

### `GET /api/employee-map`

Returns device employee number to Frappe employee ID mapping.

Response:

```json
{
  "path": "/app/employee_map.json",
  "count": 2,
  "entries": [
    { "device_employee_no": "303", "frappe_employee_id": "EMP-303" },
    { "device_employee_no": "304", "frappe_employee_id": "EMP-304" }
  ]
}
```

### `POST /api/employee-map`

Replaces the full employee map. Restart is required for the processor to use the new map.

Request:

```json
{
  "entries": [
    { "device_employee_no": "303", "frappe_employee_id": "EMP-303" },
    { "device_employee_no": "304", "frappe_employee_id": "EMP-304" }
  ]
}
```

Response:

```json
{
  "ok": true,
  "count": 2,
  "path": "/app/employee_map.json",
  "restart_required": true
}
```

## Edge Upload Endpoint

### `POST /events`

This is for edge PCs only, not the normal frontend. It requires HMAC headers:

- `X-Node-Id`
- `X-Timestamp`
- `X-Signature`

Request:

```json
{
  "events": [
    {
      "employeeNoString": "303",
      "name": "Karunkarthikeyan",
      "deviceIP": "192.168.50.11",
      "time": "2026-05-19T09:02:10+05:30",
      "serialNo": "123456",
      "eventType": "Authenticated via Fingerprint",
      "minor": 38
    }
  ]
}
```

Response:

```json
{
  "accepted": 1,
  "inserted": 1,
  "skipped": 0
}
```

## Frontend Notes

- Use `/api/live-attendance` for the Workforce pulse widget (active count, punch-ins, activity feed).
- Use `/api/status` for cards and edge node health.
- Use `/api/attendance-overview` for the first/last punch page.
- Use `/api/punch-records` for the complete raw punch listing.
- Use `/api/hr-verification` for HR review and late-coming screens.
- Use `/api/frappe-push-logs` to debug what was sent to Frappe, especially `skip_auto_attendance`.
- Use `POST /api/push` for the manual push button.
- For CSV export, fetch all pages by increasing `page` until `has_next=false`.
