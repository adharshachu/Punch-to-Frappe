# Distributed Punch Sync Guide

This guide is for the setup where:

- PC A has access to one set of punch devices.
- PC B has access to another set of punch devices.
- One central server receives data from PC A and PC B.
- Only the central server pushes checkins to Frappe every 10 minutes.

## 1. Decide The Machine Roles

Use three roles:

| Machine | Runs | Needs access to |
| --- | --- | --- |
| Central server | `attendance_sync\server.py` | Frappe website, PC A, PC B |
| PC A | `attendance_sync\edge_agent.py` | PC A punch devices, central server |
| PC B | `attendance_sync\edge_agent.py` | PC B punch devices, central server |

The central server does not need direct access to every Hikvision device.

## 2. Copy The Project To All Machines

Copy this full project folder to:

- The central server
- PC A
- PC B

On each machine, install dependencies:

```powershell
pip install -r requirements.txt
```

If you use the included virtual environment, run commands with:

```powershell
venv\Scripts\python.exe
```

instead of:

```powershell
python
```

## 3. Create Shared Secrets

Make one long secret for PC A and one long secret for PC B.

Fast path:

```powershell
python generate_sync_keys.py
```

Or double-click:

```text
generate_sync_keys.bat
```

The output looks like this:

```env
Central server .env:
SERVER_NODE_KEYS=pc-a:secret_for_pc_a,pc-b:secret_for_pc_b

pc-a .env:
EDGE_NODE_ID=pc-a
EDGE_NODE_SECRET=secret_for_pc_a

pc-b .env:
EDGE_NODE_ID=pc-b
EDGE_NODE_SECRET=secret_for_pc_b
```

Put the `SERVER_NODE_KEYS` line only on the central server.

Put the matching `EDGE_NODE_ID` and `EDGE_NODE_SECRET` only on that edge PC.

Example:

```text
pc-a secret: change_this_to_a_long_random_secret_for_pc_a
pc-b secret: change_this_to_a_long_random_secret_for_pc_b
```

Use different secrets for PC A and PC B.

How the key check works:

- PC A signs each upload using `EDGE_NODE_ID=pc-a` and its `EDGE_NODE_SECRET`.
- PC B signs each upload using `EDGE_NODE_ID=pc-b` and its `EDGE_NODE_SECRET`.
- The central server checks the signature against `SERVER_NODE_KEYS`.
- If the node ID is unknown, the secret is wrong, the body was changed, or the timestamp is too old, the server returns `401 Unauthorized`.

## 4. Configure The Central Server

On the central server, create or edit `.env`.

### Option A: Ubuntu Server With Docker

Use this option for a real Ubuntu server.

The Docker Hub image for this project is:

```text
codeaceitsolutionsllp/punch-to-frappe
```

Install Docker:

```bash
sudo apt update
sudo apt install -y ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```

Allow your user to run Docker:

```bash
sudo usermod -aG docker $USER
```

Log out and log back in after running that command.

Create the Compose, non-secret settings, and secret env files:

```bash
cp examples/env.compose.example .env
cp examples/env.server.docker.example .env.server
cp examples/env.punch.secrets.example .env.punch.secrets
chmod 600 .env .env.punch.secrets
htpasswd -B -c .htpasswd admin
chmod 600 .htpasswd
```

Edit `.env.server` for non-secret runtime settings. Put Frappe credentials,
edge node keys, and the PostgreSQL DSN only in `.env.punch.secrets`:

```bash
nano .env.server
nano .env.punch.secrets
```

Replace every placeholder. Set the same URL-safe PostgreSQL password in `.env`
`POSTGRES_PASSWORD` and `.env.punch.secrets` `POSTGRES_DSN`. Docker Compose uses the
first when creating PostgreSQL, and the application uses the second to connect.

Generate PC keys:

```bash
python3 generate_sync_keys.py
```

Put the generated `SERVER_NODE_KEYS=...` line into `.env.punch.secrets`.

Prepare the employee mapping file on the Ubuntu server.

The central server must have the final `employee_map.json`, because the central server is the machine that converts Hikvision employee numbers into Frappe employee IDs before pushing to Frappe.

If your correct mapping file is already named `employee_map.json`, keep it in the same folder as `docker-compose.yml`:

```bash
ls -l employee_map.json
```

If your correct mapping file is currently named `employee_map copy.json`, copy it into the Docker-facing filename:

```bash
cp "employee_map copy.json" employee_map.json
```

Validate that the file is valid JSON:

```bash
python3 -m json.tool employee_map.json > /tmp/employee_map_checked.json
```

The file should look like this:

```json
{
  "339": "339",
  "269": "296",
  "i36": "327",
  "00001010": "276"
}
```

In this format:

- Left side: employee number coming from the Hikvision device.
- Right side: employee ID in Frappe HRMS.
- The same `employee_map.json` is used for punches coming from both PC A and PC B.
- PC A and PC B do not need the mapping file for upload-only mode; they only send raw punch events.

Docker Compose mounts this file into the container:

```yaml
./employee_map.json:/app/employee_map.json:ro
```

So whenever you edit the mapping on the Ubuntu server, restart the container:

```bash
docker compose restart punch-sync-server
```

The Docker setup uses PostgreSQL for server storage. Data is stored in the Docker volume:

```bash
docker volume ls | grep postgres
```

Pull the published image and start:

```bash
docker compose pull
docker compose up -d
```

If you are developing from this checkout and want to build locally on the server:

```bash
docker compose -f docker-compose.yml -f docker-compose.build.yml up -d --build
```

Check status:

```bash
docker compose ps
docker compose logs -f punch-sync-server
docker compose logs -f nginx
```

Health check:

```bash
curl http://localhost:8090/health
```

Update from Docker Hub:

```bash
docker compose pull
docker compose up -d
```

Rebuild from local source instead:

```bash
docker compose -f docker-compose.yml -f docker-compose.build.yml up -d --build
```

Stop:

```bash
docker compose down
```

The container runs as UID/GID `10001:10001`, not root.

The PostgreSQL tables store:

- `inbound_events`: signed event batches received from PC A and PC B.
- `processed_events`: serial numbers already handled.
- `last_punch`: last punch per employee for duplicate-window checks.
- `retry_queue`: Frappe pushes that failed temporarily and need retry.

Nginx is the public entrypoint:

- Public HRMS and signed edge-ingest URL: `https://attendance.codeace.org`.
- Punch Sync administration: host-local `http://127.0.0.1:8090/` (use an SSH tunnel for remote administration).
- Change the public port with `DASHBOARD_PORT` in a Docker Compose `.env` file, or leave it at `8090`.
- The host-local Punch Sync dashboard is protected by Nginx using `.htpasswd`.
- `/events` uploads remain protected by their existing HMAC signatures.
- The Python app is internal to Docker on port `8080`.

### Docker Image Build In GitHub Actions

The repository includes:

```text
.github/workflows/docker-image.yml
```

It matches the sample worker style:

- Builds on pushes to `main`.
- Builds on tags like `v1.0.0`.
- Uses Docker Buildx.
- Logs in to Docker Hub with repository secrets.
- Pushes a `linux/amd64` image.
- Publishes branch, tag, SHA, and `latest` tags.

Add these GitHub repository secrets before using it:

```text
DOCKERHUB_USERNAME
DOCKERHUB_TOKEN
```

`DOCKERHUB_USERNAME` should be the Docker Hub account or organization user that can push to:

```text
codeaceitsolutionsllp/punch-to-frappe
```

`DOCKERHUB_TOKEN` should be a Docker Hub access token with push permission for that repository.

The image name is:

```text
codeaceitsolutionsllp/punch-to-frappe
```

The compose file requires `PUNCH_SYNC_IMAGE` in `.env`. Pin it to an immutable
release tag or digest, for example:

```text
codeaceitsolutionsllp/punch-to-frappe:sha-a6a7ea5
```

The default `docker-compose.yml` is meant for deployment from Docker Hub, so
`docker compose pull && docker compose up -d` is enough after a new image is
published and `.env` points at that release. Use `docker-compose.build.yml` only when you intentionally want to
build from files on that server.

The workflow publishes these tags:

- Commit SHA tag
- Branch tag
- Git tag, for example `v1.0.0`
- `latest` when pushing to the default branch

If you want a different Docker Hub image, change it in both:

```text
.github/workflows/docker-image.yml
docker-compose.yml
```

### Option B: Windows Or Direct Python

Fast path:

```powershell
Copy-Item examples\env.central-server.example .env
```

Example central server `.env`:

```env
HRMS_URL=https://your-frappe-site.example.com
HRMS_API_KEY=your_frappe_api_key
HRMS_API_SECRET=your_frappe_api_secret

EMPLOYEE_MAP=employee_map.json
STORE_PATH=data/events.db

SERVER_HOST=0.0.0.0
SERVER_PORT=8080
SERVER_NODE_KEYS=pc-a:change_this_to_a_long_random_secret_for_pc_a,pc-b:change_this_to_a_long_random_secret_for_pc_b

POLL_INTERVAL=600
DEDUP_WINDOW=30
DEFAULT_LOG_TYPE=IN
LOG_LEVEL=INFO
LOG_FILE=data/server.log
```

Important:

- `SERVER_NODE_KEYS` must contain both PC A and PC B.
- The secret for `pc-a` must match PC A's `EDGE_NODE_SECRET`.
- The secret for `pc-b` must match PC B's `EDGE_NODE_SECRET`.
- Keep `.env` private.

Start the server:

```powershell
python attendance_sync\server.py
```

Or double-click:

```text
run_central_server.bat
```

Check if it is alive from the central server:

```powershell
Invoke-RestMethod http://localhost:8080/health
```

Expected result:

```json
{
  "ok": true,
  "pending_events": 0
}
```

Important employee mapping rule:

- The central server needs the correct `employee_map.json`.
- If an employee number is missing from the map, that punch is skipped and logged.
- After changing `employee_map.json`, restart the server.

## 5. Configure PC A

On PC A, create or edit `.env`.

Use only the devices PC A can reach.

Fast path:

```powershell
Copy-Item examples\env.pc-a.example .env
```

Example PC A `.env`:

```env
DEVICES=10.10.10.166,10.10.10.128
DEVICE_USER=admin
DEVICE_PASS=your_device_password

SYNC_SERVER_URL=https://attendance.codeace.org
EDGE_NODE_ID=pc-a
EDGE_NODE_SECRET=change_this_to_a_long_random_secret_for_pc_a

POLL_INTERVAL=600
FIRST_RUN_LOOKBACK_HOURS=24
LOG_LEVEL=INFO
LOG_FILE=data/edge-agent.log
```

Replace:

- `central-server-ip` with the real IP address or hostname of the central server.
- `DEVICES` with PC A's actual device IPs.
- `DEVICE_PASS` with the real Hikvision password.

Start PC A:

```powershell
python attendance_sync\edge_agent.py
```

Or double-click:

```text
run_edge_agent.bat
```

## 6. Configure PC B

On PC B, create or edit `.env`.

Use only the devices PC B can reach.

Fast path:

```powershell
Copy-Item examples\env.pc-b.example .env
```

Example PC B `.env`:

```env
DEVICES=10.10.20.50,10.10.20.51
DEVICE_USER=admin
DEVICE_PASS=your_device_password

SYNC_SERVER_URL=https://attendance.codeace.org
EDGE_NODE_ID=pc-b
EDGE_NODE_SECRET=change_this_to_a_long_random_secret_for_pc_b

POLL_INTERVAL=600
FIRST_RUN_LOOKBACK_HOURS=24
LOG_LEVEL=INFO
LOG_FILE=data/edge-agent.log
```

Start PC B:

```powershell
python attendance_sync\edge_agent.py
```

Or double-click:

```text
run_edge_agent.bat
```

## 7. Test The Flow

Test in this order.

### Central Server Test

From PC A or PC B, run:

```powershell
Invoke-RestMethod http://127.0.0.1:8090/health
```

If this fails, check:

- Central server is running.
- Windows Firewall allows inbound port `8090`.
- PC A and PC B can reach the central server IP.

### Edge Device Test

On PC A, run a CSV export for a small date:

```powershell
python export_punch_records.py --start 2026-05-01 --end 2026-05-01 --output data\pc_a_test.csv
```

On PC B, run:

```powershell
python export_punch_records.py --start 2026-05-01 --end 2026-05-01 --output data\pc_b_test.csv
```

If CSV export is empty, check:

- Device IPs
- Device username/password
- Device date/time
- `HIKVISION_USE_HTTPS`

### Full Sync Test

1. Start the central server.
2. Start PC A edge agent.
3. Start PC B edge agent.
4. Watch central server logs.
5. Confirm new `Employee Checkin` records appear in Frappe.

## 8. Run Full-Time

After testing, run each script using one of:

- Windows Task Scheduler
- PM2
- NSSM

Use these scripts:

| Machine | Script |
| --- | --- |
| Central server | `attendance_sync\server.py` |
| PC A | `attendance_sync\edge_agent.py` |
| PC B | `attendance_sync\edge_agent.py` |

More Windows service setup notes are in `service_setup.md`.

## 9. Safety Notes

- Use a private LAN, VPN, Tailscale, ZeroTier, or HTTPS if possible.
- The upload is signed, so the server rejects fake or modified requests.
- Plain HTTP does not encrypt punch data.
- Do not share `.env`.
- Do not delete `data/events.db` unless you intentionally want to reset sync memory.

## 10. What Happens If Something Fails

If PC A or PC B cannot reach the central server:

- The edge agent logs an error.
- It does not advance its polling window.
- On the next cycle, it tries the same time range again.
- The central server de-duplicates repeated uploads.

If Frappe is down:

- The central server keeps events in SQLite.
- Failed pushes go into the retry queue.
- The server retries later.

If the same punch is sent twice:

- Duplicate `serialNo` records are ignored.
- Punches inside `DEDUP_WINDOW` are skipped.

## Quick Start Summary

Central server:

```powershell
python attendance_sync\server.py
```

PC A:

```powershell
python attendance_sync\edge_agent.py
```

PC B:

```powershell
python attendance_sync\edge_agent.py
```

Default push interval:

```env
POLL_INTERVAL=600
```
