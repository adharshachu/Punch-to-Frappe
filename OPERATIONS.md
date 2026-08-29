# Production Operations Runbook

This stack is sized for a small single-host deployment. It intentionally limits
Punch Sync to 768 MiB, PostgreSQL to 384 MiB, HRMS to 256 MiB, and Nginx to 64 MiB.
Do not raise a limit until measurements identify the responsible workload.

## Required production inputs

1. Copy the three templates:

   Install `apache2-utils` first if the host does not provide `htpasswd`.

   ```bash
   cp examples/env.compose.example .env
   cp examples/env.server.docker.example .env.server
   cp examples/env.punch.secrets.example .env.punch.secrets
   chmod 600 .env .env.punch.secrets
   htpasswd -B -c .htpasswd admin
   chmod 600 .htpasswd
   ```

2. Replace every `replace_me`, `change_this`, and example credential. Use immutable
   image tags or digests in `PUNCH_SYNC_IMAGE` and `HRMS_ATTENDANCE_IMAGE`.
3. Keep the PostgreSQL password identical in `.env` and the `POSTGRES_DSN` stored
   in `.env.punch.secrets`. Keep only non-secret, dashboard-editable values in
   `.env.server`. The `.htpasswd` file protects only the host-local Nginx
   administration endpoint; it is not an application environment requirement.
4. Confirm the Let's Encrypt certificate exists under
   `/etc/letsencrypt/live/attendance.codeace.org/` before starting Nginx.
5. Validate without changing running containers:

   ```bash
   docker compose config --quiet
   docker run --rm \
     -v "$PWD/nginx/default.conf:/etc/nginx/conf.d/default.conf:ro" \
     -v /etc/letsencrypt:/etc/letsencrypt:ro nginx:1.27-alpine nginx -t
   ```

## Baseline before a release

Save this output with the release record. It makes memory regressions and rollbacks
diagnosable instead of anecdotal.

```bash
date -Is
docker compose images --format json
free -h
swapon --show
docker stats --no-stream
docker inspect -f '{{.State.Pid}} {{.Config.Image}} {{.Image}}' punch-sync-server
PID=$(docker inspect -f '{{.State.Pid}}' punch-sync-server)
cat "/proc/$PID/smaps_rollup"
docker exec punch-sync-postgres psql -U punch_sync -d punch_sync -c \
  "select pid,state,wait_event_type,wait_event,now()-query_start as age,left(query,160) from pg_stat_activity where datname='punch_sync' order by query_start;"
journalctl -k --since '24 hours ago' | grep -Ei 'oom|out of memory|killed process' || true
curl -sS -o /dev/null -w 'punch-health code=%{http_code} time=%{time_total}s bytes=%{size_download}\n' \
  http://127.0.0.1:8090/health
curl -sS -o /dev/null -w 'hrms-login code=%{http_code} time=%{time_total}s bytes=%{size_download}\n' \
  https://attendance.codeace.org/login
```

If the host has no swap, schedule creation of a 1 GiB swap file as a separate,
approved host change. Swap is an OOM guardrail, not a performance fix.

## Controlled deploy

```bash
docker compose pull
docker compose config --quiet
docker compose up -d --no-build
docker compose ps
docker compose logs --tail 100 postgres punch-sync-server hrms-attendance nginx
```

Only containers whose image/config changed are recreated. Never use `:latest`.
Do not mount `/var/run/docker.sock`; restart containers from the host.

## Verification

1. Confirm all four health checks pass and HTTPS redirects/headers are correct.
2. Load the dashboard once, then repeat the baseline `docker stats`, `smaps_rollup`,
   PostgreSQL activity, and timed `curl` commands.
3. Run 20 serial reloads and three concurrent browser sessions. Record peak working
   set, swap-in/out, response time, response bytes, container restarts, and 5xx count.
4. Accept the release only when Punch Sync stays below 500 MiB peak, swap does not
   grow, no container restarts or OOM events occur, and warm summary p95 is below
   one second.

Container JSON logs rotate at 10 MiB with three files. Application logs should stay
on stdout (`LOG_FILE=`) so they obey this bound.

## Restart and rollback

Restart only the affected service after an editable configuration change:

```bash
docker compose restart punch-sync-server
```

For rollback, restore the previous immutable image values in `.env`, then recreate
only the application services:

```bash
docker compose pull punch-sync-server hrms-attendance
docker compose up -d --no-deps punch-sync-server hrms-attendance
docker compose ps
docker compose logs --tail 100 punch-sync-server hrms-attendance
```

Do not delete or recreate `root_postgres_data` during an application rollback.
If a database migration is not backward-compatible, use its separately reviewed
database rollback procedure before changing application images.
