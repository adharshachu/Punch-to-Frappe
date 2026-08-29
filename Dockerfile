FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN groupadd --system --gid 10001 appuser \
    && useradd --system --uid 10001 --gid appuser --home-dir /app --shell /usr/sbin/nologin appuser \
    && mkdir -p /app/data /app/logs \
    && chown -R appuser:appuser /app

COPY --chown=appuser:appuser requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=appuser:appuser attendance_sync ./attendance_sync
COPY --chown=appuser:appuser employee_map.json ./employee_map.json
COPY --chown=appuser:appuser export_punch_records.py generate_sync_keys.py ./

USER appuser

EXPOSE 8080

CMD ["python", "attendance_sync/server.py"]
