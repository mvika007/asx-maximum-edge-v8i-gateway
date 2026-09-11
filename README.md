# ASX MAXIMUM EDGE™ V8-I — Data Gateway V3.6

V3.6 is the WebSocket acceptance reliability build. It preserves the V3.x timestamped iTick architecture and V3.5.1 diagnostic path while fixing the main acceptance-job observability weaknesses identified in testing.

## V3.6 changes
- Persistent filesystem-backed acceptance-job manifests (configurable with `ASX_V8I_JOB_STATE_DIR`).
- Job lifecycle: STARTED → RUNNING → COMPLETE / FAILED / CANCELLED; stale RUNNING manifests are reported as ORPHANED rather than silently becoming NOT_FOUND.
- Heartbeat telemetry for long-running jobs.
- Explicit requested duration vs connection/collection timing telemetry.
- WebSocket opening-handshake retry with bounded attempts/backoff (`ASX_V8I_WS_CONNECT_ATTEMPTS`, `ASX_V8I_WS_RETRY_BACKOFF_SECONDS`).
- Existing source timestamp `t` freshness logic, timestamp progression, duplicate detection, event gaps, per-symbol coverage and execution authorization separation are retained.
- No REST quota is consumed by WebSocket tests.

## Acceptance ladder
1. Short 60-second single-symbol BHP acceptance.
2. 300-second single-symbol endurance.
3. 600-second single-symbol endurance.
4. Only after single-symbol endurance passes should multi-symbol streaming be evaluated.

A diagnostic PASS proves connection/authentication/subscription/first event capability. It does not prove sustained stream quality or execution authorization.

## Persistent jobs
The acceptance tools are:
- `asx_start_websocket_acceptance`
- `asx_get_websocket_acceptance_status`
- `asx_get_websocket_acceptance_result`
- `asx_websocket_job_manifest`

The job manifest is persisted before execution begins. A process restart can preserve the evidence that a job existed; it cannot falsely claim that a lost task completed. Such a stale RUNNING job is reported as ORPHANED.

## Freshness
- GREEN <= 60s
- AMBER <= 180s
- ORANGE <= 900s
- RED = invalid/stale/inconsistent/future timestamp
- UNKNOWN = source age cannot be verified

`request_latency_ms` is never market-data age. Price corroboration never proves freshness.

## Deployment
- Python 3.13
- `uv venv && uv pip install -r requirements.txt`
- `uv run server.py`
- streamable HTTP MCP server

Never commit the real iTick token. Configure it only through the deployment environment.
