# ASX MAXIMUM EDGE™ V8-I Data Gateway V3.5

## Asynchronous WebSocket Acceptance Edition

V3.5 replaces the blocking WebSocket endurance invocation with a background-job architecture. MCP requests return immediately; the WebSocket stream runs in the gateway process and is inspected through status/result tools.

### Workflow
1. `asx_start_websocket_acceptance(duration_seconds, symbols, types)` → returns a `job_id` immediately.
2. `asx_get_websocket_acceptance_status(job_id)` → poll until `COMPLETE`, `ERROR`, or `CANCELLED`.
3. `asx_get_websocket_acceptance_result(job_id)` → retrieve the completed result.

`asx_run_websocket_streaming_acceptance` remains as a compatibility wrapper and now starts the asynchronous job rather than blocking.

### Diagnostic tool
`asx_mcp_echo` remains available and returns plain text.

### Important
- Jobs are process-local in this build. A restart/redeploy clears in-memory jobs.
- WebSocket testing consumes no iTick REST calls.
- A successful stream does not grant execution authorization.
- Run acceptance during an active ASX market session for meaningful event-coverage testing.
- `NO_EVENT` is distinct from `STALE_EVENT`.
