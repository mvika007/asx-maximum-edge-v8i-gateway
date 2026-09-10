# V3.5 Changelog

## MCP-safe asynchronous WebSocket acceptance architecture

- Replaced blocking endurance invocation with background asyncio jobs.
- Added `asx_start_websocket_acceptance` for immediate job creation.
- Added `asx_get_websocket_acceptance_status` for polling.
- Added `asx_get_websocket_acceptance_result` for completed reports.
- Retained `asx_run_websocket_streaming_acceptance` as a compatibility wrapper that starts a job immediately.
- Job responses are plain JSON strings to minimize MCP response-contract risk.
- Preserved the existing iTick WebSocket engine and its timestamp/freshness logic.
- Added explicit V3.5 gateway identity.
- Kept execution authorization independent and default-denied.
- Jobs are process-local; redeployment/restart clears active jobs.
