# ASX MAXIMUM EDGE™ V8-I Data Gateway — V3.4.3

## MCP Contract Isolation Edition

### Purpose
V3.4.3 isolates the WebSocket streaming acceptance tool from MCP structured-output validation.

### Key change
`asx_run_websocket_streaming_acceptance` now returns **only a plain text string** containing strict JSON. It no longer returns a Python `dict` to the MCP framework.

The WebSocket engine itself is unchanged.

### Diagnostic default
The default acceptance duration is **30 seconds** rather than 120 seconds. This is intentional: the first invocation is a hosted-MCP contract diagnostic and should complete within common request timeouts.

Explicit durations remain supported from 1 to 1800 seconds.

### New diagnostic signal
The response includes:
- `gateway`: V3.4.3
- `mcp_contract`: `PLAIN_TEXT_JSON`
- `request`: effective parameters received by the tool
- `engine_result`: JSON-safe WebSocket acceptance result

### Interpretation
- If `asx_mcp_echo` works and this tool still returns MCP `-32603`, the failure is likely in the hosted execution/transport path rather than nested structured-output serialization.
- If this tool returns normally, the MCP response-contract issue is isolated and the next step is a controlled explicit-parameter test.

### No execution authorization change
Data integrity and streaming acceptance never grant trading authorization.
