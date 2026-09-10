# V3.4.2 — MCP Contract Isolation Edition

## Purpose
V3.4.2 isolates the recurring `MCP error -32603: Invalid response format` from the WebSocket engine.

## Changes
- Added `asx_mcp_echo`, a minimal plain-string MCP diagnostic.
- Changed `asx_run_websocket_streaming_acceptance` to return a fixed-shape object containing primitive fields only.
- The complete WebSocket acceptance result is encoded inside `result_json` rather than exposed as a dynamically-shaped nested MCP result.
- Added explicit request echo, start/end timestamps and execution authorization flag.
- Removed duplicate `@mcp.tool()` decoration from the health tool.
- Gateway identity is explicitly V3.4.2.

## Diagnostic interpretation
1. `asx_mcp_echo` fails → deployment/transport/tool registration problem.
2. Echo works but acceptance fails → acceptance implementation/schema problem.
3. Acceptance returns `status=COMPLETE` → MCP contract is functioning; inspect `result_json` for streaming gate outcome.

No execution authorization is granted by this release.
