# ASX MAXIMUM EDGE™ V8-I Data Gateway V3.4.2
## MCP Contract Isolation Edition

V3.4.2 preserves the V3.3/V3.4 data architecture while isolating MCP response-format failures from WebSocket logic.

### Primary data architecture
- iTick AU is the primary timestamped quote source.
- iTick WebSocket quote/tick events use source timestamp `t` when present.
- Migizi remains secondary price corroboration without verified source timestamps.
- Execution authorization remains separate and defaults to NOT_GRANTED.

### MCP diagnostic tools
- `asx_get_health` — server/source health.
- `asx_mcp_echo` — minimal plain-string MCP contract test.
- `asx_run_websocket_streaming_acceptance` — WebSocket acceptance with fixed-shape MCP output.
- Existing quote, tick, depth, capability, breadth and batch diagnostics retained.

### Acceptance response
The acceptance tool returns a fixed-shape object with:
- gateway
- status
- request_json
- result_json
- execution_authorized
- started_at_utc
- completed_at_utc

`result_json` contains the complete JSON-serialized WebSocket acceptance result.

### Recommended first test
Run `asx_mcp_echo` with default parameters. Expected response:
`ASX MAXIMUM EDGE V8-I V3.4.2 ECHO: MCP_V3.4.2_OK`

Then run the WebSocket acceptance tool with no parameters.

### Production policy
A fresh timestamp does not itself establish exchange licensing, depth completeness, execution authorization or profitability. Do not promote to execution grade without the complete V8-I evidence gates.
