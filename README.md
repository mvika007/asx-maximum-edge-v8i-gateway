# ASX MAXIMUM EDGE™ V8-I Data Gateway V3.4.3

**MCP Contract Isolation Edition**

V3.4.3 is a diagnostic release focused specifically on the MCP response contract for the WebSocket streaming acceptance engine.

## Core change
`asx_run_websocket_streaming_acceptance` returns one plain text string containing strict JSON. The WebSocket measurement engine is unchanged.

## First deployment checks
1. Deploy this directory to the MCP host.
2. Call `asx_get_health` and confirm the gateway reports `V3.4.3`.
3. Call `asx_mcp_echo` and confirm `MCP_V3.4.3_OK`.
4. Call `asx_run_websocket_streaming_acceptance` with no parameters.

The default diagnostic run is 30 seconds.

## Explicit test
After the default test succeeds:

```json
{
  "duration_seconds": 30,
  "symbols": ["BHP"],
  "types": "quote"
}
```

For endurance validation, request the required duration explicitly (for example 600 seconds) only after the 30-second MCP contract test succeeds.

## Safety
A fresh timestamped quote or successful stream does not grant execution authorization and does not establish profitability, exchange licensing, or complete order-book coverage.
