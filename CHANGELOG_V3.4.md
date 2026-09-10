# V3.4.1 — MCP Transport Hardening

## Purpose
V3.4.1 is a corrective deployment build based on V3.3. It preserves the V3.3 data-integrity architecture while hardening the WebSocket streaming acceptance tool against MCP `-32603 Invalid response format` failures and argument/default substitution issues.

## Changes
- Reordered `asx_run_websocket_streaming_acceptance` parameters to the documented order: `duration_seconds`, `symbols`, `types`.
- Added explicit recursive JSON sanitisation for tool outputs.
- Converts datetime, bytes, tuples/sets, non-finite floats and supported model objects into strict JSON-safe primitives.
- Guarantees the acceptance tool returns a top-level JSON object even on tool execution failure.
- Explicitly normalises and echoes the effective request parameters.
- Uses explicit `None` handling instead of `duration_seconds or default`.
- Caps/validates duration at 1–1800 seconds.
- Deduplicates and normalises symbols.
- Normalises event types.
- Hardens the short WebSocket capability result with the same JSON boundary.
- No real iTick token is included.

## Important
V3.4.1 does not grant execution authorization. A successful WebSocket acceptance result remains a data-integrity result only. `NO_EVENT` remains distinct from stale data, and gateway latency is never used as market-data age.

## Recommended first test
Use the MCP tool with exact named arguments:

```json
{
  "duration_seconds": 30,
  "symbols": ["BHP"],
  "types": "quote"
}
```

Then progress to 120 seconds and finally the 600-second endurance test during the active ASX session.
