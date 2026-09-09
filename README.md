# ASX MAXIMUM EDGE™ V8-I — Data Gateway V3.3

## Purpose
V3.3 is the Free-plan-compatible timestamp validation build. It removes the multi-symbol REST batch endpoint from the core validation path and uses iTick `/stock/quote` one symbol at a time. It also adds an iTick stock WebSocket capability test so streaming can be evaluated without consuming REST calls.

## V3.3 architecture
- **Primary REST:** iTick `/stock/quote` — one symbol per call; source field `t` is the latest-trade timestamp.
- **Primary streaming test:** iTick stock WebSocket `wss://api-free.itick.org/stock`.
- **Secondary:** Migizi for price corroboration; no verified source timestamp in this integration.
- **Yahoo:** disabled by default and reference-only.
- **Execution authorization:** always separate; defaults to `NOT_GRANTED`.

## Tools
### `asx_get_quote(symbol)`
Single-symbol timestamped quote. Uses `/stock/quote` and therefore does not depend on the problematic multi-symbol batch endpoint.

### `asx_get_quotes(symbols)`
Free-plan-compatible single-symbol collection. Each requested symbol consumes one REST call. The gateway refuses the request if its local soft budget cannot cover all symbols.

### `asx_run_gateway_test(test_symbol="BHP")`
Three single-symbol `/stock/quote` calls, spaced by two seconds. This proves timestamp presence, measured age, GREEN/AMBER/RED classification, and repeatability for one symbol. It is the Tier-1 timestamp gate.

### `asx_run_breadth_test(symbols)`
One single-symbol call per requested symbol. Run separately from the 3-call repeatability test because Free-plan REST calls are limited.

### `asx_run_websocket_test(symbols, types="quote")`
Tests connection, authentication, subscription acknowledgement, streaming events, source timestamps, and heartbeat behavior. It does **not** consume REST quota.

### `asx_test_batch_capability(symbols)`
Optional diagnostic for `/stock/quotes`. A failure is classified as batch capability unavailable/restricted and does not fail the core V3.3 timestamp architecture.

### `asx_get_ticks`, `asx_get_depth`, `asx_get_health`
Retained from V3.1. Depth remains a separate clock; if the upstream depth payload lacks a source timestamp, freshness is `UNKNOWN`.

## Freshness
- GREEN <= 60s
- AMBER <= 180s
- ORANGE <= 900s
- RED = invalid, stale, inconsistent, or future timestamp
- UNKNOWN = source age cannot be verified

`request_latency_ms` is never used as market-data age.

## WebSocket interpretation
A successful WebSocket test proves that the configured account can connect, authenticate, subscribe and receive the selected stream. iTick documents quote and tick messages with timestamp `t`; its documented depth payload does not include a timestamp, so depth remains UNKNOWN unless an upstream timestamp is independently established.

A successful stream does not by itself establish exchange-data licensing, completeness, redistribution rights, execution authorization, or trading profitability.

## Free-plan discipline
The gateway tracks a process-local rolling REST budget and uses a safety margin. Provider-side limits may differ. Do not run repeated tests unnecessarily. The WebSocket test is the preferred capability test when evaluating continuous streaming.

## Deployment
- Python 3.13
- `uv venv && uv pip install -r requirements.txt`
- `uv run server.py`
- streamable HTTP MCP server

Never commit the real iTick token. Put it only in the deployment environment.

## V3.3 WebSocket Streaming Acceptance Engine

V3.3 adds a true streaming-quality acceptance engine rather than merely extending the V3.2 timeout.

### Tool
`asx_run_websocket_streaming_acceptance`

Default: BHP,CBA,WGX; quote events; 120 seconds. For the intended endurance run use `duration_seconds=600`.

The engine records every observed market-data event and separately classifies:
- `FRESH_STREAM`: at least one GREEN event and advancing source timestamps.
- `FRESH_EVENT`: GREEN event observed but timestamp progression not yet proven.
- `STALE_STREAM`: events received but all observed events are outside GREEN.
- `NO_EVENT`: no market-data event observed for that symbol; this is not treated as stale.

It measures per-symbol event counts, quote/tick/depth counts, minimum/median/maximum source age, maximum inter-event gap, event rate, timestamp progression and duplicate timestamps. It rejects future/invalid timestamps and never infers freshness from gateway latency.

A streaming PASS requires connection, authentication, subscription, at least one fresh event, advancing source timestamps, and no invalid timestamp events. This remains a data-integrity test only; exchange licensing and execution authorization are separate controls.
