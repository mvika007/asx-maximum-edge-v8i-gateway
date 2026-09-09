# ASX MAXIMUM EDGE™ V8-I — Data Gateway V3.2

## Purpose
V3.2 is the Free-plan-compatible timestamp validation build. It removes the multi-symbol REST batch endpoint from the core validation path and uses iTick `/stock/quote` one symbol at a time. It also adds an iTick stock WebSocket capability test so streaming can be evaluated without consuming REST calls.

## V3.2 architecture
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
Optional diagnostic for `/stock/quotes`. A failure is classified as batch capability unavailable/restricted and does not fail the core V3.2 timestamp architecture.

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
