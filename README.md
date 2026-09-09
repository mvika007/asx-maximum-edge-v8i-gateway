# ASX MAXIMUM EDGE™ V8-I — Data Gateway V3.1

## Purpose

V3 is the timestamped market-data integrity layer for the V8-I Professional Intraday Live Execution Edition.

The design separates:

1. upstream market-data timestamp;
2. gateway receipt timestamp;
3. measured market-data age;
4. HTTP/request latency;
5. source quality;
6. cross-source corroboration;
7. execution authorization.

**Request latency is never treated as market-data age.**

## Primary source: iTick

When `ITICK_API_TOKEN` is configured, iTick is the primary ASX quote source.
The iTick `/stock/quotes` response documents `t` as the timestamp of the latest trade and supports Australia (`region=AU`).

The gateway records:

- `source_timestamp`
- `gateway_received_at`
- `freshness.age_seconds`
- `freshness.classification`
- `source_timestamp_kind`
- `request_latency_ms`
- per-field timestamps
- raw-record SHA-256 hash

Freshness classes:

- GREEN: <= 60 s
- AMBER: <= 180 s
- ORANGE: <= 900 s
- RED: invalid/stale/future/inconsistent
- UNKNOWN: source age cannot be verified

Thresholds are configurable by environment variables.

## Secondary sources

### Migizi

Retained as an independent price corroboration source. In the current integration it does not provide a verified upstream source timestamp, so it cannot prove freshness. Agreement with iTick is reported as `PRICE_CORROBORATED`, not temporal cross-validation.

### Yahoo Finance

Optional and disabled by default in V3. It is reference/corroboration only and cannot grant execution-grade status.

## Depth

iTick REST depth is available through `asx_get_depth(symbol)`. The documented REST depth response provides bid/ask levels but does not expose a source timestamp. V3 therefore marks depth freshness `UNKNOWN` and never borrows the quote timestamp as a depth timestamp.

This is deliberate. A future streaming implementation may use timestamped events if the authorized iTick stock WebSocket actually provides sufficient timestamp information for the subscribed ASX depth stream.

## Tick data

`asx_get_ticks()` uses iTick `/stock/ticks` and preserves the transaction timestamp `t`. It is useful for validating whether the Australian feed is genuinely updating at transaction level.

## Free-plan conservation

The V3 Tier-1 gateway test uses one iTick batch quote request per snapshot. Three snapshots therefore use three REST calls, avoiding unnecessary depth/tick calls under a 5 calls/minute Free Plan configuration.

## Execution authorization

`V8I_EXECUTION_AUTHORIZED=false` by default.

No GREEN quote, quality score, source timestamp, cross-validation result, or gateway health state can authorize a trade.

## Environment setup

1. Copy `.env.example` to your deployment environment.
2. Put the iTick token in the environment variable `ITICK_API_TOKEN` only.
3. Never place the token in `server.py`, README, GitHub, screenshots, or chat.
4. Deploy with:

```text
Install: uv venv && uv pip install -r requirements.txt
Start:   uv run server.py
Python:  3.13
Transport: streamable-http
```

## MCP tools

- `asx_get_quotes(symbols)`
- `asx_get_quote(symbol)`
- `asx_get_ticks(symbols)`
- `asx_get_depth(symbol)`
- `asx_get_health()`
- `asx_run_gateway_test()`

## Promotion rule

V3 is **not** declared execution-grade merely because iTick returns timestamps. Promotion requires empirical testing of:

- timestamp freshness;
- repeated quote consistency;
- Australian symbol coverage;
- source update behaviour during live ASX trading;
- tick-level timestamps;
- depth behaviour;
- source/licensing terms;
- operational failure handling;
- independent corroboration where available.

ASX itself distinguishes direct real-time MarketSource data from third-party vendor feeds. ASX says MarketSource supplies real-time Level 1, Level 2, trades and instrument status, and can be accessed directly or through vendors. V3 therefore treats iTick as a candidate third-party source until empirically and contractually validated for the intended use.


## V3.1 diagnostic/rate-limit hardening

V3.1 retains the V3 timestamp architecture and adds fail-closed diagnostics for iTick batch quote acquisition. The acceptance test now records HTTP status, provider code/message, returned and missing symbols, raw `t` presence, parsed timestamps, rate-limit telemetry, response previews on HTTP errors, and the gateway's process-local rolling call budget. It will not silently convert an API/rate-limit/provider failure into a "zero verified timestamps" result.

The Free Plan is documented by iTick as 5 REST calls/minute. V3.1 reserves three calls before the Tier-1 test and applies a one-call safety margin by default. The process-local budget is diagnostic only and does not replace the provider's server-side limit.

V3.1 also fixes two integrity issues in the V3 implementation: batch response dictionary keys are retained as symbol fallbacks, and measured freshness uses the actual source HTTP response receipt time rather than a timestamp captured before the source request.
