# ASX MAXIMUM EDGE™ V8-I Live Data Gateway

External MCP gateway for the first Tier-1 acquisition test.

## Target symbols

CBA, BHP, WGX, CBE, WLC

## Architecture

ASX Equity Stocks API -> external gateway -> validation -> MCP Streamable HTTP -> V8-I

## Deployment

The server uses the current MCP Python SDK v2 and Streamable HTTP at `/mcp`.

### Local smoke test

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python server.py
```

The MCP endpoint will be:

`http://localhost:8000/mcp`

### Cloud deployment

Deploy this repository to an MCP-compatible host such as Alpic. Configure:

- `ASX_API_URL`
- `ASX_API_KEY`
- `PORT` (usually supplied by the platform)

The deployed endpoint should be HTTPS and end in `/mcp`.

## Acceptance test

Call the MCP tool:

`asx_run_gateway_test`

It performs three consecutive batch acquisitions of:

- CBA
- BHP
- WGX
- CBE
- WLC

Promotion requires:

1. Three completed snapshots.
2. All five symbols present in all three.
3. Required numeric fields valid.
4. No validation errors.
5. No request failures.

A passing result says `TIER-1 ELIGIBLE`.

## Important limitation

The upstream ASX Equity Stocks API documentation describes its core data as approximately 30-second refresh/latency and warns that latency may be longer. The gateway therefore does NOT invent a source timestamp or claim exchange-grade real-time latency.

Until a trustworthy source timestamp/freshness measurement is available, the gateway must not be treated as execution-grade merely because its endpoint is reachable.

`GREEN` means the gateway's observed data passed its integrity checks; it does not mean a trading signal exists.
