ASX MAXIMUM EDGE™ V8-I — DATA GATEWAY V2
A source-agnostic market-data validation layer for the MAXIMUM EDGE™ V8-I
intraday engine.
V2 objectives
Source abstraction
Per-field timestamps
Source timestamp vs gateway receipt timestamp
Measured data age
Automatic GREEN / AMBER / ORANGE / RED / UNKNOWN
Source-quality scoring
Multi-source cross-validation
Automatic rejection of stale/inconsistent observations
Separate quote and market-depth clocks
Execution authorization kept completely separate from data integrity
Current sources
Primary: ASX Equity Stocks / Migizi Tech
Configured with:
`ASX_API_URL`
`ASX_API_KEY`
The provider currently documents approximately 30-second refresh for core quote
fields, but that declared refresh interval is NOT treated as proof of current
data age. If the upstream response does not contain a trustworthy timestamp,
V2 reports freshness as `UNKNOWN`.
Independent corroboration: Yahoo Finance chart
Enabled by default with:
`ENABLE_YAHOO_SOURCE=true`
Yahoo Finance describes ASX market data as delayed. V2 therefore uses Yahoo
primarily for independent cross-validation and timestamp auditing. It does not
promote Yahoo to exchange-real-time execution data.
Integrity classes
`GREEN`: source timestamp verified and age <= 60 seconds
`AMBER`: source timestamp verified and age <= 180 seconds
`ORANGE`: source timestamp verified and age <= 900 seconds
`RED`: stale beyond threshold, invalid, inconsistent, or impossible timestamp
`UNKNOWN`: required source age cannot be verified
A declared provider refresh interval does not substitute for a source
timestamp.
Cross-validation
V2 compares independent observations only when:
both sources provide timestamps;
timestamps are within 120 seconds;
both prices are available.
Default maximum price discrepancy is 0.50%.
If observations cannot be compared because timestamps are absent or too far
apart, status is `NOT_COMPARABLE`, not PASS.
Execution authorization
Execution authorization is intentionally separate.
`V8I_EXECUTION_AUTHORIZED` defaults to `false`.
Even if a source receives a GREEN integrity classification, the gateway never
turns that into trading permission.
The gateway reports:
data integrity
source quality
freshness
cross-validation
execution authorization state
but it does not make the trading decision.
MCP tools
`asx_get_quotes`
`asx_get_quote`
`asx_get_depth`
`asx_get_health`
`asx_run_gateway_test`
Alpic build settings
Install command:
`uv venv && uv pip install -r requirements.txt`
Start command:
`uv run server.py`
Runtime:
Python 3.13
Transport:
streamable-http
Environment variables
Required:
```text
ASX_API_URL=https://migizitech.wixsite.com/asxprices/_functions/getasxprices
ASX_API_KEY=free
```
Recommended:
```text
ENABLE_YAHOO_SOURCE=true
V8I_EXECUTION_AUTHORIZED=false
V8I_DEFAULT_SYMBOLS=CBA,BHP,WGX,CBE,WLC
V8I_GREEN_MAX_AGE_SECONDS=60
V8I_AMBER_MAX_AGE_SECONDS=180
V8I_ORANGE_MAX_AGE_SECONDS=900
V8I_CROSS_MAX_TIME_DELTA_SECONDS=120
V8I_CROSS_MAX_PRICE_DIFF_PCT=0.50
```
V2 acceptance sequence
Deploy successfully.
Run `asx_get_health`.
Run `asx_get_quotes` for CBA/BHP/WGX/CBE/WLC.
Run `asx_get_depth` separately.
Run `asx_run_gateway_test`.
Inspect source timestamps and freshness classifications.
Confirm cross-validation status.
Confirm execution authorization remains separate.
Only then connect V2 to V8-I.
Safety principle
Successful API acquisition is not the same thing as verified live-market
freshness.
V2 is intentionally conservative:
NO TIMESTAMP → UNKNOWN
STALE → REJECT
INCONSISTENT → REJECT
GREEN ≠ BUY
DATA INTEGRITY ≠ EXECUTION AUTHORIZATION
