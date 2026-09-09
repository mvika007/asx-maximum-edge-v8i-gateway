import os, time, math, hashlib
from datetime import datetime, timezone
from typing import Any

import httpx
from mcp.server import MCPServer

mcp = MCPServer("ASX MAXIMUM EDGE V8-I Live Data Gateway")

API_URL = os.getenv(
    "ASX_API_URL",
    "https://migizitech.wixsite.com/asxprices/_functions/getasxprices",
)
API_KEY = os.getenv("ASX_API_KEY", "free")
DEFAULT_SYMBOLS = ["CBA", "BHP", "WGX", "CBE", "WLC"]

def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()

def finite_number(x):
    try:
        v = float(x)
        return v if math.isfinite(v) else None
    except Exception:
        return None

def pick(d, *keys):
    for k in keys:
        if isinstance(d, dict) and k in d:
            return d[k]
    return None

def normalize_symbol(s: str) -> str:
    return s.upper().replace(".AX", "").strip()

def extract_records(payload: Any):
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for k in ("data", "results", "quotes", "stocks", "items", "response"):
            v = payload.get(k)
            if isinstance(v, list):
                return v
            if isinstance(v, dict):
                return [v]
        return [payload]
    return []

def normalize_record(r: dict, requested_symbol: str):
    symbol = normalize_symbol(str(pick(r, "symbol", "Symbol", "code", "Code", "ticker", "Ticker") or requested_symbol))
    price = finite_number(pick(r, "price", "Price", "last", "Last", "currentPrice", "CurrentPrice"))
    change = finite_number(pick(r, "chg", "Chg", "change", "Change"))
    change_pct = finite_number(pick(r, "chgPc", "ChgPc", "changePct", "ChangePct", "changePercent", "ChangePercent"))
    volume = finite_number(pick(r, "trdVol", "TrdVol", "volume", "Volume", "tradeVolume"))
    high = finite_number(pick(r, "high", "High", "dayHigh", "DayHigh"))
    low = finite_number(pick(r, "low", "Low", "dayLow", "DayLow"))
    status = pick(r, "status", "Status", "state", "State")
    return {
        "symbol": symbol,
        "price": price,
        "change": change,
        "change_pct": change_pct,
        "volume": volume,
        "high": high,
        "low": low,
        "status": status,
        "raw_hash": hashlib.sha256(json_bytes(r)).hexdigest(),
        "source_record": r,
    }

def json_bytes(x):
    import json
    return json.dumps(x, sort_keys=True, separators=(",", ":"), default=str).encode()

def validate_quote(q: dict):
    errors = []
    if q["price"] is None or q["price"] <= 0:
        errors.append("invalid_or_missing_price")
    if q["volume"] is not None and q["volume"] < 0:
        errors.append("negative_volume")
    if q["high"] is not None and q["low"] is not None and q["high"] < q["low"]:
        errors.append("high_below_low")
    if q["price"] is not None and q["high"] is not None and q["price"] > q["high"]:
        errors.append("price_above_high")
    if q["price"] is not None and q["low"] is not None and q["price"] < q["low"]:
        errors.append("price_below_low")
    if q["change"] is not None and q["change_pct"] is not None:
        # Do not fail on percentage unless previous-close reconstruction is available.
        pass
    return errors

def classify(freshness_seconds, complete, errors, repeated_ok=True):
    if errors:
        return "RED"
    if not complete:
        return "UNKNOWN"
    if freshness_seconds is None:
        return "AMBER"
    if freshness_seconds <= 60 and repeated_ok:
        return "GREEN"
    if freshness_seconds <= 300:
        return "AMBER"
    if freshness_seconds <= 900:
        return "ORANGE"
    return "RED"

async def fetch_quotes(symbols):
    params = {
        "apikey": API_KEY,
        "symbols": ",".join(symbols),
        "group": "core",
    }
    received_at = now_utc()
    t0 = time.monotonic()
    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
        response = await client.get(API_URL, params=params)
        response.raise_for_status()
        payload = response.json()
    elapsed_ms = round((time.monotonic() - t0) * 1000, 2)
    records = extract_records(payload)

    by_symbol = {}
    for r in records:
        if not isinstance(r, dict):
            continue
        q = normalize_record(r, "")
        if q["symbol"]:
            by_symbol[q["symbol"]] = q

    result = []
    for s in symbols:
        ns = normalize_symbol(s)
        q = by_symbol.get(ns)
        if q is None:
            result.append({
                "symbol": ns,
                "present": False,
                "validation_errors": ["symbol_missing_from_response"],
            })
        else:
            errs = validate_quote(q)
            q["present"] = True
            q["validation_errors"] = errs
            result.append(q)

    return {
        "gateway_received_at": received_at,
        "request_latency_ms": elapsed_ms,
        "source": "ASX Equity Stocks API",
        "api_url": API_URL,
        "symbols": result,
        "raw_record_count": len(records),
    }

@mcp.tool()
async def asx_get_quotes(symbols: list[str] | None = None) -> dict:
    """Fetch and validate a batch of ASX equity quotes."""
    syms = [normalize_symbol(x) for x in (symbols or DEFAULT_SYMBOLS)]
    return await fetch_quotes(syms)

@mcp.tool()
async def asx_get_quote(symbol: str) -> dict:
    """Fetch and validate one ASX equity quote."""
    return await fetch_quotes([normalize_symbol(symbol)])

@mcp.tool()
async def asx_get_health() -> dict:
    """Return gateway health and configuration without claiming market-data freshness."""
    return {
        "gateway": "ASX MAXIMUM EDGE V8-I Live Data Gateway",
        "status": "READY",
        "timestamp_utc": now_utc(),
        "upstream": API_URL,
        "configured_symbols": DEFAULT_SYMBOLS,
        "execution_grade": "NOT_GRANTED",
        "note": "GREEN is an integrity classification only; it does not authorize a trade.",
    }

@mcp.tool()
async def asx_run_gateway_test() -> dict:
    """Run the Tier-1 acceptance test: 3 consecutive snapshots of CBA/BHP/WGX/CBE/WLC."""
    symbols = DEFAULT_SYMBOLS
    snapshots = []
    failures = []

    for i in range(3):
        try:
            snap = await fetch_quotes(symbols)
            snapshots.append(snap)
        except Exception as e:
            failures.append({"snapshot": i + 1, "error": repr(e)})
        if i < 2:
            await __import__("asyncio").sleep(2)

    per_symbol = {}
    for s in symbols:
        observations = []
        for snap in snapshots:
            for q in snap.get("symbols", []):
                if q.get("symbol") == s:
                    observations.append(q)
        present = sum(1 for q in observations if q.get("present"))
        valid = sum(1 for q in observations if q.get("present") and not q.get("validation_errors"))
        per_symbol[s] = {
            "snapshots_observed": len(observations),
            "present_count": present,
            "valid_count": valid,
            "pass": len(observations) == 3 and valid == 3,
        }

    overall = (
        len(snapshots) == 3
        and not failures
        and all(v["pass"] for v in per_symbol.values())
    )

    return {
        "test": "V8-I Tier-1 Five-Symbol Acceptance Test",
        "run_at_utc": now_utc(),
        "symbols": symbols,
        "snapshots_completed": len(snapshots),
        "failures": failures,
        "per_symbol": per_symbol,
        "overall_pass": overall,
        "promotion": "TIER-1 ELIGIBLE" if overall else "DO NOT PROMOTE",
        "important": "This test validates acquisition/repeatability/field integrity. It does not establish true exchange-feed latency unless the upstream source supplies a trustworthy source timestamp.",
    }

if __name__ == "__main__":
    mcp.run(
        transport="streamable-http",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        streamable_http_path="/mcp",
        stateless_http=True,
        json_response=True,
    )
