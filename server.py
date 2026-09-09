import asyncio
import hashlib
import json
import math
import os
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Optional
from collections import deque

import httpx
from mcp.server import MCPServer

mcp = MCPServer("ASX MAXIMUM EDGE V8-I Data Gateway V3.1")

# ---------------------------------------------------------------------------
# V3 purpose
# ---------------------------------------------------------------------------
# V3 makes iTick the primary timestamped ASX quote source when configured.
# Migizi is retained as a secondary/corroboration source. Yahoo is optional
# reference-only corroboration. Data integrity NEVER grants execution rights.
#
# Critical rule: API request latency is NOT market-data age. Age is measured
# from the upstream source timestamp (iTick `t`) to the gateway receipt time.
# If the source timestamp is missing, freshness is UNKNOWN and the observation
# cannot be promoted to execution-grade.
# ---------------------------------------------------------------------------

ITICK_BASE_URL = os.getenv("ITICK_BASE_URL", "https://api-free.itick.org/stock").rstrip("/")
ITICK_TOKEN = os.getenv("ITICK_API_TOKEN", "").strip()
ITICK_REGION = os.getenv("ITICK_REGION", "AU").upper()
ITICK_EXCHANGE = os.getenv("ITICK_EXCHANGE", "").strip()

MIGIZI_URL = os.getenv(
    "ASX_API_URL",
    "https://migizitech.wixsite.com/asxprices/_functions/getasxprices",
)
MIGIZI_KEY = os.getenv("ASX_API_KEY", "free")

YAHOO_BASE_URL = os.getenv(
    "YAHOO_CHART_URL", "https://query1.finance.yahoo.com/v8/finance/chart"
)
ENABLE_YAHOO = os.getenv("ENABLE_YAHOO_SOURCE", "false").lower() in {
    "1", "true", "yes", "on"
}
ENABLE_MIGIZI = os.getenv("ENABLE_MIGIZI_SOURCE", "true").lower() in {
    "1", "true", "yes", "on"
}

DEFAULT_SYMBOLS = [
    s.strip().upper()
    for s in os.getenv("V8I_DEFAULT_SYMBOLS", "CBA,BHP,WGX,CBE,WLC").split(",")
    if s.strip()
]

# Freshness policy. These are intentionally conservative for intraday use.
GREEN_MAX_AGE = float(os.getenv("V8I_GREEN_MAX_AGE_SECONDS", "60"))
AMBER_MAX_AGE = float(os.getenv("V8I_AMBER_MAX_AGE_SECONDS", "180"))
ORANGE_MAX_AGE = float(os.getenv("V8I_ORANGE_MAX_AGE_SECONDS", "900"))
FUTURE_TOLERANCE = float(os.getenv("V8I_FUTURE_TIMESTAMP_TOLERANCE_SECONDS", "5"))

# Cross-source price corroboration. A timestamped iTick quote can be compared
# to a timestamp-less source only as PRICE_CORROBORATION, never as temporal
# cross-validation.
CROSS_MAX_TIME_DELTA = float(os.getenv("V8I_CROSS_MAX_TIME_DELTA_SECONDS", "120"))
CROSS_MAX_PRICE_DIFF_PCT = float(os.getenv("V8I_CROSS_MAX_PRICE_DIFF_PCT", "0.50"))

HTTP_TIMEOUT = float(os.getenv("V8I_HTTP_TIMEOUT_SECONDS", "15"))
ITICK_FREE_REST_LIMIT_PER_MINUTE = int(os.getenv("ITICK_FREE_REST_LIMIT_PER_MINUTE", "5"))
ITICK_RATE_LIMIT_SAFETY_MARGIN = int(os.getenv("ITICK_RATE_LIMIT_SAFETY_MARGIN", "1"))
ITICK_TEST_MIN_REMAINING_CALLS = int(os.getenv("ITICK_TEST_MIN_REMAINING_CALLS", "3"))

# Process-local rolling-window accounting. This is diagnostic protection, not a
# substitute for the provider's server-side rate limiter. It prevents V3.1 from
# blindly firing a Tier-1 test when the Free Plan budget is already exhausted.
_ITICK_CALL_TIMES = deque()
_ITICK_CALL_LOCK = asyncio.Lock()

async def itick_budget_status(reserve_calls: int = 0) -> dict:
    now = time.monotonic()
    async with _ITICK_CALL_LOCK:
        while _ITICK_CALL_TIMES and now - _ITICK_CALL_TIMES[0] >= 60:
            _ITICK_CALL_TIMES.popleft()
        used = len(_ITICK_CALL_TIMES)
        effective_limit = max(0, ITICK_FREE_REST_LIMIT_PER_MINUTE - ITICK_RATE_LIMIT_SAFETY_MARGIN)
        remaining = max(0, effective_limit - used)
        return {
            "configured_limit_calls_per_minute": ITICK_FREE_REST_LIMIT_PER_MINUTE,
            "safety_margin_calls": ITICK_RATE_LIMIT_SAFETY_MARGIN,
            "effective_soft_limit_calls_per_minute": effective_limit,
            "tracked_calls_last_60s": used,
            "tracked_remaining_soft_budget": remaining,
            "reserve_requested": reserve_calls,
            "reserve_available": remaining >= reserve_calls,
            "next_budget_release_seconds": round(max(0.0, 60 - (now - _ITICK_CALL_TIMES[0])) if _ITICK_CALL_TIMES else 0.0, 2),
            "tracking_scope": "current_gateway_process only",
        }

async def record_itick_call() -> None:
    async with _ITICK_CALL_LOCK:
        now = time.monotonic()
        while _ITICK_CALL_TIMES and now - _ITICK_CALL_TIMES[0] >= 60:
            _ITICK_CALL_TIMES.popleft()
        _ITICK_CALL_TIMES.append(now)

# Execution authorization is completely independent and defaults to false.
EXECUTION_AUTHORIZED = os.getenv("V8I_EXECUTION_AUTHORIZED", "false").lower() in {
    "1", "true", "yes", "on"
}
EXECUTION_AUTHORITY_SOURCE = os.getenv(
    "V8I_EXECUTION_AUTHORITY_SOURCE", "external_compliance_risk_system"
)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: Optional[datetime]) -> Optional[str]:
    return dt.isoformat() if dt else None


def finite_number(value: Any) -> Optional[float]:
    try:
        value = float(value)
        return value if math.isfinite(value) else None
    except (TypeError, ValueError):
        return None


def normalize_symbol(symbol: str) -> str:
    return str(symbol).upper().replace(".AX", "").strip()


def json_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()


def sha256(value: Any) -> str:
    return hashlib.sha256(json_bytes(value)).hexdigest()


def pick(mapping: Any, *keys: str) -> Any:
    if not isinstance(mapping, dict):
        return None
    for key in keys:
        if key in mapping:
            return mapping[key]
    return None


def parse_timestamp(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        n = float(value)
        if n > 10_000_000_000:
            n /= 1000.0
        try:
            return datetime.fromtimestamp(n, tz=timezone.utc)
        except (ValueError, OSError, OverflowError):
            return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return parse_timestamp(float(text))
    except ValueError:
        pass
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


def extract_records(payload: Any) -> list[dict]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for key in ("data", "results", "quotes", "stocks", "items", "response"):
            value = payload.get(key)
            if isinstance(value, list):
                return [x for x in value if isinstance(x, dict)]
            if isinstance(value, dict):
                # iTick batch responses use data={"CBA": {...}, "BHP": {...}}
                if key == "data" and all(isinstance(v, dict) for v in value.values()):
                    return list(value.values())
                return [value]
        return [payload]
    return []


def classify_age(source_timestamp: Optional[datetime], gateway_received_at: datetime,
                 complete: bool, errors: list[str]) -> dict:
    if errors:
        return {"classification": "RED", "age_seconds": None, "reason": "validation_error"}
    if not complete:
        return {"classification": "UNKNOWN", "age_seconds": None, "reason": "incomplete_required_fields"}
    if source_timestamp is None:
        return {"classification": "UNKNOWN", "age_seconds": None, "reason": "source_timestamp_unavailable"}

    age = (gateway_received_at - source_timestamp).total_seconds()
    if age < -FUTURE_TOLERANCE:
        return {"classification": "RED", "age_seconds": round(age, 3), "reason": "source_timestamp_in_future"}
    age = max(age, 0.0)
    if age <= GREEN_MAX_AGE:
        c = "GREEN"
    elif age <= AMBER_MAX_AGE:
        c = "AMBER"
    elif age <= ORANGE_MAX_AGE:
        c = "ORANGE"
    else:
        c = "RED"
    return {"classification": c, "age_seconds": round(age, 3), "reason": "timestamp_verified"}


def execution_authorization() -> dict:
    return {
        "execution_grade": "AUTHORIZED" if EXECUTION_AUTHORIZED else "NOT_GRANTED",
        "authorized": EXECUTION_AUTHORIZED,
        "authority_source": EXECUTION_AUTHORITY_SOURCE,
        "note": "Data integrity never grants trading authorization. Execution authorization is a separate control.",
    }


def quality_score(q: "NormalizedQuote", freshness: dict, cross: dict) -> dict:
    score = 0
    reasons = []
    if q.price is not None and q.volume is not None and q.change_pct is not None:
        score += 20; reasons.append("required_fields_complete")
    else:
        reasons.append("required_fields_incomplete")
    if not q.validation_errors:
        score += 20; reasons.append("field_validation_pass")
    else:
        reasons.append("field_validation_failed")
    if q.source_timestamp_verified:
        if freshness["classification"] == "GREEN":
            score += 35; reasons.append("verified_fresh_timestamp")
        elif freshness["classification"] == "AMBER":
            score += 25; reasons.append("verified_timestamp_older")
        elif freshness["classification"] == "ORANGE":
            score += 10; reasons.append("verified_but_stale")
        else:
            reasons.append("timestamp_invalid_or_red")
    else:
        reasons.append("no_verified_source_timestamp")
    if q.request_latency_ms is not None:
        if q.request_latency_ms <= 1000:
            score += 10; reasons.append("gateway_request_latency_under_1s")
        elif q.request_latency_ms <= 3000:
            score += 5; reasons.append("gateway_request_latency_under_3s")
        else:
            reasons.append("gateway_request_latency_high")
    if cross.get("status") == "PASS":
        score += 15; reasons.append("temporal_cross_validation_pass")
    elif cross.get("status") == "PRICE_CORROBORATED":
        score += 5; reasons.append("price_corroborated_but_timestamp_not_comparable")
    else:
        reasons.append(cross.get("status", "cross_validation_not_run").lower())
    return {"score": min(score, 100), "reasons": reasons}


@dataclass
class NormalizedQuote:
    symbol: str
    price: Optional[float]
    open: Optional[float]
    previous_close: Optional[float]
    change: Optional[float]
    change_pct: Optional[float]
    volume: Optional[float]
    turnover: Optional[float]
    high: Optional[float]
    low: Optional[float]
    status: Any
    source_name: str
    source_timestamp: Optional[str]
    field_timestamps: dict[str, Optional[str]]
    source_timestamp_verified: bool
    source_timestamp_kind: str
    request_latency_ms: Optional[float]
    source_record_hash: str
    validation_errors: list[str]


class MarketDataSource(ABC):
    name: str
    independent: bool = True

    @abstractmethod
    async def get_quotes(self, symbols: list[str]) -> dict:
        raise NotImplementedError

    async def get_ticks(self, symbols: list[str]) -> dict:
        return {"source": self.name, "supported": False, "reason": "ticks_not_supported"}

    async def get_depth(self, symbol: str) -> dict:
        return {"source": self.name, "supported": False, "reason": "depth_not_supported"}


class ITickSource(MarketDataSource):
    name = "iTick"
    independent = True

    def __init__(self, base_url: str, token: str, region: str, exchange: str = ""):
        self.base_url = base_url
        self.token = token
        self.region = region
        self.exchange = exchange

    @property
    def configured(self) -> bool:
        return bool(self.token)

    async def _request(self, path: str, params: dict) -> tuple[Any, float, datetime, Optional[str], dict]:
        if not self.token:
            raise RuntimeError("ITICK_API_TOKEN is not configured")
        started = time.monotonic()
        url = f"{self.base_url}/{path.lstrip('/')}"
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, follow_redirects=True) as client:
            try:
                response = await client.get(
                    url,
                    params=params,
                    headers={"accept": "application/json", "token": self.token},
                )
                received_at = now_utc()
                latency_ms = round((time.monotonic() - started) * 1000, 2)
                await record_itick_call()
                body_text = response.text[:2000]
                try:
                    payload = response.json()
                except Exception:
                    payload = None
                telemetry = {
                    "url": url,
                    "endpoint": f"/{path.lstrip('/')}",
                    "http_status": response.status_code,
                    "request_latency_ms": latency_ms,
                    "gateway_received_at": iso(received_at),
                    "server_date_header": response.headers.get("date"),
                    "retry_after_header": response.headers.get("retry-after"),
                    "rate_limit_headers": {
                        k: v for k, v in response.headers.items()
                        if "rate" in k.lower() or "limit" in k.lower() or "remaining" in k.lower()
                    },
                    "response_body_preview": body_text if response.status_code >= 400 else None,
                }
                if response.status_code >= 400:
                    return payload if payload is not None else {"code": response.status_code, "msg": body_text}, latency_ms, received_at, response.headers.get("date"), telemetry
                return payload, latency_ms, received_at, response.headers.get("date"), telemetry
            except Exception:
                # Network/transport exceptions may occur before an HTTP response
                # exists; do not count an unissued request against the budget.
                raise

    def _normalize_quote(self, record: dict, requested: str, latency_ms: float) -> NormalizedQuote:
        symbol = normalize_symbol(pick(record, "s", "symbol", "Symbol", "code", "Code") or requested)
        source_ts = parse_timestamp(pick(record, "t", "timestamp", "Timestamp"))
        values = {
            "price": finite_number(pick(record, "ld", "price", "Price")),
            "open": finite_number(pick(record, "o", "open", "Open")),
            "previous_close": finite_number(pick(record, "p", "previousClose", "PreviousClose")),
            "change": finite_number(pick(record, "ch", "change", "Change")),
            "change_pct": finite_number(pick(record, "chp", "changePct", "ChangePct")),
            "volume": finite_number(pick(record, "v", "volume", "Volume")),
            "turnover": finite_number(pick(record, "tu", "turnover", "Turnover")),
            "high": finite_number(pick(record, "h", "high", "High")),
            "low": finite_number(pick(record, "l", "low", "Low")),
            "status": pick(record, "ts", "status", "Status"),
        }
        errors = []
        if values["price"] is None or values["price"] <= 0:
            errors.append("invalid_or_missing_price")
        if values["volume"] is not None and values["volume"] < 0:
            errors.append("negative_volume")
        if values["high"] is not None and values["low"] is not None and values["high"] < values["low"]:
            errors.append("high_below_low")
        if values["price"] is not None and values["high"] is not None and values["price"] > values["high"] + 1e-9:
            errors.append("price_above_high")
        if values["price"] is not None and values["low"] is not None and values["price"] < values["low"] - 1e-9:
            errors.append("price_below_low")
        if values["previous_close"] not in (None, 0) and values["change"] is not None:
            derived = values["price"] - values["previous_close"] if values["price"] is not None else None
            if derived is not None and abs(derived - values["change"]) > max(0.01, abs(values["price"]) * 0.001):
                errors.append("change_inconsistent_with_price_and_previous_close")
        return NormalizedQuote(
            symbol=symbol,
            price=values["price"], open=values["open"], previous_close=values["previous_close"],
            change=values["change"], change_pct=values["change_pct"], volume=values["volume"],
            turnover=values["turnover"], high=values["high"], low=values["low"], status=values["status"],
            source_name=self.name,
            source_timestamp=iso(source_ts),
            field_timestamps={k: iso(source_ts) for k in ("price","open","previous_close","change","change_pct","volume","turnover","high","low","status")},
            source_timestamp_verified=source_ts is not None,
            source_timestamp_kind="iTick_latest_trade_timestamp_t" if source_ts else "not_provided",
            request_latency_ms=latency_ms,
            source_record_hash=sha256(record),
            validation_errors=errors,
        )

    async def get_quotes(self, symbols: list[str]) -> dict:
        if not self.configured:
            return {"source": self.name, "available": False, "configured": False, "error": "ITICK_API_TOKEN not configured", "quotes": {}, "diagnostics": {"stage": "configuration"}}
        symbols = [normalize_symbol(x) for x in symbols]
        budget_before = await itick_budget_status()
        params = {"region": self.region, "codes": ",".join(symbols)}
        if self.exchange:
            params["exchange"] = self.exchange
        try:
            payload, latency_ms, received_at, server_date, telemetry = await self._request("quotes", params)
        except Exception as exc:
            return {
                "source": self.name, "available": False, "configured": True, "error": repr(exc), "quotes": {},
                "diagnostics": {"stage": "transport", "budget_before": budget_before, "exception": repr(exc)},
            }

        response_code = payload.get("code") if isinstance(payload, dict) else None
        response_msg = payload.get("msg") if isinstance(payload, dict) else None
        if response_code not in (None, 0):
            return {
                "source": self.name, "available": False, "configured": True,
                "error": response_msg or f"iTick code={response_code}", "quotes": {},
                "received_at": iso(received_at), "request_latency_ms": latency_ms,
                "endpoint": "/stock/quotes", "region": self.region,
                "diagnostics": {
                    "stage": "provider_response", "http_status": telemetry.get("http_status"),
                    "provider_code": response_code, "provider_message": response_msg,
                    "budget_before": budget_before, "budget_after": await itick_budget_status(),
                    "telemetry": telemetry,
                },
            }

        raw_data = payload.get("data") if isinstance(payload, dict) else None
        records_with_keys = []
        if isinstance(raw_data, dict):
            for key, value in raw_data.items():
                if isinstance(value, dict):
                    records_with_keys.append((str(key), value))
        elif isinstance(raw_data, list):
            records_with_keys = [("", x) for x in raw_data if isinstance(x, dict)]
        else:
            records = extract_records(raw_data)
            records_with_keys = [("", x) for x in records]

        requested_set = set(symbols)
        quotes = {}
        timestamp_presence = {}
        unmatched_records = []
        for key, record in records_with_keys:
            fallback_symbol = normalize_symbol(key) if key else ""
            q = self._normalize_quote(record, fallback_symbol, latency_ms)
            if not q.symbol and fallback_symbol:
                q.symbol = fallback_symbol
            timestamp_presence[q.symbol or fallback_symbol] = {
                "t_present": pick(record, "t", "timestamp", "Timestamp") is not None,
                "t_raw": pick(record, "t", "timestamp", "Timestamp"),
                "t_parsed": q.source_timestamp,
                "source_timestamp_verified": q.source_timestamp_verified,
            }
            symbol = q.symbol or fallback_symbol
            if symbol in requested_set:
                quotes[symbol] = q
            else:
                unmatched_records.append({"response_key": key, "resolved_symbol": symbol, "record_keys": sorted(record.keys())})

        missing_symbols = [s for s in symbols if s not in quotes]
        missing_timestamps = [s for s, q in quotes.items() if not q.source_timestamp_verified]
        budget_after = await itick_budget_status()
        diagnostics = {
            "stage": "parsed_successfully",
            "http_status": telemetry.get("http_status"),
            "provider_code": response_code,
            "provider_message": response_msg,
            "requested_symbols": symbols,
            "response_data_type": type(raw_data).__name__,
            "raw_record_count": len(records_with_keys),
            "returned_symbols": sorted(quotes.keys()),
            "missing_symbols": missing_symbols,
            "missing_timestamps": missing_timestamps,
            "timestamp_presence": timestamp_presence,
            "unmatched_records": unmatched_records,
            "budget_before": budget_before,
            "budget_after": budget_after,
            "telemetry": telemetry,
        }
        return {
            "source": self.name, "available": True, "configured": True,
            "received_at": iso(received_at), "request_latency_ms": latency_ms,
            "source_server_date_header": server_date,
            "endpoint": "/stock/quotes", "region": self.region,
            "quotes": quotes, "raw_record_count": len(records_with_keys),
            "diagnostics": diagnostics,
            "note": "iTick t is treated as the upstream latest-trade timestamp; gateway receipt and request latency are measured separately.",
        }

    async def get_ticks(self, symbols: list[str]) -> dict:
        if not self.configured:
            return {"source": self.name, "available": False, "error": "ITICK_API_TOKEN not configured"}
        params = {"region": self.region, "codes": ",".join(symbols)}
        if self.exchange:
            params["exchange"] = self.exchange
        try:
            payload, latency_ms, received_at, server_date, telemetry = await self._request("ticks", params)
        except Exception as exc:
            return {"source": self.name, "available": False, "error": repr(exc)}
        if payload.get("code") not in (None, 0):
            return {"source": self.name, "available": False, "error": payload.get("msg") or f"iTick code={payload.get('code')}"}
        data = payload.get("data") or {}
        records = list(data.values()) if isinstance(data, dict) else extract_records(data)
        out = []
        for record in records:
            out.append({
                "symbol": normalize_symbol(pick(record, "s", "symbol") or ""),
                "price": finite_number(pick(record, "ld", "price")),
                "volume": finite_number(pick(record, "v", "volume")),
                "source_timestamp": iso(parse_timestamp(pick(record, "t", "timestamp"))),
                "session": pick(record, "te", "session"),
                "direction": pick(record, "d", "direction"),
                "raw_record_hash": sha256(record),
            })
        return {
            "source": self.name, "available": True, "endpoint": "/stock/ticks",
            "gateway_received_at": iso(received_at), "request_latency_ms": latency_ms,
            "source_server_date_header": server_date, "ticks": out, "diagnostics": {"telemetry": telemetry, "budget": await itick_budget_status()},
            "note": "Tick timestamps are source transaction timestamps. Informational/licensing restrictions must be respected.",
        }

    async def get_depth(self, symbol: str) -> dict:
        if not self.configured:
            return {"source": self.name, "available": False, "error": "ITICK_API_TOKEN not configured"}
        params = {"region": self.region, "code": normalize_symbol(symbol)}
        if self.exchange:
            params["exchange"] = self.exchange
        try:
            payload, latency_ms, received_at, server_date, telemetry = await self._request("depth", params)
        except Exception as exc:
            return {"source": self.name, "available": False, "error": repr(exc)}
        if payload.get("code") not in (None, 0):
            return {"source": self.name, "available": False, "error": payload.get("msg") or f"iTick code={payload.get('code')}"}
        data = payload.get("data") or {}
        asks = data.get("a") or []
        bids = data.get("b") or []
        # iTick REST depth documentation does not expose a source timestamp in
        # the depth response. Therefore depth freshness is UNKNOWN by design.
        return {
            "source": self.name, "available": True, "supported": True,
            "symbol": normalize_symbol(symbol), "asks": asks, "bids": bids,
            "source_timestamp": None, "source_timestamp_verified": False,
            "gateway_received_at": iso(received_at), "request_latency_ms": latency_ms,
            "source_server_date_header": server_date, "diagnostics": {"telemetry": telemetry, "budget": await itick_budget_status()},
            "freshness": {"classification": "UNKNOWN", "age_seconds": None, "reason": "iTick_depth_response_has_no_documented_source_timestamp"},
            "note": "Do not infer depth age from quote timestamp or gateway request latency. Depth requires its own timestamped stream to become freshness-verifiable.",
        }


class MigiziSource(MarketDataSource):
    name = "ASX Equity Stocks / Migizi Tech"
    independent = True

    def __init__(self, api_url: str, api_key: str):
        self.api_url, self.api_key = api_url, api_key

    async def get_quotes(self, symbols: list[str]) -> dict:
        started = time.monotonic(); requested_at = now_utc()
        try:
            async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, follow_redirects=True) as client:
                response = await client.get(self.api_url, params={"apikey": self.api_key, "symbols": ",".join(symbols), "group": "core"})
                received_at = now_utc(); response.raise_for_status(); payload = response.json()
            latency = round((time.monotonic() - started) * 1000, 2)
        except Exception as exc:
            return {"source": self.name, "available": False, "error": repr(exc), "quotes": {}}
        records = extract_records(payload); quotes = {}
        for record in records:
            symbol = normalize_symbol(pick(record, "symbol","Symbol","code","Code","ticker","Ticker") or "")
            if symbol not in {normalize_symbol(s) for s in symbols}: continue
            price = finite_number(pick(record,"price","Price","last","Last","currentPrice","CurrentPrice"))
            change = finite_number(pick(record,"chg","Chg","change","Change"))
            chp = finite_number(pick(record,"chgPc","ChgPc","changePct","ChangePct","changePercent","ChangePercent"))
            volume = finite_number(pick(record,"trdVol","TrdVol","volume","Volume","tradeVolume"))
            high = finite_number(pick(record,"high","High","dayHigh","DayHigh")); low = finite_number(pick(record,"low","Low","dayLow","DayLow"))
            errors=[]
            if price is None or price <= 0: errors.append("invalid_or_missing_price")
            if volume is not None and volume < 0: errors.append("negative_volume")
            if high is not None and low is not None and high < low: errors.append("high_below_low")
            q=NormalizedQuote(symbol,price,finite_number(pick(record,"open","Open")),finite_number(pick(record,"prevClose","PrevClose","previousClose")),change,chp,volume,finite_number(pick(record,"trdVal","TrdVal","turnover")),high,low,pick(record,"status","Status"),self.name,None,{k:None for k in ("price","open","previous_close","change","change_pct","volume","turnover","high","low","status")},False,"not_provided",latency,sha256(record),errors)
            quotes[symbol]=q
        return {"source":self.name,"available":True,"received_at":iso(received_at),"request_latency_ms":latency,"requested_at":iso(requested_at),"quotes":quotes,"note":"Migizi source does not expose a verifiable upstream source timestamp in this integration; declared refresh is not substituted for age."}


class YahooChartSource(MarketDataSource):
    name = "Yahoo Finance chart"
    independent = True

    async def get_quotes(self, symbols: list[str]) -> dict:
        received_at=now_utc(); quotes={}; errors={}
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT,follow_redirects=True,headers={"User-Agent":"Mozilla/5.0"}) as client:
            for symbol in symbols:
                started=time.monotonic()
                try:
                    r=await client.get(f"{YAHOO_BASE_URL}/{normalize_symbol(symbol)}.AX",params={"interval":"1m","range":"1d","events":"div,splits"}); r.raise_for_status(); payload=r.json(); latency=round((time.monotonic()-started)*1000,2)
                    result=((payload.get("chart") or {}).get("result") or [None])[0]
                    if not result: raise ValueError("Yahoo returned no chart result")
                    meta=result.get("meta") or {}; ts=parse_timestamp(meta.get("regularMarketTime")); price=finite_number(meta.get("regularMarketPrice")); prev=finite_number(meta.get("previousClose")); vol=finite_number(meta.get("regularMarketVolume"))
                    if price is None: raise ValueError("Yahoo price unavailable")
                    change=price-prev if prev is not None else None; chp=(change/prev*100) if change is not None and prev else None
                    q=NormalizedQuote(normalize_symbol(symbol),price,finite_number(meta.get("regularMarketOpen")),prev,change,chp,vol,None,finite_number(meta.get("regularMarketDayHigh")),finite_number(meta.get("regularMarketDayLow")),meta.get("marketState"),self.name,iso(ts),{k:iso(ts) for k in ("price","open","previous_close","change","change_pct","volume","turnover","high","low","status")},ts is not None,"regularMarketTime" if ts else "not_provided",latency,sha256(result),[])
                    quotes[q.symbol]=q
                except Exception as exc: errors[normalize_symbol(symbol)]=repr(exc)
        return {"source":self.name,"available":bool(quotes),"received_at":iso(received_at),"quotes":quotes,"errors":errors,"note":"Yahoo ASX data is treated as delayed/reference-only; it cannot grant execution-grade status."}


def build_sources() -> list[MarketDataSource]:
    sources: list[MarketDataSource] = [ITickSource(ITICK_BASE_URL, ITICK_TOKEN, ITICK_REGION, ITICK_EXCHANGE)]
    if ENABLE_MIGIZI: sources.append(MigiziSource(MIGIZI_URL, MIGIZI_KEY))
    if ENABLE_YAHOO: sources.append(YahooChartSource())
    return sources


async def fetch_quotes(symbols: list[str]) -> dict:
    results=await asyncio.gather(*(s.get_quotes(symbols) for s in build_sources()))
    return {r["source"]:r for r in results}


def temporal_cross_validation(symbol: str, source_results: dict[str,dict]) -> dict:
    observations=[]
    for name,res in source_results.items():
        q=res.get("quotes",{}).get(symbol)
        if q: observations.append(q)
    if len(observations)<2:
        return {"status":"SOURCE_UNAVAILABLE","symbol":symbol,"sources_compared":[q.source_name for q in observations],"reason":"fewer_than_two_observations"}
    timestamped=[q for q in observations if q.source_timestamp_verified]
    if len(timestamped)<2:
        # Still perform a price corroboration, but explicitly do not call it
        # temporal validation.
        itick=next((q for q in timestamped if q.source_name=="iTick"),None)
        other=next((q for q in observations if q is not itick),None)
        if itick and other and itick.price is not None and other.price is not None:
            diff=abs(itick.price-other.price)/max(abs(itick.price),abs(other.price),1e-9)*100
            return {"status":"PRICE_CORROBORATED" if diff<=CROSS_MAX_PRICE_DIFF_PCT else "PRICE_DISAGREEMENT","symbol":symbol,"sources_compared":[itick.source_name,other.source_name],"price_difference_pct":round(diff,4),"reason":"secondary source has no verifiable source timestamp; temporal freshness remains unproven for that source"}
        return {"status":"NOT_COMPARABLE","symbol":symbol,"sources_compared":[q.source_name for q in observations],"reason":"insufficient timestamped independent observations"}
    pairs=[]
    for i,a in enumerate(timestamped):
        for b in timestamped[i+1:]:
            ta,tb=parse_timestamp(a.source_timestamp),parse_timestamp(b.source_timestamp)
            if not ta or not tb or a.price is None or b.price is None: continue
            delta=abs((ta-tb).total_seconds())
            if delta>CROSS_MAX_TIME_DELTA: continue
            diff=abs(a.price-b.price)/max(abs(a.price),abs(b.price),1e-9)*100
            pairs.append({"source_a":a.source_name,"source_b":b.source_name,"time_delta_seconds":round(delta,3),"price_a":a.price,"price_b":b.price,"price_difference_pct":round(diff,4),"pass":diff<=CROSS_MAX_PRICE_DIFF_PCT})
    if not pairs: return {"status":"NOT_COMPARABLE","symbol":symbol,"sources_compared":[q.source_name for q in observations],"reason":"timestamped observations too far apart or incomplete"}
    return {"status":"PASS" if all(p["pass"] for p in pairs) else "FAIL","symbol":symbol,"sources_compared":[q.source_name for q in observations],"pairs":pairs,"reason":"timestamped independent observations agree" if all(p["pass"] for p in pairs) else "timestamped independent observations disagree"}


def enrich(q: NormalizedQuote, gateway_received_at: datetime, cross: dict) -> dict:
    complete=q.price is not None and q.change_pct is not None and q.volume is not None
    freshness=classify_age(parse_timestamp(q.source_timestamp),gateway_received_at,complete,q.validation_errors)
    integrity=freshness["classification"]
    if cross.get("status") in {"FAIL","PRICE_DISAGREEMENT"}: integrity="RED"
    data=asdict(q)
    data.update({"freshness":freshness,"integrity_classification":integrity,"quality":quality_score(q,freshness,cross),"cross_validation":cross})
    return data


@mcp.tool()
async def asx_get_quotes(symbols: list[str] | None = None) -> dict:
    """V3.1 primary timestamped quote pipeline with source diagnostics and rate-limit telemetry."""
    syms=[normalize_symbol(x) for x in (symbols or DEFAULT_SYMBOLS) if normalize_symbol(x)]
    source_results=await fetch_quotes(syms)
    gateway_received_at=now_utc()
    output={}
    for symbol in syms:
        cross=temporal_cross_validation(symbol,source_results)
        output[symbol]={}
        for name,res in source_results.items():
            q=res.get("quotes",{}).get(symbol)
            if q is None:
                continue
            receipt=parse_timestamp(res.get("received_at")) or gateway_received_at
            output[symbol][name]=enrich(q,receipt,cross)
    return {
        "gateway":"ASX MAXIMUM EDGE V8-I Data Gateway V3.1",
        "gateway_received_at":iso(gateway_received_at),
        "timestamp_model":{"source_timestamp":"iTick t = latest trade timestamp","source_gateway_receipt":"source HTTP response receipt timestamp","measured_age":"source_gateway_receipt - source_timestamp","request_latency_is_not_market_data_age":True},
        "symbols":syms,"sources":{name:{k:res.get(k) for k in ("available","configured","error","errors","received_at","request_latency_ms","endpoint","region","note","diagnostics")} for name,res in source_results.items()},
        "quotes":output,"execution_authorization":execution_authorization(),
        "itick_budget":await itick_budget_status(),
        "policy":{"missing_source_timestamp":"UNKNOWN","GREEN_requires_verified_source_age":True,"stale_or_invalid":"RED","temporal_cross_validation_required_for_secondary_timestamp_claim":True,"price_corroboration_never_proves_freshness":True},
    }


@mcp.tool()
async def asx_get_quote(symbol: str) -> dict:
    """Fetch one symbol through the V3 timestamped integrity pipeline."""
    return await asx_get_quotes([normalize_symbol(symbol)])


@mcp.tool()
async def asx_get_ticks(symbols: list[str] | None = None) -> dict:
    """Fetch iTick transaction-level data with each trade's source timestamp."""
    syms=[normalize_symbol(x) for x in (symbols or DEFAULT_SYMBOLS) if normalize_symbol(x)]
    src=ITickSource(ITICK_BASE_URL,ITICK_TOKEN,ITICK_REGION,ITICK_EXCHANGE)
    result=await src.get_ticks(syms)
    return {"gateway":"ASX MAXIMUM EDGE V8-I Data Gateway V3.1","execution_authorization":execution_authorization(),"tick_result":result}


@mcp.tool()
async def asx_get_depth(symbol: str) -> dict:
    """Fetch iTick Level-2 depth for one symbol. Depth freshness remains UNKNOWN unless the upstream response carries its own timestamp."""
    src=ITickSource(ITICK_BASE_URL,ITICK_TOKEN,ITICK_REGION,ITICK_EXCHANGE)
    result=await src.get_depth(normalize_symbol(symbol))
    return {"gateway":"ASX MAXIMUM EDGE V8-I Data Gateway V3.1","quote_and_depth_clocks_separate":True,"execution_authorization":execution_authorization(),"depth_result":result}


@mcp.tool()
async def asx_get_health() -> dict:
    """Return V3.1 health, timestamp model, source configuration, rate-limit diagnostics and execution authorization."""
    itick_configured=bool(ITICK_TOKEN)
    return {
        "gateway":"ASX MAXIMUM EDGE V8-I Data Gateway V3.1","status":"READY","timestamp_utc":iso(now_utc()),
        "primary_source":{"name":"iTick","configured":itick_configured,"region":ITICK_REGION,"base_url":ITICK_BASE_URL,"role":"primary_timestamped_quote_source","source_timestamp_field":"t","source_timestamp_semantics":"latest trade timestamp","execution_grade":"NOT_GRANTED_BY_CONFIGURATION"},
        "secondary_sources":[
            {"name":"ASX Equity Stocks / Migizi Tech","enabled":ENABLE_MIGIZI,"role":"price corroboration; no verified source timestamp in this integration"},
            {"name":"Yahoo Finance chart","enabled":ENABLE_YAHOO,"role":"reference/corroboration; not execution-grade"},
        ],
        "configured_symbols":DEFAULT_SYMBOLS,
        "freshness_classes":{"GREEN":f"verified source age <= {GREEN_MAX_AGE:g}s","AMBER":f"verified source age <= {AMBER_MAX_AGE:g}s","ORANGE":f"verified source age <= {ORANGE_MAX_AGE:g}s","RED":"invalid/stale/inconsistent/future timestamp","UNKNOWN":"source age cannot be verified"},
        "rest_rate_limit_assumption":f"{ITICK_FREE_REST_LIMIT_PER_MINUTE} calls/minute (configure to match plan)",
        "rate_limit_diagnostics":await itick_budget_status(),
        "execution_authorization":execution_authorization(),
        "critical_policy":"A GREEN quote proves timestamp freshness only. It does not prove exchange licensing, order-book completeness, execution authorization, or trading profitability.",
    }


@mcp.tool()
async def asx_run_gateway_test() -> dict:
    """V3.1 Tier-1 diagnostic acceptance test.

    Uses three /stock/quotes batch calls, spaced by 2 seconds, and records the
    provider response, timestamp presence, returned symbols, missing symbols,
    rate-limit telemetry and freshness outcome. It fails closed and reports
    the exact stage of failure rather than silently converting an iTick error
    into zero timestamp observations.
    """
    required_calls=3
    preflight=await itick_budget_status(reserve_calls=required_calls)
    if not preflight.get("reserve_available"):
        return {
            "test":"V8-I Data Gateway V3.1 Tier-1 Timestamp Acceptance Test",
            "version":"3.1", "run_at_utc":iso(now_utc()), "symbols":DEFAULT_SYMBOLS,
            "snapshots_completed":0, "failures":[], "preflight":preflight,
            "per_symbol":{}, "verified_green_observations":[],
            "acquisition_repeatability_pass":False,
            "promotion":"DO NOT PROMOTE — INSUFFICIENT iTick RATE-LIMIT BUDGET",
            "diagnostic_root_cause":"The gateway process has insufficient tracked Free Plan budget for three batch snapshots. Wait for the rolling window to clear and rerun.",
            "execution_authorization":execution_authorization(),
        }

    snapshots=[]; failures=[]
    for i in range(3):
        try:
            snap=await asx_get_quotes(DEFAULT_SYMBOLS)
            snapshots.append(snap)
        except Exception as exc:
            failures.append({"snapshot":i+1,"stage":"gateway_call","error":repr(exc)})
        if i<2:
            await asyncio.sleep(2)

    green=[]; per_symbol={}; diagnostic_failures=[]
    for symbol in DEFAULT_SYMBOLS:
        observations=[]
        for idx,snap in enumerate(snapshots, start=1):
            source_block=snap.get("sources",{}).get("iTick",{})
            quote=snap.get("quotes",{}).get(symbol,{}).get("iTick")
            diagnostics=source_block.get("diagnostics") or {}
            if quote:
                observations.append({
                    "snapshot":idx, "price":quote.get("price"),
                    "source_timestamp":quote.get("source_timestamp"),
                    "timestamp_verified":quote.get("source_timestamp_verified"),
                    "freshness":quote.get("freshness"),
                    "integrity":quote.get("integrity_classification"),
                    "quality_score":(quote.get("quality") or {}).get("score"),
                })
                if quote.get("freshness",{}).get("classification")=="GREEN":
                    green.append((symbol,idx,quote.get("freshness",{}).get("age_seconds")))
            else:
                diagnostic_failures.append({
                    "symbol":symbol,"snapshot":idx,
                    "source_available":source_block.get("available"),
                    "source_error":source_block.get("error"),
                    "http_status":diagnostics.get("http_status"),
                    "provider_code":diagnostics.get("provider_code"),
                    "provider_message":diagnostics.get("provider_message"),
                    "returned_symbols":diagnostics.get("returned_symbols"),
                    "missing_symbols":diagnostics.get("missing_symbols"),
                    "missing_timestamps":diagnostics.get("missing_timestamps"),
                    "timestamp_presence":diagnostics.get("timestamp_presence"),
                    "budget_after":diagnostics.get("budget_after"),
                })
        timestamped=sum(1 for x in observations if x["timestamp_verified"])
        greens=sum(1 for x in observations if x["freshness"].get("classification")=="GREEN")
        red=sum(1 for x in observations if x["integrity"]=="RED")
        per_symbol[symbol]={
            "observations":len(observations),
            "expected_snapshots":len(snapshots),
            "iTick_timestamp_verified":timestamped,
            "iTick_green_count":greens,
            "red_count":red,
            "pass":len(observations)==len(snapshots)==3 and timestamped==3 and red==0,
            "observations_detail":observations,
        }

    acquisition=not failures and len(snapshots)==3 and all(v["pass"] for v in per_symbol.values())
    promotion=("V3.1 TIMESTAMP GATE PASSED — EXECUTION AUTHORIZATION REMAINS SEPARATE" if acquisition and green
               else "DO NOT PROMOTE")
    return {
        "test":"V8-I Data Gateway V3.1 Tier-1 Timestamp Acceptance Test",
        "version":"3.1", "run_at_utc":iso(now_utc()), "symbols":DEFAULT_SYMBOLS,
        "snapshots_completed":len(snapshots), "failures":failures,
        "preflight":preflight, "post_test_budget":await itick_budget_status(),
        "per_symbol":per_symbol, "verified_green_observations":green,
        "diagnostic_failures":diagnostic_failures,
        "acquisition_repeatability_pass":acquisition,
        "execution_authorization":execution_authorization(),
        "promotion":promotion,
        "important":[
            "iTick source t is the only freshness clock for primary quotes.",
            "Gateway request latency is never treated as market-data age.",
            "Provider/API/rate-limit errors are surfaced explicitly and are never converted into missing-timestamp observations.",
            "The batch parser preserves response dictionary keys as symbol fallbacks.",
            "Depth requires its own timestamped source before it can be marked GREEN."
        ],
    }


if __name__ == "__main__":
    mcp.run(transport="streamable-http",host="0.0.0.0",port=int(os.getenv("PORT","8000")),streamable_http_path="/mcp",stateless_http=True,json_response=True)
