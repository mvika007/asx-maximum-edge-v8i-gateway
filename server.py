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
import websockets
from mcp.server import MCPServer

mcp = MCPServer("ASX MAXIMUM EDGE V8-I Data Gateway V3.2")

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
ITICK_WS_URL = os.getenv("ITICK_WS_URL", "wss://api-free.itick.org/stock")
ITICK_WS_TIMEOUT = float(os.getenv("ITICK_WS_TIMEOUT_SECONDS", "12"))
ITICK_WS_TEST_SYMBOLS = [s.strip().upper() for s in os.getenv("ITICK_WS_TEST_SYMBOLS", "BHP,CBA,WGX").split(",") if s.strip()]
ITICK_WS_TEST_TYPES = os.getenv("ITICK_WS_TEST_TYPES", "quote").strip() or "quote"

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
    async def get_quote(self, symbol: str) -> dict:
        """Free-plan-compatible single-symbol REST quote using /stock/quote.

        This deliberately avoids the /stock/quotes multi-symbol endpoint used by
        V3.1's acceptance test. iTick documents /stock/quote as the single-symbol
        endpoint and exposes `t` as the latest-trade timestamp.
        """
        symbol = normalize_symbol(symbol)
        if not self.configured:
            return {"source": self.name, "available": False, "configured": False,
                    "error": "ITICK_API_TOKEN not configured", "quotes": {},
                    "diagnostics": {"stage": "configuration"}}
        budget_before = await itick_budget_status()
        params = {"region": self.region, "code": symbol}
        if self.exchange:
            params["exchange"] = self.exchange
        try:
            payload, latency_ms, received_at, server_date, telemetry = await self._request("quote", params)
        except Exception as exc:
            return {"source": self.name, "available": False, "configured": True,
                    "error": repr(exc), "quotes": {},
                    "diagnostics": {"stage": "transport", "budget_before": budget_before,
                                    "exception": repr(exc)}}
        response_code = payload.get("code") if isinstance(payload, dict) else None
        response_msg = payload.get("msg") if isinstance(payload, dict) else None
        if response_code not in (None, 0):
            return {"source": self.name, "available": False, "configured": True,
                    "error": response_msg or f"iTick code={response_code}", "quotes": {},
                    "received_at": iso(received_at), "request_latency_ms": latency_ms,
                    "endpoint": "/stock/quote", "region": self.region,
                    "diagnostics": {"stage": "provider_response", "http_status": telemetry.get("http_status"),
                                    "provider_code": response_code, "provider_message": response_msg,
                                    "budget_before": budget_before, "budget_after": await itick_budget_status(),
                                    "telemetry": telemetry}}
        record = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(record, dict):
            return {"source": self.name, "available": False, "configured": True,
                    "error": "iTick single quote returned no data object", "quotes": {},
                    "received_at": iso(received_at), "request_latency_ms": latency_ms,
                    "endpoint": "/stock/quote", "region": self.region,
                    "diagnostics": {"stage": "payload_shape", "http_status": telemetry.get("http_status"),
                                    "provider_code": response_code, "provider_message": response_msg,
                                    "budget_before": budget_before, "budget_after": await itick_budget_status(),
                                    "telemetry": telemetry}}
        q = self._normalize_quote(record, symbol, latency_ms)
        return {"source": self.name, "available": True, "configured": True,
                "received_at": iso(received_at), "request_latency_ms": latency_ms,
                "endpoint": "/stock/quote", "region": self.region,
                "quotes": {q.symbol: q},
                "diagnostics": {"stage": "success", "http_status": telemetry.get("http_status"),
                                "provider_code": response_code, "provider_message": response_msg,
                                "budget_before": budget_before, "budget_after": await itick_budget_status(),
                                "telemetry": telemetry}}

    async def get_quotes(self, symbols: list[str]) -> dict:
        raise NotImplementedError

    async def websocket_test(self, symbols: list[str], types: str = "quote", timeout: float = ITICK_WS_TIMEOUT) -> dict:
        """Test free-plan stock WebSocket capability without consuming REST quota."""
        if not self.configured:
            return {"supported": False, "stage": "configuration", "error": "ITICK_API_TOKEN not configured"}
        symbols = [normalize_symbol(x) for x in symbols if normalize_symbol(x)]
        if not symbols:
            return {"supported": False, "stage": "input", "error": "no symbols supplied"}
        types = ",".join(sorted(set(t.strip() for t in types.split(",") if t.strip())))
        uri = self.base_url.replace("https://", "wss://").replace("http://", "ws://") if self.base_url.startswith(("http://", "https://")) else ITICK_WS_URL
        if "api-free.itick.org" not in uri and uri.rstrip("/") == "wss://api.itick.org/stock":
            # Explicitly honor configured WS URL if production is intentionally supplied.
            pass
        else:
            uri = ITICK_WS_URL
        params = ",".join(f"{s}${self.region}" for s in symbols)
        started = time.monotonic()
        events=[]
        auth=False
        connected=False
        subscribed=False
        quote_events=[]
        tick_events=[]
        depth_events=[]
        error_events=[]
        try:
            async with websockets.connect(uri, additional_headers={"token": self.token}, open_timeout=timeout, close_timeout=3) as ws:
                connected_at=now_utc()
                try:
                    raw=await asyncio.wait_for(ws.recv(), timeout=min(timeout, 5))
                    msg=json.loads(raw) if isinstance(raw,(str,bytes,bytearray)) else raw
                    events.append(msg)
                    if isinstance(msg,dict) and msg.get("code") == 1:
                        connected = msg.get("msg") == "Connected Successfully"
                        auth = msg.get("resAc") == "auth"
                except asyncio.TimeoutError:
                    pass
                # The documented client flow authenticates via the token header;
                # after connection the server may emit Connected Successfully and/or auth.
                if not auth:
                    try:
                        raw=await asyncio.wait_for(ws.recv(), timeout=min(timeout, 3))
                        msg=json.loads(raw) if isinstance(raw,(str,bytes,bytearray)) else raw
                        events.append(msg)
                        if isinstance(msg,dict) and msg.get("resAc") == "auth" and msg.get("code") == 1:
                            auth=True
                        elif isinstance(msg,dict) and msg.get("code") == 0:
                            error_events.append(msg)
                    except asyncio.TimeoutError:
                        pass
                await ws.send(json.dumps({"ac":"subscribe","params":params,"types":types}))
                raw=await asyncio.wait_for(ws.recv(), timeout=min(timeout, 5))
                msg=json.loads(raw) if isinstance(raw,(str,bytes,bytearray)) else raw
                events.append(msg)
                if isinstance(msg,dict) and msg.get("code") == 1 and msg.get("resAc") == "subscribe":
                    subscribed=True
                elif isinstance(msg,dict) and msg.get("code") == 0:
                    error_events.append(msg)
                deadline=time.monotonic()+timeout
                while time.monotonic() < deadline:
                    try:
                        raw=await asyncio.wait_for(ws.recv(), timeout=max(0.2, min(2.0, deadline-time.monotonic())))
                    except asyncio.TimeoutError:
                        await ws.send(json.dumps({"ac":"ping","params":str(int(time.time()*1000))}))
                        continue
                    msg=json.loads(raw) if isinstance(raw,(str,bytes,bytearray)) else raw
                    events.append(msg)
                    if isinstance(msg,dict) and isinstance(msg.get("data"),dict):
                        data=msg["data"]
                        typ=data.get("type")
                        if typ == "quote": quote_events.append(data)
                        elif typ == "tick": tick_events.append(data)
                        elif typ == "depth": depth_events.append(data)
                        if typ in {"quote","tick","depth"} and len(quote_events)+len(tick_events)+len(depth_events) >= len(symbols):
                            # One message per requested symbol is sufficient for capability proof.
                            if types == "quote" or quote_events:
                                break
        except Exception as exc:
            return {"supported": False, "stage":"websocket_connection_or_subscription", "error":repr(exc),
                    "url":uri,"symbols":symbols,"types":types,
                    "elapsed_seconds":round(time.monotonic()-started,3),"events_received":len(events),
                    "events":events[-10:],"connected_observed":connected,"auth_observed":auth,"subscription_acknowledged":subscribed,
                    "quote_events":quote_events[:10],"tick_events":tick_events[:10],"depth_events":depth_events[:5],
                    "errors":error_events[-10:],
                    "execution_grade":"NOT_GRANTED"}
        source_now=now_utc()
        def event_summary(items):
            out=[]
            for d in items[:20]:
                ts=parse_timestamp(d.get("t"))
                age=(source_now-ts).total_seconds() if ts else None
                out.append({"symbol":normalize_symbol(d.get("s") or ""),"price":finite_number(d.get("ld")),
                            "source_timestamp":iso(ts),"age_seconds":round(max(age,0),3) if age is not None else None,
                            "timestamp_verified":ts is not None,"type":d.get("type")})
            return out
        return {"supported":(auth or connected) and subscribed and bool(quote_events or tick_events or depth_events),
                "stage":"complete","url":uri,"symbols":symbols,"types":types,
                "elapsed_seconds":round(time.monotonic()-started,3),"connected_at":iso(connected_at),
                "gateway_observed_at":iso(source_now),"auth_observed":auth,
                "subscription_acknowledged":subscribed,"events_received":len(events),
                "quote_events":event_summary(quote_events),"tick_events":event_summary(tick_events),
                "depth_events":event_summary(depth_events),"errors":error_events[-10:],
                "policy":{"rest_quota_consumed":False,"source_timestamp_for_quote_tick":"iTick t",
                           "depth_source_timestamp":"not provided in documented depth payload; remains UNKNOWN",
                           "websocket_capability_is_not_execution_authorization":True},
                "execution_grade":"NOT_GRANTED"}

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

    async def get_quote(self, symbol: str) -> dict:
        """Free-plan-compatible single-symbol REST quote using /stock/quote.

        This deliberately avoids the /stock/quotes multi-symbol endpoint used by
        V3.1's acceptance test. iTick documents /stock/quote as the single-symbol
        endpoint and exposes `t` as the latest-trade timestamp.
        """
        symbol = normalize_symbol(symbol)
        if not self.configured:
            return {"source": self.name, "available": False, "configured": False,
                    "error": "ITICK_API_TOKEN not configured", "quotes": {},
                    "diagnostics": {"stage": "configuration"}}
        budget_before = await itick_budget_status()
        params = {"region": self.region, "code": symbol}
        if self.exchange:
            params["exchange"] = self.exchange
        try:
            payload, latency_ms, received_at, server_date, telemetry = await self._request("quote", params)
        except Exception as exc:
            return {"source": self.name, "available": False, "configured": True,
                    "error": repr(exc), "quotes": {},
                    "diagnostics": {"stage": "transport", "budget_before": budget_before,
                                    "exception": repr(exc)}}
        response_code = payload.get("code") if isinstance(payload, dict) else None
        response_msg = payload.get("msg") if isinstance(payload, dict) else None
        if response_code not in (None, 0):
            return {"source": self.name, "available": False, "configured": True,
                    "error": response_msg or f"iTick code={response_code}", "quotes": {},
                    "received_at": iso(received_at), "request_latency_ms": latency_ms,
                    "endpoint": "/stock/quote", "region": self.region,
                    "diagnostics": {"stage": "provider_response", "http_status": telemetry.get("http_status"),
                                    "provider_code": response_code, "provider_message": response_msg,
                                    "budget_before": budget_before, "budget_after": await itick_budget_status(),
                                    "telemetry": telemetry}}
        record = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(record, dict):
            return {"source": self.name, "available": False, "configured": True,
                    "error": "iTick single quote returned no data object", "quotes": {},
                    "received_at": iso(received_at), "request_latency_ms": latency_ms,
                    "endpoint": "/stock/quote", "region": self.region,
                    "diagnostics": {"stage": "payload_shape", "http_status": telemetry.get("http_status"),
                                    "provider_code": response_code, "provider_message": response_msg,
                                    "budget_before": budget_before, "budget_after": await itick_budget_status(),
                                    "telemetry": telemetry}}
        q = self._normalize_quote(record, symbol, latency_ms)
        return {"source": self.name, "available": True, "configured": True,
                "received_at": iso(received_at), "request_latency_ms": latency_ms,
                "endpoint": "/stock/quote", "region": self.region,
                "quotes": {q.symbol: q},
                "diagnostics": {"stage": "success", "http_status": telemetry.get("http_status"),
                                "provider_code": response_code, "provider_message": response_msg,
                                "budget_before": budget_before, "budget_after": await itick_budget_status(),
                                "telemetry": telemetry}}

    async def get_quotes(self, symbols: list[str]) -> dict:
        """Fetch symbols through the Free-plan-compatible single-symbol endpoint.

        Each symbol consumes one REST call. The gateway enforces its local soft
        budget before making calls and stops rather than manufacturing data.
        """
        symbols = [normalize_symbol(x) for x in symbols]
        if not symbols:
            return {"source": self.name, "available": False, "configured": self.configured,
                    "error": "no symbols supplied", "quotes": {}}
        if not self.configured:
            return {"source": self.name, "available": False, "configured": False,
                    "error": "ITICK_API_TOKEN not configured", "quotes": {}}
        before = await itick_budget_status()
        if before["tracked_remaining_soft_budget"] < len(symbols):
            return {"source": self.name, "available": False, "configured": True,
                    "error": "insufficient local Free-plan REST budget for requested symbol count",
                    "quotes": {}, "diagnostics": {"stage": "local_budget_guard",
                    "requested_symbols": symbols, "requested_calls": len(symbols), "budget_before": before,
                    "policy": "single-symbol /stock/quote; one REST call per symbol"}}
        results = []
        for symbol in symbols:
            results.append(await self.get_quote(symbol))
        quotes = {}
        errors = []
        diagnostics = []
        for result in results:
            quotes.update(result.get("quotes", {}))
            if not result.get("available"):
                errors.append({"symbol": next((s for s in symbols if s not in quotes), None),
                               "error": result.get("error"), "diagnostics": result.get("diagnostics")})
            diagnostics.append(result.get("diagnostics"))
        return {"source": self.name, "available": len(quotes) > 0 and not errors,
                "configured": True, "quotes": quotes,
                "received_at": iso(now_utc()),
                "endpoint": "/stock/quote",
                "region": self.region,
                "errors": errors,
                "diagnostics": {"stage": "single_symbol_batch",
                                 "symbols_requested": symbols,
                                 "calls_consumed": len(results),
                                 "per_call": diagnostics,
                                 "budget_after": await itick_budget_status()}}

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

    async def get_quote(self, symbol: str) -> dict:
        """Free-plan-compatible single-symbol REST quote using /stock/quote.

        This deliberately avoids the /stock/quotes multi-symbol endpoint used by
        V3.1's acceptance test. iTick documents /stock/quote as the single-symbol
        endpoint and exposes `t` as the latest-trade timestamp.
        """
        symbol = normalize_symbol(symbol)
        if not self.configured:
            return {"source": self.name, "available": False, "configured": False,
                    "error": "ITICK_API_TOKEN not configured", "quotes": {},
                    "diagnostics": {"stage": "configuration"}}
        budget_before = await itick_budget_status()
        params = {"region": self.region, "code": symbol}
        if self.exchange:
            params["exchange"] = self.exchange
        try:
            payload, latency_ms, received_at, server_date, telemetry = await self._request("quote", params)
        except Exception as exc:
            return {"source": self.name, "available": False, "configured": True,
                    "error": repr(exc), "quotes": {},
                    "diagnostics": {"stage": "transport", "budget_before": budget_before,
                                    "exception": repr(exc)}}
        response_code = payload.get("code") if isinstance(payload, dict) else None
        response_msg = payload.get("msg") if isinstance(payload, dict) else None
        if response_code not in (None, 0):
            return {"source": self.name, "available": False, "configured": True,
                    "error": response_msg or f"iTick code={response_code}", "quotes": {},
                    "received_at": iso(received_at), "request_latency_ms": latency_ms,
                    "endpoint": "/stock/quote", "region": self.region,
                    "diagnostics": {"stage": "provider_response", "http_status": telemetry.get("http_status"),
                                    "provider_code": response_code, "provider_message": response_msg,
                                    "budget_before": budget_before, "budget_after": await itick_budget_status(),
                                    "telemetry": telemetry}}
        record = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(record, dict):
            return {"source": self.name, "available": False, "configured": True,
                    "error": "iTick single quote returned no data object", "quotes": {},
                    "received_at": iso(received_at), "request_latency_ms": latency_ms,
                    "endpoint": "/stock/quote", "region": self.region,
                    "diagnostics": {"stage": "payload_shape", "http_status": telemetry.get("http_status"),
                                    "provider_code": response_code, "provider_message": response_msg,
                                    "budget_before": budget_before, "budget_after": await itick_budget_status(),
                                    "telemetry": telemetry}}
        q = self._normalize_quote(record, symbol, latency_ms)
        return {"source": self.name, "available": True, "configured": True,
                "received_at": iso(received_at), "request_latency_ms": latency_ms,
                "endpoint": "/stock/quote", "region": self.region,
                "quotes": {q.symbol: q},
                "diagnostics": {"stage": "success", "http_status": telemetry.get("http_status"),
                                "provider_code": response_code, "provider_message": response_msg,
                                "budget_before": budget_before, "budget_after": await itick_budget_status(),
                                "telemetry": telemetry}}

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

    async def get_quote(self, symbol: str) -> dict:
        """Free-plan-compatible single-symbol REST quote using /stock/quote.

        This deliberately avoids the /stock/quotes multi-symbol endpoint used by
        V3.1's acceptance test. iTick documents /stock/quote as the single-symbol
        endpoint and exposes `t` as the latest-trade timestamp.
        """
        symbol = normalize_symbol(symbol)
        if not self.configured:
            return {"source": self.name, "available": False, "configured": False,
                    "error": "ITICK_API_TOKEN not configured", "quotes": {},
                    "diagnostics": {"stage": "configuration"}}
        budget_before = await itick_budget_status()
        params = {"region": self.region, "code": symbol}
        if self.exchange:
            params["exchange"] = self.exchange
        try:
            payload, latency_ms, received_at, server_date, telemetry = await self._request("quote", params)
        except Exception as exc:
            return {"source": self.name, "available": False, "configured": True,
                    "error": repr(exc), "quotes": {},
                    "diagnostics": {"stage": "transport", "budget_before": budget_before,
                                    "exception": repr(exc)}}
        response_code = payload.get("code") if isinstance(payload, dict) else None
        response_msg = payload.get("msg") if isinstance(payload, dict) else None
        if response_code not in (None, 0):
            return {"source": self.name, "available": False, "configured": True,
                    "error": response_msg or f"iTick code={response_code}", "quotes": {},
                    "received_at": iso(received_at), "request_latency_ms": latency_ms,
                    "endpoint": "/stock/quote", "region": self.region,
                    "diagnostics": {"stage": "provider_response", "http_status": telemetry.get("http_status"),
                                    "provider_code": response_code, "provider_message": response_msg,
                                    "budget_before": budget_before, "budget_after": await itick_budget_status(),
                                    "telemetry": telemetry}}
        record = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(record, dict):
            return {"source": self.name, "available": False, "configured": True,
                    "error": "iTick single quote returned no data object", "quotes": {},
                    "received_at": iso(received_at), "request_latency_ms": latency_ms,
                    "endpoint": "/stock/quote", "region": self.region,
                    "diagnostics": {"stage": "payload_shape", "http_status": telemetry.get("http_status"),
                                    "provider_code": response_code, "provider_message": response_msg,
                                    "budget_before": budget_before, "budget_after": await itick_budget_status(),
                                    "telemetry": telemetry}}
        q = self._normalize_quote(record, symbol, latency_ms)
        return {"source": self.name, "available": True, "configured": True,
                "received_at": iso(received_at), "request_latency_ms": latency_ms,
                "endpoint": "/stock/quote", "region": self.region,
                "quotes": {q.symbol: q},
                "diagnostics": {"stage": "success", "http_status": telemetry.get("http_status"),
                                "provider_code": response_code, "provider_message": response_msg,
                                "budget_before": budget_before, "budget_after": await itick_budget_status(),
                                "telemetry": telemetry}}

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
    """V3.2 primary timestamped quote pipeline using Free-plan-compatible single-symbol REST calls."""
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
        "gateway":"ASX MAXIMUM EDGE V8-I Data Gateway V3.2",
        "gateway_received_at":iso(gateway_received_at),
        "timestamp_model":{"source_timestamp":"iTick t = latest trade timestamp","source_gateway_receipt":"source HTTP response receipt timestamp","measured_age":"source_gateway_receipt - source_timestamp","request_latency_is_not_market_data_age":True},
        "symbols":syms,"sources":{name:{k:res.get(k) for k in ("available","configured","error","errors","received_at","request_latency_ms","endpoint","region","note","diagnostics")} for name,res in source_results.items()},
        "quotes":output,"execution_authorization":execution_authorization(),
        "itick_budget":await itick_budget_status(),
        "policy":{"missing_source_timestamp":"UNKNOWN","GREEN_requires_verified_source_age":True,"stale_or_invalid":"RED","temporal_cross_validation_required_for_secondary_timestamp_claim":True,"price_corroboration_never_proves_freshness":True},
    }


@mcp.tool()
async def asx_get_quote(symbol: str) -> dict:
    """Fetch one symbol through the V3.2 single-symbol /stock/quote timestamped integrity pipeline."""
    return await asx_get_quotes([normalize_symbol(symbol)])


@mcp.tool()
async def asx_get_ticks(symbols: list[str] | None = None) -> dict:
    """Fetch iTick transaction-level data with each trade's source timestamp."""
    syms=[normalize_symbol(x) for x in (symbols or DEFAULT_SYMBOLS) if normalize_symbol(x)]
    src=ITickSource(ITICK_BASE_URL,ITICK_TOKEN,ITICK_REGION,ITICK_EXCHANGE)
    result=await src.get_ticks(syms)
    return {"gateway":"ASX MAXIMUM EDGE V8-I Data Gateway V3.2","execution_authorization":execution_authorization(),"tick_result":result}


@mcp.tool()
async def asx_get_depth(symbol: str) -> dict:
    """Fetch iTick Level-2 depth for one symbol. Depth freshness remains UNKNOWN unless the upstream response carries its own timestamp."""
    src=ITickSource(ITICK_BASE_URL,ITICK_TOKEN,ITICK_REGION,ITICK_EXCHANGE)
    result=await src.get_depth(normalize_symbol(symbol))
    return {"gateway":"ASX MAXIMUM EDGE V8-I Data Gateway V3.2","quote_and_depth_clocks_separate":True,"execution_authorization":execution_authorization(),"depth_result":result}


@mcp.tool()
async def asx_run_websocket_test(symbols: list[str] | None = None, types: str = "quote", timeout_seconds: float | None = None) -> dict:
    """Test iTick stock WebSocket capability without consuming REST calls."""
    syms=[normalize_symbol(x) for x in (symbols or ITICK_WS_TEST_SYMBOLS) if normalize_symbol(x)]
    src=ITickSource(ITICK_BASE_URL,ITICK_TOKEN,ITICK_REGION,ITICK_EXCHANGE)
    result=await src.websocket_test(syms, types=types, timeout=timeout_seconds or ITICK_WS_TIMEOUT)
    return {"gateway":"ASX MAXIMUM EDGE V8-I Data Gateway V3.2","websocket_result":result,
            "execution_authorization":execution_authorization(),
            "important":["WebSocket testing consumes no iTick REST calls.","Quote/tick events use iTick source timestamp t when present.","A successful stream proves capability, not exchange licensing or execution authorization."]}


@mcp.tool()
async def asx_get_health() -> dict:
    """Return V3.2 health, timestamp model, source configuration, REST/WebSocket diagnostics and execution authorization."""
    itick_configured=bool(ITICK_TOKEN)
    return {
        "gateway":"ASX MAXIMUM EDGE V8-I Data Gateway V3.2","status":"READY","timestamp_utc":iso(now_utc()),
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
async def asx_run_gateway_test(test_symbol: str | None = None) -> dict:
    """V3.2 Tier-1 single-symbol timestamp repeatability test.

    Uses three /stock/quote calls for ONE symbol, spaced by 2 seconds. This is
    intentionally Free-plan compatible and avoids the V3.1 multi-symbol batch
    endpoint that produced provider-side "your request is too much" responses.
    """
    symbol=normalize_symbol(test_symbol or os.getenv("V8I_TEST_SYMBOL", "BHP"))
    required_calls=3
    preflight=await itick_budget_status(reserve_calls=required_calls)
    if not preflight.get("reserve_available"):
        return {"test":"V8-I Data Gateway V3.2 Tier-1 Single-Symbol Timestamp Repeatability Test",
                "version":"3.2","run_at_utc":iso(now_utc()),"test_symbol":symbol,
                "snapshots_completed":0,"preflight":preflight,"verified_green_observations":[],
                "acquisition_repeatability_pass":False,
                "promotion":"DO NOT PROMOTE — INSUFFICIENT LOCAL iTick REST BUDGET",
                "diagnostic_root_cause":"Three single-symbol /stock/quote calls are required. Wait for the rolling budget to clear.",
                "execution_authorization":execution_authorization()}
    observations=[]; failures=[]
    for i in range(3):
        result=await ITickSource(ITICK_BASE_URL,ITICK_TOKEN,ITICK_REGION,ITICK_EXCHANGE).get_quote(symbol)
        q=result.get("quotes",{}).get(symbol)
        if q:
            received=parse_timestamp(result.get("received_at")) or now_utc()
            fresh=classify_age(parse_timestamp(q.source_timestamp),received,q.price is not None and q.volume is not None,q.validation_errors)
            observations.append({"snapshot":i+1,"price":q.price,"source_timestamp":q.source_timestamp,
                                 "timestamp_verified":q.source_timestamp_verified,"freshness":fresh,
                                 "integrity":"RED" if q.validation_errors else fresh["classification"],
                                 "quality_score":quality_score(q,fresh,{})["score"],
                                 "request_latency_ms":q.request_latency_ms})
        else:
            failures.append({"snapshot":i+1,"error":result.get("error"),"diagnostics":result.get("diagnostics")})
        if i<2:
            await asyncio.sleep(2)
    verified=sum(1 for o in observations if o["timestamp_verified"])
    green=sum(1 for o in observations if o["freshness"]["classification"]=="GREEN")
    red=sum(1 for o in observations if o["integrity"]=="RED")
    repeatability=(len(observations)==3 and verified==3 and red==0)
    return {"test":"V8-I Data Gateway V3.2 Tier-1 Single-Symbol Timestamp Repeatability Test",
            "version":"3.2","run_at_utc":iso(now_utc()),"test_symbol":symbol,
            "endpoint":"/stock/quote","snapshots_completed":len(observations),"failures":failures,
            "observations":observations,"verified_timestamp_observations":verified,
            "verified_green_observations":green,"red_observations":red,
            "preflight":preflight,"post_test_budget":await itick_budget_status(),
            "acquisition_repeatability_pass":repeatability,
            "promotion":"V3.2 TIMESTAMP REPEATABILITY GATE PASSED — EXECUTION AUTHORIZATION REMAINS SEPARATE" if repeatability and green else "DO NOT PROMOTE",
            "policy":["One REST call per symbol.","Gateway request latency is not market-data age.","iTick t is the primary source timestamp.","This test does not test multi-symbol batch capability.","A passing timestamp test does not prove exchange licensing, depth freshness, or profitability."],
            "execution_authorization":execution_authorization()}


@mcp.tool()
async def asx_run_breadth_test(symbols: list[str] | None = None) -> dict:
    """One-shot breadth test using one /stock/quote call per symbol.

    On the Free plan, the caller should run this separately from the 3-call
    repeatability test because each symbol consumes one REST request.
    """
    syms=[normalize_symbol(x) for x in (symbols or DEFAULT_SYMBOLS) if normalize_symbol(x)]
    budget=await itick_budget_status(reserve_calls=len(syms))
    if not budget.get("reserve_available"):
        return {"test":"V8-I V3.2 Single-Symbol Breadth Test","version":"3.2","symbols":syms,
                "pass":False,"promotion":"DO NOT PROMOTE — INSUFFICIENT REST BUDGET","budget":budget,
                "note":"Run after the rolling Free-plan budget clears."}
    results=[]
    src=ITickSource(ITICK_BASE_URL,ITICK_TOKEN,ITICK_REGION,ITICK_EXCHANGE)
    for symbol in syms:
        result=await src.get_quote(symbol)
        q=result.get("quotes",{}).get(symbol)
        if q:
            received=parse_timestamp(result.get("received_at")) or now_utc()
            fresh=classify_age(parse_timestamp(q.source_timestamp),received,q.price is not None and q.volume is not None,q.validation_errors)
            results.append({"symbol":symbol,"price":q.price,"source_timestamp":q.source_timestamp,
                            "age_seconds":fresh.get("age_seconds"),"freshness":fresh.get("classification"),
                            "timestamp_verified":q.source_timestamp_verified,"validation_errors":q.validation_errors})
        else:
            results.append({"symbol":symbol,"error":result.get("error"),"diagnostics":result.get("diagnostics")})
    passed=all(r.get("timestamp_verified") and r.get("freshness") in {"GREEN","AMBER"} and not r.get("validation_errors") for r in results)
    return {"test":"V8-I V3.2 Single-Symbol Breadth Test","version":"3.2","endpoint":"/stock/quote",
            "symbols":syms,"results":results,"pass":passed,"promotion":"BREADTH PASS — TIMESTAMPED SYMBOL COVERAGE VERIFIED" if passed else "DO NOT PROMOTE",
            "post_test_budget":await itick_budget_status(),"execution_authorization":execution_authorization()}


@mcp.tool()
async def asx_test_batch_capability(symbols: list[str] | None = None) -> dict:
    """Classify /stock/quotes batch capability separately from the core feed.

    This is deliberately diagnostic and is not used by the Tier-1 promotion gate.
    It may consume one REST call.
    """
    syms=[normalize_symbol(x) for x in (symbols or ["BHP","CBA"]) if normalize_symbol(x)]
    if len(syms)<2: syms=["BHP","CBA"]
    budget=await itick_budget_status(reserve_calls=1)
    if not budget.get("reserve_available"):
        return {"test":"V3.2 Batch Capability Test","status":"NOT_RUN","reason":"insufficient REST budget","budget":budget}
    src=ITickSource(ITICK_BASE_URL,ITICK_TOKEN,ITICK_REGION,ITICK_EXCHANGE)
    result=await src._request("quotes", {"region":ITICK_REGION,"codes":",".join(syms)})
    payload,latency,received,server_date,telemetry=result
    code=payload.get("code") if isinstance(payload,dict) else None
    msg=payload.get("msg") if isinstance(payload,dict) else None
    status="AVAILABLE" if code in (None,0) else "UNAVAILABLE_OR_RESTRICTED"
    return {"test":"V3.2 Batch Capability Test","endpoint":"/stock/quotes","symbols":syms,
            "status":status,"provider_code":code,"provider_message":msg,"request_latency_ms":latency,
            "received_at":iso(received),"telemetry":telemetry,
            "note":"Batch capability is not required for V3.2 Tier-1 promotion. Single-symbol /stock/quote is the Free-plan core path.",
            "post_test_budget":await itick_budget_status()}


if __name__ == "__main__":
    mcp.run(transport="streamable-http",host="0.0.0.0",port=int(os.getenv("PORT","8000")),streamable_http_path="/mcp",stateless_http=True,json_response=True)
