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
from pathlib import Path

V36_STATE_DIR = Path(os.getenv("ASX_V8I_JOB_STATE_DIR", "/tmp/asx_v8i_ws_jobs"))
V36_JOB_HEARTBEAT_SECONDS = max(5.0, float(os.getenv("ASX_V8I_JOB_HEARTBEAT_SECONDS", "10")))
V36_ORPHAN_AFTER_SECONDS = max(30.0, float(os.getenv("ASX_V8I_JOB_ORPHAN_AFTER_SECONDS", "45")))
V36_WS_CONNECT_ATTEMPTS = max(1, int(os.getenv("ASX_V8I_WS_CONNECT_ATTEMPTS", "2")))
V36_WS_RETRY_BACKOFF_SECONDS = max(0.5, float(os.getenv("ASX_V8I_WS_RETRY_BACKOFF_SECONDS", "2")))

def _v36_atomic_write_json(path: Path, payload: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=True, separators=(",", ":"), default=str), encoding="utf-8")
    tmp.replace(path)

def _v36_job_path(job_id: str) -> Path:
    return V36_STATE_DIR / f"{job_id}.json"

def _v36_read_job(job_id: str):
    path = _v36_job_path(job_id)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except Exception as exc:
        return {"job_id": job_id, "status": "CORRUPT", "error": repr(exc)}

import httpx
import websockets
from mcp.server import MCPServer

mcp = MCPServer("ASX MAXIMUM EDGE V8-I Data Gateway V3.6")

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
ITICK_WS_TIMEOUT = float(os.getenv("ITICK_WS_TIMEOUT_SECONDS", "15"))
ITICK_WS_ENDURANCE_SECONDS = float(os.getenv("ITICK_WS_ENDURANCE_SECONDS", "120"))
ITICK_WS_MIN_ACTIVE_SYMBOLS = int(os.getenv("ITICK_WS_MIN_ACTIVE_SYMBOLS", "1"))
ITICK_WS_MAX_EVENTS = int(os.getenv("ITICK_WS_MAX_EVENTS", "10000"))
ITICK_WS_MAX_EVENT_AGE = float(os.getenv("ITICK_WS_MAX_EVENT_AGE_SECONDS", "60"))
ITICK_WS_MAX_STATIONARY_EVENTS = int(os.getenv("ITICK_WS_MAX_STATIONARY_EVENTS", "3"))
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

    async def websocket_connection_diagnostic(self, symbols: list[str], types: str = "quote", timeout: float = 15.0) -> dict:
        """Stage-by-stage WebSocket diagnostic. Does not consume REST quota."""
        started_wall = now_utc(); started = time.monotonic()
        stages=[]; errors=[]; control=[]; market=[]
        syms=list(dict.fromkeys(normalize_symbol(x) for x in symbols if normalize_symbol(x)))
        if not self.configured:
            return {"test":"V3.6 WebSocket Connection Diagnostic","status":"NOT_RUN","stage":"configuration",
                    "error":"ITICK_API_TOKEN not configured","execution_grade":"NOT_GRANTED"}
        if not syms:
            return {"test":"V3.6 WebSocket Connection Diagnostic","status":"NOT_RUN","stage":"input",
                    "error":"no symbols supplied","execution_grade":"NOT_GRANTED"}
        types=",".join(sorted(set(t.strip().lower() for t in str(types).split(",") if t.strip()))) or "quote"
        params=",".join(f"{sym}${self.region}" for sym in syms)
        uri=ITICK_WS_URL
        def mark(stage,status,**extra):
            stages.append({"stage":stage,"status":status,"elapsed_ms":round((time.monotonic()-started)*1000,1),**extra})
        try:
            mark("preflight","PASS",url=uri,symbols=syms,types=types,timeout_seconds=timeout)
            async with websockets.connect(uri, additional_headers={"token": self.token},
                                           open_timeout=float(timeout), close_timeout=3,
                                           ping_interval=20, ping_timeout=10) as ws:
                mark("websocket_handshake","PASS")
                connected=True
                auth=False
                subscribed=False
                # Observe provider control messages before subscription.
                deadline=time.monotonic()+min(5.0,float(timeout))
                while time.monotonic()<deadline and not auth:
                    try:
                        raw=await asyncio.wait_for(ws.recv(), timeout=max(0.2,deadline-time.monotonic()))
                    except asyncio.TimeoutError:
                        mark("authentication_observation","TIMEOUT")
                        break
                    receipt=now_utc()
                    try: msg=json.loads(raw) if isinstance(raw,(str,bytes,bytearray)) else raw
                    except Exception as exc:
                        errors.append({"stage":"authentication_observation","error":repr(exc)})
                        continue
                    control.append({"received_at":iso(receipt),"message":msg})
                    if isinstance(msg,dict) and msg.get("code")==1 and msg.get("resAc")=="auth":
                        auth=True; mark("authentication","PASS",received_at=iso(receipt),response=msg)
                    elif isinstance(msg,dict) and msg.get("code")==1 and msg.get("msg")=="Connected Successfully":
                        mark("provider_connected_message","PASS",received_at=iso(receipt),response=msg)
                if not auth:
                    mark("authentication","FAIL",reason="authentication acknowledgement not observed")
                # Subscribe even if auth was not explicitly observed, matching the existing engine.
                send_at=now_utc(); await ws.send(json.dumps({"ac":"subscribe","params":params,"types":types}))
                mark("subscription_send","PASS",sent_at=iso(send_at),params=params,types=types)
                ack_deadline=time.monotonic()+min(8.0,float(timeout))
                while time.monotonic()<ack_deadline and not subscribed:
                    try:
                        raw=await asyncio.wait_for(ws.recv(), timeout=max(0.2,ack_deadline-time.monotonic()))
                    except asyncio.TimeoutError:
                        mark("subscription_ack","TIMEOUT")
                        break
                    receipt=now_utc()
                    try: msg=json.loads(raw) if isinstance(raw,(str,bytes,bytearray)) else raw
                    except Exception as exc:
                        errors.append({"stage":"subscription_ack","error":repr(exc)}); continue
                    control.append({"received_at":iso(receipt),"message":msg})
                    if isinstance(msg,dict) and msg.get("code")==1 and msg.get("resAc")=="subscribe":
                        subscribed=True; mark("subscription_ack","PASS",received_at=iso(receipt),response=msg)
                    elif isinstance(msg,dict) and msg.get("code")==0:
                        errors.append({"stage":"subscription","received_at":iso(receipt),"message":msg})
                    data=msg.get("data") if isinstance(msg,dict) else None
                    if isinstance(data,dict) and str(data.get("type") or "").lower() in {"quote","tick","depth"}:
                        market.append({"received_at":iso(receipt),"symbol":normalize_symbol(data.get("s") or data.get("symbol") or ""),"type":str(data.get("type") or "").lower(),"source_timestamp":iso(parse_timestamp(data.get("t"))),"price":finite_number(data.get("ld") or data.get("price"))})
                # Short first-event probe after subscription.
                if subscribed:
                    probe_deadline=time.monotonic()+min(10.0,float(timeout))
                    while time.monotonic()<probe_deadline and not market:
                        try:
                            raw=await asyncio.wait_for(ws.recv(),timeout=min(2.0,max(0.2,probe_deadline-time.monotonic())))
                        except asyncio.TimeoutError:
                            continue
                        receipt=now_utc()
                        try: msg=json.loads(raw) if isinstance(raw,(str,bytes,bytearray)) else raw
                        except Exception: continue
                        data=msg.get("data") if isinstance(msg,dict) else None
                        if isinstance(data,dict) and str(data.get("type") or "").lower() in {"quote","tick","depth"}:
                            market.append({"received_at":iso(receipt),"symbol":normalize_symbol(data.get("s") or data.get("symbol") or ""),"type":str(data.get("type") or "").lower(),"source_timestamp":iso(parse_timestamp(data.get("t"))),"price":finite_number(data.get("ld") or data.get("price"))})
                    mark("first_market_event","PASS" if market else "NO_EVENT",count=len(market))
                else:
                    mark("first_market_event","BLOCKED",reason="subscription not acknowledged")
        except Exception as exc:
            mark("websocket_handshake","FAIL",error=repr(exc))
            errors.append({"stage":"connection","error":repr(exc),"exception_type":type(exc).__name__})
        elapsed=time.monotonic()-started
        return {"test":"V3.6 WebSocket Connection Diagnostic","status":"PASS" if market and not errors else "FAILED",
                "gateway":"ASX MAXIMUM EDGE V8-I Data Gateway V3.6","started_at_utc":iso(started_wall),
                "completed_at_utc":iso(now_utc()),"elapsed_seconds":round(elapsed,3),"url":uri,
                "symbols":syms,"types":types,"stages":stages,"market_events":market,
                "control_message_count":len(control),"errors":errors,
                "execution_authorization":execution_authorization(),
                "interpretation":"Diagnostic only; a PASS does not grant execution authorization or prove continuous streaming quality."}

    async def websocket_test(self, symbols: list[str], types: str = "quote", timeout: float = ITICK_WS_TIMEOUT) -> dict:
        """Short capability probe. Does not consume REST quota."""
        return await self._websocket_stream_test(symbols, types, timeout, mode="capability")

    async def websocket_endurance_test(self, symbols: list[str], types: str = "quote",
                                       duration_seconds: float = ITICK_WS_ENDURANCE_SECONDS) -> dict:
        """V8-I streaming acceptance engine.

        Distinguishes NO_EVENT from STALE_EVENT and FRESH_EVENT. For every
        market-data event it records source timestamp t, gateway receipt time,
        age, price, symbol and event type. It measures timestamp progression,
        event frequency, gaps and per-symbol coverage. A quiet symbol is not
        automatically failed; a received stale/repeated stream is treated
        differently from no observed event.
        """
        return await self._websocket_stream_test(symbols, types, duration_seconds, mode="endurance")

    async def _websocket_stream_test(self, symbols: list[str], types: str,
                                     duration_seconds: float, mode: str) -> dict:
        if not self.configured:
            return {"supported": False, "promotion": "DO NOT PROMOTE", "stage": "configuration",
                    "error": "ITICK_API_TOKEN not configured", "execution_grade": "NOT_GRANTED"}
        symbols = list(dict.fromkeys(normalize_symbol(x) for x in symbols if normalize_symbol(x)))
        if not symbols:
            return {"supported": False, "promotion": "DO NOT PROMOTE", "stage": "input",
                    "error": "no symbols supplied", "execution_grade": "NOT_GRANTED"}
        try:
            duration = max(1.0, min(float(duration_seconds), 1800.0))
        except Exception:
            duration = 120.0
        types = ",".join(sorted(set(t.strip().lower() for t in str(types).split(",") if t.strip()))) or "quote"
        uri = ITICK_WS_URL
        params = ",".join(f"{sym}${self.region}" for sym in symbols)
        started_monotonic = time.monotonic()
        connected_at = None
        gateway_started = now_utc()
        events = []
        market_events = []
        control_events = []
        errors = []
        auth = False
        connected = False
        subscribed = False
        subscription_ack_at = None
        last_event_monotonic = None
        per_symbol = {sym: {"market_events": 0, "quote_events": 0, "tick_events": 0,
                            "depth_events": 0, "fresh_events": 0, "stale_events": 0,
                            "invalid_timestamp_events": 0, "timestamp_progressions": 0,
                            "duplicate_timestamp_events": 0, "source_timestamps": [],
                            "ages_seconds": [], "event_times": [], "prices": []}
                     for sym in symbols}
        prev_ts = {sym: None for sym in symbols}
        prev_receipt = {sym: None for sym in symbols}
        max_gap = {sym: 0.0 for sym in symbols}
        stationary_counts = {sym: 0 for sym in symbols}
        first_market_at = None
        last_market_at = None
        close_reason = "duration_elapsed"
        websocket_error = None

        def classify_event(data: dict, receipt: datetime) -> dict:
            sym = normalize_symbol(data.get("s") or data.get("symbol") or "")
            typ = str(data.get("type") or "unknown").lower()
            ts = parse_timestamp(data.get("t"))
            price = finite_number(data.get("ld") or data.get("price"))
            age = (receipt - ts).total_seconds() if ts else None
            if ts is None:
                freshness = "UNKNOWN"
                reason = "source_timestamp_unavailable"
            elif age < -FUTURE_TOLERANCE:
                freshness = "RED"; reason = "source_timestamp_in_future"
            elif age <= GREEN_MAX_AGE:
                freshness = "GREEN"; reason = "timestamp_verified_fresh"
            elif age <= AMBER_MAX_AGE:
                freshness = "AMBER"; reason = "timestamp_verified_older"
            elif age <= ORANGE_MAX_AGE:
                freshness = "ORANGE"; reason = "timestamp_verified_stale"
            else:
                freshness = "RED"; reason = "timestamp_verified_too_old"
            return {"symbol": sym, "type": typ, "price": price, "source_timestamp": iso(ts),
                    "gateway_received_at": iso(receipt),
                    "age_seconds": round(max(age, 0.0), 3) if age is not None else None,
                    "timestamp_verified": ts is not None, "freshness": freshness, "reason": reason}

        connection_attempts=[]
        try:
            last_exc=None
            ws_cm=None
            for attempt in range(1, V36_WS_CONNECT_ATTEMPTS+1):
                attempt_started=time.monotonic()
                try:
                    ws_cm=websockets.connect(uri, additional_headers={"token": self.token},
                                             open_timeout=min(15.0, max(5.0, duration)),
                                             close_timeout=3, ping_interval=20, ping_timeout=10)
                    ws = await ws_cm.__aenter__()
                    connection_attempts.append({"attempt":attempt,"status":"PASS","elapsed_ms":round((time.monotonic()-attempt_started)*1000,1)})
                    break
                except Exception as exc:
                    last_exc=exc
                    connection_attempts.append({"attempt":attempt,"status":"FAIL","elapsed_ms":round((time.monotonic()-attempt_started)*1000,1),"error":repr(exc),"exception_type":type(exc).__name__})
                    if attempt < V36_WS_CONNECT_ATTEMPTS:
                        await asyncio.sleep(V36_WS_RETRY_BACKOFF_SECONDS * attempt)
            if ws_cm is None or 'ws' not in locals():
                raise last_exc or TimeoutError("WebSocket connection failed")
            try:
                connected_at = now_utc()
                # Collect initial provider messages briefly, without requiring a
                # particular ordering of Connected/auth acknowledgements.
                initial_deadline = time.monotonic() + min(5.0, max(2.0, duration))
                while time.monotonic() < initial_deadline and not auth:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=max(0.2, initial_deadline-time.monotonic()))
                    except asyncio.TimeoutError:
                        break
                    receipt = now_utc()
                    msg = json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else raw
                    events.append(msg)
                    if isinstance(msg, dict) and msg.get("code") == 1 and msg.get("msg") == "Connected Successfully":
                        connected = True
                    if isinstance(msg, dict) and msg.get("code") == 1 and msg.get("resAc") == "auth":
                        auth = True; connected = True
                    if isinstance(msg, dict) and isinstance(msg.get("data"), dict) and msg["data"].get("type") in {"quote","tick","depth"}:
                        ev = classify_event(msg["data"], receipt); market_events.append(ev); last_event_monotonic = time.monotonic()
                    else:
                        control_events.append({"received_at": iso(receipt), "message": msg})
                await ws.send(json.dumps({"ac": "subscribe", "params": params, "types": types}))
                ack_deadline = time.monotonic() + min(8.0, max(3.0, duration))
                while time.monotonic() < ack_deadline and not subscribed:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=max(0.2, ack_deadline-time.monotonic()))
                    except asyncio.TimeoutError:
                        break
                    receipt = now_utc(); msg = json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else raw
                    events.append(msg)
                    if isinstance(msg, dict) and msg.get("code") == 1 and msg.get("resAc") == "subscribe":
                        subscribed = True; subscription_ack_at = receipt
                    elif isinstance(msg, dict) and msg.get("code") == 0:
                        errors.append({"stage": "subscription", "received_at": iso(receipt), "message": msg})
                    if isinstance(msg, dict) and isinstance(msg.get("data"), dict) and msg["data"].get("type") in {"quote","tick","depth"}:
                        market_events.append(classify_event(msg["data"], receipt)); last_event_monotonic = time.monotonic()
                    else:
                        control_events.append({"received_at": iso(receipt), "message": msg})
                if not subscribed:
                    close_reason = "subscription_ack_timeout"
                else:
                    deadline = time.monotonic() + duration
                    while time.monotonic() < deadline and len(market_events) < ITICK_WS_MAX_EVENTS:
                        wait = min(3.0, max(0.2, deadline-time.monotonic()))
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=wait)
                        except asyncio.TimeoutError:
                            # Protocol heartbeat only; never counts as market data.
                            try:
                                await ws.send(json.dumps({"ac": "ping", "params": str(int(time.time()*1000))}))
                            except Exception as exc:
                                errors.append({"stage": "heartbeat", "error": repr(exc)})
                            continue
                        receipt = now_utc(); msg = json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else raw
                        events.append(msg)
                        data = msg.get("data") if isinstance(msg, dict) else None
                        if isinstance(data, dict) and str(data.get("type") or "").lower() in {"quote","tick","depth"}:
                            ev = classify_event(data, receipt)
                            market_events.append(ev); last_event_monotonic = time.monotonic()
                            if first_market_at is None: first_market_at = receipt
                            last_market_at = receipt
                        else:
                            if isinstance(msg, dict) and msg.get("code") == 0:
                                errors.append({"stage": "stream", "received_at": iso(receipt), "message": msg})
                            control_events.append({"received_at": iso(receipt), "message": msg})
            finally:
                try:
                    await ws_cm.__aexit__(None, None, None)
                except Exception:
                    pass
        except Exception as exc:
            websocket_error = repr(exc)
            errors.append({"stage": "connection", "error": websocket_error})
            close_reason = "connection_error"

        # Analyze actual market-data events. A symbol with zero events is not
        # marked stale: it is explicitly NO_EVENT, because a quiet stock cannot
        # prove freshness or staleness without a received event.
        for ev in market_events:
            sym = ev["symbol"]
            if sym not in per_symbol:
                continue
            p = per_symbol[sym]; p["market_events"] += 1; p[f'{ev["type"]}_events'] = p.get(f'{ev["type"]}_events', 0) + 1
            if ev["age_seconds"] is None:
                p["invalid_timestamp_events"] += 1
            else:
                p["ages_seconds"].append(ev["age_seconds"])
                if ev["freshness"] == "GREEN": p["fresh_events"] += 1
                elif ev["freshness"] in {"AMBER","ORANGE","RED"}: p["stale_events"] += 1
            if ev["source_timestamp"]:
                ts = parse_timestamp(ev["source_timestamp"])
                if prev_ts[sym] is not None:
                    delta = (ts - prev_ts[sym]).total_seconds()
                    if delta > 0: p["timestamp_progressions"] += 1; stationary_counts[sym] = 0
                    elif delta == 0: p["duplicate_timestamp_events"] += 1; stationary_counts[sym] += 1
                    else: errors.append({"stage":"temporal_order","symbol":sym,"error":"source_timestamp_regressed",
                                         "previous":iso(prev_ts[sym]),"current":iso(ts)})
                prev_ts[sym] = ts; p["source_timestamps"].append(ev["source_timestamp"])
            if ev["gateway_received_at"]:
                receipt = parse_timestamp(ev["gateway_received_at"])
                if prev_receipt[sym] is not None:
                    max_gap[sym] = max(max_gap[sym], (receipt-prev_receipt[sym]).total_seconds())
                prev_receipt[sym] = receipt; p["event_times"].append(ev["gateway_received_at"])
            if ev["price"] is not None: p["prices"].append(ev["price"])

        elapsed = time.monotonic() - started_monotonic
        for sym,p in per_symbol.items():
            ages=p["ages_seconds"]
            p["min_age_seconds"] = round(min(ages),3) if ages else None
            p["median_age_seconds"] = round(sorted(ages)[len(ages)//2],3) if ages else None
            p["max_age_seconds"] = round(max(ages),3) if ages else None
            p["max_inter_event_gap_seconds"] = round(max_gap[sym],3) if max_gap[sym] else None
            p["temporal_progression_pass"] = p["timestamp_progressions"] > 0
            p["status"] = "FRESH_STREAM" if p["fresh_events"] > 0 and p["timestamp_progressions"] > 0 else ("FRESH_EVENT" if p["fresh_events"] > 0 else ("STALE_STREAM" if p["stale_events"] > 0 else "NO_EVENT"))
            p["stationary_timestamp_warning"] = stationary_counts[sym] >= ITICK_WS_MAX_STATIONARY_EVENTS
            p["event_rate_per_minute"] = round(p["market_events"] / max(elapsed/60.0, 1/60.0), 3)
            p.pop("source_timestamps", None); p.pop("event_times", None); p.pop("prices", None)

        active_symbols = [s for s,p in per_symbol.items() if p["market_events"] > 0]
        fresh_symbols = [s for s,p in per_symbol.items() if p["fresh_events"] > 0]
        progressing_symbols = [s for s,p in per_symbol.items() if p["timestamp_progressions"] > 0]
        stale_symbols = [s for s,p in per_symbol.items() if p["stale_events"] > 0]
        invalid_symbols = [s for s,p in per_symbol.items() if p["invalid_timestamp_events"] > 0]
        stream_quality_symbols = [s for s in symbols if per_symbol[s]["fresh_events"] > 0 and per_symbol[s]["timestamp_progressions"] > 0]
        stream_quality_pass = bool(connected and auth and subscribed and len(active_symbols) >= ITICK_WS_MIN_ACTIVE_SYMBOLS and stream_quality_symbols and not invalid_symbols)
        if mode == "capability":
            promotion = "WEBSOCKET CAPABILITY PASS — STREAMING QUALITY NOT YET PROVEN" if (connected and auth and subscribed and market_events) else "DO NOT PROMOTE"
        else:
            promotion = "WEBSOCKET STREAMING ACCEPTANCE PASS — EXECUTION AUTHORIZATION REMAINS SEPARATE" if stream_quality_pass else "DO NOT PROMOTE"
        return {
            "test": "V8-I WebSocket Streaming Acceptance Engine" if mode == "endurance" else "V8-I WebSocket Capability Test",
            "version": "3.3", "mode": mode, "status": "COMPLETE", "url": uri,
            "symbols_requested": symbols, "types_requested": types,
            "duration_requested_seconds": round(duration,3), "elapsed_seconds": round(elapsed,3),
            "connected_at": iso(connected_at), "gateway_started_at": iso(gateway_started),
            "connection_attempts": connection_attempts,
            "collection_started_at": iso(subscription_ack_at) if subscription_ack_at else None,
            "collection_observed_at": iso(now_utc()),
            "gateway_observed_at": iso(now_utc()), "auth_observed": auth,
            "subscription_acknowledged": subscribed, "subscription_ack_at": iso(subscription_ack_at),
            "close_reason": close_reason, "websocket_error": websocket_error,
            "total_protocol_messages": len(events), "market_data_events": len(market_events),
            "quote_events": sum(1 for e in market_events if e["type"] == "quote"),
            "tick_events": sum(1 for e in market_events if e["type"] == "tick"),
            "depth_events": sum(1 for e in market_events if e["type"] == "depth"),
            "active_symbols": active_symbols, "fresh_symbols": fresh_symbols,
            "progressing_timestamp_symbols": progressing_symbols, "stale_symbols": stale_symbols,
            "invalid_timestamp_symbols": invalid_symbols,
            "per_symbol": per_symbol,
            "events_sample": market_events[-50:], "errors": errors[-20:],
            "acceptance": {
                "connect_pass": connected,
                "authentication_pass": auth,
                "subscription_pass": subscribed,
                "market_event_received": bool(market_events),
                "fresh_event_observed": bool(fresh_symbols),
                "timestamp_progression_observed": bool(progressing_symbols),
                "same_symbol_fresh_and_progressing": stream_quality_symbols,
                "no_invalid_timestamps": not bool(invalid_symbols),
                "quiet_symbol_policy": "NO_EVENT is not treated as stale; no freshness claim is made without a received market-data event",
                "stream_quality_pass": stream_quality_pass,
                "minimum_active_symbols": ITICK_WS_MIN_ACTIVE_SYMBOLS,
                "active_symbol_count": len(active_symbols),
            },
            "policy": {
                "rest_quota_consumed": False,
                "source_timestamp_for_quote_tick": "iTick t",
                "depth_source_timestamp": "not provided in documented payload; remains UNKNOWN unless observed in event",
                "gateway_request_latency_is_not_market_data_age": True,
                "no_event_is_not_stale": True,
                "execution_authorization_separate": True,
                "api_credential_not_exposed": True,
                "connection_retry_attempts": V36_WS_CONNECT_ATTEMPTS,
                "retry_backoff_seconds": V36_WS_RETRY_BACKOFF_SECONDS,
                "persistent_job_manifest": True,
            },
            "execution_grade": "NOT_GRANTED",
            "promotion": promotion,
        }

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
        "gateway":"ASX MAXIMUM EDGE V8-I Data Gateway V3.6",
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
    return {"gateway":"ASX MAXIMUM EDGE V8-I Data Gateway V3.6","execution_authorization":execution_authorization(),"tick_result":result}


@mcp.tool()
async def asx_get_depth(symbol: str) -> dict:
    """Fetch iTick Level-2 depth for one symbol. Depth freshness remains UNKNOWN unless the upstream response carries its own timestamp."""
    src=ITickSource(ITICK_BASE_URL,ITICK_TOKEN,ITICK_REGION,ITICK_EXCHANGE)
    result=await src.get_depth(normalize_symbol(symbol))
    return {"gateway":"ASX MAXIMUM EDGE V8-I Data Gateway V3.6","quote_and_depth_clocks_separate":True,"execution_authorization":execution_authorization(),"depth_result":result}


_WS_JOBS = {}
_WS_JOB_TASKS = {}
_WS_JOB_LOCK = asyncio.Lock()

def _new_job_id():
    return "ws-" + hashlib.sha1(f"{time.time_ns()}".encode()).hexdigest()[:12]

async def _v36_persist(job):
    await asyncio.to_thread(_v36_atomic_write_json, _v36_job_path(job["job_id"]), job)

async def _v36_heartbeat(job):
    job["heartbeat_at"] = iso(now_utc())
    await _v36_persist(job)

async def _run_ws_acceptance_job(job_id, request):
    job = _v36_read_job(job_id) or _WS_JOBS.get(job_id)
    if not job:
        return
    job["status"] = "RUNNING"; job["started_at"] = iso(now_utc()); job["heartbeat_at"] = job["started_at"]
    await _v36_persist(job)
    async def heartbeat_loop():
        while True:
            await asyncio.sleep(V36_JOB_HEARTBEAT_SECONDS)
            job["heartbeat_at"] = iso(now_utc())
            await _v36_persist(job)
    hb_task = asyncio.create_task(heartbeat_loop())
    try:
        src=ITickSource(ITICK_BASE_URL,ITICK_TOKEN,ITICK_REGION,ITICK_EXCHANGE)
        result=await src.websocket_endurance_test(request["symbols"],types=request["types"],duration_seconds=request["duration_seconds"])
        job["status"]="COMPLETE"
        job["completed_at"]=iso(now_utc())
        job["heartbeat_at"]=job["completed_at"]
        job["result"]=result
        job["execution_authorization"]=execution_authorization()
        await _v36_persist(job)
    except asyncio.CancelledError:
        job["status"]="CANCELLED"; job["completed_at"]=iso(now_utc()); job["heartbeat_at"]=job["completed_at"]; job["error"]="job task cancelled"
        await _v36_persist(job)
        raise
    except Exception as exc:
        job["status"]="FAILED"; job["completed_at"]=iso(now_utc()); job["heartbeat_at"]=job["completed_at"]; job["error"]=repr(exc); job["exception_type"]=type(exc).__name__
        job["execution_authorization"]=execution_authorization()
        await _v36_persist(job)
    finally:
        hb_task.cancel()
        try: await hb_task
        except asyncio.CancelledError: pass

@mcp.tool()
async def asx_start_websocket_acceptance(duration_seconds: float=30.0, symbols: list[str] | None=None, types: str="quote") -> str:
    """Start a V3.6 WebSocket acceptance job with persistent job records and heartbeat telemetry."""
    syms=[normalize_symbol(x) for x in (symbols or ITICK_WS_TEST_SYMBOLS) if normalize_symbol(x)]
    try: duration=max(1.0,min(float(duration_seconds),1800.0))
    except Exception: duration=30.0
    job_id=_new_job_id(); created=iso(now_utc())
    request={"duration_seconds":duration,"symbols":syms,"types":types,"gateway_version":"3.6"}
    job={"gateway":"ASX MAXIMUM EDGE V8-I Data Gateway V3.6","job_id":job_id,"status":"STARTED","created_at":created,"started_at":None,"heartbeat_at":None,"completed_at":None,"request":request,"result":None,"execution_authorization":execution_authorization()}
    async with _WS_JOB_LOCK:
        _WS_JOBS[job_id]=job
        await _v36_persist(job)
        _WS_JOB_TASKS[job_id]=asyncio.create_task(_run_ws_acceptance_job(job_id,request))
    return json.dumps({"gateway":"ASX MAXIMUM EDGE V8-I Data Gateway V3.6","status":"STARTED","job_id":job_id,"created_at":created,"request":request,"persistence":"filesystem-backed job manifest","execution_authorization":execution_authorization()})

@mcp.tool()
async def asx_get_websocket_acceptance_status(job_id: str) -> str:
    j=_v36_read_job(job_id) or _WS_JOBS.get(job_id)
    if not j:
        return json.dumps({"gateway":"ASX MAXIMUM EDGE V8-I Data Gateway V3.6","job_id":job_id,"status":"NOT_FOUND"})
    status=j.get("status")
    if status=="RUNNING" and j.get("heartbeat_at"):
        hb=parse_timestamp(j["heartbeat_at"])
        if hb and (now_utc()-hb).total_seconds()>V36_ORPHAN_AFTER_SECONDS:
            status="ORPHANED"
    return json.dumps({"gateway":"ASX MAXIMUM EDGE V8-I Data Gateway V3.6","job_id":job_id,"status":status,"created_at":j.get("created_at"),"started_at":j.get("started_at"),"heartbeat_at":j.get("heartbeat_at"),"completed_at":j.get("completed_at"),"request":j.get("request"),"persistence":"filesystem-backed job manifest"})

@mcp.tool()
async def asx_get_websocket_acceptance_result(job_id: str) -> str:
    j=_v36_read_job(job_id) or _WS_JOBS.get(job_id)
    if not j:
        return json.dumps({"gateway":"ASX MAXIMUM EDGE V8-I Data Gateway V3.6","job_id":job_id,"status":"NOT_FOUND"})
    if j.get("status")=="RUNNING" and j.get("heartbeat_at"):
        hb=parse_timestamp(j["heartbeat_at"])
        if hb and (now_utc()-hb).total_seconds()>V36_ORPHAN_AFTER_SECONDS:
            return json.dumps({"gateway":"ASX MAXIMUM EDGE V8-I Data Gateway V3.6","job_id":job_id,"status":"ORPHANED","message":"Persistent manifest exists but heartbeat is stale; no result is claimed."})
    return json.dumps(j.get("result") or {"gateway":"ASX MAXIMUM EDGE V8-I Data Gateway V3.6","job_id":job_id,"status":j.get("status"),"error":j.get("error"),"message":"Result not yet available"})

@mcp.tool()
async def asx_websocket_job_manifest(job_id: str) -> str:
    """Inspect persistent acceptance-job manifest without claiming market-data success."""
    j=_v36_read_job(job_id)
    return json.dumps(j or {"gateway":"ASX MAXIMUM EDGE V8-I Data Gateway V3.6","job_id":job_id,"status":"NOT_FOUND"})

@mcp.tool()
async def asx_run_websocket_streaming_acceptance(
    symbols: list[str] | None = None,
    types: str = "quote",
    duration_seconds: float | None = None,
) -> dict:
    """Run the V8-I WebSocket Streaming Acceptance Engine.

    Default duration is configurable (120s). Use 600 seconds for the intended
    10-minute endurance run. WebSocket traffic consumes no iTick REST calls.
    The engine measures event freshness, timestamp progression, event gaps and
    per-symbol coverage. NO_EVENT is distinct from STALE_EVENT.
    """
    syms=[normalize_symbol(x) for x in (symbols or ITICK_WS_TEST_SYMBOLS) if normalize_symbol(x)]
    src=ITickSource(ITICK_BASE_URL,ITICK_TOKEN,ITICK_REGION,ITICK_EXCHANGE)
    result=await src.websocket_endurance_test(syms, types=types,
        duration_seconds=duration_seconds or ITICK_WS_ENDURANCE_SECONDS)
    return {"gateway":"ASX MAXIMUM EDGE V8-I Data Gateway V3.6",
            "streaming_acceptance":result,
            "execution_authorization":execution_authorization()}


@mcp.tool()
async def asx_run_websocket_test(symbols: list[str] | None = None, types: str = "quote", timeout_seconds: float | None = None) -> dict:
    """Short WebSocket capability probe. For acceptance use asx_run_websocket_streaming_acceptance."""
    syms=[normalize_symbol(x) for x in (symbols or ITICK_WS_TEST_SYMBOLS) if normalize_symbol(x)]
    src=ITickSource(ITICK_BASE_URL,ITICK_TOKEN,ITICK_REGION,ITICK_EXCHANGE)
    result=await src.websocket_test(syms, types=types, timeout=timeout_seconds or ITICK_WS_TIMEOUT)
    return {"gateway":"ASX MAXIMUM EDGE V8-I Data Gateway V3.6","websocket_result":result,
            "execution_authorization":execution_authorization(),
            "important":["WebSocket testing consumes no iTick REST calls.","Quote/tick events use iTick source timestamp t when present.","A successful capability probe does not prove continuous streaming quality, exchange licensing or execution authorization."]}


@mcp.tool()
async def asx_get_health() -> dict:
    """Return V3.2 health, timestamp model, source configuration, REST/WebSocket diagnostics and execution authorization."""
    itick_configured=bool(ITICK_TOKEN)
    return {
        "gateway":"ASX MAXIMUM EDGE V8-I Data Gateway V3.6","status":"READY","timestamp_utc":iso(now_utc()),
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
        "websocket_acceptance_jobs":{"persistent_manifests":True,"state_dir":"/tmp/asx_v8i_ws_jobs (configurable)","orphan_detection_seconds":V36_ORPHAN_AFTER_SECONDS},
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
