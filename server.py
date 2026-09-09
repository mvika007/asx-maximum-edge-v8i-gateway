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

import httpx
from mcp.server import MCPServer

mcp = MCPServer("ASX MAXIMUM EDGE V8-I Data Gateway V3")

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

    async def _request(self, path: str, params: dict) -> tuple[Any, float, datetime, Optional[str]]:
        if not self.token:
            raise RuntimeError("ITICK_API_TOKEN is not configured")
        started = time.monotonic()
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, follow_redirects=True) as client:
            response = await client.get(
                f"{self.base_url}/{path.lstrip('/')}",
                params=params,
                headers={"accept": "application/json", "token": self.token},
            )
            received_at = now_utc()
            response.raise_for_status()
            payload = response.json()
            return payload, round((time.monotonic() - started) * 1000, 2), received_at, response.headers.get("date")

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
            return {"source": self.name, "available": False, "configured": False, "error": "ITICK_API_TOKEN not configured", "quotes": {}}
        params = {"region": self.region, "codes": ",".join(symbols)}
        if self.exchange:
            params["exchange"] = self.exchange
        try:
            payload, latency_ms, received_at, server_date = await self._request("quotes", params)
        except Exception as exc:
            return {"source": self.name, "available": False, "configured": True, "error": repr(exc), "quotes": {}}

        if payload.get("code") not in (None, 0):
            return {"source": self.name, "available": False, "configured": True, "error": payload.get("msg") or f"iTick code={payload.get('code')}", "quotes": {}}

        raw_data = payload.get("data") or {}
        records = list(raw_data.values()) if isinstance(raw_data, dict) else extract_records(raw_data)
        quotes = {}
        for record in records:
            q = self._normalize_quote(record, "", latency_ms)
            if q.symbol in {normalize_symbol(s) for s in symbols}:
                quotes[q.symbol] = q
        return {
            "source": self.name, "available": True, "configured": True,
            "received_at": iso(received_at), "request_latency_ms": latency_ms,
            "source_server_date_header": server_date,
            "endpoint": "/stock/quotes", "region": self.region,
            "quotes": quotes, "raw_record_count": len(records),
            "note": "iTick t is treated as the upstream latest-trade timestamp; gateway latency is measured separately.",
        }

    async def get_ticks(self, symbols: list[str]) -> dict:
        if not self.configured:
            return {"source": self.name, "available": False, "error": "ITICK_API_TOKEN not configured"}
        params = {"region": self.region, "codes": ",".join(symbols)}
        if self.exchange:
            params["exchange"] = self.exchange
        try:
            payload, latency_ms, received_at, server_date = await self._request("ticks", params)
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
            "source_server_date_header": server_date, "ticks": out,
            "note": "Tick timestamps are source transaction timestamps. Informational/licensing restrictions must be respected.",
        }

    async def get_depth(self, symbol: str) -> dict:
        if not self.configured:
            return {"source": self.name, "available": False, "error": "ITICK_API_TOKEN not configured"}
        params = {"region": self.region, "code": normalize_symbol(symbol)}
        if self.exchange:
            params["exchange"] = self.exchange
        try:
            payload, latency_ms, received_at, server_date = await self._request("depth", params)
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
            "source_server_date_header": server_date,
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
    """V3 primary timestamped quote pipeline. iTick is primary when configured; secondary sources are corroboration only."""
    syms=[normalize_symbol(x) for x in (symbols or DEFAULT_SYMBOLS) if normalize_symbol(x)]
    gateway_received_at=now_utc(); source_results=await fetch_quotes(syms); output={}
    for symbol in syms:
        cross=temporal_cross_validation(symbol,source_results)
        output[symbol]={name:enrich(q,gateway_received_at,cross) for name,res in source_results.items() if (q:=res.get("quotes",{}).get(symbol)) is not None}
    return {
        "gateway":"ASX MAXIMUM EDGE V8-I Data Gateway V3",
        "gateway_received_at":iso(gateway_received_at),
        "timestamp_model":{"source_timestamp":"iTick t = latest trade timestamp","gateway_timestamp":"local UTC receipt time","measured_age":"gateway_received_at - source_timestamp","request_latency_is_not_market_data_age":True},
        "symbols":syms,"sources":{name:{k:res.get(k) for k in ("available","configured","error","errors","received_at","request_latency_ms","endpoint","region","note")} for name,res in source_results.items()},
        "quotes":output,"execution_authorization":execution_authorization(),
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
    return {"gateway":"ASX MAXIMUM EDGE V8-I Data Gateway V3","execution_authorization":execution_authorization(),"tick_result":result}


@mcp.tool()
async def asx_get_depth(symbol: str) -> dict:
    """Fetch iTick Level-2 depth for one symbol. Depth freshness remains UNKNOWN unless the upstream response carries its own timestamp."""
    src=ITickSource(ITICK_BASE_URL,ITICK_TOKEN,ITICK_REGION,ITICK_EXCHANGE)
    result=await src.get_depth(normalize_symbol(symbol))
    return {"gateway":"ASX MAXIMUM EDGE V8-I Data Gateway V3","quote_and_depth_clocks_separate":True,"execution_authorization":execution_authorization(),"depth_result":result}


@mcp.tool()
async def asx_get_health() -> dict:
    """Return V3 health, timestamp model, source configuration and execution authorization."""
    itick_configured=bool(ITICK_TOKEN)
    return {
        "gateway":"ASX MAXIMUM EDGE V8-I Data Gateway V3","status":"READY","timestamp_utc":iso(now_utc()),
        "primary_source":{"name":"iTick","configured":itick_configured,"region":ITICK_REGION,"base_url":ITICK_BASE_URL,"role":"primary_timestamped_quote_source","source_timestamp_field":"t","source_timestamp_semantics":"latest trade timestamp","execution_grade":"NOT_GRANTED_BY_CONFIGURATION"},
        "secondary_sources":[
            {"name":"ASX Equity Stocks / Migizi Tech","enabled":ENABLE_MIGIZI,"role":"price corroboration; no verified source timestamp in this integration"},
            {"name":"Yahoo Finance chart","enabled":ENABLE_YAHOO,"role":"reference/corroboration; not execution-grade"},
        ],
        "configured_symbols":DEFAULT_SYMBOLS,
        "freshness_classes":{"GREEN":f"verified source age <= {GREEN_MAX_AGE:g}s","AMBER":f"verified source age <= {AMBER_MAX_AGE:g}s","ORANGE":f"verified source age <= {ORANGE_MAX_AGE:g}s","RED":"invalid/stale/inconsistent/future timestamp","UNKNOWN":"source age cannot be verified"},
        "rest_rate_limit_assumption":f"{ITICK_FREE_REST_LIMIT_PER_MINUTE} calls/minute (configure to match plan)",
        "execution_authorization":execution_authorization(),
        "critical_policy":"A GREEN quote proves timestamp freshness only. It does not prove exchange licensing, order-book completeness, execution authorization, or trading profitability.",
    }


@mcp.tool()
async def asx_run_gateway_test() -> dict:
    """V3 acceptance test using one iTick batch quote request per snapshot.
    Three snapshots are spaced 2 seconds apart. It does not call depth/ticks,
    preserving the Free Plan's 5 REST-calls/minute budget. Promotion requires
    verified source timestamps and at least one GREEN observation; execution
    authorization remains separate.
    """
    snapshots=[]; failures=[]
    for i in range(3):
        try: snapshots.append(await asx_get_quotes(DEFAULT_SYMBOLS))
        except Exception as exc: failures.append({"snapshot":i+1,"error":repr(exc)})
        if i<2: await asyncio.sleep(2)
    green=[]; per_symbol={}
    for symbol in DEFAULT_SYMBOLS:
        obs=[]
        for snap in snapshots:
            for source,q in snap.get("quotes",{}).get(symbol,{}).items():
                obs.append({"source":source,"freshness":q.get("freshness"),"integrity":q.get("integrity_classification"),"price":q.get("price"),"source_timestamp":q.get("source_timestamp")})
                if q.get("freshness",{}).get("classification")=="GREEN": green.append((symbol,source,q.get("freshness",{}).get("age_seconds")))
        itick_obs=[x for x in obs if x["source"]=="iTick"]
        per_symbol[symbol]={"observations":len(obs),"iTick_observations":len(itick_obs),"iTick_timestamp_verified":sum(1 for x in itick_obs if x["source_timestamp"]),"iTick_green_count":sum(1 for x in itick_obs if x["freshness"].get("classification")=="GREEN"),"pass":len(itick_obs)==len(snapshots) and all(x["integrity"]!="RED" for x in itick_obs)}
    acquisition=not failures and len(snapshots)==3 and all(v["pass"] for v in per_symbol.values())
    promotion="V3 TIMESTAMP GATE PASSED — EXECUTION AUTHORIZATION REMAINS SEPARATE" if acquisition and green else ("ENGINEERING ACQUISITION PASS — FRESHNESS NOT PROVEN" if acquisition else "DO NOT PROMOTE")
    return {"test":"V8-I Data Gateway V3 Tier-1 Timestamp Acceptance Test","run_at_utc":iso(now_utc()),"symbols":DEFAULT_SYMBOLS,"snapshots_completed":len(snapshots),"failures":failures,"per_symbol":per_symbol,"verified_green_observations":green,"acquisition_repeatability_pass":acquisition,"execution_authorization":execution_authorization(),"promotion":promotion,"important":["iTick source t is the only freshness clock for primary quotes.","Gateway request latency is never treated as market-data age.","Migizi/Yahoo price agreement cannot certify freshness when their source timestamps are absent/stale.","Depth requires its own timestamped source before it can be marked GREEN."]}


if __name__ == "__main__":
    mcp.run(transport="streamable-http",host="0.0.0.0",port=int(os.getenv("PORT","8000")),streamable_http_path="/mcp",stateless_http=True,json_response=True)
