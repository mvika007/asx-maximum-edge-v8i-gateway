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

mcp = MCPServer("ASX MAXIMUM EDGE V8-I Data Gateway V2")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MIGIZI_URL = os.getenv(
    "ASX_API_URL",
    "https://migizitech.wixsite.com/asxprices/_functions/getasxprices",
)
MIGIZI_KEY = os.getenv("ASX_API_KEY", "free")

# Yahoo Finance is used as an OPTIONAL independent corroboration source.
# It is not treated as an execution-grade source merely because it returns a
# timestamp; its ASX data is described as delayed by Yahoo.
YAHOO_BASE_URL = os.getenv(
    "YAHOO_CHART_URL",
    "https://query1.finance.yahoo.com/v8/finance/chart",
)
ENABLE_YAHOO = os.getenv("ENABLE_YAHOO_SOURCE", "true").lower() in {
    "1", "true", "yes", "on"
}

DEFAULT_SYMBOLS = [
    s.strip().upper()
    for s in os.getenv("V8I_DEFAULT_SYMBOLS", "CBA,BHP,WGX,CBE,WLC").split(",")
    if s.strip()
]

# Integrity thresholds. These are deliberately conservative.
GREEN_MAX_AGE = float(os.getenv("V8I_GREEN_MAX_AGE_SECONDS", "60"))
AMBER_MAX_AGE = float(os.getenv("V8I_AMBER_MAX_AGE_SECONDS", "180"))
ORANGE_MAX_AGE = float(os.getenv("V8I_ORANGE_MAX_AGE_SECONDS", "900"))
CROSS_MAX_TIME_DELTA = float(os.getenv("V8I_CROSS_MAX_TIME_DELTA_SECONDS", "120"))
CROSS_MAX_PRICE_DIFF_PCT = float(os.getenv("V8I_CROSS_MAX_PRICE_DIFF_PCT", "0.50"))

# Execution authorization is intentionally independent from data integrity.
# Default is ALWAYS false.
EXECUTION_AUTHORIZED = os.getenv(
    "V8I_EXECUTION_AUTHORIZED", "false"
).lower() in {"1", "true", "yes", "on"}

EXECUTION_AUTHORITY_SOURCE = os.getenv(
    "V8I_EXECUTION_AUTHORITY_SOURCE", "external_compliance_risk_system"
)

HTTP_TIMEOUT = float(os.getenv("V8I_HTTP_TIMEOUT_SECONDS", "15"))


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

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
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), default=str
    ).encode()


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
        # Accept seconds or milliseconds since epoch.
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

    # Numeric string.
    try:
        return parse_timestamp(float(text))
    except ValueError:
        pass

    normalized = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
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
                return [value]
        return [payload]
    return []


def classify_age(
    source_timestamp: Optional[datetime],
    gateway_received_at: datetime,
    *,
    complete: bool,
    errors: list[str],
    corroboration: str = "NOT_RUN",
) -> dict:
    if errors:
        return {
            "classification": "RED",
            "age_seconds": None,
            "reason": "validation_error",
        }

    if not complete:
        return {
            "classification": "UNKNOWN",
            "age_seconds": None,
            "reason": "incomplete_required_fields",
        }

    if source_timestamp is None:
        return {
            "classification": "UNKNOWN",
            "age_seconds": None,
            "reason": "source_timestamp_unavailable",
        }

    age = (gateway_received_at - source_timestamp).total_seconds()

    # Future source timestamps are treated as a clock/integrity problem.
    if age < -5:
        return {
            "classification": "RED",
            "age_seconds": round(age, 3),
            "reason": "source_timestamp_in_future",
        }

    if age <= GREEN_MAX_AGE:
        classification = "GREEN"
    elif age <= AMBER_MAX_AGE:
        classification = "AMBER"
    elif age <= ORANGE_MAX_AGE:
        classification = "ORANGE"
    else:
        classification = "RED"

    return {
        "classification": classification,
        "age_seconds": round(max(age, 0.0), 3),
        "reason": "timestamp_verified",
    }


def source_quality_score(
    *,
    source_timestamp: Optional[datetime],
    gateway_received_at: datetime,
    required_complete: bool,
    validation_errors: list[str],
    request_latency_ms: Optional[float],
    declared_refresh_seconds: Optional[float],
    cross_status: str,
    source_is_independent: bool,
) -> dict:
    score = 0.0
    reasons = []

    if required_complete:
        score += 20
        reasons.append("required_fields_complete")
    else:
        reasons.append("required_fields_incomplete")

    if not validation_errors:
        score += 20
        reasons.append("field_validation_pass")
    else:
        reasons.append("field_validation_failed")

    if source_timestamp is not None:
        age = max(
            0.0,
            (gateway_received_at - source_timestamp).total_seconds(),
        )
        if age <= GREEN_MAX_AGE:
            score += 30
            reasons.append("verified_fresh_timestamp")
        elif age <= AMBER_MAX_AGE:
            score += 20
            reasons.append("verified_timestamp_but_older")
        elif age <= ORANGE_MAX_AGE:
            score += 10
            reasons.append("verified_but_stale")
        else:
            reasons.append("verified_but_very_stale")
    else:
        reasons.append("no_source_timestamp")

    if request_latency_ms is not None:
        if request_latency_ms <= 1000:
            score += 10
            reasons.append("low_request_latency")
        elif request_latency_ms <= 3000:
            score += 5
            reasons.append("moderate_request_latency")
        else:
            reasons.append("high_request_latency")

    if declared_refresh_seconds is not None:
        # This is a metadata quality signal, NOT a substitute for source age.
        score += 5
        reasons.append("declared_refresh_interval_available")

    if cross_status == "PASS":
        score += 15
        reasons.append("independent_cross_validation_pass")
    elif cross_status == "NOT_COMPARABLE":
        reasons.append("cross_validation_not_comparable")
    elif cross_status == "FAIL":
        reasons.append("independent_cross_validation_failed")
    elif cross_status == "SOURCE_UNAVAILABLE":
        reasons.append("secondary_source_unavailable")

    return {
        "score": int(round(min(score, 100))),
        "reasons": reasons,
        "independent_source_available": source_is_independent,
    }


def execution_authorization() -> dict:
    # Deliberately independent from all data scoring.
    return {
        "execution_grade": "AUTHORIZED" if EXECUTION_AUTHORIZED else "NOT_GRANTED",
        "authorized": EXECUTION_AUTHORIZED,
        "authority_source": EXECUTION_AUTHORITY_SOURCE,
        "note": (
            "Data integrity never grants trading authorization. "
            "Execution authorization must come from a separate compliance/"
            "risk-control process."
        ),
    }


# ---------------------------------------------------------------------------
# Normalized source model
# ---------------------------------------------------------------------------

@dataclass
class NormalizedQuote:
    symbol: str
    price: Optional[float]
    change: Optional[float]
    change_pct: Optional[float]
    volume: Optional[float]
    high: Optional[float]
    low: Optional[float]
    market_cap_m: Optional[float]
    status: Any
    source_name: str
    source_timestamp: Optional[str]
    field_timestamps: dict[str, Optional[str]]
    source_timestamp_verified: bool
    source_timestamp_kind: str
    declared_refresh_seconds: Optional[float]
    request_latency_ms: Optional[float]
    source_record_hash: str
    validation_errors: list[str]


class MarketDataSource(ABC):
    name: str
    independent: bool = True

    @abstractmethod
    async def get_quotes(self, symbols: list[str]) -> dict:
        raise NotImplementedError

    async def get_depth(self, symbols: list[str]) -> dict:
        return {
            "source": self.name,
            "supported": False,
            "reason": "depth_not_supported_by_source",
        }


class MigiziSource(MarketDataSource):
    name = "ASX Equity Stocks / Migizi Tech"
    independent = True

    def __init__(self, api_url: str, api_key: str):
        self.api_url = api_url
        self.api_key = api_key

    def _normalize(self, record: dict, requested: str) -> NormalizedQuote:
        symbol = normalize_symbol(
            pick(record, "symbol", "Symbol", "code", "Code", "ticker", "Ticker")
            or requested
        )

        record_ts = parse_timestamp(
            pick(
                record,
                "timestamp",
                "Timestamp",
                "time",
                "Time",
                "updatedAt",
                "UpdatedAt",
                "lastUpdated",
                "LastUpdated",
                "dateTime",
                "DateTime",
            )
        )

        field_map = {
            "price": ("price", "Price", "last", "Last", "currentPrice", "CurrentPrice"),
            "change": ("chg", "Chg", "change", "Change"),
            "change_pct": (
                "chgPc",
                "ChgPc",
                "changePct",
                "ChangePct",
                "changePercent",
                "ChangePercent",
            ),
            "volume": ("trdVol", "TrdVol", "volume", "Volume", "tradeVolume"),
            "high": ("high", "High", "dayHigh", "DayHigh"),
            "low": ("low", "Low", "dayLow", "DayLow"),
            "market_cap_m": ("MCm", "MC", "marketCap", "MarketCap"),
            "status": ("status", "Status", "state", "State"),
        }

        values = {}
        for field, keys in field_map.items():
            value = pick(record, *keys)
            values[field] = (
                value if field == "status" else finite_number(value)
            )

        # If a field has an explicit timestamp, prefer it. Otherwise inherit
        # the record-level timestamp. If neither exists, leave it UNKNOWN.
        field_timestamps = {}
        for field in field_map:
            explicit = parse_timestamp(
                pick(
                    record,
                    f"{field}Timestamp",
                    f"{field}_timestamp",
                    f"{field}Time",
                    f"{field}_time",
                )
            )
            field_timestamps[field] = iso(explicit or record_ts)

        errors = []
        price = values["price"]
        volume = values["volume"]
        high = values["high"]
        low = values["low"]

        if price is None or price <= 0:
            errors.append("invalid_or_missing_price")
        if volume is not None and volume < 0:
            errors.append("negative_volume")
        if high is not None and low is not None and high < low:
            errors.append("high_below_low")
        if price is not None and high is not None and price > high:
            errors.append("price_above_high")
        if price is not None and low is not None and price < low:
            errors.append("price_below_low")

        return NormalizedQuote(
            symbol=symbol,
            price=price,
            change=values["change"],
            change_pct=values["change_pct"],
            volume=volume,
            high=high,
            low=low,
            market_cap_m=values["market_cap_m"],
            status=values["status"],
            source_name=self.name,
            source_timestamp=iso(record_ts),
            field_timestamps=field_timestamps,
            source_timestamp_verified=record_ts is not None,
            source_timestamp_kind=(
                "upstream_record_timestamp" if record_ts else "not_provided"
            ),
            declared_refresh_seconds=30.0,
            request_latency_ms=None,
            source_record_hash=sha256(record),
            validation_errors=errors,
        )

    async def _request(self, params: dict) -> tuple[Any, float, datetime]:
        received_at = now_utc()
        started = time.monotonic()
        async with httpx.AsyncClient(
            timeout=HTTP_TIMEOUT, follow_redirects=True
        ) as client:
            response = await client.get(self.api_url, params=params)
            response.raise_for_status()
            payload = response.json()
        latency_ms = round((time.monotonic() - started) * 1000, 2)
        return payload, latency_ms, received_at

    async def get_quotes(self, symbols: list[str]) -> dict:
        params = {
            "apikey": self.api_key,
            "symbols": ",".join(symbols),
            "group": "core",
        }
        try:
            payload, latency_ms, received_at = await self._request(params)
        except Exception as exc:
            return {
                "source": self.name,
                "available": False,
                "error": repr(exc),
                "received_at": iso(now_utc()),
                "quotes": {},
            }

        records = extract_records(payload)
        by_symbol = {}
        for record in records:
            q = self._normalize(record, "")
            if q.symbol:
                q.request_latency_ms = latency_ms
                by_symbol[q.symbol] = q

        quotes = {}
        for symbol in symbols:
            q = by_symbol.get(normalize_symbol(symbol))
            if q:
                quotes[q.symbol] = q

        return {
            "source": self.name,
            "available": True,
            "received_at": iso(received_at),
            "request_latency_ms": latency_ms,
            "declared_refresh_seconds": 30,
            "quotes": quotes,
            "raw_record_count": len(records),
        }

    async def get_depth(self, symbols: list[str]) -> dict:
        params = {
            "apikey": self.api_key,
            "symbols": ",".join(symbols),
            "group": "mdepth",
        }
        try:
            payload, latency_ms, received_at = await self._request(params)
        except Exception as exc:
            return {
                "source": self.name,
                "available": False,
                "supported": True,
                "error": repr(exc),
            }

        records = extract_records(payload)
        output = {}
        for record in records:
            symbol = normalize_symbol(
                pick(record, "symbol", "Symbol", "code", "Code", "ticker", "Ticker")
                or ""
            )
            if not symbol:
                continue

            source_ts = parse_timestamp(
                pick(
                    record,
                    "timestamp",
                    "Timestamp",
                    "time",
                    "Time",
                    "updatedAt",
                    "UpdatedAt",
                    "lastUpdated",
                    "LastUpdated",
                )
            )

            output[symbol] = {
                "symbol": symbol,
                "trade_count": finite_number(
                    pick(record, "TrdNum", "trdNum", "tradeCount")
                ),
                "trade_value": finite_number(
                    pick(record, "TrdVal", "trdVal", "tradeValue")
                ),
                "buy_orders": finite_number(
                    pick(record, "BuyOrders", "buyOrders")
                ),
                "buy_volume": finite_number(
                    pick(record, "BuyVol", "buyVol")
                ),
                "sell_orders": finite_number(
                    pick(record, "SellOrders", "sellOrders")
                ),
                "sell_volume": finite_number(
                    pick(record, "SellVol", "sellVol")
                ),
                "order_ratio": finite_number(
                    pick(record, "OrderRatio", "orderRatio")
                ),
                "volume_ratio": finite_number(
                    pick(record, "VolRatio", "volRatio")
                ),
                "depth_source_timestamp": iso(source_ts),
                "depth_source_timestamp_verified": source_ts is not None,
                "depth_gateway_received_at": iso(received_at),
                "request_latency_ms": latency_ms,
                "declared_refresh_seconds": 900.0,
                "freshness": (
                    classify_age(
                        source_ts,
                        received_at,
                        complete=True,
                        errors=[],
                    )
                    if source_ts
                    else {
                        "classification": "UNKNOWN",
                        "age_seconds": None,
                        "reason": "source_timestamp_unavailable",
                    }
                ),
                "raw_record_hash": sha256(record),
            }

        return {
            "source": self.name,
            "available": True,
            "supported": True,
            "declared_refresh_seconds": 900,
            "gateway_received_at": iso(received_at),
            "request_latency_ms": latency_ms,
            "depth": output,
            "note": (
                "Depth freshness is evaluated independently from quote "
                "freshness. A declared 15-minute refresh interval is metadata "
                "and is not substituted for a source timestamp."
            ),
        }


class YahooChartSource(MarketDataSource):
    name = "Yahoo Finance chart"
    independent = True

    def _normalize_result(
        self, symbol: str, result: dict, latency_ms: float
    ) -> Optional[NormalizedQuote]:
        meta = result.get("meta", {}) or {}
        normalized = normalize_symbol(symbol)
        regular_time = parse_timestamp(meta.get("regularMarketTime"))

        timestamps = result.get("timestamp") or []
        indicators = result.get("indicators", {}) or {}
        quote_arrays = indicators.get("quote") or [{}]
        quote0 = quote_arrays[0] if quote_arrays else {}
        closes = quote0.get("close") or []
        highs = quote0.get("high") or []
        lows = quote0.get("low") or []
        volumes = quote0.get("volume") or []

        last_close = next((x for x in reversed(closes) if finite_number(x) is not None), None)
        last_high = next((x for x in reversed(highs) if finite_number(x) is not None), None)
        last_low = next((x for x in reversed(lows) if finite_number(x) is not None), None)
        last_volume = next((x for x in reversed(volumes) if finite_number(x) is not None), None)

        bar_time = None
        for ts in reversed(timestamps):
            bar_time = parse_timestamp(ts)
            if bar_time:
                break

        source_ts = regular_time or bar_time
        previous_close = finite_number(meta.get("previousClose"))
        price = finite_number(meta.get("regularMarketPrice"))
        if price is None:
            price = finite_number(last_close)

        change = None
        change_pct = None
        if price is not None and previous_close not in (None, 0):
            change = price - previous_close
            change_pct = change / previous_close * 100.0

        if last_volume is None:
            last_volume = finite_number(meta.get("regularMarketVolume"))

        if price is None:
            return None

        field_timestamps = {
            "price": iso(source_ts),
            "change": iso(source_ts),
            "change_pct": iso(source_ts),
            "volume": iso(source_ts),
            "high": iso(bar_time or source_ts),
            "low": iso(bar_time or source_ts),
            "market_cap_m": None,
            "status": iso(source_ts),
        }

        errors = []
        if price <= 0:
            errors.append("invalid_or_missing_price")
        if last_volume is not None and last_volume < 0:
            errors.append("negative_volume")
        if (
            finite_number(last_high) is not None
            and finite_number(last_low) is not None
            and float(last_high) < float(last_low)
        ):
            errors.append("high_below_low")

        return NormalizedQuote(
            symbol=normalized,
            price=price,
            change=change,
            change_pct=change_pct,
            volume=last_volume,
            high=finite_number(last_high),
            low=finite_number(last_low),
            market_cap_m=finite_number(meta.get("marketCap")),
            status=meta.get("marketState"),
            source_name=self.name,
            source_timestamp=iso(source_ts),
            field_timestamps=field_timestamps,
            source_timestamp_verified=source_ts is not None,
            source_timestamp_kind=(
                "regularMarketTime" if regular_time else
                "latest_chart_bar_timestamp" if bar_time else
                "not_provided"
            ),
            # Yahoo itself labels ASX market data as delayed; this is not an
            # assertion that the delay is exactly 15/20 minutes for equities.
            declared_refresh_seconds=None,
            request_latency_ms=latency_ms,
            source_record_hash=sha256(result),
            validation_errors=errors,
        )

    async def get_quotes(self, symbols: list[str]) -> dict:
        received_at = now_utc()
        quotes = {}
        errors = {}

        async with httpx.AsyncClient(
            timeout=HTTP_TIMEOUT,
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0"},
        ) as client:
            for symbol in symbols:
                ticker = f"{normalize_symbol(symbol)}.AX"
                started = time.monotonic()
                try:
                    response = await client.get(
                        f"{YAHOO_BASE_URL.rstrip('/')}/{ticker}",
                        params={
                            "interval": "1m",
                            "range": "1d",
                            "events": "div,splits",
                        },
                    )
                    response.raise_for_status()
                    payload = response.json()
                    latency_ms = round((time.monotonic() - started) * 1000, 2)
                    result = ((payload.get("chart") or {}).get("result") or [None])[0]
                    if not result:
                        raise ValueError("Yahoo returned no chart result")
                    q = self._normalize_result(symbol, result, latency_ms)
                    if q:
                        quotes[q.symbol] = q
                except Exception as exc:
                    errors[normalize_symbol(symbol)] = repr(exc)

        return {
            "source": self.name,
            "available": bool(quotes),
            "received_at": iso(received_at),
            "quotes": quotes,
            "errors": errors,
            "note": (
                "Yahoo Finance describes ASX market data as delayed. Its "
                "timestamps are used for source-time auditing/cross-validation, "
                "not as proof of exchange-real-time execution data."
            ),
        }


# ---------------------------------------------------------------------------
# Source registry and cross-validation
# ---------------------------------------------------------------------------

def build_sources() -> list[MarketDataSource]:
    sources: list[MarketDataSource] = [
        MigiziSource(MIGIZI_URL, MIGIZI_KEY)
    ]
    if ENABLE_YAHOO:
        sources.append(YahooChartSource())
    return sources


async def fetch_from_sources(symbols: list[str]) -> dict:
    sources = build_sources()
    results = await asyncio.gather(
        *(source.get_quotes(symbols) for source in sources),
        return_exceptions=False,
    )
    return {result["source"]: result for result in results}


def quote_dict(q: NormalizedQuote) -> dict:
    return asdict(q)


def cross_validate(
    symbol: str,
    source_results: dict[str, dict],
) -> dict:
    observations = []
    for source_name, result in source_results.items():
        q = result.get("quotes", {}).get(symbol)
        if q:
            observations.append(q)

    if len(observations) < 2:
        return {
            "status": "SOURCE_UNAVAILABLE",
            "symbol": symbol,
            "sources_compared": [q.source_name for q in observations],
            "reason": "fewer_than_two_independent_observations",
        }

    # Compare every pair, but only make a PASS/FAIL decision when timestamps
    # are close enough to be economically comparable.
    comparable_pairs = []
    for i, a in enumerate(observations):
        for b in observations[i + 1:]:
            ta = parse_timestamp(a.source_timestamp)
            tb = parse_timestamp(b.source_timestamp)
            if ta is None or tb is None:
                continue
            delta = abs((ta - tb).total_seconds())
            if delta > CROSS_MAX_TIME_DELTA:
                continue

            if a.price is None or b.price is None:
                continue

            denominator = max(abs(a.price), abs(b.price), 1e-9)
            diff_pct = abs(a.price - b.price) / denominator * 100.0
            comparable_pairs.append(
                {
                    "source_a": a.source_name,
                    "source_b": b.source_name,
                    "time_delta_seconds": round(delta, 3),
                    "price_a": a.price,
                    "price_b": b.price,
                    "price_difference_pct": round(diff_pct, 4),
                    "pass": diff_pct <= CROSS_MAX_PRICE_DIFF_PCT,
                }
            )

    if not comparable_pairs:
        return {
            "status": "NOT_COMPARABLE",
            "symbol": symbol,
            "sources_compared": [q.source_name for q in observations],
            "reason": (
                "source timestamps missing or too far apart for a valid "
                "cross-source comparison"
            ),
        }

    passed = all(pair["pass"] for pair in comparable_pairs)
    return {
        "status": "PASS" if passed else "FAIL",
        "symbol": symbol,
        "sources_compared": [q.source_name for q in observations],
        "pairs": comparable_pairs,
        "reason": (
            "independent observations agree within configured tolerance"
            if passed
            else "independent observations disagree beyond configured tolerance"
        ),
    }


def enrich_quote(
    q: NormalizedQuote,
    received_at: datetime,
    cross: dict,
) -> dict:
    data = quote_dict(q)
    complete = q.price is not None and q.change_pct is not None and q.volume is not None

    freshness = classify_age(
        parse_timestamp(q.source_timestamp),
        received_at,
        complete=complete,
        errors=q.validation_errors,
        corroboration=cross["status"],
    )

    quality = source_quality_score(
        source_timestamp=parse_timestamp(q.source_timestamp),
        gateway_received_at=received_at,
        required_complete=complete,
        validation_errors=q.validation_errors,
        request_latency_ms=q.request_latency_ms,
        declared_refresh_seconds=q.declared_refresh_seconds,
        cross_status=cross["status"],
        source_is_independent=q.source_name != "ASX MAXIMUM EDGE V8-I",
    )

    # A cross-source failure is a hard integrity problem when the sources are
    # temporally comparable. A missing secondary source is not treated as a
    # false PASS.
    integrity = freshness["classification"]
    if cross["status"] == "FAIL":
        integrity = "RED"

    data.update(
        {
            "freshness": freshness,
            "integrity_classification": integrity,
            "quality": quality,
            "cross_validation": cross,
        }
    )
    return data


# ---------------------------------------------------------------------------
# MCP tools
# ---------------------------------------------------------------------------

@mcp.tool()
async def asx_get_quotes(symbols: list[str] | None = None) -> dict:
    """Fetch normalized quotes from all configured sources, timestamp them,
    score source quality, assess freshness, and cross-validate where possible.
    """
    syms = [
        normalize_symbol(x)
        for x in (symbols or DEFAULT_SYMBOLS)
        if normalize_symbol(x)
    ]
    received_at = now_utc()
    source_results = await fetch_from_sources(syms)

    output = {}
    for symbol in syms:
        cross = cross_validate(symbol, source_results)
        output[symbol] = {
            source_name: enrich_quote(q, received_at, cross)
            for source_name, result in source_results.items()
            if (q := result.get("quotes", {}).get(symbol)) is not None
        }

    return {
        "gateway": "ASX MAXIMUM EDGE V8-I Data Gateway V2",
        "gateway_received_at": iso(received_at),
        "symbols": syms,
        "sources": {
            name: {
                "available": result.get("available", False),
                "error": result.get("error"),
                "errors": result.get("errors", {}),
                "received_at": result.get("received_at"),
                "request_latency_ms": result.get("request_latency_ms"),
                "note": result.get("note"),
            }
            for name, result in source_results.items()
        },
        "quotes": output,
        "execution_authorization": execution_authorization(),
        "policy": {
            "green_does_not_authorize_trading": True,
            "missing_source_timestamp_is_unknown": True,
            "stale_or_inconsistent_quotes_are_rejected": True,
        },
    }


@mcp.tool()
async def asx_get_quote(symbol: str) -> dict:
    """Fetch one symbol through the V2 multi-source integrity pipeline."""
    return await asx_get_quotes([normalize_symbol(symbol)])


@mcp.tool()
async def asx_get_depth(symbols: list[str] | None = None) -> dict:
    """Fetch market depth separately from quote data.
    Depth has its own source timestamp/freshness classification.
    """
    syms = [
        normalize_symbol(x)
        for x in (symbols or DEFAULT_SYMBOLS)
        if normalize_symbol(x)
    ]
    received_at = now_utc()
    migizi = MigiziSource(MIGIZI_URL, MIGIZI_KEY)
    result = await migizi.get_depth(syms)

    return {
        "gateway": "ASX MAXIMUM EDGE V8-I Data Gateway V2",
        "gateway_received_at": iso(received_at),
        "quote_and_depth_clocks_separate": True,
        "execution_authorization": execution_authorization(),
        "depth_result": result,
    }


@mcp.tool()
async def asx_get_health() -> dict:
    """Return V2 health, source configuration, integrity policy, and
    execution authorization. Health never implies execution permission.
    """
    return {
        "gateway": "ASX MAXIMUM EDGE V8-I Data Gateway V2",
        "status": "READY",
        "timestamp_utc": iso(now_utc()),
        "sources": [
            {
                "name": "ASX Equity Stocks / Migizi Tech",
                "enabled": True,
                "role": "primary",
                "declared_core_refresh_seconds": 30,
                "source_timestamp_support": "opportunistic_only",
            },
            {
                "name": "Yahoo Finance chart",
                "enabled": ENABLE_YAHOO,
                "role": "independent_corroboration",
                "source_timestamp_support": "regularMarketTime_or_chart_bar",
                "execution_grade": "NOT_GRANTING",
            },
        ],
        "configured_symbols": DEFAULT_SYMBOLS,
        "integrity_classes": {
            "GREEN": f"verified source timestamp age <= {GREEN_MAX_AGE:g}s",
            "AMBER": f"verified source timestamp age <= {AMBER_MAX_AGE:g}s",
            "ORANGE": f"verified source timestamp age <= {ORANGE_MAX_AGE:g}s",
            "RED": "invalid/stale/inconsistent/future-timestamp data",
            "UNKNOWN": "source age cannot be verified",
        },
        "execution_authorization": execution_authorization(),
        "note": (
            "Integrity classification is not trading authorization. "
            "No source quality score can authorize a trade."
        ),
    }


@mcp.tool()
async def asx_run_gateway_test() -> dict:
    """V2 Tier-1 acceptance test: 3 snapshots of the five default symbols.
    Requires acquisition, repeatability, field integrity, and reports
    timestamp/cross-source status without pretending latency is proven.
    """
    snapshots = []
    failures = []

    for i in range(3):
        try:
            snapshots.append(await asx_get_quotes(DEFAULT_SYMBOLS))
        except Exception as exc:
            failures.append({"snapshot": i + 1, "error": repr(exc)})
        if i < 2:
            await asyncio.sleep(2)

    per_symbol = {}
    for symbol in DEFAULT_SYMBOLS:
        observations = []
        for snap in snapshots:
            observations.append(snap.get("quotes", {}).get(symbol, {}))

        source_presence = {}
        for obs in observations:
            for source_name, q in obs.items():
                source_presence.setdefault(source_name, []).append(q)

        per_source = {}
        for source_name, qs in source_presence.items():
            valid = sum(
                1
                for q in qs
                if q.get("integrity_classification") not in {"RED"}
                and not q.get("validation_errors")
            )
            per_source[source_name] = {
                "snapshots_observed": len(qs),
                "non_red_count": valid,
                "pass": len(qs) == 3 and valid == 3,
            }

        cross_statuses = [
            q.get("cross_validation", {}).get("status")
            for obs in observations
            for q in obs.values()
        ]

        per_symbol[symbol] = {
            "snapshots_observed": len(observations),
            "source_presence": per_source,
            "cross_validation_statuses": cross_statuses,
            "pass": len(observations) == 3 and all(
                v["pass"] for v in per_source.values()
            ),
        }

    acquisition_pass = (
        len(snapshots) == 3
        and not failures
        and all(v["pass"] for v in per_symbol.values())
    )

    # Promotion is deliberately NOT the same as acquisition_pass.
    timestamp_verified_any = False
    for snap in snapshots:
        for symbol_data in snap.get("quotes", {}).values():
            for q in symbol_data.values():
                if q.get("freshness", {}).get("classification") == "GREEN":
                    timestamp_verified_any = True

    return {
        "test": "V8-I Data Gateway V2 Tier-1 Acceptance Test",
        "run_at_utc": iso(now_utc()),
        "symbols": DEFAULT_SYMBOLS,
        "snapshots_completed": len(snapshots),
        "failures": failures,
        "per_symbol": per_symbol,
        "acquisition_repeatability_field_integrity_pass": acquisition_pass,
        "verified_green_observation_seen": timestamp_verified_any,
        "execution_authorization": execution_authorization(),
        "promotion": (
            "ENGINEERING PASS — FRESHNESS STILL UNPROVEN"
            if acquisition_pass and not timestamp_verified_any
            else (
                "INTEGRITY PIPELINE PASS — EXECUTION AUTHORIZATION REMAINS SEPARATE"
                if acquisition_pass
                else "DO NOT PROMOTE"
            )
        ),
        "important": (
            "A declared refresh interval is not substituted for a source "
            "timestamp. GREEN requires verified source age. Cross-validation "
            "cannot certify freshness when source timestamps are absent or "
            "not temporally comparable."
        ),
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
