"""Instrument identity, public stock observations and exchange session boundaries.

Prices are USD/share; USDC is a payment currency, not a quote conversion.
Source contracts and calendar references are documented in mappings/binance_markets.md.
"""
import os
import time
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from btc_collection import finite_number, epoch_ms

HOUR = 3600000
ET = ZoneInfo("America/New_York")
EQUITY_BASE = "https://www.binance.com/bapi/equity/"
CHART_PATH = "v1/public/equity/kline/chart"
PRICE_STATE_KEYS = ("market_structure_state", "market_pattern_history", "snapshots", "last_structure_state",
                    "last_market", "last_market_24h_delta", "last_seven_day_comparison", "latest_valid_report", "latest_observation_report")


@dataclass(frozen=True)
class Instrument:
    asset_id: str
    price_symbol: str
    derivatives_symbol: str
    derivatives_relation: str
    price_currency: str
    volume_unit: str
    payment_currency: str
    calendar: str

    @property
    def equity(self):
        return self.calendar == "US_EQUITY"

    def identity(self):
        return asdict(self)

    def parameter(self, name, default):
        # BTC keeps its existing settings. Each equity can override the same rules.
        value = os.getenv(self.asset_id + "_" + name) if self.equity else None
        return type(default)(value) if value is not None else default


INSTRUMENTS = {
    "BTC": Instrument("BTC", "BTCUSDT", "BTCUSDT", "direct", "USDT", "BTC", "USDT", "24/7"),
    "QQQ": Instrument("QQQ", "EQ_QQQ", "QQQUSDT", "direct", "USD", "shares", "USDC", "US_EQUITY"),
    "VOO": Instrument("VOO", "EQ_VOO", "SPYUSDT", "proxy", "USD", "shares", "USDC", "US_EQUITY"),
}
_instrument = ContextVar("market_instrument", default=INSTRUMENTS["BTC"])
_calendar = ContextVar("equity_calendar", default=None)


def current():
    return _instrument.get()


@contextmanager
def asset_scope(asset):
    spec = asset if isinstance(asset, Instrument) else INSTRUMENTS[str(asset).upper()]
    token = _instrument.set(spec)
    calendar_token = _calendar.set(None)
    try:
        yield spec
    finally:
        _instrument.reset(token)
        _calendar.reset(calendar_token)


def state_path(btc_path):
    spec = current()
    if not spec.equity:
        return btc_path
    stem, suffix = os.path.splitext(btc_path)
    return os.path.expanduser(os.getenv(spec.asset_id + "_STATE_FILE", stem + "." + spec.asset_id.lower() + suffix))


def parse_command(text):
    parts = (text or "").strip().upper().split()
    if not parts:
        return None
    command = parts[0].split("@", 1)[0]
    if command == "/MACRO" and len(parts) == 1:
        return "QUERY_MACRO:COMMON"
    queries = {"/" + asset: asset for asset in INSTRUMENTS}
    if command in queries and len(parts) == 1:
        action, asset = "QUERY_PUSH", queries[command]
    elif command in ("/FORCE", "/REPAIR") and len(parts) <= 2:
        action, asset = ("FORCE_PUSH" if command == "/FORCE" else "REPAIR"), (parts[1] if len(parts) == 2 else "BTC")
        if asset not in INSTRUMENTS:
            return None
    else:
        return None
    return action if asset == "BTC" else action + ":" + asset


def command_identity(command):
    if command == "QUERY_MACRO:COMMON":
        return "QUERY_MACRO", "COMMON"
    action, _, asset = str(command).partition(":")
    if action not in ("QUERY_PUSH", "FORCE_PUSH", "REPAIR") or (asset and asset not in INSTRUMENTS):
        raise ValueError("unknown_market_command")
    return action, asset or "BTC"


# Published exchange holidays; unknown years are explicitly unverified.
_HOLIDAYS = {
    2025: "01-01 01-09 01-20 02-17 04-18 05-26 06-19 07-04 09-01 11-27 12-25",
    2026: "01-01 01-19 02-16 04-03 05-25 06-19 07-03 09-07 11-26 12-25",
    2027: "01-01 01-18 02-15 03-26 05-31 06-18 07-05 09-06 11-25 12-24",
    2028: "01-17 02-21 04-14 05-29 06-19 07-04 09-04 11-23 12-25",
}
_EARLY = {2025: "07-03 11-28 12-24", 2026: "11-27 12-24", 2027: "11-26", 2028: "07-03 11-24"}


def trading_day(day):
    return day.year in _HOLIDAYS and day.weekday() < 5 and day.strftime("%m-%d") not in _HOLIDAYS[day.year].split()


def calendar_session(timestamp, calendar=None):
    local = datetime.fromtimestamp(timestamp, ET)
    day, minute = local.date(), local.hour * 60 + local.minute
    trade_date = day + timedelta(days=1) if minute >= 1200 else day
    result = {"timezone": "America/New_York", "trading_date": trade_date.isoformat(),
              "calendar_known": trade_date.year in _HOLIDAYS, "open": False, "phase": "CLOSED"}
    if calendar and calendar["start_ms"] <= timestamp * 1000 < calendar["end_ms"]:
        result.update(calendar_known=True, calendar_source="Binance Stocks calendar/periods")
        for period in calendar["periods"]:
            if period["start_ms"] <= timestamp * 1000 < period["end_ms"]:
                return {**result, "open": True, "phase": period["phase"]}
        return {**result, "reason": "source_calendar_closed"}
    result["calendar_source"] = "NYSE published calendar 2025-2028"
    if not result["calendar_known"]:
        return {**result, "phase": "UNKNOWN", "reason": "calendar_year_unverified"}
    if not trading_day(trade_date):
        return {**result, "reason": "weekend_or_exchange_holiday"}
    early = day.strftime("%m-%d") in _EARLY.get(day.year, "").split()
    regular_end, late_end = (780, 1020) if early else (960, 1200)
    if minute >= 1200 or minute < 240:
        phase = "OVERNIGHT"
    elif minute < 570:
        phase = "PRE"
    elif minute < regular_end:
        phase = "REGULAR"
    elif minute < late_end:
        phase = "POST"
    else:
        return {**result, "reason": "early_close"}
    return {**result, "open": True, "phase": phase, "early_close": early}


def equity_get(path, params=None, max_age=0, *, language=None):
    raise RuntimeError("retired_undocumented_stock_rest")


def normalize_calendar(receipt):
    data = receipt["data"]
    phases = {"OVERNIGHT_EARLY": "OVERNIGHT", "OVERNIGHT_LATE": "OVERNIGHT",
              "OVERNIGHT": "OVERNIGHT", "PRE_MARKET": "PRE", "MARKET_OPEN": "REGULAR", "POST_MARKET": "POST"}
    days, periods = [], []
    for day in data["days"]:
        day_start = datetime.combine(date.fromisoformat(day["date"]), datetime.min.time(), ET)
        start, end = int(day_start.timestamp() * 1000), int((day_start + timedelta(days=1)).timestamp() * 1000)
        days.append((start, end))
        for row in day["periods"]:
            opening, closing = epoch_ms(row.get("startTime")), epoch_ms(row.get("endTime"))
            if closing is not None and closing % 1000 == 999:
                closing += 1  # The late overnight segment uses an inclusive end.
            if row.get("type") not in phases or opening is None or closing is None or not start <= opening < closing <= end:
                raise ValueError("invalid_equity_calendar_period")
            periods.append({"start_ms": opening, "end_ms": closing, "phase": phases[row["type"]]})
    if not days:
        raise ValueError("empty_equity_calendar")
    periods.sort(key=lambda row: row["start_ms"])
    if any(left["end_ms"] > right["start_ms"] for left, right in zip(periods, periods[1:])):
        raise ValueError("overlapping_equity_calendar")
    return {"start_ms": min(day[0] for day in days), "end_ms": max(day[1] for day in days),
            "periods": periods, "retrieved_at": receipt["retrieved_at"],
            "endpoint_url": EQUITY_BASE + "v1/public/equity/calendar/periods"}


def calendar_observation():
    return _calendar.get()


def fetch_calendar():
    # The documented WS calendar broadcasts transitions, not historical periods.
    _calendar.set(None)
    return None


def market_session(spec=None, now=None):
    from market_history import read_meta
    spec, now = spec or current(), time.time() if now is None else now
    if not spec.equity:
        return {"open": True, "phase": "24/7", "calendar_known": True}
    session = calendar_session(now)
    if not session["open"]:
        return session
    price = read_meta("stock_price:" + spec.asset_id, default={})
    health = read_meta("stream:stocks", default={})
    status = read_meta("stock_tradingStatus:" + spec.asset_id, default={})
    tradability = read_meta("stock_tradability:" + spec.asset_id, default={})
    phases = {"ON": "OVERNIGHT", "PRE": "PRE", "O": "REGULAR", "POST": "POST", "C": "CLOSED"}
    phase = phases.get(price.get("phase"), "UNKNOWN")
    fresh = (health.get("connected") is True and
             0 <= now * 1000 - health.get("last_response_ms", 0) <= 60000 and
             0 <= now * 1000 - price.get("retrieved_at_ms", 0) <= 60000)
    halt = status.get("status") in ("TRADING_HALTED", "TRADING_PAUSED", "TRADING_SUSPENDED", "UNAVAILABLE")
    restricted = tradability.get("tradability") in ("NONE", "OFFMARKET", "DELISTING")
    return {**session, "open": fresh and phase == session["phase"] and not halt and not restricted,
            "phase": phase if fresh else "UNKNOWN", "source_phase": price.get("phase"),
            "status_confirmed": bool(status or tradability), "reason": None if fresh else "public_stream_not_ready",
            "source_as_of_ms": price.get("source_as_of_ms")}


def expected_hour_closes(start_ms, end_ms, calendar=None):
    calendar = _calendar.get() if calendar is None else calendar
    first = int(start_ms) // HOUR * HOUR
    return [opening + HOUR - 1 for opening in range(first, int(end_ms) + 1, HOUR)
            if start_ms <= opening + HOUR - 1 <= end_ms
            and calendar_session((opening + HOUR // 2) / 1000, calendar)["open"]]


def latest_expected_close(now_ms):
    end = int(now_ms) // HOUR * HOUR - 1
    values = expected_hour_closes(end - 10 * 24 * HOUR, end)
    return values[-1] if values else None


def history_contiguous(bars, calendar=None):
    if not bars:
        return False
    times = [row["close_time"] for row in bars]
    return times == expected_hour_closes(times[0], times[-1], calendar)


def normalize_stock_bars(responses, spec, now_ms):
    bars = {}
    for session, receipt in responses.items():
        data = receipt["data"]
        if data.get("symbol") != spec.asset_id or data.get("adjustmentMode") != "ADJUSTED" or data.get("timeframe") != "1H":
            raise ValueError("stock_history_identity_or_adjustment_mismatch")
        for row in data.get("bars") or []:
            opening, close = epoch_ms(row.get("t")), epoch_ms(row.get("ct"))
            if opening is None or close is None or close >= now_ms:
                continue
            prices = {key: finite_number(row.get(raw)) for key, raw in (("open", "o"), ("high", "h"), ("low", "l"), ("close", "c"))}
            volume = finite_number(row.get("v"))
            if (close - opening != HOUR - 1 or opening % HOUR or volume is None or volume < 0
                    or any(value is None or value <= 0 for value in prices.values())
                    or not prices["low"] <= min(prices["open"], prices["close"]) <= max(prices["open"], prices["close"]) <= prices["high"]):
                raise ValueError("invalid_stock_hour_bar")
            bar = {**prices, "open_time": opening, "close_time": close, "volume": volume,
                   "volume_unit": "shares", "price_currency": "USD", "calendar": "US_EQUITY",
                   "symbol": spec.price_symbol, "session": session, "adjustment_mode": "ADJUSTED",
                   "signal_source": "Binance Stocks " + spec.price_symbol + " ADJUSTED 1h",
                   "endpoint_url": EQUITY_BASE + CHART_PATH, "retrieved_at_ms": int(receipt["retrieved_at"] * 1000)}
            if opening in bars and any(bars[opening][key] != bar[key] for key in (*prices, "volume")):
                raise ValueError("conflicting_stock_sessions")
            bars[opening] = bar
    return [bars[key] for key in sorted(bars)]


def fetch_stock_bars(state, spec=None, limit=1000):
    from market_history import read_rows
    spec = spec or current()
    if state.get("price_history_origin") != "binance_public_ws":
        legacy = {key: state[key] for key in (*PRICE_STATE_KEYS, "bars_1h", "price_adjustment_mode") if key in state}
        if legacy:
            state.setdefault("legacy_stock_history", legacy)
        for key in (*PRICE_STATE_KEYS, "bars_1h"):
            state.pop(key, None)
        state["price_history_origin"] = "binance_public_ws"
        state["price_history_revision"] = int(state.get("price_history_revision") or 0) + 1
        state["history_revision_event"] = {"at": time.time(), "reason": "retired_stock_rest",
                                           "invalidated_anchors": True}
    bars = read_rows("stock_1h", spec.asset_id, limit=limit)
    state["bars_1h"] = bars
    state["price_adjustment_mode"] = "UNKNOWN"
    return bars


def stock_quality(bars, now_ms=None):
    now_ms = time.time() * 1000 if now_ms is None else now_ms
    target = latest_expected_close(now_ms)
    latest = bars[-1] if bars else {}
    recent = [row for row in bars if row["close_time"] >= (target or 0) - 72 * HOUR]
    reasons = []
    if any(row.get("adjustment_mode") != "ADJUSTED" for row in bars) or not bars:
        reasons.append("stock_adjustment_unverified")
    if latest.get("close_time") != target:
        reasons.append("target_hour_missing")
    if not history_contiguous(recent):
        reasons.append("trading_hour_missing")
    if len(bars) < 30:
        reasons.append("insufficient_price_history")
    natural_window_current = latest.get("close_time") == int(now_ms) // HOUR * HOUR - 1
    return {"usable": not reasons, "source": latest.get("signal_source"), "reasons": reasons,
            "latest_close_ms": latest.get("close_time"), "target_close_ms": target, "loaded_bars": len(bars),
            "closed_bars_only": True, "calendar": "US_EQUITY", "age_sec": (now_ms - latest["close_time"]) / 1000 if bars else None,
            "natural_window_current": natural_window_current,
            "contiguous_4h": natural_window_current and len(bars) >= 9 and bars[-1]["open_time"] - bars[-9]["open_time"] == 8 * HOUR}


def stock_quote(spec=None):
    from market_history import read_meta
    from market_sources import STOCK_WS
    spec = spec or current()
    row = read_meta("stock_price:" + spec.asset_id, default={})
    if not row or not 0 <= time.time() * 1000 - row.get("retrieved_at_ms", 0) <= 60000:
        raise ValueError("public_stock_quote_missing_or_stale")
    return {"price": row["price"], "mcap": None, "chg24": None, "vol24": None,
            "session_volume_shares": None, "previous_close": row.get("previous_close"),
            "source": "Binance Stocks public WS " + spec.price_symbol, "price_currency": "USD", "volume_unit": "shares",
            "quote_source_as_of_ms": row.get("source_as_of_ms"), "quote_source_time_missing": row.get("source_time_missing", True),
            "quote_retrieved_at": row["retrieved_at_ms"] / 1000, "quote_endpoint_url": STOCK_WS}


def not_applicable(reason):
    return {"status": "not_applicable", "reason": reason, "value": {}, "source": None,
            "updated_at": None, "applicable": False, "source_as_of_ms": None}


def derivative_identity(record, spec=None):
    spec = spec or current()
    return {**record, "instrument": spec.derivatives_symbol, "relation": spec.derivatives_relation,
            "observed_asset": spec.derivatives_symbol.removesuffix("USDT"), "target_asset": spec.asset_id,
            "quote_currency": "USDT", "analysis_eligible": spec.derivatives_relation == "direct"}


def regular_funding_is_fresh(values, now_ms):
    stamp = finite_number(values.get("latest_funding_time_ms"))
    interval = finite_number(values.get("current_interval_hours"))
    if interval is None:
        observed = ((values.get("settlement_windows") or {}).get("8h") or {}).get("interval_ms")
        interval = finite_number(observed)
        interval = interval / HOUR if interval is not None else None
    return bool(stamp and interval and interval > 0 and 0 <= now_ms - stamp <= interval * HOUR + 300000)
