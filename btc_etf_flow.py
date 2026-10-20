"""Anonymous USD net creations/redemptions, with source-specific validation."""
import json
import math
import re
from datetime import date, datetime, timedelta
from decimal import Decimal

from btc_etf import FUNDS

ENDPOINT = "https://www.tftc.io/bitcoin-etf-flows/data.json"
PAGE = "https://www.tftc.io/bitcoin-etf-flows"
VENUE = "TFTC"
COINBOSS_ENDPOINT = "https://api.coinboss.com/api/etf/detail"
COINBOSS_PAGE = "https://www.coinboss.com/bitcoin-etf"
COINBOSS_VENUE = "CoinBoss"
WINDOWS = (1, 5, 30)


def select_source(records):
    """Prefer complete usable histories, then date; never splice sources."""
    def rank(record):
        count = sum(_number((record.get("value") or {}).get(f"flow_{days}d_usd")) for days in WINDOWS)
        usable = record.get("status") in ("ok", "partial") and not record.get("source_stale") and count > 0
        return (usable, count == len(WINDOWS), count,
                record.get("source_date") or "", not record.get("cache_used"), record.get("venue") == VENUE)
    return max(records, key=rank)


def is_flow_source(record):
    return (record.get("venue"), record.get("endpoint_url")) in (
        (VENUE, ENDPOINT), (COINBOSS_VENUE, COINBOSS_ENDPOINT))


def source_page(venue):
    return {VENUE: PAGE, COINBOSS_VENUE: COINBOSS_PAGE}.get(venue)


def source_day(value):
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError("invalid_flow_date")
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise ValueError("invalid_flow_date") from None


def _number(value):
    return type(value) in (int, float) and math.isfinite(value)


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate_flow_field")
        result[key] = value
    return result


def _reject_constant(_):
    raise ValueError("nonfinite_flow_json")


def row_problem(row, venue=VENUE):
    funds = row.get("perEtfUsd")
    if not isinstance(funds, dict) or sorted(funds) != list(FUNDS):
        return "flow_fund_scope_gap"
    total = row.get("totalNetInflow")
    if not _number(total) or not all(_number(v) for v in funds.values()):
        return "invalid_flow_number"
    try:
        summed = math.fsum(funds.values())
    except OverflowError:
        return "invalid_flow_number"
    # Published JSON uses binary floating point, including fractions of cents.
    # A micro-dollar absolute allowance covers its observed summation residue;
    # no rounding to millions or inference from BTC balances is allowed.
    # CoinBoss republishes USD millions (recent Farside rows at 0.1m,
    # older rows at 0.001m). Independent rounding of 12 funds and the
    # published total can differ by at most 13 * half the reporting unit.
    tolerance = 1e-6
    if venue == COINBOSS_VENUE:
        try:
            numbers = [Decimal(str(v)) for v in [total, *funds.values()]]
            if any(v % 1000 for v in numbers):
                return "flow_precision_changed"
            quantum = 1000 if any(v % 100000 for v in numbers) else 100000
            tolerance = (len(FUNDS) + 1) * quantum / 2
        except ArithmeticError:
            return "invalid_flow_number"
    if not math.isclose(total, summed, rel_tol=1e-12, abs_tol=tolerance):
        return "flow_fund_total_mismatch"
    return None


def trading_dates(end, count):
    # market_assets -> btc_collection -> btc_evidence; import after contract load.
    import market_assets
    day, result = source_day(end), []
    while len(result) < count:
        if day.year not in market_assets._HOLIDAYS:
            raise ValueError("flow_calendar_year_unverified")
        if market_assets.trading_day(day):
            result.append(day.isoformat())
        day -= timedelta(days=1)
    return list(reversed(result))


def window(rows, count, end, venue=VENUE):
    """A missing/partial day invalidates its window; it is never skipped."""
    try:
        dates = trading_dates(end, count)
    except ValueError as exc:
        return None, str(exc)
    sample = rows[-count:]
    if len(sample) != count or [r.get("date") for r in sample] != dates:
        return None, "incomplete_trading_day_samples"
    for row in sample:
        problem = row_problem(row, venue)
        if problem:
            return None, problem
    amount = float(sum((Decimal(str(row["totalNetInflow"])) for row in sample), Decimal(0)))
    return (amount, None) if _number(amount) else (None, "invalid_flow_number")


def _load_json(text):
    if not isinstance(text, str) or len(text) > 2 * 1024 * 1024:
        raise ValueError("invalid_flow_response")
    try:
        return json.loads(text, object_pairs_hook=_unique, parse_constant=_reject_constant)
    except (ValueError, TypeError) as exc:
        raise ValueError(str(exc) if isinstance(exc, ValueError) else "invalid_flow_json") from None


def _parsed(rows, end, venue):
    amounts, quality = {}, {}
    for count in WINDOWS:
        amount, reason = window(rows, count, end, venue)
        amounts[f"flow_{count}d_usd"] = amount
        quality[f"{count}d"] = {"status": "ok" if reason is None else "unavailable", "reason": reason,
                                 "expected_count": count, "end_date": end}
    return {"history": rows, "source_date": end, "flow_scope": list(FUNDS),
            "values": amounts, "window_quality": quality}


def parse_json(text):
    data = _load_json(text)
    if not isinstance(data, dict) or data.get("units") != "USD":
        raise ValueError("flow_unit_mismatch")
    end = source_day(data.get("updatedThrough")).isoformat()
    days = data.get("days")
    if not isinstance(days, list) or not days or len(days) > 10000:
        raise ValueError("missing_flow_days")
    dates = [source_day(row.get("date") if isinstance(row, dict) else None).isoformat() for row in days]
    if dates != sorted(set(dates)):
        raise ValueError("invalid_flow_day_order")
    if dates[-1] != end:
        raise ValueError("flow_updated_through_mismatch")
    rows = []
    for item in days[-30:]:
        funds = item.get("perEtfUsd")
        rows.append({"date": item["date"],
                     "totalNetInflow": item.get("netFlowUsd") if _number(item.get("netFlowUsd")) else None,
                     "perEtfUsd": {k: v if _number(v) else None for k, v in funds.items()}
                                    if isinstance(funds, dict) else {}})
    return _parsed(rows, end, VENUE)


def parse_coinboss_json(text):
    data = _load_json(text)
    if not isinstance(data, dict) or data.get("success") is not True or data.get("asset") != "BTC":
        raise ValueError("invalid_coinboss_response")
    if data.get("unit") != "million_usd":
        raise ValueError("flow_unit_mismatch")
    dates, daily = data.get("dates"), data.get("data")
    if not isinstance(dates, list) or not dates or len(dates) > 120 or not isinstance(daily, dict):
        raise ValueError("missing_flow_days")
    order = [source_day(day).isoformat() for day in dates]
    if order != sorted(set(order)) or set(daily) != set(order):
        raise ValueError("invalid_flow_day_order")
    tickers = data.get("tickers")
    if not isinstance(tickers, list) or sorted(
            item.get("ticker", "") if isinstance(item, dict) else "" for item in tickers) != sorted([*FUNDS, "TOTAL"]):
        raise ValueError("flow_fund_scope_gap")
    def usd(value):
        return float(Decimal(str(value)) * 1000000) if _number(value) else None
    rows = []
    for day in order[-30:]:
        item = daily[day]
        if not isinstance(item, dict):
            raise ValueError("invalid_flow_rows")
        rows.append({"date": day, "totalNetInflow": usd(item.get("TOTAL")),
                     "perEtfUsd": {k: usd(v) for k, v in item.items() if k != "TOTAL"}})
    return _parsed(rows, order[-1], COINBOSS_VENUE)


def expected_source_date(now, ready_hour=20):
    """Expected published session, shared by both daily ETF domains."""
    import market_assets
    local = datetime.fromtimestamp(now, market_assets.ET)
    expected = local.date() - timedelta(days=1 if local.hour < ready_hour else 0)
    while True:
        if expected.year not in market_assets._HOLIDAYS:
            raise ValueError("flow_calendar_year_unverified")
        if market_assets.trading_day(expected):
            return expected
        expected -= timedelta(days=1)


def freshness(day, now, ready_hour=20, grace_days=1):
    """Use the existing publication grace, with known NYSE holidays respected."""
    import market_assets
    observed = source_day(day)
    local = datetime.fromtimestamp(now, market_assets.ET)
    if observed > local.date():
        return "future_flow_date", None
    close_minute = 780 if local.strftime("%m-%d") in market_assets._EARLY.get(local.year, "").split() else 960
    if observed == local.date() and local.hour * 60 + local.minute < close_minute:
        return "flow_session_not_closed", None
    try:
        expected = expected_source_date(now, ready_hour)
    except ValueError as exc:
        return str(exc), None
    if not market_assets.trading_day(observed):
        return "flow_non_trading_date", expected.isoformat()
    lag = (expected - observed).days
    return (f"{day}<expected:{expected.isoformat()}" if lag > max(0, grace_days) else None), expected.isoformat()
