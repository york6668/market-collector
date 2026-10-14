"""Anonymous TFTC USD net creations/redemptions, with complete trading windows."""
import json
import math
import re
from datetime import date, datetime, timedelta
from decimal import Decimal

from btc_etf import FUNDS

ENDPOINT = "https://www.tftc.io/bitcoin-etf-flows/data.json"
PAGE = "https://www.tftc.io/bitcoin-etf-flows"
VENUE = "TFTC"
WINDOWS = (1, 5, 30)


def is_flow_source(record):
    return record.get("venue") == VENUE and record.get("endpoint_url") == ENDPOINT


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


def row_problem(row):
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
    if not math.isclose(total, summed, rel_tol=1e-12, abs_tol=1e-6):
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


def window(rows, count, end):
    """A missing/partial day invalidates its window; it is never skipped."""
    try:
        dates = trading_dates(end, count)
    except ValueError as exc:
        return None, str(exc)
    sample = rows[-count:]
    if len(sample) != count or [r.get("date") for r in sample] != dates:
        return None, "incomplete_trading_day_samples"
    for row in sample:
        problem = row_problem(row)
        if problem:
            return None, problem
    amount = float(sum((Decimal(str(row["totalNetInflow"])) for row in sample), Decimal(0)))
    return (amount, None) if _number(amount) else (None, "invalid_flow_number")


def parse_json(text):
    if not isinstance(text, str) or len(text) > 2 * 1024 * 1024:
        raise ValueError("invalid_flow_response")
    try:
        data = json.loads(text, object_pairs_hook=_unique, parse_constant=_reject_constant)
    except (ValueError, TypeError) as exc:
        raise ValueError(str(exc) if isinstance(exc, ValueError) else "invalid_flow_json") from None
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
    amounts, quality = {}, {}
    for count in WINDOWS:
        amount, reason = window(rows, count, end)
        amounts[f"flow_{count}d_usd"] = amount
        quality[f"{count}d"] = {"status": "ok" if reason is None else "unavailable", "reason": reason,
                                 "expected_count": count, "end_date": end}
    return {"history": rows, "source_date": end, "flow_scope": list(FUNDS),
            "values": amounts, "window_quality": quality}


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
    expected = local.date() - timedelta(days=1 if local.hour < ready_hour else 0)
    while True:
        if expected.year not in market_assets._HOLIDAYS:
            return "flow_calendar_year_unverified", None
        if market_assets.trading_day(expected):
            break
        expected -= timedelta(days=1)
    if not market_assets.trading_day(observed):
        return "flow_non_trading_date", expected.isoformat()
    lag = (expected - observed).days
    return (f"{day}<expected:{expected.isoformat()}" if lag > max(0, grace_days) else None), expected.isoformat()
