"""Pure normalization for collector observation windows; no I/O or state."""

import math
import re
from datetime import date
from btc_evidence import settlement_window

HOUR_MS = 3600000


def finite_number(value):
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (ValueError, TypeError, OverflowError):
        return None


def epoch_ms(value):
    value = finite_number(value)
    return int(value) if value is not None and value > 0 and value.is_integer() else None


def iso_date(value):
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        return None
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError:
        return None


def time_window(rows, key, expected_count, interval_ms):
    times = [epoch_ms(row.get(key)) for row in rows]
    valid = bool(times) and all(t is not None for t in times)
    contiguous = bool(valid and interval_ms and all(
        b - a == interval_ms for a, b in zip(times, times[1:])))
    return {
        "timestamp_field": key, "start_ms": times[0] if valid else None,
        "end_ms": times[-1] if valid else None, "interval_ms": interval_ms,
        "expected_count": expected_count, "actual_count": len(rows),
        "contiguous": contiguous,
        "complete": bool(contiguous and expected_count and len(rows) == expected_count),
    }


def settlement_rows(raw, time_key="fundingTime", current_ms=None, rate_type="Regular"):
    by_time = {}
    for item in raw or []:
        if not isinstance(item, dict):
            continue
        if item.get("rateType", "Regular") != rate_type:
            continue
        stamp = epoch_ms(item.get(time_key))
        if stamp is None:
            continue
        rate = finite_number(item.get("fundingRate"))
        if current_ms is not None and stamp > current_ms:
            rate = None  # Preserve the future observation so the window cannot fall back.
        row = {"time": stamp, "rate": rate}
        if stamp in by_time:
            # Even equal-rate duplicates cannot prove two distinct settlements.
            row["rate"] = None
        by_time[stamp] = row
    return [by_time[key] for key in sorted(by_time)]


def settled_window(rows, hours, current_interval_hours=None, *, as_of_ms=None):
    """A boundary observation proves elapsed coverage; current metadata is not history."""
    quality, window = settlement_window(rows, hours=hours, as_of_ms=as_of_ms)
    interval = quality["interval_ms"]
    current_interval = finite_number(current_interval_hours)
    reasons = []
    if (not window or not all(epoch_ms(row.get("time")) for row in window)
            or abs(window[-1]["time"] - window[0]["time"] - hours * HOUR_MS) > 200):
        reasons.append("missing_boundary_sample")
    if not quality["contiguous"]:
        reasons.append("historical_interval_unproven_or_changed")
    if current_interval and interval != current_interval * HOUR_MS:
        reasons.append("current_interval_differs_from_observed_history")
    if any(row.get("rate") is None for row in window):
        reasons.append("missing_settled_rate")
    if not quality["complete"]:
        reasons.append("incomplete_settlement_window")
    quality.update(complete=not reasons, reasons=reasons,
                   hours=hours, interval_basis="observed_settlement_spacing",
                   current_interval_metadata_scope="current_only")
    settled = window[1:]  # The first bucket proves the boundary; it is not in the sum.
    total = sum(row["rate"] for row in settled) * 100 if not reasons else None
    return total, quality, window


def dated_flow_rows(raw):
    by_date = {}
    invalid_dates = 0
    duplicate_dates = 0
    for row in raw or []:
        if not isinstance(row, dict):
            invalid_dates += 1
            continue
        day = iso_date(row.get("date"))
        if day is None:
            invalid_dates += 1
            continue
        value = finite_number(row.get("totalNetInflow"))
        if day in by_date:
            duplicate_dates += 1
            if by_date[day]["totalNetInflow"] != value:
                value = None
        by_date[day] = {"date": day, "totalNetInflow": value}
    return [by_date[key] for key in sorted(by_date)], {
        "invalid_date_rows": invalid_dates, "duplicate_date_rows": duplicate_dates,
    }


def dated_flow_window(rows, count):
    window = rows[-count:]
    dates = [row["date"] for row in window]
    valid_count = sum(row.get("totalNetInflow") is not None for row in window)
    complete = len(window) == count and valid_count == count
    quality = {"expected_count": count, "actual_count": len(window),
               "valid_count": valid_count, "complete": complete,
               "sample_dates": dates, "start_date": dates[0] if dates else None,
               "end_date": dates[-1] if dates else None,
               "basis": "available_trading_day_observations"}
    return (sum(row["totalNetInflow"] for row in window) if complete else None), quality


def series_receipt(series, series_id, endpoint_url, retrieved_at, fields=None, limit=450):
    """Observation dates are not publication timestamps."""
    rows = []
    for item in series or []:
        day = iso_date(item.get("date"))
        value = finite_number(item.get("value"))
        if day is not None:
            rows.append({"date": day, "value": value})
    rows.sort(key=lambda row: row["date"])
    rows = rows[-limit:]
    return {"series_id": series_id, "endpoint_url": endpoint_url, "rows": rows,
            "retrieved_at": retrieved_at, "published_at": None,
            "publication_time_missing": True, "fields": list(fields or []),
            "sample_dates": [row["date"] for row in rows],
            "start_date": rows[0]["date"] if rows else None,
            "end_date": rows[-1]["date"] if rows else None}



def raw_series_receipt(rows, date_key, series_id, endpoint_url, retrieved_at, fields=None):
    rows = [dict(row) for row in rows if isinstance(row, dict)][:500]
    dates = sorted({day for row in rows for day in [iso_date(row.get(date_key))] if day})
    return {"series_id": series_id, "endpoint_url": endpoint_url, "rows": rows,
            "retrieved_at": retrieved_at, "published_at": None,
            "publication_time_missing": True, "fields": list(fields or []),
            "sample_dates": dates, "start_date": dates[0] if dates else None,
            "end_date": dates[-1] if dates else None}
