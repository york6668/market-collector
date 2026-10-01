"""Receiver projection for V1 and V2 stock data, retaining local consumers.

The original attachment stays in the reader's blob store. Only bounded typed
measurements are projected; collector prose and trading judgments never pass.
"""
import copy
import hashlib
import re
from urllib.parse import urlsplit

import btc_evidence as btc
from market_evidence import report_asset

SCHEMA = "MARKET_COLLECTOR_EVIDENCE_V1"
ASSETS = {
    "QQQ": {"price_symbol": "EQ_QQQ", "derivatives_symbol": "QQQUSDT", "derivatives_relation": "direct"},
    "VOO": {"price_symbol": "EQ_VOO", "derivatives_symbol": "SPYUSDT", "derivatives_relation": "proxy"},
}
STOCK_PATHS = {
    "stock_quote": "/bapi/equity/v1/public/equity/market/get-info",
    "stock_klines": "/bapi/equity/v1/public/equity/kline/chart",
    "stock_calendar": "/bapi/equity/v1/public/equity/calendar/periods",
}
DATASET_SOURCES = {
    "stock_quote": ["stock_quote"], "stock_1h": ["stock_klines"],
    "derivative_oi": ["open_interest"], "funding": ["public_funding"],
    "dividend_adjustments": ["public_funding"], "basis": ["public_basis"],
}
NOT_APPLICABLE = ["BTC_ETF_FLOW", "CRYPTO_FEAR_GREED", "STABLECOIN_FLOW", "SPOT_CVD", "BTC_LIQUIDATION_HYPERLIQUID"]
HOUR = 3600000
DATASET_REASONS = frozenset("""invalid_dataset unsupported_dataset missing_dataset source_contract_mismatch
instrument_relation_mismatch invalid_source_retrieval_time missing_source_time source_time_after_retrieval
unit_mismatch missing_price stock_series_contract_mismatch bar_outside_trading_session
historical_calendar_coverage_limited incomplete_stock_window funding_rate_type_or_unit_mismatch
mixed_funding_rate_type incomplete_8h_settlement_window incomplete_24h_settlement_window
incomplete_240h_settlement_window invalid_basis_prices empty_measurements invalid_rows
invalid_measurement unclosed_or_future invalid_ohlcv invalid_oi conflicting_duplicate
calendar_unavailable invalid_calendar invalid_calendar_period stock_adjustment_unverified""".split())


def _number(value, positive=False):
    return btc._number(value) and (value > 0 if positive else True)


def _stamp(value):
    return _number(value, True) and int(value) == value


def _identity(sid, source, instrument, public_stock=False):
    if not isinstance(source, dict):
        return False
    if sid in STOCK_PATHS:
        url = source.get("endpoint_url")
        if public_stock and sid in ("stock_quote", "stock_klines"):
            from market_sources import STOCK_WS
            return (url == STOCK_WS and source.get("source_id") == sid
                    and source.get("access_scope") == "public" and source.get("venue") == "Binance"
                    and source.get("canonical_origin") == urlsplit(STOCK_WS).hostname
                    and source.get("label") == instrument["price_symbol"])
        return (btc._public_url(url) and source.get("source_id") == sid
                and source.get("access_scope") == "public" and source.get("venue") == "Binance"
                and source.get("canonical_origin") == "www.binance.com"
                and urlsplit(url).hostname == "www.binance.com" and urlsplit(url).path == STOCK_PATHS[sid]
                and source.get("label") == ("US_EQUITY" if sid == "stock_calendar" else instrument["price_symbol"]))
    if not btc._source_identity(sid, source):
        return False
    return sid != "open_interest" or source.get("label") == instrument["derivatives_symbol"]


def _source(sid, source):
    # No arbitrary label, query string, quality prose, or claimed interpretation.
    value = btc._project_source(sid, source)
    value["endpoint_url"] = value["endpoint_url"].split("?", 1)[0]
    if sid in STOCK_PATHS or sid == "open_interest":
        value["label"] = source["label"]
    return value


def _rows(rows, fields, time_key, generated_ms, issues, *, candle=False):
    if not isinstance(rows, list) or len(rows) > 5000:
        issues.append("invalid_rows")
        return []
    selected, conflicts = {}, set()
    for row in rows:
        if not isinstance(row, dict) or any(not _number(row.get(k)) for k in fields):
            issues.append("invalid_measurement")
            continue
        at = row[time_key]
        if not _stamp(at) or at > generated_ms:
            issues.append("unclosed_or_future")
            continue
        safe = {k: row[k] for k in fields}
        if candle:
            if (not _stamp(row["open_time"]) or not 0 < row["close_time"] - row["open_time"] <= HOUR
                    or any(row[k] <= 0 for k in ("open", "high", "low", "close"))
                    or not row["low"] <= min(row["open"], row["close"]) <= max(row["open"], row["close"]) <= row["high"]
                    or row["volume"] < 0):
                issues.append("invalid_ohlcv")
                continue
        elif "quantity" in fields and (row["quantity"] < 0 or row["notional_value"] < 0):
            issues.append("invalid_oi")
            continue
        if at in selected and selected[at] != safe:
            conflicts.add(at)
            issues.append("conflicting_duplicate")
        selected[at] = safe
    return [selected[at] for at in sorted(selected) if at not in conflicts]


def _calendar(value, sources, instrument, generated_ms, issues):
    source = sources.get("stock_calendar")
    if not _identity("stock_calendar", source, instrument) or not isinstance(value, dict):
        issues.append("calendar_unavailable")
        return None
    start, end = value.get("start_ms"), value.get("end_ms")
    retrieved = value.get("retrieved_at")
    raw = value.get("periods")
    if (not _stamp(start) or not _stamp(end) or start >= end or not _number(retrieved, True)
            or retrieved * 1000 > generated_ms or not isinstance(raw, list) or len(raw) > 5000):
        issues.append("invalid_calendar")
        return None
    periods = []
    for row in raw:
        if (not isinstance(row, dict) or row.get("phase") not in ("PRE", "REGULAR", "POST", "OVERNIGHT")
                or not _stamp(row.get("start_ms")) or not _stamp(row.get("end_ms"))
                or not start <= row["start_ms"] < row["end_ms"] <= end):
            issues.append("invalid_calendar_period")
            return None
        periods.append({k: row[k] for k in ("start_ms", "end_ms", "phase")})
    return {"start_ms": start, "end_ms": end, "retrieved_at": retrieved,
            "periods": sorted(periods, key=lambda p: p["start_ms"])}


def stock_windows(rows, calendar, generated_ms):
    """Natural-time windows filtered by explicit exchange sessions, not 24/7 BTC."""
    end = int(generated_ms // HOUR * HOUR)
    result = {}
    for hours in (4, 24, 72):
        start = end - hours * HOUR
        sample = [r for r in rows if start <= r["open_time"] and r["close_time"] < end]
        covered = bool(calendar and calendar["start_ms"] <= start and calendar["end_ms"] >= end)
        expected = {t for t in range(start, end, HOUR) if calendar and any(
            p["start_ms"] < t + HOUR and p["end_ms"] > t for p in calendar["periods"])}
        actual = {int(r["open_time"]) for r in sample}
        complete = covered and actual == expected
        result[str(hours) + "h"] = {
            "start_ms": start, "end_ms": end, "calendar_covered": covered,
            "expected_count": len(expected) if covered else None, "actual_count": len(sample),
            "complete": complete, "market_closed": covered and not expected,
            "missing_open_times": sorted(expected - actual) if covered else [],
            "price_change_pct": (sample[-1]["close"] / sample[0]["open"] - 1) * 100 if complete and sample else None,
            "volume_shares": sum(r["volume"] for r in sample) if complete else None,
        }
    return result


def _dataset(name, ds, sources, instrument, generated_ms, public_stock=False):
    issues = []
    if not isinstance(ds, dict):
        return None, ["invalid_dataset"]
    if name == "macro":
        source_errors = {sid: btc._source_problems(sid, dict(source, source_dates={"observation_dates": source["source_dates"]})
                                                 if isinstance(source.get("source_dates"), list) else source,
                                                 btc._aware_time(btc.iso_time(generated_ms / 1000)))
                         for sid, source in sources.items() if sid.startswith("macro_")}
        projected = btc._project_dataset(name, ds, sources)
        projected["source_ids"] = [sid for sid in projected["source_ids"] if sid.startswith("macro_") and _identity(sid, sources.get(sid), instrument)]
        allowed_series = {sid[6:] for sid in projected["source_ids"]}
        projected["rows"] = [r for r in projected.get("rows", []) if r["series_id"] in allowed_series]
        for sid in projected["source_ids"]:
            issues.extend(source_errors.get(sid, []))
        return projected, sorted(set(issues))
    if name not in DATASET_SOURCES:
        return None, ["unsupported_dataset"]
    refs = DATASET_SOURCES[name]
    if ds.get("source_ids") not in (refs, refs + ["stock_calendar"] if name == "stock_1h" else refs) or not all(_identity(sid, sources.get(sid), instrument, public_stock) for sid in refs):
        return None, ["source_contract_mismatch"]
    stock = name in ("stock_quote", "stock_1h")
    expected_instrument = instrument["price_symbol" if stock else "derivatives_symbol"]
    relation = "direct" if stock else instrument["derivatives_relation"]
    if ds.get("instrument") != expected_instrument or ds.get("relation") != relation:
        return None, ["instrument_relation_mismatch"]
    result = {"source_ids": list(refs), "instrument": expected_instrument, "relation": relation}
    for sid in refs:
        source = sources[sid]
        retrieved, observed = (btc._aware_time(source.get(key)) for key in ("retrieved_at", "as_of"))
        if retrieved is None or retrieved.timestamp() * 1000 > generated_ms:
            return None, ["invalid_source_retrieval_time"]
        if observed is None:
            issues.append("missing_source_time")
        elif observed > retrieved:
            return None, ["source_time_after_retrieval"]
    if name == "stock_quote":
        if ds.get("units") != {"price": "USD/share", "volume": "shares"}:
            return None, ["unit_mismatch"]
        values = ds.get("values") or {}
        result["values"] = {k: values[k] for k in ("price", "previous_close", "session_volume_shares")
                            if _number(values.get(k)) and (values[k] >= 0 if k == "session_volume_shares" else values[k] > 0)}
        if "price" not in result["values"]:
            issues.append("missing_price")
        result["units"] = {"price": "USD/share", "volume": "shares"}
        result["source_time_missing"] = "missing_source_time" in issues
        if public_stock:
            session = ds.get("market_session") or {}
            result["market_session"] = {"open": session.get("open") if type(session.get("open")) is bool else None,
                "phase": session.get("phase") if session.get("phase") in ("REGULAR", "PRE", "POST", "OVERNIGHT", "CLOSED") else "UNKNOWN"}
    elif name == "stock_1h":
        mode = "UNKNOWN" if public_stock else "ADJUSTED"
        if (ds.get("adjustment_mode") != mode or ds.get("calendar") != "US_EQUITY"
                or ds.get("units") != {"price": "USD/share", "volume": "shares"}
                or public_stock and ds.get("analysis_eligible") is not False):
            return None, ["stock_series_contract_mismatch"]
        rows = _rows(ds.get("rows"), ("open_time", "close_time", "open", "high", "low", "close", "volume"),
                     "close_time", generated_ms, issues, candle=True)
        calendar = _calendar(ds.get("calendar_periods"), sources, instrument, generated_ms, issues)
        if calendar:
            allowed = []
            for row in rows:
                if calendar["start_ms"] <= row["open_time"] and row["close_time"] < calendar["end_ms"]:
                    if not any(p["start_ms"] <= row["close_time"] and p["end_ms"] > row["open_time"] for p in calendar["periods"]):
                        issues.append("bar_outside_trading_session")
                        continue
                else:
                    issues.append("historical_calendar_coverage_limited")
                allowed.append(row)
            rows = allowed
            result["source_ids"].append("stock_calendar")
        windows = stock_windows(rows, calendar, generated_ms)
        if public_stock:
            issues.append("stock_adjustment_unverified")
            for window in windows.values():
                window.update(analysis_eligible=False, price_change_pct=None)
        if any(not w["complete"] for w in windows.values()):
            issues.append("incomplete_stock_window")
        result.update(rows=rows, units={"price": "USD/share", "volume": "shares"}, calendar="US_EQUITY",
                      adjustment_mode=mode, calendar_periods=calendar, windows=windows,
                      history_revision_detected=ds.get("history_revision_event") is not None or ds.get("history_revision_detected") is True)
        if public_stock:
            result["analysis_eligible"] = False
    elif name == "derivative_oi":
        if ds.get("units") != {"notional_value": "USDT", "quantity": "contracts"}:
            return None, ["unit_mismatch"]
        result.update(rows=_rows(ds.get("rows"), ("timestamp", "quantity", "notional_value"), "timestamp", generated_ms, issues),
                      units={"notional_value": "USDT", "quantity": "contracts"})
    elif name in ("funding", "dividend_adjustments"):
        kind = "Regular" if name == "funding" else "Special"
        if ds.get("rate_type") != kind or (ds.get("units") or {}).get("rate") != "fraction":
            return None, ["funding_rate_type_or_unit_mismatch"]
        input_rows = ds.get("rows")
        if isinstance(input_rows, list):
            if any(not isinstance(r, dict) or r.get("rate_type", kind) != kind for r in input_rows):
                issues.append("mixed_funding_rate_type")
            input_rows = [r for r in input_rows if isinstance(r, dict) and r.get("rate_type", kind) == kind]
        rows = _rows(input_rows, ("time", "rate"), "time", generated_ms, issues)
        result.update(rows=rows, rate_type=kind, units={"rate": "fraction"})
        if kind == "Regular":
            result["settlement_windows"] = {}
            result["values"] = {}
            for hours in (8, 24, 240):
                window, sample = btc.settlement_window(rows, hours=hours, as_of_ms=generated_ms)
                result["settlement_windows"][str(hours) + "h"] = window
                result["values"]["funding_%dh_pct" % hours] = sum(r["rate"] for r in sample[1:]) * 100 if window["complete"] else None
                if not window["complete"]:
                    issues.append("incomplete_%dh_settlement_window" % hours)
            if rows:
                result["values"].update(latest_funding_time_ms=rows[-1]["time"], latest_settled_rate_pct=rows[-1]["rate"] * 100)
        else:
            # Special adjustments are events, not periodic funding settlements.
            # The sample gives no completeness interval for this event history.
            result['history_completeness'] = 'UNKNOWN'
            result['observed_event_windows'] = {}
            for hours in (8, 24, 240):
                start = generated_ms - hours * HOUR
                sample = [r for r in rows if start < r['time'] <= generated_ms]
                result['observed_event_windows'][str(hours) + 'h'] = {
                    'start_ms': start, 'end_ms': generated_ms, 'event_count': len(sample),
                    'observed_adjustment_pct': sum(r['rate'] for r in sample) * 100}
            result['values'] = ({'latest_adjustment_time_ms': rows[-1]['time'],
                                 'latest_adjustment_rate_pct': rows[-1]['rate'] * 100} if rows else {})
    elif name == "basis":
        values = ds.get("values") or {}
        if not all(_number(values.get(k), True) for k in ("mark_price", "index_price")):
            return None, ["invalid_basis_prices"]
        result["values"] = {k: values[k] for k in ("mark_price", "index_price")}
        result["values"]["basis_pct"] = ((values["mark_price"] - values["index_price"])
                                            / values["index_price"] * 100)
        result["units"] = {"price": "USDT", "basis_pct": "percent"}
    # A malformed sample invalidates this measurement, rather than silently
    # turning a filtered subset into the original observation window.
    if any(code in issues for code in ('invalid_rows', 'invalid_measurement',
            'unclosed_or_future', 'invalid_ohlcv', 'invalid_oi', 'conflicting_duplicate')):
        result.update(rows=[], values={}, measurement_rejected=True)
        result.pop('windows', None)
        result.pop('settlement_windows', None)
    if "rows" in result and not result["rows"] and name != "dividend_adjustments":
        issues.append("empty_measurements")
    return result, sorted(set(issues))


def inspect(bundle, report_text=None, *, public_stock=False):
    validation = {"status": "rejected", "reasons": [], "dataset_reasons": {},
                  "independent_verified_sources": 0, "role": "auxiliary_observation"}
    try:
        schema = "MARKET_COLLECTOR_EVIDENCE_V2" if public_stock else SCHEMA
        if not isinstance(bundle, dict) or bundle.get("schema") != schema:
            raise ValueError("unknown_schema")
        asset = bundle.get("asset_id")
        if asset not in ASSETS:
            raise ValueError("unsupported_asset")
        instrument = {"asset_id": asset, "calendar": "US_EQUITY", "payment_currency": "USDC",
                      "price_currency": "USD", "volume_unit": "shares", **ASSETS[asset]}
        if bundle.get("instrument") != instrument:
            raise ValueError("invalid_instrument_identity")
        encoded = btc.canonical_bytes({k: v for k, v in bundle.items() if k != "bundle_id"})
        prefix = "market-v2:" if public_stock else "market:"
        if len(encoded) > btc.MAX_BYTES or bundle.get("bundle_id") != prefix + hashlib.sha256(encoded).hexdigest():
            raise ValueError("bundle_hash_mismatch")
        generated = btc._aware_time(bundle.get("generated_at"))
        if generated is None or bundle.get("role") != "auxiliary_observation" or not isinstance(bundle.get("report_id"), str) or not bundle["report_id"]:
            raise ValueError("invalid_envelope")
        if not isinstance(bundle.get("text_sha256"), str) or not re.fullmatch(r"[a-f0-9]{64}", bundle["text_sha256"]):
            raise ValueError("invalid_text_hash")
        if report_text is not None and (not isinstance(report_text, str) or btc.text_hash(report_text) != bundle["text_sha256"]):
            raise ValueError("text_hash_mismatch")
        if report_text is not None and report_asset(report_text) != asset:
            raise ValueError("text_asset_mismatch")
        if not isinstance(bundle.get("datasets"), dict) or not isinstance(bundle.get("sources"), dict):
            raise ValueError("invalid_datasets_or_sources")
        from market_evidence import validate_bundle as validate_contract
        contract = validate_contract(bundle, report_text, public_stock=public_stock)
        if contract['status'] == 'rejected':
            return contract, None
        datasets, used = {}, set()
        for name, ds in bundle["datasets"].items():
            projected, issues = _dataset(name, ds, bundle["sources"], instrument, generated.timestamp() * 1000, public_stock)
            if issues:
                validation["dataset_reasons"][name if name in DATASET_SOURCES or name == "macro" else "__unsupported__"] = issues
            if projected is not None:
                datasets[name] = projected
                used.update(projected["source_ids"])
            elif name in DATASET_SOURCES:
                stock = name.startswith('stock_')
                datasets[name] = {
                    'instrument': instrument['price_symbol' if stock else 'derivatives_symbol'],
                    'relation': 'direct' if stock else instrument['derivatives_relation'],
                    'source_ids': [], 'rows': [], 'values': {},
                    'analysis_eligible': False, 'measurement_rejected': True,
                    'quality': {'reasons': issues},
                }
        for name in ("stock_quote", "stock_1h"):
            if name not in bundle["datasets"]:
                validation["dataset_reasons"][name] = ["missing_dataset"]
        validation["status"] = "pending_text" if report_text is None else "degraded_auxiliary" if validation["dataset_reasons"] else "valid_auxiliary"
        projection = {"schema": schema, "role": "auxiliary_observation", "asset_id": asset,
                      "bundle_id": bundle["bundle_id"], "report_id": bundle["report_id"], "generated_at": bundle["generated_at"],
                      "instrument": instrument, "not_applicable": list(NOT_APPLICABLE), "datasets": datasets,
                      "sources": {sid: _source(sid, bundle["sources"][sid]) for sid in sorted(used)},
                      "validation": validation, "independent_verified_sources": 0,
                      "requires_independent_verification": True, "requires_current_source_check": True}
        return validation, projection
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError, RecursionError) as exc:
        reason = str(exc) if type(exc) is ValueError and str(exc) in {
            "unknown_schema", "unsupported_asset", "invalid_instrument_identity", "bundle_hash_mismatch", "invalid_envelope",
            "invalid_text_hash", "text_hash_mismatch", "text_asset_mismatch", "invalid_datasets_or_sources"} else "invalid_bundle_values"
        validation["reasons"].append(reason)
        return validation, None


def validate_bundle(bundle, report_text=None):
    return inspect(bundle, report_text)[0]


def auxiliary_observation(bundle, report_text):
    validation, projection = inspect(bundle, report_text)
    return projection if validation["status"] in ("valid_auxiliary", "degraded_auxiliary") else None


def sanitize_observation(item, *, public_stock=False):
    """Re-project local exports at the consumer boundary; never grant trust."""
    asset = item.get("asset_id")
    expected = {"asset_id": asset, "calendar": "US_EQUITY", "payment_currency": "USDC", "price_currency": "USD",
                "volume_unit": "shares", **ASSETS.get(asset, {})}
    quality = item.get("validation") or {}
    schema = "MARKET_COLLECTOR_EVIDENCE_V2" if public_stock else SCHEMA
    if (asset not in ASSETS or item.get("schema") != schema or item.get("instrument") != expected
            or item.get("role") != "auxiliary_observation" or type(item.get("independent_verified_sources")) is not int
            or item["independent_verified_sources"] != 0 or item.get("requires_independent_verification") is not True
            or item.get("requires_current_source_check") is not True or quality.get("status") not in ("valid_auxiliary", "degraded_auxiliary")
            or quality.get("role") != "auxiliary_observation" or type(quality.get("independent_verified_sources")) is not int
            or quality["independent_verified_sources"] != 0 or quality.get("reasons") != []):
        raise ValueError("invalid_equity_observation")
    generated = btc._aware_time(item.get("generated_at"))
    if generated is None or not isinstance(item.get("datasets"), dict) or not isinstance(item.get("sources"), dict):
        raise ValueError("invalid_equity_observation")
    datasets, reasons, used = {}, {}, set()
    for name, codes in (quality.get("dataset_reasons") or {}).items():
        if name in DATASET_SOURCES or name == "macro":
            kept = sorted({code for code in codes if isinstance(code, str) and code in DATASET_REASONS}) if isinstance(codes, list) else []
            if kept:
                reasons[name] = kept
    for name, ds in item["datasets"].items():
        projected, issues = _dataset(name, ds, item["sources"], expected, generated.timestamp() * 1000, public_stock)
        if projected is not None:
            datasets[name] = projected
            used.update(projected["source_ids"])
        if issues:
            key = name if name in DATASET_SOURCES or name == "macro" else "__unsupported__"
            reasons[key] = sorted(set(reasons.get(key, []) + issues))
    return {"asset_id": asset, "instrument": expected, "generated_at": item["generated_at"],
            "datasets": datasets, "sources": {sid: _source(sid, item["sources"][sid]) for sid in sorted(used)},
            "dataset_reasons": reasons, "not_applicable": list(NOT_APPLICABLE)}
