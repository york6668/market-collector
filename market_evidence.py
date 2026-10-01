"""Asset-scoped extension of the portable BTC evidence contract.

Only numeric observations and registered source identities reach the projection.
VOO's SPY observations remain a proxy even when the attachment is valid.
"""
import copy
import hashlib
import math
import re
from urllib.parse import urlsplit

import btc_evidence as base
from market_assets import INSTRUMENTS, EQUITY_BASE, CHART_PATH, HOUR, history_contiguous, regular_funding_is_fresh
from market_sources import STOCK_WS

SCHEMA = "MARKET_COLLECTOR_EVIDENCE_V1"
STOCK_ROWS = {"open_time", "close_time", "open", "high", "low", "close", "volume"}
OI_ROWS = {"timestamp", "quantity", "notional_value"}
BASE_DATASETS = {"funding", "basis", "macro"}
STOCK_DATASETS = BASE_DATASETS | {"stock_1h", "stock_quote", "derivative_oi", "dividend_adjustments"}
base._SOURCE_CONTRACTS.update({
    "stock_klines": [("Binance", "www.binance.com", "/bapi/equity/" + CHART_PATH)],
    "stock_quote": [("Binance", "www.binance.com", "/bapi/equity/v1/public/equity/market/get-info")],
    "stock_calendar": [("Binance", "www.binance.com", "/bapi/equity/v1/public/equity/calendar/periods")],
})


def report_asset(text, fallback=None):
    match = re.search(r"(?:^|\n)\s*📊\s+(BTC|QQQ|VOO)(?=\s|｜)", text or "")
    return match[1] if match else fallback


def build_bundle(payload, report_text, report_id, generated_at, full_report_text=None, *, public_stock=False):
    spec = INSTRUMENTS[payload["instrument"]["asset_id"]]
    if public_stock:
        records = dict(payload.get("public_records") or {})
        for name in BASE_DATASETS:
            records.setdefault(name, {"status": "unavailable", "reason": "not_collected", "value": {}})
        payload = {**payload, "public_records": records}
    # Reuse composite macro and settled funding receipts, not crypto datasets.
    old = base._build_btc_bundle(payload, report_text, report_id, generated_at, full_report_text)
    datasets = {name: old["datasets"][name] for name in BASE_DATASETS if name in old["datasets"]}
    sources = {sid: value for sid, value in old["sources"].items() if sid.startswith("macro_") or sid in ("public_funding", "public_basis")}
    bars = list(payload.get("raw_signal_bars") or [])[-1000:]
    last = bars[-1] if bars else {}
    sources["stock_klines"] = base._source("stock_klines", spec.price_symbol, last.get("endpoint_url"), "Binance",
        base.iso_time(last.get("retrieved_at_ms", 0) / 1000), base.iso_time(last.get("close_time", 0) / 1000))
    market = payload.get("market") or {}
    sources["stock_quote"] = base._source("stock_quote", spec.price_symbol, market.get("quote_endpoint_url"), "Binance",
        base.iso_time(market.get("quote_retrieved_at")), None)
    datasets["stock_1h"] = {"source_ids": ["stock_klines"], "rows": [base._typed_fields(row, STOCK_ROWS) for row in bars],
        "units": {"price": "USD/share", "volume": "shares"}, "adjustment_mode": "ADJUSTED", "closed_bars": True,
        "calendar": "US_EQUITY", "history_revision_event": payload.get("history_revision_event")}
    calendar = payload.get("calendar_periods")
    if calendar:
        sources["stock_calendar"] = base._source("stock_calendar", "US_EQUITY", calendar.get("endpoint_url"), "Binance",
            base.iso_time(calendar.get("retrieved_at")), None)
        datasets["stock_1h"]["calendar_periods"] = calendar
    datasets["stock_quote"] = {"source_ids": ["stock_quote"],
        "values": {key: market.get(key) for key in ("price", "session_volume_shares", "previous_close")},
        "units": {"price": "USD/share", "volume": "shares"}, "source_time_missing": True}
    if public_stock:
        # V2 preserves all V1 data domains, with the new source's actual contract.
        # This is never an ADJUSTED V1 wrapper around the public stream.
        datasets["stock_1h"].update(adjustment_mode="UNKNOWN", analysis_eligible=False)
        datasets["stock_quote"]["source_time_missing"] = market.get("quote_source_as_of_ms") is None
        sources["stock_quote"]["as_of"] = base.iso_time(market["quote_source_as_of_ms"] / 1000) if market.get("quote_source_as_of_ms") is not None else None
        for sid, endpoint in (("stock_klines", last.get("endpoint_url")), ("stock_quote", market.get("quote_endpoint_url"))):
            sources[sid].update(endpoint_url=endpoint, canonical_origin=urlsplit(endpoint or "").hostname)
        datasets["stock_quote"]["market_session"] = copy.deepcopy(payload.get("market_session") or {})
    rows = payload.get("derivative_bars") or []
    last_oi = rows[-1] if rows else {}
    sources["open_interest"] = base._source("open_interest", spec.derivatives_symbol, last_oi.get("oi_endpoint_url"), "Binance",
        base.iso_time(last_oi.get("oi_retrieved_at_ms", 0) / 1000), base.iso_time(last_oi.get("oi_timestamp", 0) / 1000))
    datasets["derivative_oi"] = {"source_ids": ["open_interest"], "rows": [
        {"timestamp": row.get("oi_timestamp"), "quantity": row.get("oi"), "notional_value": row.get("oi_value")}
        for row in rows], "units": {"quantity": "contracts", "notional_value": "USDT"}}
    regular = datasets.get("funding") or {}
    values = regular.get("values") or {}
    special = values.pop("special_settlements", [])
    datasets["dividend_adjustments"] = {"source_ids": ["public_funding"], "rows": special,
        "rate_type": "Special", "units": {"rate": "fraction"}}
    regular["rate_type"] = "Regular"
    for name, ds in datasets.items():
        ds["instrument"] = (spec.price_symbol if name.startswith("stock_") else
                            "GLOBAL" if name == "macro" else spec.derivatives_symbol)
        ds["relation"] = "shared" if name == "macro" else "direct" if name.startswith("stock_") else spec.derivatives_relation
    body = base._clean({"schema": "MARKET_COLLECTOR_EVIDENCE_V2" if public_stock else SCHEMA, "asset_id": spec.asset_id, "instrument": spec.identity(),
        "report_id": report_id, "generated_at": generated_at, "text_sha256": base.text_hash(report_text),
        "role": "auxiliary_observation", "sources": sources, "datasets": datasets,
        "quality": payload.get("signal_data_quality"), "full_report_text": full_report_text,
        "mechanical_observation": old.get("mechanical_observation"),
        "applicability": (payload.get("completeness") or {}).get("not_applicable", [])})
    body["bundle_id"] = ("market-v2:" if public_stock else "market:") + hashlib.sha256(base.canonical_bytes(body)).hexdigest()
    return body


def _stock_source_identity(sid, source):
    return (isinstance(source, dict) and sid in ("stock_klines", "stock_quote")
            and source.get("source_id") == sid and source.get("endpoint_url") == STOCK_WS
            and source.get("canonical_origin") == urlsplit(STOCK_WS).hostname
            and source.get("venue") == "Binance" and source.get("access_scope") == "public")


def _source_problems(sid, source, generated, public_stock):
    problems = base._source_problems(sid, source, generated)
    if public_stock and sid in ("stock_klines", "stock_quote"):
        if _stock_source_identity(sid, source):
            # The V1 helper accepts HTTPS only; V2 registers this WSS source.
            problems = [p for p in problems if p not in (
                "missing_public_endpoint", "canonical_origin_mismatch", "source_contract_mismatch")]
        elif "source_contract_mismatch" not in problems:
            problems.append("source_contract_mismatch")
    return problems


def validate_bundle(bundle, report_text=None, *, public_stock=False):
    result = {"status": "rejected", "reasons": [], "dataset_reasons": {}, "independent_verified_sources": 0,
              "role": "auxiliary_observation"}
    try:
        spec = INSTRUMENTS.get(bundle.get("asset_id"))
        if not spec or not spec.equity or bundle.get("instrument") != spec.identity():
            result["reasons"].append("instrument_identity_mismatch")
            return result
        body = {key: value for key, value in bundle.items() if key != "bundle_id"}
        encoded = base.canonical_bytes(body)
        prefix = "market-v2:" if public_stock else "market:"
        if len(encoded) > base.MAX_BYTES or bundle.get("bundle_id") != prefix + hashlib.sha256(encoded).hexdigest():
            result["reasons"].append("bundle_hash_or_size_mismatch")
        generated = base._aware_time(bundle.get("generated_at"))
        if generated is None or not isinstance(bundle.get("report_id"), str) or not bundle["report_id"]:
            result["reasons"].append("invalid_report_identity")
        if bundle.get("schema") != ("MARKET_COLLECTOR_EVIDENCE_V2" if public_stock else SCHEMA) or bundle.get("role") != "auxiliary_observation":
            result["reasons"].append("invalid_schema_or_role")
        digest = bundle.get("text_sha256")
        if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
            result["reasons"].append("invalid_text_hash")
        if report_text is not None and (not isinstance(report_text, str) or base.text_hash(report_text) != digest
                                        or report_asset(report_text) != spec.asset_id):
            result["reasons"].append("text_or_asset_mismatch")
        sources, datasets = bundle.get("sources"), bundle.get("datasets")
        if not isinstance(sources, dict) or not isinstance(datasets, dict):
            result["reasons"].append("invalid_datasets_or_sources")
        if result["reasons"]:
            return result
        errors = {sid: _source_problems(sid, source, generated, public_stock) for sid, source in sources.items()}
        cutoff = generated.timestamp() * 1000
        supported = STOCK_DATASETS
        for name in supported | set(datasets):
            ds = datasets.get(name)
            problems = []
            if name not in supported or not isinstance(ds, dict):
                result["dataset_reasons"][name] = ["unsupported_or_missing_dataset"]
                continue
            expected_symbol = spec.price_symbol if name.startswith("stock_") else "GLOBAL" if name == "macro" else spec.derivatives_symbol
            expected_relation = "shared" if name == "macro" else "direct" if name.startswith("stock_") else spec.derivatives_relation
            if ds.get("instrument") != expected_symbol or ds.get("relation") != expected_relation:
                result["reasons"].append("dataset_instrument_mismatch")
            refs = ds.get("source_ids")
            if not isinstance(refs, list) or not refs or any(not isinstance(sid, str) or sid not in sources for sid in refs):
                problems.append("invalid_source_reference")
                refs = []
            for sid in refs:
                problems.extend(errors.get(sid, []))
            if name == "funding":
                if refs != ["public_funding"]:
                    problems.append("dataset_source_mismatch")
                problems.extend(funding_problems(ds, sources.get("public_funding"), cutoff))
            elif name in BASE_DATASETS:
                problems.extend(base._dataset_problems(name, ds, sources, errors, generated))
            else:
                required_source = {"stock_1h": "stock_klines", "stock_quote": "stock_quote",
                                   "derivative_oi": "open_interest", "dividend_adjustments": "public_funding"}[name]
                if refs != [required_source]:
                    problems.append("dataset_source_mismatch")
                rows = ds.get("rows", [])
                if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
                    problems.append("invalid_rows")
                    rows = []
                time_key = "close_time" if name == "stock_1h" else "timestamp" if name == "derivative_oi" else "time"
                stamps = [row.get(time_key) for row in rows]
                if rows and (any(not base._number(stamp) or stamp <= 0 or stamp > cutoff for stamp in stamps)
                             or stamps != sorted(set(stamps))):
                    problems.append("invalid_observation_times")
                if name == "stock_1h":
                    if not rows or ds.get("adjustment_mode") != ("UNKNOWN" if public_stock else "ADJUSTED") or ds.get("closed_bars") is not True:
                        problems.append("unproven_adjusted_closed_bars")
                    if public_stock:
                        problems.append("stock_adjustment_unverified")
                        if ds.get("analysis_eligible") is not False:
                            problems.append("invalid_analysis_eligibility")
                    if ds.get("units") != {"price": "USD/share", "volume": "shares"}:
                        problems.append("stock_unit_mismatch")
                    for row in rows:
                        if (any(not base._number(row.get(key)) for key in STOCK_ROWS)
                            or not 0 < row["low"] <= min(row["open"], row["close"]) <= max(row["open"], row["close"]) <= row["high"]
                            or row["volume"] < 0 or row["close_time"] - row["open_time"] != HOUR - 1
                            or row["open_time"] % HOUR):
                            problems.append("invalid_stock_bar")
                    calendar = ds.get("calendar_periods")
                    if calendar and not valid_calendar(calendar, sources.get("stock_calendar"), cutoff):
                        problems.append("invalid_calendar_observation")
                    if not any(p != "stock_adjustment_unverified" for p in problems) and not history_contiguous(rows, calendar):
                        problems.append("missing_trading_hours")
                elif name == "stock_quote":
                    if public_stock:
                        if ds.get("source_time_missing") is not (sources.get("stock_quote", {}).get("as_of") is None):
                            problems.append("invalid_source_time")
                    elif sources.get("stock_quote", {}).get("as_of") is not None or ds.get("source_time_missing") is not True:
                        problems.append("invented_quote_source_time")
                    if not base._number((ds.get("values") or {}).get("price")) or ds["values"]["price"] <= 0:
                        problems.append("missing_quote")
                    if ds.get("source_time_missing"):
                        problems.append("source_time_missing")
                elif name == "derivative_oi":
                    if not rows or any(not base._number(row.get(key)) or row[key] < 0 for row in rows for key in OI_ROWS):
                        problems.append("invalid_or_missing_oi")
                    if ds.get("units") != {"quantity": "contracts", "notional_value": "USDT"}:
                        problems.append("derivative_unit_mismatch")
                elif name == "dividend_adjustments":
                    if ds.get("rate_type") != "Special" or any(not base._number(row.get("rate")) for row in rows):
                        problems.append("invalid_special_settlement")
                    if ds.get("units") != {"rate": "fraction"}:
                        problems.append("settlement_unit_mismatch")
                for sid in refs:
                    retrieved = base._aware_time(sources[sid].get("retrieved_at"))
                    if retrieved and stamps and all(base._number(stamp) for stamp in stamps) and max(stamps) > retrieved.timestamp() * 1000:
                        problems.append("observation_after_retrieval")
            if problems:
                result["dataset_reasons"][name] = sorted(set(problems))
        regular, special = datasets.get("funding") or {}, datasets.get("dividend_adjustments") or {}
        if regular.get("rate_type") != "Regular":
            result["reasons"].append("regular_funding_type_mismatch")
        if result["reasons"]:
            return result
        result["status"] = "pending_text" if report_text is None else "degraded_auxiliary" if result["dataset_reasons"] else "valid_auxiliary"
        return result
    except (ValueError, TypeError, OverflowError, AttributeError, KeyError, IndexError, RecursionError):
        result["reasons"].append("invalid_bundle_values")
        return result


def valid_calendar(calendar, source, cutoff):
    if not isinstance(calendar, dict) or not isinstance(source, dict) or not base._source_identity("stock_calendar", source):
        return False
    start, end = calendar.get("start_ms"), calendar.get("end_ms")
    retrieved = calendar.get("retrieved_at")
    if (not all(base._number(value) and value > 0 for value in (start, end, retrieved))
            or not 0 < end - start <= 32 * 24 * HOUR or retrieved * 1000 > cutoff
            or base.iso_time(retrieved) != source.get("retrieved_at")):
        return False
    previous = start
    for period in calendar.get("periods", []):
        if (not isinstance(period, dict) or period.get("phase") not in ("REGULAR", "PRE", "POST", "OVERNIGHT")
                or not all(base._number(period.get(key)) for key in ("start_ms", "end_ms"))
                or not previous <= period["start_ms"] < period["end_ms"] <= end):
            return False
        previous = period["end_ms"]
    return isinstance(calendar.get("periods"), list)


def funding_problems(ds, source, cutoff):
    """Validate each elapsed window independently of older interval changes."""
    from btc_collection import settled_window
    rows, values = ds.get("rows"), ds.get("values") or {}
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        return ["invalid_rows"]
    if not rows:
        return ["missing_regular_settlements"]
    retrieved = base._aware_time((source or {}).get("retrieved_at"))
    cutoff = min(cutoff, retrieved.timestamp() * 1000) if retrieved else cutoff
    stamps = [row.get("time") for row in rows]
    if any(not base._number(t) or not 0 < t <= cutoff for t in stamps) or stamps != sorted(set(stamps)):
        return ["invalid_observation_times"]
    if any(not base._number(row.get("rate")) for row in rows):
        return ["missing_settled_rate"]
    interval, problems = values.get("current_interval_hours"), []
    if not regular_funding_is_fresh(values, cutoff):
        problems.append("regular_settlement_stale")
    for hours in (8, 24, 240):
        total, quality, _ = settled_window(rows, hours, interval, as_of_ms=cutoff)
        claim = ds.get("window") if hours == 240 else (values.get("settlement_windows") or {}).get(str(hours) + "h")
        if not isinstance(claim, dict) or any(claim.get(key) != quality[key] for key in base._WINDOW_FIELDS | {"hours"}):
            problems.append("invalid_settlement_window")
        if not quality["complete"]:
            problems.append("historical_interval_unproven_or_changed" if hours == 240 else "incomplete_settlement_window")
        if hours != 240:
            observed = values.get("funding_" + str(hours) + "h_pct")
            if (total is None and observed is not None) or (total is not None and (
                    not base._number(observed) or not math.isclose(observed, total, rel_tol=1e-9, abs_tol=1e-8))):
                problems.append("invalid_settled_sum")
    if (values.get("latest_funding_time_ms") != rows[-1]["time"] or not base._number(values.get("latest_settled_rate_pct"))
            or not math.isclose(values["latest_settled_rate_pct"], rows[-1]["rate"] * 100, rel_tol=1e-9, abs_tol=1e-8)):
        problems.append("invalid_latest_settlement")
    return problems


def auxiliary_observation(bundle, report_text, *, public_stock=False):
    validation = validate_bundle(bundle, report_text, public_stock=public_stock)
    if validation["status"] not in ("valid_auxiliary", "degraded_auxiliary"):
        return None
    datasets = {}
    for name, ds in bundle["datasets"].items():
        if name in BASE_DATASETS:
            projected = base._project_dataset(name, ds, bundle["sources"])
        elif name in ("stock_1h", "derivative_oi", "dividend_adjustments", "stock_quote"):
            keys = STOCK_ROWS if name == "stock_1h" else OI_ROWS if name == "derivative_oi" else {"time", "rate"}
            projected = {"source_ids": list(ds.get("source_ids") or []),
                         "rows": [base._typed_fields(row, keys) for row in ds.get("rows") or [] if isinstance(row, dict)],
                         "values": base._typed_fields(ds.get("values") or {}, {"price", "session_volume_shares", "previous_close"})}
            if name == "stock_quote":
                projected["source_time_missing"] = ds.get("source_time_missing") is True
                if public_stock:
                    session = ds.get("market_session") or {}
                    projected["market_session"] = {"open": session.get("open") if type(session.get("open")) is bool else None,
                        "phase": session.get("phase") if session.get("phase") in ("REGULAR", "PRE", "POST", "OVERNIGHT", "CLOSED", "UNKNOWN") else "UNKNOWN"}
            if name == "stock_1h":
                projected.update(adjustment_mode="UNKNOWN" if public_stock else "ADJUSTED", units={"price": "USD/share", "volume": "shares"})
                if public_stock:
                    projected["analysis_eligible"] = False
        else:
            continue
        projected.update(instrument=ds["instrument"], relation=ds["relation"])
        if name in ("funding", "dividend_adjustments"):
            projected["rate_type"] = "Regular" if name == "funding" else "Special"
        issues = validation["dataset_reasons"].get(name, [])
        projected.setdefault("quality", {})["reasons"] = issues
        if any(reason.startswith("invalid_") or reason in (
                "unproven_adjusted_closed_bars", "stock_unit_mismatch", "derivative_unit_mismatch", "settlement_unit_mismatch",
                "dataset_source_mismatch", "invented_quote_source_time", "observation_after_retrieval", "basis_value_mismatch",
                "source_contract_mismatch", "canonical_origin_mismatch", "missing_public_endpoint", "source_time_order") for reason in issues):
            projected.update(rows=[], values={}, measurement_rejected=True)
        datasets[name] = projected
    return {"role": "auxiliary_observation", "asset_id": bundle["asset_id"], "instrument": INSTRUMENTS[bundle["asset_id"]].identity(),
            "report_id": bundle["report_id"], "bundle_id": bundle["bundle_id"], "generated_at": bundle["generated_at"],
            "datasets": datasets, "sources": {sid: base._project_source(sid, source) for sid, source in bundle["sources"].items()
                                               if isinstance(source, dict) and (_stock_source_identity(sid, source)
                                                   if public_stock and sid in ("stock_klines", "stock_quote")
                                                   else base._source_identity(sid, source))},
            "validation": validation, "independent_verified_sources": 0}
