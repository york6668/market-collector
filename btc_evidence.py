"""Portable collector evidence contract. Validation never verifies market claims.

The text hash binds exact UTF-8 Telegram text. bundle_id addresses the canonical
JSON body without bundle_id; neither hash authenticates the publisher. Readers
must also enforce their configured Telegram chat/sender allowlist.
"""

import hashlib
import copy
import ipaddress
import re
import json
import math
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit
import btc_etf
import btc_etf_flow

SCHEMA = "BTC_COLLECTOR_EVIDENCE_V1"
MAX_BYTES = 5 * 1024 * 1024


def canonical_bytes(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


def text_hash(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def iso_time(seconds):
    if seconds is None:
        return None
    try:
        return datetime.fromtimestamp(float(seconds), timezone.utc).isoformat()
    except (ValueError, TypeError, OverflowError, OSError):
        return None


def _clean(value):
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {str(k): _clean(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_clean(v) for v in value]
    return value






def _source(source_id, label, endpoint, venue, retrieved_at=None, as_of=None,
            source_dates=None):
    if venue is None:
        venue = next((v for v, host, path in _source_contract(source_id)
                      if _url_host(endpoint) == host and urlsplit(endpoint).path == path), None)
    return {
        "source_id": source_id, "label": label, "venue": venue,
        "endpoint_url": endpoint if _public_url(endpoint) else None,
        "canonical_origin": _url_host(endpoint),
        "access_scope": "public", "retrieved_at": retrieved_at,
        "as_of": as_of, "source_dates": source_dates or {},
    }




def _flow_rows(bars):
    rows = []
    for bar in bars or []:
        total = bar.get("quote_volume")
        buy = bar.get("taker_buy_quote_volume", bar.get("taker_buy_quote"))
        # Legacy spot receipts retain total and net, which permit exact recovery.
        recovered = False
        if buy is None and _number(total) and _number(bar.get("net_quote")):
            buy = (total + bar["net_quote"]) / 2
            recovered = True
        valid = _number(total) and _number(buy) and 0 <= buy <= total
        rows.append({
            "open_time": bar.get("open_time"), "close_time": bar.get("close_time"),
            "open": bar.get("open"), "high": bar.get("high"),
            "low": bar.get("low"), "close": bar.get("close"),
            "base_volume": bar.get("volume"), "quote_volume": total,
            "taker_buy_quote": buy if valid else None,
            "taker_sell_quote": total - buy if valid else None,
            "net_taker_quote": buy - (total - buy) if valid else None,
            "flow_valid": valid, "buy_recovered_from_net": recovered,
        })
    return rows


def build_bundle(payload, report_text, report_id, generated_at, full_report_text=None):
    if payload.get("evidence_version") == 2:
        from market_evidence_v2 import build_bundle as build_v2
        return build_v2(payload, report_text, report_id, generated_at, full_report_text)
    if (payload.get("instrument") or {}).get("asset_id", "BTC") != "BTC":
        from market_evidence import build_bundle as build_market_bundle
        return build_market_bundle(payload, report_text, report_id, generated_at, full_report_text)
    return _build_btc_bundle(payload, report_text, report_id, generated_at, full_report_text)


def _build_btc_bundle(payload, report_text, report_id, generated_at, full_report_text=None):
    """Build evidence from collected observations; no source time is invented."""
    records = payload.get("public_records") or {}
    bars = list(payload.get("raw_signal_bars") or [])[-240:]
    latest = bars[-1] if bars else {}
    sources, datasets = {}, {}
    futures_rows = _flow_rows(bars)
    sources["futures_klines"] = _source(
        "futures_klines", "Binance Futures Kline", latest.get("endpoint_url"), "Binance",
        iso_time(latest.get("retrieved_at_ms") / 1000) if _number(latest.get("retrieved_at_ms")) else None,
        iso_time(latest.get("close_time") / 1000) if _number(latest.get("close_time")) else None)
    datasets["futures_1h"] = {
        "source_ids": ["futures_klines"], "rows": futures_rows,
        "units": {"price": "USDT/BTC", "quote_volume": "USDT", "base_volume": "BTC"},
        "formula": "sell=total-buy; net=buy-sell; taker flow is a proxy",
        "window": _window(futures_rows, "close_time", 240, 3600000),
        "closed_bars": True,
        "quality": {"missing_flow": any(not r["flow_valid"] for r in futures_rows)},
    }
    oi_hl = latest.get("oi_time_basis") == "received_at"
    currency = "USD" if oi_hl else "USDT"
    oi_rows = [{"timestamp": r.get("oi_timestamp"), "close_time": r.get("close_time"),
                "quantity_btc": r.get("oi"), "notional_usd": r.get("oi_value"),
                "notional_value": r.get("oi_value"), "notional_currency": currency,
                "time_basis": r.get("oi_time_basis"),
                "source": r.get("oi_source"), "aligned": r.get("oi_aligned"),
                "drift_ms": r.get("oi_drift_ms")} for r in bars]
    oi_as_of = None if oi_hl else latest.get("oi_source_as_of_ms")
    sources["open_interest"] = _source(
        "open_interest", latest.get("oi_source"), latest.get("oi_endpoint_url"),
        "Hyperliquid" if oi_hl else "Binance",
        iso_time(latest.get("oi_retrieved_at_ms") / 1000) if _number(latest.get("oi_retrieved_at_ms")) else None,
        iso_time(oi_as_of / 1000) if _number(oi_as_of) else None)
    sources["open_interest"]["time_basis"] = latest.get("oi_time_basis")
    datasets["oi_1h"] = {
        "source_ids": ["open_interest"], "rows": oi_rows,
        "units": {"quantity_btc": "BTC", "notional_value": currency,
                  "notional_usd": currency},
        "formula": "notional_usd is a legacy alias; no FX conversion; no long/short direction",
        "window": _window(oi_rows, "close_time", 240, 3600000),
        "quality": {"alignment": payload.get("signal_data_quality") or {},
                    "source_time_missing": oi_as_of is None},
    }

    for name, record in records.items():
        record = record or {}
        value = record.get("value") or {}
        if not isinstance(value, dict):
            value = {"value": value}
        quality = {k: record.get(k) for k in (
            "status", "reason", "stale", "source_stale", "snapshot_used", "coverage",
            "source_stale_reason", "source_time_missing", "publication_time_missing")}
        if name == "macro":
            refs, field_sources = [], {}
            for item in value.get("history") or []:
                if not isinstance(item, dict) or not item.get("series_id"):
                    continue
                sid = "macro_" + str(item["series_id"])
                dates = {"observation": item["end_date"]} if item.get("end_date") else {}
                sources[sid] = _source(sid, item["series_id"], item.get("endpoint_url"),
                                       item.get("venue"), iso_time(item.get("retrieved_at")),
                                       item.get("published_at"), dates)
                sources[sid]["access_scope"] = item.get("access_scope") or "public"
                sources[sid]["publication_time_missing"] = item.get("published_at") is None
                sources[sid]["time_basis"] = "observation_date"
                if sid not in refs:
                    refs.append(sid)
                for field in item.get("fields") or []:
                    field_sources.setdefault(field, []).append(sid)
            datasets[name] = {
                "source_ids": refs, "rows": value.get("history") or [],
                "values": {k: v for k, v in value.items() if k != "history"},
                "field_sources": field_sources, "quality": quality,
                "units": record.get("units") or {},
                "formula": "composite of dated observations; publication time unknown unless supplied",
            }
            continue
        sid = "public_" + name
        source_ms = record.get("source_as_of_ms")
        source_dates = record.get("source_date") or value.get("source_date")
        sources[sid] = _source(
            sid, record.get("source"), record.get("endpoint_url"), record.get("venue"),
            iso_time(record.get("retrieved_at", record.get("updated_at"))), iso_time(source_ms / 1000) if _number(source_ms) else None,
            source_dates)
        sources[sid]["requests"] = record.get("source_requests") or []
        sources[sid]["time_basis"] = record.get("time_basis")
        quality["source_time_missing"] = (record.get("source_time_missing") is True
                                         or not sources[sid]["as_of"] and not sources[sid]["source_dates"])
        ds = {"source_ids": [sid], "values": value, "quality": quality,
              "units": record.get("units") or {}, "formula": record.get("formula")}
        if name == "spot_cvd":
            rows = _flow_rows(value.get("hourly_flow"))
            ds.update(rows=rows, closed_bars=True, window=_window(rows, "close_time", 240, 3600000),
                      units={"price": "USDT/BTC", "quote_volume": "USDT", "cvd_pct": "percent"},
                      formula="net_taker_share_pct=100*sum(buy-sell)/sum(total); not cumulative CVD")
            ds["values"] = {k: v for k, v in value.items() if k != "hourly_flow"}
            ds["quality"]["missing_flow"] = any(not r["flow_valid"] for r in rows)
        elif name in ("funding", "hyperliquid_funding", "binance_funding_shadow"):
            rows = value.get("history") or []
            ds.update(rows=rows, formula="actual settled rates; predictions and current interval metadata are separate",
                      units={"rate": "fraction", "funding_pct": "percent"})
            ds["window"] = value.get("window_quality") or _window([], "time", 24, 3600000)
            ds["values"] = {k: v for k, v in value.items() if k != "history"}
        elif name == "etf":
            ds["formula"] = "5D/30D require 5/30 complete trading-day observations"
            ds["units"] = {"flow": "USD", "holdings": "BTC"}
            ds["rows"] = value.get("history") or []
            ds["values"] = {k: v for k, v in value.items() if k != "history"}
            if btc_etf.is_holdings_source(record):
                ds["formula"] = "Bitbo BTC balances; published USD creations minus redemptions; complete trading-day windows"
                ds["holdings_scope"] = record.get("holdings_scope")
                ds["holdings_rows"] = record.get("holdings_rows")
                ds["holding_7d_basis"] = record.get("holding_7d_basis")
                ds["field_sources"] = {"holdings_btc": [sid], "holding_7d_pct": [sid]}
                holding_dates = source_dates if isinstance(source_dates, dict) else {}
                sources[sid]["source_dates"] = {k: v for k, v in holding_dates.items() if k == "holdings"}
                ds["rows"] = []
                flow = record.get("flow_record") or {}
                if btc_etf_flow.is_flow_source(flow) and any(
                        _number(value.get(f"flow_{days}d_usd")) for days in btc_etf_flow.WINDOWS):
                    history_sid = sid + "_history"
                    sources[history_sid] = _source(history_sid, flow["venue"] + " US spot BTC ETF USD net flows",
                        flow["endpoint_url"], flow["venue"], iso_time(flow.get("retrieved_at")),
                        None, {f"flow_{days}d": flow.get("source_date") for days in btc_etf_flow.WINDOWS})
                    sources[history_sid]["time_basis"] = "observation_date"
                    ds["source_ids"].append(history_sid)
                    ds["rows"] = value.get("history") or []
                    ds["flow_scope"] = flow.get("flow_scope")
                    ds["field_sources"].update({f"flow_{days}d_usd": [history_sid] for days in btc_etf_flow.WINDOWS})
                if "holdings_record" in record and value.get("holdings_btc") is None:
                    ds["source_ids"].remove(sid)
                    del sources[sid]
                    ds["field_sources"].pop("holdings_btc")
                    ds["field_sources"].pop("holding_7d_pct")
                # Dates of failed/expired domains stay in the collection record,
                # not in measurements proven by the other domain's source.
                measured_dates = value.get("source_date") if isinstance(value.get("source_date"), dict) else {}
                ds["values"]["source_date"] = {k: v for k, v in measured_dates.items()
                    if k == "holdings" and sid in ds["source_ids"]
                    or k.startswith("flow_") and sid + "_history" in ds["source_ids"]}
                datasets[name] = ds
                continue
            history_sid = sid + "_history"
            flow_source = record.get("flow_1d_source")
            dates = source_dates if isinstance(source_dates, dict) else {}
            history_fields = {"flow_5d", "flow_30d"}
            if flow_source == "history":
                history_fields.add("flow_1d")
            history_dates = {k: v for k, v in dates.items() if k in history_fields and v}
            sources[sid]["source_dates"] = {k: v for k, v in dates.items()
                if k not in history_fields and (k != "flow_1d" or flow_source == "current")}
            sources[sid]["retrieved_at"] = iso_time(record.get("current_retrieved_at",
                record.get("retrieved_at", record.get("updated_at"))))
            sources[history_sid] = _source(history_sid, "SoSoValue historical inflow",
                record.get("history_endpoint_url"), "SoSoValue", iso_time(record.get("history_retrieved_at",
                    record.get("retrieved_at", record.get("updated_at")))),
                None, history_dates)
            ds["source_ids"].append(history_sid)
            # Old records do not prove which endpoint supplied the daily value.
            daily_refs = [sid] if flow_source == "current" else [history_sid] if flow_source == "history" else []
            ds["field_sources"] = {"flow_1d_usd": daily_refs, "flow_5d_usd": [history_sid],
                                   "flow_30d_usd": [history_sid]}
        elif name == "stablecoin":
            ds["formula"] = "USD-pegged circulating supply difference; not exchange deposits or BTC purchases"
            ds["units"] = {"supply": "USD"}
            ds["rows"] = value.get("history") or []
            ds["values"] = {k: v for k, v in value.items() if k != "history"}
        datasets[name] = ds

    market = payload.get("market") or {}
    for name, fields, prefix, venue in (
            ("market_current", ("price", "chg24", "vol24"), "quote", market.get("source")),
            ("market_cap", ("mcap",), "mcap", market.get("mcap_source"))):
        values = {key: market.get(key) for key in fields}
        if not any(_number(value) for value in values.values()):
            continue
        sid = name
        stamp = market.get(prefix + "_source_as_of_ms")
        sources[sid] = _source(sid, venue, market.get(prefix + "_endpoint_url"), venue,
                              iso_time(market.get(prefix + "_retrieved_at")),
                              iso_time(stamp / 1000) if _number(stamp) else None)
        datasets[name] = {
            "source_ids": [sid], "values": values,
            "units": {"price": "USDT/BTC" if "Binance" in str(venue) else "USD/BTC",
                      "vol24": "USDT" if "Binance" in str(venue) else "USD",
                      "mcap": "USD", "chg24": "percent"},
            "quality": {"source_time_missing": stamp is None},
            "formula": "current quote, separate from last completed hourly close",
        }
    fear = payload.get("fear_greed") or {}
    if isinstance(payload.get("fear_greed"), dict):
        stamp = fear.get("source_as_of_ms")
        sources["fear_greed"] = _source("fear_greed", "Alternative.me", fear.get("endpoint_url"),
            "Alternative.me", iso_time(fear.get("retrieved_at")),
            iso_time(stamp / 1000) if _number(stamp) else None)
        datasets["fear_greed"] = {
            "source_ids": ["fear_greed"], "values": {k: v for k, v in fear.items() if k != "history"},
            "rows": fear.get("history") or [], "units": {"value": "index 0..100"},
            "quality": {"source_time_missing": stamp is None,
                        "source_stale": fear.get("source_stale") is True,
                        "status": "ok" if fear.get("available") is True and _number(fear.get("value")) else "unavailable"},
        }
    body = _clean({
        "schema": SCHEMA, "asset_id": "BTC", "report_id": str(report_id), "generated_at": generated_at,
        "text_sha256": text_hash(report_text), "role": "auxiliary_observation",
        "sources": sources, "datasets": datasets,
        "quality": payload.get("signal_data_quality") or {},
        "collection_metrics": payload.get("collector_metrics") or {},
        "mechanical_observation": {"rule_state": payload.get("rule_state"),
                                  "status_layer": payload.get("status_layer"),
                                  "market_patterns": payload.get("market_patterns")},
        "full_report_text": full_report_text,
    })
    body["bundle_id"] = "btc:" + hashlib.sha256(canonical_bytes(body)).hexdigest()
    return body


def encode_bundle(bundle):
    encoded = canonical_bytes(bundle)
    if len(encoded) > MAX_BYTES:
        raise ValueError("evidence_size_limit")
    return encoded






def auxiliary_observation(bundle, report_text):
    if isinstance(bundle, dict) and bundle.get("schema") in ("BTC_COLLECTOR_EVIDENCE_V2", "MARKET_COLLECTOR_EVIDENCE_V2"):
        from collector_v2_reader import auxiliary_observation as project_v2
        return project_v2(bundle, report_text)
    if isinstance(bundle, dict) and bundle.get("schema") == "MARKET_COLLECTOR_EVIDENCE_V1":
        from market_reader import auxiliary_observation as project_market
        return project_market(bundle, report_text)
    """Projection deliberately has no strategy claims, decisions or account facts."""
    validation = validate_bundle(bundle, report_text)
    if validation.get("status") not in ("valid_auxiliary", "degraded_auxiliary"):
        return None
    datasets = {name: _project_dataset(name, ds, bundle["sources"]) for name, ds in bundle["datasets"].items()
                if name in _DATASET_SOURCE_IDS and isinstance(ds, dict)}
    refs = {sid for ds in datasets.values() for sid in ds.get("source_ids", [])}
    sources = {sid: _project_source(sid, source)
               for sid, source in bundle["sources"].items()
               if sid in refs and isinstance(source, dict) and _source_identity(sid, source)}
    dataset_reasons = validation["dataset_reasons"]
    validation["dataset_reasons"] = {name: reasons for name, reasons in dataset_reasons.items()
        if name in _DATASET_SOURCE_IDS or name in ("__bundle__", "__sources__")}
    if any(name not in _DATASET_SOURCE_IDS and name not in ("__bundle__", "__sources__")
           for name in dataset_reasons):
        validation["dataset_reasons"]["__unsupported__"] = ["unsupported_dataset"]
    return {"role": "auxiliary_observation", "asset_id": "BTC", "bundle_id": bundle["bundle_id"],
            "report_id": bundle["report_id"], "generated_at": bundle["generated_at"],
            "datasets": datasets, "sources": sources,
            "validation": validation, "independent_verified_sources": 0}

_HOUR_MS = 3600000
_WINDOW_FIELDS = {
    "start_ms", "end_ms", "timestamp_field", "interval_ms",
    "expected_count", "actual_count", "contiguous", "complete",
}
_FIXED_WINDOWS = {
    "futures_1h": ("close_time", 240, _HOUR_MS),
    "oi_1h": ("close_time", 240, _HOUR_MS),
    "spot_cvd": ("close_time", 240, _HOUR_MS),
}

# These identify the supported public API contracts, never authenticated facts.
_BINANCE = "fapi.binance.com"
_HL = ("Hyperliquid", "api.hyperliquid.xyz", "/info")
_SOURCE_CONTRACTS = {
    "futures_klines": [("Binance", _BINANCE, "/fapi/v1/klines")],
    "open_interest": [("Binance", _BINANCE, "/futures/data/openInterestHist"), _HL],
    "public_spot_cvd": [("Binance", "api.binance.com", "/api/v3/klines")],
    "public_funding": [("Binance", _BINANCE, "/fapi/v1/fundingRate"), _HL],
    "public_binance_funding_shadow": [("Binance", _BINANCE, "/fapi/v1/fundingRate")],
    "public_hyperliquid_funding": [_HL],
    "public_basis": [("Binance", _BINANCE, "/fapi/v1/premiumIndex"), _HL],
    "public_binance_basis_shadow": [("Binance", _BINANCE, "/fapi/v1/premiumIndex")],
    "public_hyperliquid_basis": [_HL],
    "public_etf": [("SoSoValue", "api.sosovalue.xyz", "/openapi/v1/etf/us-btc-spot/currentEtfDataMetrics"),
                   (btc_etf.VENUE, "bitbo.io", "/treasuries/us-etfs/")],
    "public_etf_history": [("SoSoValue", "api.sosovalue.xyz", "/openapi/v1/etf/us-btc-spot/historicalInflowChart"),
                           (btc_etf_flow.VENUE, "www.tftc.io", "/bitcoin-etf-flows/data.json"),
                           (btc_etf_flow.COINBOSS_VENUE, "api.coinboss.com", "/api/etf/detail")],
    "public_stablecoin": [("DefiLlama", "stablecoins.llama.fi", "/stablecoins")],
    "market_current": [("Binance Spot 24hr", "api.binance.com", "/api/v3/ticker/24hr"),
                       ("Binance Spot", "api.binance.com", "/api/v3/ticker/24hr"),
                       ("CoinGecko", "api.coingecko.com", "/api/v3/coins/bitcoin")],
    "market_cap": [("CoinGecko", "api.coingecko.com", "/api/v3/coins/bitcoin")],
    "fear_greed": [("Alternative.me", "api.alternative.me", "/fng/")],
}
_FRED_IDS = set("DFEDTARU DFEDTARL DFF WALCL M2SL CUSR0000SEHA CUSR0000SEHC CPIMEDSL CUSR0000SEEB CUSR0000SETD PCU5241265241262 CES0500000003 FYFSGDA188S A091RC1Q027SBEA W006RC1Q027SBEA GFDEGDQ188S".split())
_TREASURY_PATH = "/resource-center/data-chart-center/interest-rates/pages/xml"
_FISCAL_PATHS = {
    "treasury_mts": "v1/accounting/mts/mts_table_1",
    "treasury_interest": "v2/accounting/od/interest_expense",
    "treasury_auction": "v1/accounting/od/auctions_query",
    "treasury_buyback": "v1/accounting/od/buybacks_operations",
}


def _source_contract(sid):
    if sid in _SOURCE_CONTRACTS:
        return _SOURCE_CONTRACTS[sid]
    if not isinstance(sid, str) or not sid.startswith("macro_"):
        return []
    series = sid[6:]
    if series in _FRED_IDS:
        return [("FRED", "fred.stlouisfed.org", "/graph/fredgraph.csv")]
    if series in ("treasury_us10y", "treasury_real_10y"):
        return [("US Treasury", "home.treasury.gov", _TREASURY_PATH)]
    if series in _FISCAL_PATHS:
        return [("US Treasury", "api.fiscaldata.treasury.gov",
                 "/services/api/fiscal_service/" + _FISCAL_PATHS[series])]
    symbol = {"yahoo_us10y": "%5ETNX", "yahoo_dxy": "DX-Y.NYB", "yahoo_gold": "GC=F"}.get(series)
    if symbol:
        return [("Yahoo Finance", host, "/v8/finance/chart/" + symbol)
                for host in ("query1.finance.yahoo.com", "query2.finance.yahoo.com")]
    if series == "fedwatch":
        return [("CME", "www.cmegroup.com", "/markets/interest-rates/cme-fedwatch-tool.html")]
    return []


def _source_identity(sid, source):
    endpoint = source.get("endpoint_url")
    host = _url_host(endpoint)
    return bool(host and source.get("source_id") == sid
                and source.get("canonical_origin") == host and source.get("access_scope") == "public"
                and (source.get("venue"), host, urlsplit(endpoint).path)
                in _source_contract(sid))


def _project_source(sid, source):
    result = {key: source.get(key) for key in (
        "source_id", "venue", "endpoint_url", "canonical_origin")}
    result["access_scope"] = "public"
    for key in ("retrieved_at", "as_of"):
        result[key] = source.get(key) if _aware_time(source.get(key)) else None
    dates = _date_leaves(source.get("source_dates", {}), exclude_scheduled=True)
    result["source_dates"] = sorted({day.isoformat() for day in dates}) if dates else []
    if source.get("time_basis") in ("received_at", "source_observation", "observation_date"):
        result["time_basis"] = source["time_basis"]
    result["publication_time_missing"] = source.get("publication_time_missing") is True
    return result


_DATASET_SOURCE_IDS = {name: {"public_" + name} for name in (
    "funding", "hyperliquid_funding", "binance_funding_shadow", "basis",
    "hyperliquid_basis", "binance_basis_shadow", "spot_cvd", "stablecoin")}
_DATASET_SOURCE_IDS.update(futures_1h={"futures_klines"}, oi_1h={"open_interest"},
                           etf={"public_etf", "public_etf_history"}, macro=None,
                           **{name: {name} for name in ("market_current", "market_cap", "fear_greed")})
_FLOW_FIELDS = set("open_time close_time open high low close base_volume quote_volume taker_buy_quote taker_sell_quote net_taker_quote flow_valid buy_recovered_from_net".split())
_ROW_FIELDS = {
    "futures_1h": _FLOW_FIELDS, "spot_cvd": _FLOW_FIELDS,
    "oi_1h": set("timestamp close_time quantity_btc notional_usd notional_value notional_currency time_basis aligned drift_ms".split()),
    "etf": {"date", "totalNetInflow"},
    "stablecoin": set("current_usd previous_day_usd previous_week_usd source_as_of_ms".split()),
    "fear_greed": {"timestamp", "value"},
}
_VALUE_FIELDS = {
    "market_current": {"price", "chg24", "vol24"}, "market_cap": {"mcap"}, "fear_greed": {"value"},
    "spot_cvd": set("cvd_1h_pct cvd_4h_pct cvd_24h_pct volume_4h_pct volume_24h_pct net_1h_quote net_4h_quote net_24h_quote".split()),
    "etf": set("flow_1d_usd flow_5d_usd flow_30d_usd flow_1d_assets_pct flow_5d_assets_pct flow_30d_assets_pct holdings_btc holdings_market_pct holding_7d_pct".split()),
    "stablecoin": set("total_usd flow_24h_usd flow_7d_usd day_coverage_pct week_coverage_pct asset_count day_asset_count week_asset_count".split()),
}
_FUNDING_NAMES = {"funding", "hyperliquid_funding", "binance_funding_shadow"}
_BASIS_NAMES = {"basis", "hyperliquid_basis", "binance_basis_shadow"}
_MACRO_ROW_FIELDS = set("date value record_date auction_date operation_date current_month_gross_rcpt_amt current_month_gross_outly_amt current_month_dfct_sur_amt month_expense_amt fytd_expense_amt bid_to_cover_ratio offering_amt total_par_amt_accepted max_par_amt_redeemed cut_probability hike_probability hold_probability expected_rate".split())
_BOOLEAN_FIELDS = {"flow_valid", "buy_recovered_from_net", "aligned"}
_DATE_FIELDS = {"date", "record_date", "auction_date", "operation_date"}
_STRING_FIELDS = {"notional_currency": {"USD", "USDT"},
                  "time_basis": {"received_at", "source_observation"}}


def _typed_fields(item, keys):
    result = {}
    for key in keys.intersection(item):
        value = item[key]
        if value is None:
            result[key] = None
        elif key in _BOOLEAN_FIELDS:
            if type(value) is bool:
                result[key] = value
        elif key in _DATE_FIELDS:
            if isinstance(value, str) and _date_leaves(value):
                result[key] = value
        elif key in _STRING_FIELDS:
            if isinstance(value, str) and value in _STRING_FIELDS[key]:
                result[key] = value
        elif _number(value):
            result[key] = value
    return result


def _oi_currency(sources, refs):
    hosts = {_url_host(sources[sid].get("endpoint_url")) for sid in refs
             if isinstance(sources.get(sid), dict)}
    return "USD" if hosts == {"api.hyperliquid.xyz"} else "USDT" if hosts == {_BINANCE} else None


def _row_measurements(name, row, currency=None):
    """Validation and projection share exclusions; null remains missing, never zero."""
    projected = _typed_fields(row, _ROW_FIELDS.get(name, set()))
    problems = []

    def require(key, predicate, reason):
        value = row.get(key)
        if not _number(value) or not predicate(value):
            problems.append(reason)
            if value is not None:
                projected.pop(key, None)
            return False
        return True

    if name in ("futures_1h", "spot_cvd"):
        prices = ("open", "high", "low", "close")
        price_checks = [require(key, lambda value: value > 0, "invalid_ohlc") for key in prices]
        available = {key: row[key] for key, valid in zip(prices, price_checks) if valid}
        ordered_pairs = (("low", "high"), ("low", "open"), ("low", "close"),
                         ("open", "high"), ("close", "high"))
        # A missing price cannot make contradictions among the other prices valid.
        if any(low in available and high in available and available[low] > available[high]
               for low, high in ordered_pairs):
            problems.append("invalid_ohlc")
            for key in prices:
                if row.get(key) is not None:
                    projected.pop(key, None)
        require("base_volume", lambda value: value >= 0, "invalid_base_volume")
        flow = ("quote_volume", "taker_buy_quote", "taker_sell_quote", "net_taker_quote")
        valid = all(_number(row.get(key)) for key in flow)
        if valid:
            total, buy, sell, net = (row[key] for key in flow)
            valid = (0 <= buy <= total and sell >= 0
                     and math.isclose(sell, total - buy, rel_tol=1e-9, abs_tol=1e-8)
                     and math.isclose(net, buy - sell, rel_tol=1e-9, abs_tol=1e-8))
        if row.get("flow_valid") is not True or not valid:
            problems.append("invalid_flow")
            for key in (*flow, "flow_valid"):
                if row.get(key) is not None:
                    projected.pop(key, None)
    elif name == "oi_1h":
        require("quantity_btc", lambda value: value >= 0, "invalid_oi_value")
        amount_ok = require("notional_value", lambda value: value >= 0, "invalid_oi_notional")
        legacy = row.get("notional_usd")
        legacy_ok = legacy is None or require("notional_usd", lambda value: value >= 0, "invalid_oi_notional")
        if amount_ok and legacy_ok and legacy is not None:
            amount_ok = math.isclose(row["notional_value"], legacy, rel_tol=1e-9, abs_tol=1e-8)
            if not amount_ok:
                problems.append("oi_notional_alias_mismatch")
        currency_ok = (row.get("notional_currency") in ("USD", "USDT")
                       and (currency is None or row["notional_currency"] == currency))
        if not currency_ok:
            problems.append("oi_notional_currency_mismatch")
        if not amount_ok or not legacy_ok or not currency_ok:
            for key in ("notional_value", "notional_usd", "notional_currency"):
                if row.get(key) is not None:
                    projected.pop(key, None)
    return projected, sorted(set(problems))


def _project_dataset(name, ds, sources):
    """Only typed measurement fields cross the strategy boundary; no arbitrary trees."""
    refs = ds.get("source_ids") if isinstance(ds.get("source_ids"), list) else []
    result = {"source_ids": [sid for sid in refs
                             if isinstance(sid, str) and _source_contract(sid)]}
    row_keys = _ROW_FIELDS.get(name, set())
    value_keys = _VALUE_FIELDS.get(name, set())
    if name in _FUNDING_NAMES:
        row_keys = {"time", "rate", "premium"}
        value_keys = set("funding_1h_pct funding_8h_pct funding_24h_pct latest_settled_rate_pct latest_funding_time_ms current_interval_hours actual_interval_hours".split())
    if name in _BASIS_NAMES:
        value_keys = set("basis_pct premium_pct mark_price index_price open_interest_btc open_interest_usd received_at_ms last_funding_rate_pct next_funding_time".split())
    rows = ds.get("rows")
    if isinstance(rows, list):
        if name == "macro":
            result["rows"] = []
            for item in rows:
                if not isinstance(item, dict) or not _source_contract("macro_" + str(item.get("series_id"))):
                    continue
                # Composite macro decisions and free text never enter the projection.
                samples = item.get("rows")
                result["rows"].append({"series_id": item["series_id"],
                    "rows": [_typed_fields(row, _MACRO_ROW_FIELDS) for row in samples if isinstance(row, dict)]
                            if isinstance(samples, list) else []})
        else:
            currency = _oi_currency(sources, result["source_ids"])
            result["rows"] = [(_row_measurements(name, row, currency)[0]
                               if name in _FIXED_WINDOWS else _typed_fields(row, row_keys))
                              for row in rows if isinstance(row, dict)]
    if isinstance(ds.get("values"), dict):
        result["values"] = _typed_fields(ds["values"], value_keys)
    if name == "etf" and isinstance(ds.get("field_sources"), dict):
        result["field_sources"] = {field: [sid for sid in refs if isinstance(sid, str)
            and sid in _DATASET_SOURCE_IDS["etf"]] for field, refs in ds["field_sources"].items()
            if field in value_keys and isinstance(refs, list)}
    window = ds.get("window")
    if isinstance(window, dict):
        result["window"] = {k: copy.deepcopy(v) for k, v in window.items() if k in _WINDOW_FIELDS
            and (v is None
                 or k in ("contiguous", "complete") and type(v) is bool
                 or k == "timestamp_field" and v in ("time", "close_time")
                 or k in ("start_ms", "end_ms", "interval_ms", "expected_count", "actual_count")
                    and _number(v) and int(v) == v and v >= (0 if k == "actual_count" else 1))}
    return result


def _number(value):
    if type(value) not in (int, float):
        return False
    try:
        return math.isfinite(value)
    except (OverflowError, ValueError, TypeError):
        return False


def _url_host(value):
    if not isinstance(value, str) or not value or any(c.isspace() for c in value):
        return None
    if "\\" in value:
        return None
    try:
        parsed = urlsplit(value)
        host = parsed.hostname
        if (parsed.scheme != "https" or not host or parsed.username is not None
                or parsed.password is not None or parsed.port not in (None, 443)
                or parsed.fragment):
            return None
        host = host.rstrip(".").lower()
        try:
            ipaddress.ip_address(host)
        except ValueError:
            pass
        else:
            return None
        labels = host.split(".")
        if (len(host) > 253 or len(labels) < 2
                or not re.fullmatch(r"[a-z][a-z0-9-]*", labels[-1])
                or not all(re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", x)
                           for x in labels)):
            return None
        return host
    except (ValueError, TypeError, AttributeError, OverflowError):
        return None


def _public_url(value):
    return _url_host(value) is not None


def _aware_time(value):
    if not isinstance(value, str):
        return None
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return result.astimezone(timezone.utc) if result.utcoffset() is not None else None
    except (ValueError, TypeError, OverflowError):
        return None


def _date_leaves(value, exclude_scheduled=False):
    """Return strict ISO calendar dates; None means a malformed date tree."""
    pending = [(value, 0)]
    dates = []
    visited = 0
    while pending:
        item, depth = pending.pop()
        visited += 1
        if depth > 32 or visited > 10000:
            return None
        if isinstance(item, dict):
            pending.extend((child, depth + 1) for key, child in item.items()
                           if not exclude_scheduled or key not in ("next_meeting_date", "fed_next_meeting_date"))
        elif isinstance(item, list):
            pending.extend((child, depth + 1) for child in item)
        elif isinstance(item, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", item):
            try:
                dates.append(datetime.strptime(item, "%Y-%m-%d").date())
            except ValueError:
                return None
        else:
            return None
    return dates


def _source_problems(sid, source, generated):
    if not isinstance(source, dict):
        return ["invalid_source"]
    problems = []
    if (not isinstance(sid, str) or not sid
            or source.get("source_id") != sid):
        problems.append("source_id_mismatch")
    host = _url_host(source.get("endpoint_url"))
    if host is None:
        problems.append("missing_public_endpoint")
    if host is None or source.get("canonical_origin") != host:
        problems.append("canonical_origin_mismatch")
    if source.get("access_scope") != "public":
        problems.append("invalid_access_scope")
    if not _source_identity(sid, source):
        problems.append("source_contract_mismatch")
    retrieved = _aware_time(source.get("retrieved_at"))
    as_of = _aware_time(source.get("as_of"))
    if retrieved is None:
        problems.append("invalid_retrieved_at")
    elif retrieved > generated:
        problems.append("source_time_order")
    if source.get("as_of") is not None and as_of is None:
        problems.append("invalid_as_of")
    if as_of is not None and retrieved is not None and as_of > retrieved:
        problems.append("source_time_order")
    raw_dates = source.get("source_dates", {})
    if isinstance(raw_dates, dict):
        raw_dates = {key: value for key, value in raw_dates.items()
                     if key not in ("next_meeting_date", "fed_next_meeting_date")}
    dates = _date_leaves(raw_dates, exclude_scheduled=True) if isinstance(raw_dates, (dict, str)) else None
    if dates is None:
        problems.append("invalid_source_dates")
    elif dates and retrieved is not None and max(dates) > retrieved.date():
        problems.append("source_date_after_retrieval")
    if as_of is None and not dates:
        problems.append("missing_source_time")
    if source.get("publication_time_missing") is True:
        problems.append("missing_publication_time")
    if source.get("time_basis") == "received_at" and as_of is not None:
        problems.append("receipt_time_is_not_source_time")
    return problems


def settlement_window(rows, expected=None, interval_ms=None, *, hours=None, as_of_ms=None):
    """Compare settlement buckets within 100 ms; retain every original timestamp.

    Only 1/2/4/8-hour historical spacing is supported. Current exchange metadata
    cannot establish that spacing. Off-grid, repeated and missing buckets fail;
    original timestamps remain subject to generation/retrieval-time validation.
    """
    safe_rows = list(rows) if isinstance(rows, (list, tuple)) else []

    def bucket(row):
        stamp = row.get("time") if isinstance(row, dict) else None
        if not _number(stamp) or stamp <= 0 or int(stamp) != stamp:
            return None
        edge = (int(stamp) + _HOUR_MS // 2) // _HOUR_MS * _HOUR_MS
        if as_of_ms is not None and (stamp > as_of_ms or edge > as_of_ms):
            return None
        return edge if abs(stamp - edge) <= 100 else None

    last_bucket = bucket(safe_rows[-1]) if safe_rows else None
    boundary = last_bucket - hours * _HOUR_MS if last_bucket is not None and hours is not None else None
    if boundary is not None:
        # Ignore only observations certainly older than this window. A malformed
        # or off-grid observation inside it must remain present and fail proof.
        safe_rows = [row for row in safe_rows if not isinstance(row, dict)
                     or not _number(row.get("time")) or row["time"] >= boundary - 100]
    buckets = [bucket(row) for row in safe_rows]
    valid = bool(buckets) and all(b is not None for b in buckets)
    gaps = {b - a for a, b in zip(buckets, buckets[1:])} if valid else set()
    supported = tuple(h * _HOUR_MS for h in (1, 2, 4, 8))
    observed = next(iter(gaps)) if len(gaps) == 1 else None
    if interval_ms is None:
        interval_ms = observed if observed in supported else None
    if expected is None and hours is not None and interval_ms in supported:
        expected = int(hours * _HOUR_MS / interval_ms) + 1 if hours * _HOUR_MS % interval_ms == 0 else None
    contiguous = bool(valid and buckets and interval_ms in supported
                      and all(b - a == interval_ms for a, b in zip(buckets, buckets[1:])))
    bounded = boundary is None or bool(buckets and buckets[0] == boundary)
    times = [row.get("time") if isinstance(row, dict) else None for row in safe_rows]
    times_valid = bool(times) and all(_number(t) and t > 0 for t in times)
    return {
        "start_ms": times[0] if times_valid else None,
        "end_ms": times[-1] if times_valid else None,
        "timestamp_field": "time", "interval_ms": interval_ms,
        "expected_count": expected, "actual_count": len(safe_rows),
        "contiguous": contiguous,
        "complete": bool(contiguous and bounded and type(expected) is int
                         and expected > 0 and len(safe_rows) == expected),
    }, safe_rows


def _window(rows, key, expected=None, interval_ms=None):
    safe_rows = rows if isinstance(rows, list) else []
    times = [row.get(key) if isinstance(row, dict) and isinstance(key, str)
             else None for row in safe_rows]
    valid = bool(times) and all(_number(t) and t > 0 for t in times)
    interval_valid = _number(interval_ms) and interval_ms > 0
    compared = ([_hour_end(t) for t in times] if key == "close_time" and interval_ms == _HOUR_MS
                else times)
    contiguous = bool(valid and interval_valid and all(t is not None for t in compared) and all(
        b - a == interval_ms for a, b in zip(compared, compared[1:])))
    return {
        "start_ms": times[0] if valid else None,
        "end_ms": times[-1] if valid else None,
        "timestamp_field": key, "interval_ms": interval_ms,
        "expected_count": expected, "actual_count": len(safe_rows),
        "contiguous": contiguous,
        "complete": bool(contiguous and type(expected) is int and expected > 0
                         and len(safe_rows) == expected),
    }


def _hour_end(value):
    """Canonical comparison edge only; preserve the original H-1/H receipt."""
    if not _number(value) or value <= 0 or int(value) != value:
        return None
    value = int(value)
    remainder = value % _HOUR_MS
    return value if remainder == 0 else value + 1 if remainder == _HOUR_MS - 1 else None


def _core_hour_problems(datasets):
    result, hours = {}, {}
    for name in _FIXED_WINDOWS:
        if name not in datasets:
            result[name] = ["missing_dataset"]
            continue
        ds = datasets[name]
        rows = ds.get("rows") if isinstance(ds, dict) else None
        if not isinstance(rows, list) or not rows:
            continue
        edges = [_hour_end(row.get("close_time")) if isinstance(row, dict) else None for row in rows]
        if any(edge is None for edge in edges):
            result[name] = ["invalid_utc_hour_boundary"]
        elif len(edges) == 240 and all(b - a == _HOUR_MS for a, b in zip(edges, edges[1:])):
            hours[name] = edges
    if len(hours) > 1 and len({tuple(edges) for edges in hours.values()}) > 1:
        for name in hours:
            result.setdefault(name, []).append("core_hour_alignment_mismatch")
    return result


def _data_date_problems(ds, generated):
    pending = [(ds, 0)]
    visited = 0
    while pending:
        item, depth = pending.pop()
        visited += 1
        if depth > 32 or visited > 100000:
            return ["invalid_data_dates"]
        if isinstance(item, dict):
            for key, value in item.items():
                if isinstance(key, str) and (key in ("date", "source_date", "source_dates")
                                             or key.endswith("_date") or key.endswith("_dates")):
                    scheduled = key in ("next_meeting_date", "fed_next_meeting_date")
                    if scheduled and value is None:
                        continue
                    dates = _date_leaves(value)
                    if dates is None:
                        return ["invalid_data_dates"]
                    if dates and not scheduled and max(dates) > generated.date():
                        return ["data_date_after_generation"]
                elif isinstance(value, (dict, list)):
                    pending.append((value, depth + 1))
        elif isinstance(item, list):
            pending.extend((value, depth + 1) for value in item
                           if isinstance(value, (dict, list)))
    return []


def _row_problems(name, rows, generated_ms, sources, refs):
    currency = _oi_currency(sources, refs)
    problems = [reason for row in rows for reason in _row_measurements(name, row, currency)[1]]
    if name in ("futures_1h", "spot_cvd"):
        for row in rows:
            opened, closed = row.get("open_time"), row.get("close_time")
            edge = _hour_end(closed)
            if (not _number(opened) or opened < 0 or opened % _HOUR_MS != 0
                    or edge is None or edge - opened != _HOUR_MS
                    or closed > generated_ms):
                problems.append("invalid_bar_time")
    elif name == "oi_1h":
        endpoint_hosts = {_url_host(sources[sid].get("endpoint_url"))
                          for sid in refs if isinstance(sources.get(sid), dict)}
        is_hl = endpoint_hosts == {"api.hyperliquid.xyz"}
        max_drift = 90000 if is_hl else 300000
        seen = set()
        for row in rows:
            stamp, closed = row.get("timestamp"), row.get("close_time")
            drift = row.get("drift_ms")
            valid = (_number(stamp) and _number(closed) and _number(drift)
                     and stamp > 0 and 0 < closed <= generated_ms and stamp <= generated_ms
                     and abs(stamp - closed) <= max_drift
                     and drift == abs(stamp - closed) and row.get("aligned") is True)
            if is_hl:
                valid = valid and stamp <= closed
            if not valid or stamp in seen:
                problems.append("invalid_oi_alignment")
            if _number(stamp):
                seen.add(stamp)
    return problems


def _dataset_problems(name, ds, sources, source_errors, generated):
    if not isinstance(ds, dict):
        return ["invalid_dataset"]
    problems = []
    quality = ds.get("quality", {})
    if not isinstance(quality, dict):
        problems.append("invalid_quality")
        quality = {}
    if quality.get("status") in ("unavailable", "error") or quality.get("missing_flow"):
        problems.append("missing_data")
    if quality.get("publication_time_missing"):
        problems.append("missing_publication_time")
    if quality.get("source_time_missing"):
        problems.append("missing_source_time")
    if any(quality.get(k) for k in ("stale", "source_stale", "snapshot_used")):
        problems.append("stale_or_cache_fallback")
    refs = ds.get("source_ids")
    if (not isinstance(refs, list) or not refs
            or not all(isinstance(sid, str) for sid in refs)):
        refs = []
        problems.append("missing_source")
    for sid in refs:
        if sid not in sources:
            problems.append("missing_source")
        else:
            problems.extend(source_errors.get(sid, []))
    if name not in _DATASET_SOURCE_IDS:
        problems.append("unsupported_dataset")
    elif name == "macro":
        if any(not sid.startswith("macro_") for sid in refs):
            problems.append("dataset_source_mismatch")
    elif name == "etf" and btc_etf_flow.is_flow_source(sources.get("public_etf_history") or {}):
        expected_refs = {"public_etf_history"}
        if btc_etf.is_holdings_source(sources.get("public_etf") or {}):
            expected_refs.add("public_etf")
        if set(refs) != expected_refs:
            problems.append("dataset_source_mismatch")
    elif set(refs) != ({"public_etf"} if name == "etf" and
            btc_etf.is_holdings_source(sources.get("public_etf") or {}) else _DATASET_SOURCE_IDS[name]):
        problems.append("dataset_source_mismatch")
    rows = ds.get("rows")
    requires_window = name in _FIXED_WINDOWS or name in _FUNDING_NAMES
    has_window = "window" in ds
    if has_window and not requires_window:
        problems.append("unexpected_window")
    if requires_window or has_window:
        window = ds.get("window")
        rows_valid = isinstance(rows, list) and all(isinstance(row, dict) for row in rows)
        if not rows_valid:
            problems.append("invalid_rows")
        if not isinstance(window, dict) or not _WINDOW_FIELDS.issubset(window):
            problems.append("invalid_window")
        else:
            key, expected, interval = (window["timestamp_field"], window["expected_count"],
                                       window["interval_ms"])
            spec_valid = (isinstance(key, str) and bool(key)
                          and type(expected) is int and expected > 0
                          and _number(interval) and interval > 0
                          and type(window["actual_count"]) is int
                          and type(window["contiguous"]) is bool
                          and type(window["complete"]) is bool)
            if not spec_valid:
                problems.append("invalid_window")
            if name in _FIXED_WINDOWS and (key, expected, interval) != _FIXED_WINDOWS[name]:
                problems.append("window_contract_mismatch")
            if window["complete"] is not True:
                problems.append("incomplete_window")
            if rows_valid and spec_valid:
                cutoff_ms = generated.timestamp() * 1000
                if name in _FUNDING_NAMES:
                    for sid in refs:
                        retrieved = _aware_time((sources.get(sid) or {}).get("retrieved_at"))
                        if retrieved is not None:
                            cutoff_ms = min(cutoff_ms, retrieved.timestamp() * 1000)
                actual = (settlement_window(rows, expected, interval, as_of_ms=cutoff_ms)[0]
                          if name in _FUNDING_NAMES and key == "time"
                          else _window(rows, key, expected, interval))
                if any(window[field] != actual[field] for field in _WINDOW_FIELDS):
                    problems.append("window_mismatch")
                if not actual["complete"]:
                    problems.append("incomplete_window")
                if any(not _number(row.get(key)) or row[key] <= 0
                       or row[key] > generated.timestamp() * 1000 for row in rows):
                    problems.append("invalid_row_time")
        if rows_valid and name in _FIXED_WINDOWS:
            if name in ("futures_1h", "spot_cvd") and ds.get("closed_bars") is not True:
                problems.append("unproven_closed_bars")
            problems.extend(_row_problems(name, rows, generated.timestamp() * 1000,
                                          sources, refs))
    if name in _FUNDING_NAMES:
        problems.extend(_funding_problems(ds, sources, refs))
    if name in _BASIS_NAMES:
        values = ds.get("values") if isinstance(ds.get("values"), dict) else {}
        mark, index, basis = (values.get(k) for k in ("mark_price", "index_price", "basis_pct"))
        if (not all(_number(v) for v in (mark, index, basis)) or mark <= 0 or index <= 0
                or not math.isclose(basis, (mark - index) / index * 100, rel_tol=1e-9, abs_tol=1e-8)):
            problems.append("basis_value_mismatch")
    tftc = name == "etf" and btc_etf_flow.is_flow_source(sources.get("public_etf_history") or {})
    if name == "etf" and btc_etf.is_holdings_source(sources.get("public_etf") or {}):
        problems.extend(_etf_holdings_problems(ds, sources["public_etf"], flow_supported=tftc))
        if tftc:
            problems.extend(_etf_flow_problems(ds, sources["public_etf_history"], generated))
    elif tftc:
        values = ds.get("values") or {}
        if any(values.get(k) is not None for k in ("holdings_btc", "holding_7d_pct", "holdings_market_pct")):
            problems.append("flow_source_cannot_supply_holdings")
        problems.extend(_etf_flow_problems(ds, sources["public_etf_history"], generated))
    elif name == "etf":
        if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
            problems.append("invalid_rows")
        else:
            values = ds.get("values") if isinstance(ds.get("values"), dict) else {}
            field_sources = ds.get("field_sources") if isinstance(ds.get("field_sources"), dict) else {}
            daily_refs = field_sources.get("flow_1d_usd")
            if values.get("flow_1d_usd") is not None:
                if daily_refs not in (["public_etf"], ["public_etf_history"]):
                    problems.append("daily_flow_source_unknown")
                elif daily_refs == ["public_etf_history"]:
                    daily = values["flow_1d_usd"]
                    latest = rows[-1].get("totalNetInflow") if rows else None
                    if (not _number(daily) or not _number(latest)
                            or not math.isclose(daily, latest, rel_tol=1e-9, abs_tol=1e-8)):
                        problems.append("daily_flow_history_mismatch")
            dates = [row.get("date") for row in rows]
            if (not all(isinstance(day, str) for day in dates)
                    or dates != sorted(set(dates))):
                problems.append("invalid_trading_day_order")
            for count in (5, 30):
                sample = rows[-count:]
                if len(sample) != count or any(not _number(row.get("totalNetInflow")) for row in sample):
                    problems.append("incomplete_trading_day_samples")
                else:
                    observed = values.get("flow_%dd_usd" % count)
                    expected = sum(row["totalNetInflow"] for row in sample)
                    if not _number(observed) or not math.isclose(observed, expected, rel_tol=1e-9, abs_tol=1e-8):
                        problems.append("trading_day_sum_mismatch")
    problems.extend(_data_date_problems(ds, generated))
    problems.extend(_observation_time_problems(name, ds, sources, refs))
    return sorted(set(problems))


def _etf_holdings_problems(ds, source, flow_supported=False):
    values = ds.get("values") if isinstance(ds.get("values"), dict) else {}
    problems = []
    try:
        btc_etf.validate_holdings(ds.get("holdings_scope"), ds.get("holdings_rows"), values.get("holdings_btc"))
    except ValueError as exc:
        problems.append(str(exc))
    if (not flow_supported and (ds.get("rows") != [] or any(values.get(k) is not None for k in btc_etf.FLOW_FIELDS))
            or values.get("holdings_market_pct") is not None):
        problems.append("holdings_source_cannot_supply_flow_or_assets")
    field_sources = ds.get("field_sources") or {}
    if not isinstance(field_sources, dict) or field_sources.get("holdings_btc") != ["public_etf"]:
        problems.append("holdings_source_unknown")
    day = (source.get("source_dates") or {}).get("holdings")
    if not isinstance(day, str) or _date_leaves(day) is None:
        problems.append("missing_holdings_date")
    if (values.get("source_date") or {}).get("holdings") != day:
        problems.append("holdings_date_mismatch")
    change = values.get("holding_7d_pct")
    if change is not None:
        basis = ds.get("holding_7d_basis") or {}
        try:
            if not btc_etf.is_holdings_source(basis):
                raise ValueError("holding_7d_source_mismatch")
            btc_etf.validate_holdings(basis.get("holdings_scope"), basis.get("holdings_rows"), basis.get("holdings_btc"))
            prior_day = (datetime.strptime(day, "%Y-%m-%d") - timedelta(days=7)).date().isoformat()
            if basis.get("source_date") != prior_day or basis["holdings_btc"] <= 0:
                raise ValueError("invalid_holding_7d_basis")
            expected = (values["holdings_btc"] / basis["holdings_btc"] - 1) * 100
            if not _number(change) or not math.isclose(change, expected, rel_tol=1e-9, abs_tol=1e-8):
                raise ValueError("holding_7d_value_mismatch")
        except (ValueError, TypeError, KeyError) as exc:
            problems.append(str(exc) if isinstance(exc, ValueError) else "invalid_holding_7d_basis")
    return problems


def _etf_flow_problems(ds, source, generated):
    values = ds.get("values") if isinstance(ds.get("values"), dict) else {}
    rows = ds.get("rows")
    refs = ds.get("field_sources") or {}
    dates = source.get("source_dates") or {}
    problems = []
    if ds.get("units") != {"flow": "USD", "holdings": "BTC"}:
        problems.append("etf_unit_mismatch")
    if ds.get("flow_scope") != list(btc_etf.FUNDS):
        problems.append("flow_fund_scope_gap")
    if not isinstance(rows, list) or not rows or any(not isinstance(row, dict) for row in rows):
        return problems + ["invalid_flow_rows"]
    day = dates.get("flow_1d")
    try:
        order = [btc_etf_flow.source_day(row.get("date")).isoformat() for row in rows]
        if order != sorted(set(order)) or order[-1] != day or len(rows) > 30:
            raise ValueError("invalid_flow_day_order")
        stale, _ = btc_etf_flow.freshness(day, generated.timestamp())
        if stale:
            problems.append("stale_flow_source_date")
    except ValueError as exc:
        return problems + [str(exc)]
    for count in btc_etf_flow.WINDOWS:
        key = f"flow_{count}d_usd"
        observed = values.get(key)
        if observed is None:
            continue
        if refs.get(key) != ["public_etf_history"]:
            problems.append("flow_field_source_mismatch")
        if dates.get(f"flow_{count}d") != day or (values.get("source_date") or {}).get(f"flow_{count}d") != day:
            problems.append("flow_date_mismatch")
        expected, reason = btc_etf_flow.window(rows, count, day, source.get("venue"))
        if reason:
            problems.append(reason)
        elif not _number(observed) or not math.isclose(observed, expected, rel_tol=1e-12, abs_tol=1e-6):
            problems.append("trading_day_sum_mismatch")
    if any(values.get(f"flow_{count}d_assets_pct") is not None for count in btc_etf_flow.WINDOWS):
        problems.append("flow_source_cannot_supply_assets_pct")
    return problems


def _observation_time_problems(name, ds, sources, refs):
    problems = []
    def compare(source, stamps=(), dates=()):
        if not isinstance(source, dict):
            return
        retrieved, as_of = (_aware_time(source.get(key)) for key in ("retrieved_at", "as_of"))
        source_dates = _date_leaves(source.get("source_dates", {}), exclude_scheduled=True) or []
        if stamps:
            if retrieved and max(stamps) > retrieved.timestamp() * 1000:
                problems.append("observation_after_retrieval")
            if as_of and max(stamps) > as_of.timestamp() * 1000:
                problems.append("observation_after_as_of")
        if dates:
            if retrieved and max(dates) > retrieved.date():
                problems.append("observation_after_retrieval")
            coverage = as_of.date() if as_of else max(source_dates) if source_dates else None
            if coverage and max(dates) > coverage:
                problems.append("observation_after_as_of")
    rows = ds.get("rows") if isinstance(ds.get("rows"), list) else []
    if name == "macro":
        for item in rows:
            if not isinstance(item, dict):
                continue
            dates = []
            for row in item.get("rows", []) if isinstance(item.get("rows"), list) else []:
                if isinstance(row, dict):
                    for key in ("date", "record_date", "auction_date", "operation_date"):
                        if key in row:
                            dates.extend(_date_leaves(row[key]) or [])
            compare(sources.get("macro_" + str(item.get("series_id"))), dates=dates)
        return problems
    key = {"futures_1h": "close_time", "spot_cvd": "close_time", "oi_1h": "timestamp",
           "fear_greed": "timestamp"}.get(name, "time" if name in _FUNDING_NAMES else None)
    stamps = [row[key] * (1000 if name == "fear_greed" else 1) for row in rows
              if isinstance(row, dict) and key and _number(row.get(key))]
    values = ds.get("values") if isinstance(ds.get("values"), dict) else {}
    if name in _BASIS_NAMES and _number(values.get("received_at_ms")):
        stamps.append(values["received_at_ms"])
    dates = [day for row in rows if isinstance(row, dict)
             for day in (_date_leaves(row.get("date")) or [])] if name == "etf" else []
    for sid in refs:
        if name != "etf" or sid == "public_etf_history":
            compare(sources.get(sid), stamps, dates)
    return problems


def _funding_problems(ds, sources, refs):
    rows = ds.get("rows")
    if (not isinstance(rows, list) or not rows or any(not isinstance(row, dict)
            or not _number(row.get("time")) or not _number(row.get("rate")) for row in rows)):
        return ["missing_settled_rate"]
    values = ds.get("values") if isinstance(ds.get("values"), dict) else {}
    window = ds.get("window") if isinstance(ds.get("window"), dict) else {}
    is_hl = all(_url_host(sources[sid].get("endpoint_url")) == "api.hyperliquid.xyz"
                for sid in refs if isinstance(sources.get(sid), dict)) and bool(refs)
    problems = []
    def metric(key, expected):
        observed = values.get(key)
        if not _number(observed) or not math.isclose(observed, expected, rel_tol=1e-9, abs_tol=1e-8):
            problems.append("settled_sum_mismatch")
    if is_hl:
        if (window.get("timestamp_field"), window.get("expected_count"), window.get("interval_ms")) != ("time", 24, _HOUR_MS):
            problems.append("window_contract_mismatch")
        for hours in (1, 8, 24):
            sample = rows[-hours:]
            if not settlement_window(sample, hours, _HOUR_MS)[0]["complete"]:
                problems.append("incomplete_settlement_window")
            else:
                metric("funding_%dh_pct" % hours, sum(row["rate"] for row in sample) * 100)
    else:
        history, selected = settlement_window(rows, hours=240)
        interval = history["interval_ms"]
        if (not history["complete"] or len(selected) != len(rows)
                or window.get("timestamp_field") != "time" or window.get("hours") != 240):
            problems.append("window_contract_mismatch")
        claims = values.get("settlement_windows") if isinstance(values.get("settlement_windows"), dict) else {}
        for hours in (8, 24):
            expected, sample = settlement_window(rows, interval_ms=interval, hours=hours)
            if not expected["complete"]:
                problems.append("incomplete_settlement_window")
                continue
            claim = claims.get("%dh" % hours)
            if not isinstance(claim, dict) or any(claim.get(k) != v for k, v in expected.items()):
                problems.append("settlement_window_mismatch")
            metric("funding_%dh_pct" % hours, sum(row["rate"] for row in sample[1:]) * 100)
        metric("latest_settled_rate_pct", rows[-1]["rate"] * 100)
    if values.get("latest_funding_time_ms") != rows[-1]["time"]:
        problems.append("settlement_time_mismatch")
    return problems


def decode_bundle(data):
    if not isinstance(data, (bytes, bytearray, str)):
        raise ValueError("invalid_evidence_bytes")
    try:
        size = len(data.encode("utf-8")) if isinstance(data, str) else len(data)
        if size > MAX_BYTES:
            raise ValueError("evidence_size_limit")
        if isinstance(data, (bytes, bytearray)):
            data = bytes(data).decode("utf-8", errors="strict")
        if data.startswith("\\ufeff"):
            raise ValueError("evidence_utf8_bom_not_allowed")
        def unique_pairs(pairs):
            result = {}
            for key, value in pairs:
                if key in result:
                    raise ValueError("duplicate_json_key")
                result[key] = value
            return result
        def finite_float(value):
            result = float(value)
            if not _number(result):
                raise ValueError("nonfinite_json")
            return result
        result = json.loads(data, object_pairs_hook=unique_pairs, parse_float=finite_float,
                            parse_constant=lambda _: (_ for _ in ()).throw(ValueError("nonfinite_json")))
        if not isinstance(result, dict):
            raise ValueError("evidence_bundle_must_be_object")
        return result
    except (RecursionError, OverflowError, TypeError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("invalid_evidence_json") from exc


def validate_bundle(bundle, report_text=None):
    if isinstance(bundle, dict) and bundle.get("schema") in ("BTC_COLLECTOR_EVIDENCE_V2", "MARKET_COLLECTOR_EVIDENCE_V2"):
        from market_evidence_v2 import validate_bundle as validate_v2
        return validate_v2(bundle, report_text)
    if isinstance(bundle, dict) and bundle.get("schema") == "MARKET_COLLECTOR_EVIDENCE_V1":
        from market_evidence import validate_bundle as validate_market
        return validate_market(bundle, report_text)
    result = {"status": "rejected", "reasons": [], "dataset_reasons": {},
              "independent_verified_sources": 0, "role": "auxiliary_observation"}
    try:
        if not isinstance(bundle, dict) or bundle.get("schema") != SCHEMA:
            result["reasons"].append("unknown_schema")
            return result
        if bundle.get("asset_id", "BTC") != "BTC":
            result["reasons"].append("instrument_identity_mismatch")
            return result
        try:
            body = {k: v for k, v in bundle.items() if k != "bundle_id"}
            encoded = canonical_bytes(body)
            if len(encoded) > MAX_BYTES:
                result["reasons"].append("evidence_size_limit")
            expected = "btc:" + hashlib.sha256(encoded).hexdigest()
            if bundle.get("bundle_id") != expected:
                result["reasons"].append("bundle_hash_mismatch")
        except (ValueError, TypeError, OverflowError, RecursionError):
            result["reasons"].append("invalid_json_values")
        if bundle.get("role") != "auxiliary_observation":
            result["reasons"].append("invalid_role")
        if not isinstance(bundle.get("report_id"), str) or not bundle["report_id"]:
            result["reasons"].append("missing_report_id")
        generated = _aware_time(bundle.get("generated_at"))
        if generated is None:
            result["reasons"].append("invalid_generated_at")
        digest = bundle.get("text_sha256")
        if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
            result["reasons"].append("invalid_text_hash")
        if report_text is not None:
            if not isinstance(report_text, str) or digest != text_hash(report_text):
                result["reasons"].append("text_hash_mismatch")
        if not isinstance(bundle.get("datasets"), dict) or not isinstance(bundle.get("sources"), dict):
            result["reasons"].append("invalid_datasets_or_sources")
        if result["reasons"]:
            return result
        sources = bundle["sources"]
        source_errors = {sid: _source_problems(sid, source, generated)
                         for sid, source in sources.items()}
        referenced = set()
        for name, ds in bundle["datasets"].items():
            problems = _dataset_problems(name, ds, sources, source_errors, generated)
            if problems:
                result["dataset_reasons"][name] = problems
            if isinstance(ds, dict) and isinstance(ds.get("source_ids"), list):
                referenced.update(sid for sid in ds["source_ids"] if isinstance(sid, str))
        for name, reasons in _core_hour_problems(bundle["datasets"]).items():
            result["dataset_reasons"][name] = sorted(set(
                result["dataset_reasons"].get(name, []) + reasons))
        unused_errors = sorted({reason for sid, reasons in source_errors.items()
                                if sid not in referenced for reason in reasons})
        if unused_errors:
            result["dataset_reasons"]["__sources__"] = unused_errors
        if not bundle["datasets"]:
            result["dataset_reasons"]["__bundle__"] = ["empty_datasets"]
        result["status"] = ("pending_text" if report_text is None else
                            "degraded_auxiliary" if result["dataset_reasons"] else "valid_auxiliary")
        return result
    except (ValueError, TypeError, OverflowError, RecursionError, AttributeError, KeyError, IndexError):
        result["reasons"].append("invalid_bundle_values")
        result["status"] = "rejected"
        return result
