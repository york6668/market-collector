"""Anonymous, documented sources and resumable history jobs.

Network work runs outside report generation. All sources are anonymous; no paid or account API fallback.
"""
import csv
import io
import math
import os
import threading
import time
import zipfile
from contextlib import closing
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

import btc_collection_io as http
import seller_exhaustion
from market_history import DAY, HOUR, HistoryStore, history_path
from market_sources import SPOT, FUTURES, OI, FUNDING, ARCHIVE, STABLE, VIX, VXN, CFTC, HL, STOCK_WS, BYBIT_WS, provenance
_workers = {}
_worker_lock = threading.Lock()


class RetryLater(RuntimeError):
    def __init__(self, seconds):
        super().__init__("public_source_rate_limited")
        self.seconds = max(1, seconds)


def response(url, *, params=None, body=None):
    result = (http.post(url, json=body, timeout=(5, 20)) if body is not None else
              http.get(url, params=params, timeout=(5, 20)))
    return checked_response(result)


def checked_response(result):
    if result.status_code in (418, 429, 503):
        delay = result.headers.get("Retry-After", "60")
        try:
            seconds = float(delay)
        except (ValueError, TypeError):
            try:
                seconds = parsedate_to_datetime(delay).timestamp() - time.time()
            except (ValueError, TypeError, OverflowError):
                seconds = 60
        raise RetryLater(seconds)
    result.raise_for_status()
    return result


def number(value, minimum=None):
    if isinstance(value, bool):
        raise ValueError("invalid_number")
    value = float(value)
    if not math.isfinite(value) or minimum is not None and value < minimum:
        raise ValueError("invalid_number")
    return value


def milliseconds(value):
    value = int(value)
    return value // 1000 if value >= 100000000000000 else value


def received_ms(cutoff):
    return max(cutoff, int(time.time() * 1000))


def klines(raw, now_ms):
    rows = []
    for item in raw:
        # Archive files can have a header; API arrays cannot.
        if not item or str(item[0]) in ("open_time", "open_time_us"):
            continue
        opening, close = milliseconds(item[0]), milliseconds(item[6])
        if close >= now_ms:
            continue
        o, h, l, c = (number(item[i], 0) for i in (1, 2, 3, 4))
        total, buy = number(item[7], 0), number(item[10], 0)
        if (opening % HOUR or close != opening + HOUR - 1 or not 0 < l <= min(o, c) <= max(o, c) <= h
                or buy > total):
            raise ValueError("invalid_kline")
        rows.append({"observed_ms": close, "open_time": opening, "close_time": close,
                     "open": o, "high": h, "low": l, "close": c, "volume": number(item[5], 0),
                     "quote_volume": total, "taker_buy_quote": buy})
    return rows


def _first_gap(store, series, symbol, start, end):
    present = {r["open_time"] for r in store.rows(series, symbol, start_ms=start, end_ms=end)
               if "open_time" in r}
    return next((t for t in range(start, end, HOUR) if t not in present), None)


def sync_klines(store, symbol, *, spot=False, now_ms=None):
    now_ms = int(time.time() * 1000 if now_ms is None else now_ms)
    end = now_ms // HOUR * HOUR
    start, series = end - 180 * DAY, "spot_1h" if spot else "futures_1h"
    endpoint = SPOT if spot else FUTURES
    def rest(a, b):
        raw = response(endpoint, params={"symbol": symbol, "interval": "1h", "startTime": a,
                       "endTime": b - 1, "limit": 1000}).json()
        rows = [r for r in klines(raw, now_ms) if a <= r["open_time"] < b]
        store.put(series, symbol, rows, provenance(endpoint, "USDT", interval_ms=HOUR),
                  received_ms=received_ms(now_ms))

    # Always repair recent data first. A permanent old gap must not starve the
    # current window, including before listing and during TradFi closures.
    rest(end - 3 * DAY, end)
    scan_key = "kline_scan:" + series + ":" + symbol
    scan = store.get_meta(scan_key, {})
    cursor = max(start, scan.get('cursor', start))
    if cursor >= end:
        if now_ms - scan.get('finished_ms', 0) < 6 * HOUR:
            return 3600
        cursor = start
    missing = _first_gap(store, series, symbol, cursor, end)
    if missing is None:
        store.set_meta(scan_key, {'cursor':end, 'finished_ms':now_ms})
        return 3600
    cursor = missing
    month = datetime.fromtimestamp(cursor / 1000, timezone.utc).strftime("%Y-%m")
    month_start = datetime.strptime(month, '%Y-%m').replace(tzinfo=timezone.utc)
    next_month = (month_start + timedelta(days=32)).replace(day=1)
    month_end = int(next_month.timestamp() * 1000)
    archive_key = "archive_checked:" + series + ":" + symbol + ":" + month
    used_archive = False
    if missing is not None and cursor < end - 32 * DAY and now_ms-store.get_meta(archive_key, 0) > 21600000:
        month = datetime.fromtimestamp(cursor / 1000, timezone.utc).strftime("%Y-%m")
        directory = "spot" if spot else "futures/um"
        url = f"{ARCHIVE}/{directory}/monthly/klines/{symbol}/1h/{symbol}-1h-{month}.zip"
        result = http.get(url, timeout=(5, 20))
        if result.status_code == 200:
            with zipfile.ZipFile(io.BytesIO(result.content)) as archive:
                names = archive.namelist()
                if len(names) != 1 or archive.getinfo(names[0]).file_size > 16 * 1024 * 1024:
                    raise ValueError("unexpected_archive_size")
                raw = csv.reader(io.StringIO(archive.read(names[0]).decode("utf-8-sig")))
                rows = [r for r in klines(raw, now_ms) if start <= r["open_time"] < end]
            store.put(series, symbol, rows, provenance(url, "USDT", interval_ms=HOUR), received_ms=received_ms(now_ms))
            store.set_meta(archive_key, now_ms)
            used_archive = bool(rows)
        elif result.status_code != 404:
            checked_response(result)
        else:
            store.set_meta(archive_key, now_ms)
    following = min(end, month_end if used_archive else cursor + 1000 * HOUR)
    if not used_archive:
        rest(cursor, following)
    # Empty intervals are revisited on the next scan, never filled with zeros.
    store.set_meta(scan_key, {'cursor':following, 'finished_ms':now_ms if following >= end else 0})
    return 30 if following < end else 3600


def sync_oi(store, symbol, now_ms):
    end = now_ms // HOUR * HOUR
    # Bound each request to fewer than 500 hourly observations. With a wider
    # interval the provider can return the newest 500 and omit its older part.
    # Rechecking the small retained window repairs holes and source revisions.
    start = end - 29 * DAY
    while start <= end:
        page_end = min(end, start + 499 * HOUR)
        raw = response(OI, params={"symbol": symbol, "period": "1h", "startTime": start,
                                   "endTime": page_end, "limit": 500}).json()
        rows = []
        for item in raw:
            if item["symbol"] != symbol or not start <= int(item['timestamp']) <= page_end:
                raise ValueError("oi_symbol_mismatch")
            rows.append({"observed_ms": int(item["timestamp"]), "quantity": number(item["sumOpenInterest"], 0),
                         "notional_value": number(item["sumOpenInterestValue"], 0)})
        store.put("derivative_oi", symbol, rows, provenance(OI, "USDT", interval_ms=HOUR,
                  quantity_unit="BTC" if symbol == "BTCUSDT" else "contracts"), received_ms=received_ms(now_ms))
        start = page_end + HOUR
    return 3600


def sync_funding(store, symbol, now_ms, *, hyperliquid=False):
    series, endpoint = ("hl_funding", HL) if hyperliquid else ("regular_funding", FUNDING)
    latest = store.rows(series, symbol, limit=1)
    start = max(now_ms - 180 * DAY, latest[-1]["observed_ms"] - 3 * DAY) if latest else now_ms - 180 * DAY
    # Pagination is persisted after each response; a restart resumes from data.
    for _ in range(3):
        raw = (response(endpoint, body={"type": "fundingHistory", "coin": symbol, "startTime": start,
                                       "endTime": now_ms}).json() if hyperliquid else
               response(endpoint, params={"symbol": symbol, "startTime": start, "endTime": now_ms, "limit": 1000}).json())
        rows, special = [], []
        for item in raw:
            stamp = int(item["time" if hyperliquid else "fundingTime"])
            row = {"observed_ms": stamp, "rate": number(item["fundingRate"])}
            kind = item.get("rateType", "Regular")
            if hyperliquid or kind == "Regular":
                rows.append(row)
            elif kind == "Special":
                special.append(row)
            else:
                raise ValueError("unknown_funding_type")
        store.put(series, symbol, rows, provenance(endpoint, "fraction"), received_ms=received_ms(now_ms))
        store.put("dividend_adjustments", symbol, special, provenance(endpoint, "fraction"), received_ms=received_ms(now_ms))
        if not raw:
            return 3600
        last = max(int(r["time" if hyperliquid else "fundingTime"]) for r in raw)
        if last < start:
            raise ValueError("funding_cursor_did_not_advance")
        start = last + 1
        if len(raw) < (500 if hyperliquid else 1000):
            return 3600
    return 30


def sync_stablecoins(store, now_ms):
    raw = response(STABLE).json()
    rows = []
    for item in raw:
        stamp = int(item["date"]) * 1000
        if not now_ms - 3 * 366 * DAY <= stamp <= now_ms:
            continue
        supply = (item.get("totalCirculating") or {}).get("peggedUSD")
        market_value = (item.get("totalCirculatingUSD") or {}).get("peggedUSD")
        if supply is not None:
            rows.append({"observed_ms": stamp, "supply": number(supply, 0),
                         "market_value_usd": number(market_value, 0) if market_value is not None else None})
    if not rows:
        raise ValueError("stablecoin_history_missing")
    store.put("stablecoin_supply", "peggedUSD", rows,
              provenance(STABLE, "USD_peg_units", time_basis="observation_date"), received_ms=received_ms(now_ms))
    return 21600


def sync_volatility(store, symbol, now_ms):
    endpoint = VIX if symbol == "VIX" else VXN
    start = datetime.fromtimestamp((now_ms - 3 * 366 * DAY) / 1000, timezone.utc).date().isoformat()
    raw = response(endpoint, params={"id": "VXNCLS", "cosd": start} if symbol == "VXN" else None)
    rows = []
    for item in csv.DictReader(io.StringIO(raw.text)):
        day = item.get("DATE") or item.get("observation_date")
        value = item.get("CLOSE") if symbol == "VIX" else item.get("VXNCLS")
        if not day or value in (".", "", None):
            continue
        parsed = datetime.strptime(day, "%m/%d/%Y" if "/" in day else "%Y-%m-%d").replace(tzinfo=timezone.utc)
        if parsed.date().isoformat() >= start:
            rows.append({"observed_ms": int(parsed.timestamp() * 1000), "date": parsed.date().isoformat(),
                         "value": number(value, 0)})
    if not rows:
        raise ValueError("volatility_history_missing")
    store.put("volatility", symbol, rows, provenance(endpoint, "index_points", time_basis="observation_date"), received_ms=received_ms(now_ms))
    return 21600


def sync_cftc(store, now_ms):
    start = datetime.fromtimestamp((now_ms - 3 * 366 * DAY) / 1000, timezone.utc).date().isoformat()
    raw = response(CFTC, params={"$where": "cftc_contract_market_code in('13874A','209742') AND report_date_as_yyyy_mm_dd >= '" + start + "T00:00:00'",
               "$order": "report_date_as_yyyy_mm_dd ASC", "$limit": 2000}).json()
    groups = {"SP500": [], "NASDAQ100": []}
    for item in raw:
        name = {"13874A": "SP500", "209742": "NASDAQ100"}.get(item["cftc_contract_market_code"])
        if name is None:
            raise ValueError("cftc_contract_mismatch")
        day = item["report_date_as_yyyy_mm_dd"][:10]
        stamp = int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp() * 1000)
        oi = number(item["open_interest_all"], 0)
        groups[name].append({"observed_ms": stamp, "date": day, "open_interest": oi,
            "asset_manager_net": number(item["asset_mgr_positions_long"]) - number(item["asset_mgr_positions_short"]),
            "leveraged_net": number(item["lev_money_positions_long"]) - number(item["lev_money_positions_short"])})
    for symbol, rows in groups.items():
        if not rows:
            raise ValueError("cftc_history_missing")
        store.put("cftc", symbol, rows, provenance(CFTC, "contracts", time_basis="Tuesday_observation_not_publication",
                  publication_time_missing=True), received_ms=received_ms(now_ms))
    return 21600


def archive_hyperliquid(ctx, received_ms, path=None):
    # AssetCtx has no source observation timestamp. Keep received time as such.
    row = {"observed_ms": int(received_ms), "item_key": str(int(received_ms) // 60000),
           "source_as_of_ms": None, "oi": number(ctx["openInterest"], 0),
           "mark_price": number(ctx["markPx"], 0), "funding": number(ctx["funding"]) if ctx.get("funding") is not None else None,
           "day_volume": number(ctx["dayNtlVlm"], 0) if ctx.get("dayNtlVlm") is not None else None}
    with closing(HistoryStore(path or history_path())) as store:
        store.put("hl_context", "BTC", [row], provenance(HL, "BTC", price_unit="USD/BTC", volume_unit="USD",
                  time_basis="retrieved_at", source_time_missing=True), received_ms=int(received_ms))


def _jobs():
    jobs = {"spot_BTCUSDT": lambda s, n: sync_klines(s, "BTCUSDT", spot=True, now_ms=n),
            "stablecoins": sync_stablecoins, "VIX": lambda s, n: sync_volatility(s, "VIX", n),
            "VXN": lambda s, n: sync_volatility(s, "VXN", n), "CFTC": sync_cftc,
            "hl_funding": lambda s, n: sync_funding(s, "BTC", n, hyperliquid=True)}
    for kind in seller_exhaustion.INPUTS:
        jobs["seller_input_" + kind] = lambda s, n, kind=kind: seller_exhaustion.sync(s, kind, n)
    for symbol in ("BTCUSDT", "QQQUSDT", "SPYUSDT"):
        jobs["futures_" + symbol] = lambda s, n, symbol=symbol: sync_klines(s, symbol, now_ms=n)
        jobs["oi_" + symbol] = lambda s, n, symbol=symbol: sync_oi(s, symbol, n)
        jobs["funding_" + symbol] = lambda s, n, symbol=symbol: sync_funding(s, symbol, n)
    return jobs


def run_once(path, *, now_ms=None, jobs=None, log=lambda _: None):
    now_ms = int(time.time() * 1000 if now_ms is None else now_ms)
    with closing(HistoryStore(path)) as store:
        for name, job in (_jobs() if jobs is None else jobs).items():
            state = store.get_meta("job:" + name, {})
            if state.get("next_ms", 0) > now_ms:
                continue
            try:
                delay = job(store, now_ms)
                state = {"failures": 0, "last_success_ms": now_ms, "next_ms": now_ms + delay * 1000}
                store.health_sample(name, now_ms, True)
            except Exception as exc:
                failures = min(16, state.get("failures", 0) + 1)
                delay = exc.seconds if isinstance(exc, (RetryLater, seller_exhaustion.SourceUnavailable)) else min(1800, 15 * 2 ** failures)
                state = {**state, "failures": failures, "last_error": type(exc).__name__,
                         "next_ms": now_ms + int(delay * 1000)}
                log(f"public history {name}: {type(exc).__name__}")
                store.health_sample(name, now_ms, False, type(exc).__name__)
            store.set_meta("job:" + name, state)


def start_history_worker(path, log=lambda _: None):
    with _worker_lock:
        if path in _workers and _workers[path].is_alive():
            return _workers[path]
        def run():
            while True:
                try:
                    run_once(path, log=log)
                except Exception as exc:
                    log(f"public history storage: {type(exc).__name__}")
                time.sleep(15)
        thread = threading.Thread(target=run, name="market-history", daemon=True)
        _workers[path] = thread
        thread.start()
        return thread
