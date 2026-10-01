"""Read-only long-window measurements. These never change trading rules."""
import os
import time
from contextlib import closing
from datetime import datetime, timezone

from market_history import DAY, HOUR, HistoryStore, flow_summary, history_path
from market_sources import BYBIT_WS, provenance

WINDOWS = (1, 7, 14)


def _record(series, symbol, rows, unit, *, target, relation="direct", max_age_ms=2 * HOUR):
    last = rows[-1] if rows else {}
    return {"series": series, "symbol": symbol, "target_asset": target, "relation": relation,
            "status": "ok" if rows else "collecting", "unit": unit, "rows": rows, "values": {},
            "as_of_ms": last.get("observed_ms"), "retrieved_at_ms": last.get("retrieved_at_ms"),
            "max_age_ms": max_age_ms, "source": last.get("provenance"), "analysis_eligible": False}


def _changes(rows, field, now_ms):
    if not rows:
        return {}
    last = rows[-1]
    values = {"latest": last.get(field)}
    for days in WINDOWS:
        target = last["observed_ms"] - days * DAY
        previous = [r for r in rows if target - HOUR <= r["observed_ms"] <= target]
        before = previous[-1].get(field) if previous else None
        after = last.get(field)
        values[f"change_{days}d"] = after - before if after is not None and before is not None else None
    return values


def derived_values(series, rows, now_ms):
    if series in ('stablecoin_supply', 'hl_context', 'derivative_oi'):
        return _changes(rows, {'stablecoin_supply':'supply', 'hl_context':'oi',
                              'derivative_oi':'notional_value'}[series], now_ms)
    if series in ('regular_funding', 'hl_funding'):
        values = {}
        for days in WINDOWS:
            selected = [r for r in rows if now_ms-days*DAY < r['observed_ms'] <= now_ms]
            complete = (len({r['observed_ms']//HOUR for r in selected}) == days*24
                        if series == 'hl_funding' else bool(rows) and rows[0]['observed_ms'] <= now_ms-days*DAY)
            values[f'observed_sum_{days}d'] = sum(r['rate'] for r in selected) if complete else None
        return values
    if series == 'volatility':
        return {'latest':rows[-1]['value'] if rows else None,
            **{f'change_{d}sessions':rows[-1]['value']-rows[-1-d]['value'] if len(rows)>d else None for d in (5,10)}}
    if series == 'cftc':
        values = {k:rows[-1][k] if rows else None for k in ('asset_manager_net','leveraged_net','open_interest')}
        values.update({k+'_weekly_change':rows[-1][k]-rows[-2][k] if len(rows)>1 and
            rows[-1]['observed_ms']-rows[-2]['observed_ms']==7*DAY else None for k in ('asset_manager_net','leveraged_net')})
        return values
    return {}


def collect(asset, path=None, now_ms=None):
    now_ms = int(time.time() * 1000 if now_ms is None else now_ms)
    path = path or history_path()
    symbol = {"BTC": "BTCUSDT", "QQQ": "QQQUSDT", "VOO": "SPYUSDT"}[asset]
    relation = "proxy" if asset == "VOO" else "direct"
    result = {}
    if not os.path.exists(path):
        return {"history": {"status": "collecting", "reason": "history_not_initialized", "rows": [], "values": {}}}
    with closing(HistoryStore(path, readonly=True)) as store:
        def get(series, instrument, days=15, limit=None):
            return store.rows(series, instrument, start_ms=now_ms - days * DAY,
                              end_ms=now_ms, known_ms=now_ms, limit=limit)
        for name, series, instrument in ([('spot_cvd_long', 'spot_1h', 'BTCUSDT')] if asset == 'BTC' else []) + [
                ('derivative_cvd_long', 'futures_1h', symbol)]:
            rows = get(series, instrument)
            item = _record(series, instrument, rows, "USDT", target=asset,
                           relation=relation if series == 'futures_1h' else 'direct')
            item.update(flow_summary(rows, now_ms))
            if any(w["status"] != "ok" for w in item["windows"].values()):
                item["status"] = "partial" if rows else "collecting"
            result[name] = item
        rows = get("derivative_oi", symbol)
        item = _record("derivative_oi", symbol, rows, "USDT", target=asset, relation=relation)
        item["values"] = derived_values('derivative_oi', rows, now_ms)
        item["comparison_basis"] = "endpoint_snapshots_not_directional_flow"
        result["derivative_oi_long"] = item
        rows = get("regular_funding", symbol)
        item = _record("regular_funding", symbol, rows, "fraction", target=asset, relation=relation, max_age_ms=12*HOUR)
        item["values"] = derived_values('regular_funding', rows, now_ms)
        item["coverage_basis"] = "observed_settlements_only_variable_interval"
        result["funding_long"] = item
        if asset == "BTC":
            rows = get("stablecoin_supply", "peggedUSD", days=40)
            item = _record("stablecoin_supply", "peggedUSD", rows, "USD_peg_units", target=asset, max_age_ms=2*DAY)
            item["values"] = derived_values('stablecoin_supply', rows, now_ms)
            item["basis"] = "circulating_supply_not_exchange_flow"
            result["stablecoin_supply_long"] = item
            raw = get("hl_context", "BTC")
            buckets = {r["observed_ms"] // HOUR: r for r in raw}
            rows = [buckets[k] for k in sorted(buckets)]
            item = _record("hl_context", "BTC", rows, "BTC", target=asset)
            item["values"] = derived_values('hl_context', rows, now_ms)
            item["time_basis"] = "retrieved_at_source_timestamp_missing"
            result["hyperliquid_oi_long"] = item
            rows = get("hl_funding", "BTC")
            item = _record("hl_funding", "BTC", rows, "fraction", target=asset)
            item["values"] = derived_values('hl_funding', rows, now_ms)
            result["hyperliquid_funding_long"] = item
            end = now_ms // HOUR * HOUR
            events = get("bybit_liquidation", "BTCUSDT")
            bins = {}
            for event in events:
                hour = event["observed_ms"] // HOUR * HOUR
                row = bins.setdefault(hour, {"observed_ms": hour + HOUR - 1, "long_quantity": 0.0,
                    "short_quantity": 0.0, "long_estimated_notional": 0.0, "short_estimated_notional": 0.0,
                    "count": 0, "provenance": event["provenance"], "retrieved_at_ms": now_ms,
                    "last_event_retrieved_ms": event["retrieved_at_ms"]})
                row[event["side"] + "_quantity"] += event["quantity"]
                row[event["side"] + "_estimated_notional"] += event["estimated_notional"]
                row["count"] += 1
                row['last_event_retrieved_ms'] = max(row['last_event_retrieved_ms'], event['retrieved_at_ms'])
            item = _record("bybit_liquidation", "BTCUSDT", [bins[k] for k in sorted(bins) if k < end],
                           "USDT", target=asset)
            item["windows"] = {}
            for days in WINDOWS:
                cover = store.coverage("bybit_liquidation", "BTCUSDT", end-days*DAY, end)
                selected = [r for r in item["rows"] if end-days*DAY <= r["observed_ms"] < end]
                values = {key: sum(r[key] for r in selected) for key in
                          ("long_quantity", "short_quantity", "long_estimated_notional", "short_estimated_notional", "count")}
                item["windows"][f"{days}d"] = {"coverage": cover, "observed": values,
                    "status": "ok" if cover['complete'] else "partial" if cover['covered_ms'] else "collecting"}
            item["status"] = item["windows"]["1d"]["status"]
            item["as_of_ms"] = end-1
            item["basis"] = "Bybit_BTCUSDT_bankruptcy_price_estimate_not_global_liquidations"
            item['time_basis'] = 'derived_hour_close_with_separate_event_retrieval_time'
            item["stream_health"] = store.get_meta("stream:bybit", {})
            item["source"] = provenance(BYBIT_WS, "USDT", notional_basis="bankruptcy_price_estimate")
            item["retrieved_at_ms"] = item["stream_health"].get("last_response_ms")
            result["bybit_liquidation_long"] = item
        else:
            rows = get("stock_5m", asset)
            result["stock_5m_public"] = _record("stock_5m", asset, rows, "USD/share", target=asset)
            vol_symbol = "VIX" if asset == "VOO" else "VXN"
            rows = get("volatility", vol_symbol, days=45)
            item = _record("volatility", vol_symbol, rows, "index_points", target=asset,
                           relation="reference", max_age_ms=5*DAY)
            item["values"] = derived_values('volatility', rows, now_ms)
            result["equity_volatility"] = item
            instrument = "SP500" if asset == "VOO" else "NASDAQ100"
            rows = get("cftc", instrument, days=45)
            item = _record("cftc", instrument, rows, "contracts", target=asset,
                           relation="reference", max_age_ms=12*DAY)
            item["values"] = derived_values('cftc', rows, now_ms)
            result["equity_positioning"] = item
            for key, reason in (("equity_etf", "public_stable_source_missing"), ("equity_spot_cvd", "aggressor_direction_missing")):
                result[key] = {"status": "unavailable", "reason": reason, "target_asset": asset,
                               "rows": [], "values": {}, "analysis_eligible": False}
        for item in result.values():
            item['calculated_at_ms'] = now_ms
            comparable = [value for key,value in item.get('values',{}).items()
                          if key.startswith(('change_', 'observed_sum_'))]
            if comparable and any(value is None for value in comparable):
                item['status'] = 'partial' if any(value is not None for value in comparable) else 'collecting'
            if item.get("as_of_ms") is not None and now_ms-item['as_of_ms'] > item.get('max_age_ms', 2*HOUR):
                item["status"] = "stale"
    return result


def report_lines(observations):
    labels = {"spot_cvd_long": "Binance现货净主动成交", "derivative_cvd_long": "合约净主动成交",
        "derivative_oi_long": "合约OI名义值变化", "funding_long": "普通Funding已观察结算和",
        "stablecoin_supply_long": "美元锚定稳定币供应量变化", "hyperliquid_oi_long": "Hyperliquid OI变化",
        "hyperliquid_funding_long": "Hyperliquid已结算Funding", "bybit_liquidation_long": "Bybit场内爆仓估算",
        "equity_volatility": "股票波动率", "equity_positioning": "CFTC股指期货仓位"}
    statuses = {"ok": "正常", "collecting": "积累中", "partial": "窗口不完整", "stale": "过期",
                "unavailable": "来源暂缺"}
    def fmt(value):
        return "待确认" if value is None else f"{value:+,.2f}"
    lines = ["", "📅 日/周辅助观察"]
    for name, item in observations.items():
        if name not in labels:
            continue
        stamp = item.get('as_of_ms')
        day = datetime.fromtimestamp(stamp/1000, timezone.utc).strftime('%m-%d') if stamp else 'NA'
        heading = f"{labels[name]} {item.get('symbol','')}（{statuses.get(item['status'],item['status'])}，截至{day}）"
        if name in ('spot_cvd_long','derivative_cvd_long'):
            values = [item['windows'][f'{d}d']['net_quote'] for d in WINDOWS]
            text = " / ".join(fmt(v) for v in values) + " USDT（1/7/14日）"
            if name == 'spot_cvd_long' and item.get('curve'):
                last = item['curve'][-1]
                origin = datetime.fromtimestamp(last['start_ms']/1000, timezone.utc).strftime('%m-%d %H:%M UTC')
                text += f"；累计{fmt(last['value'])}（起点{origin}）"
        elif name == 'bybit_liquidation_long':
            parts = []
            for d in (1, 7):
                window = item['windows'][f'{d}d']
                values = window['observed']
                if not window['coverage']['covered_ms']:
                    parts.append(f'{d}日暂无覆盖，无法汇总')
                    continue
                parts.append(f"{d}日已采集 多{fmt(values['long_estimated_notional'])}/空{fmt(values['short_estimated_notional'])} USDT，覆盖{window['coverage']['covered_ms']/HOUR:.1f}/{d*24}小时")
            text = '；'.join(parts) + '（破产价估算）'
        elif name == 'equity_volatility':
            values = item['values']
            text = f"{fmt(values.get('latest'))}；5/10交易日变化 {fmt(values.get('change_5sessions'))}/{fmt(values.get('change_10sessions'))}"
        elif name == 'equity_positioning':
            values = item['values']
            text = f"资管净持仓{fmt(values.get('asset_manager_net'))}（周变{fmt(values.get('asset_manager_net_weekly_change'))}）；杠杆资金{fmt(values.get('leveraged_net'))}（周变{fmt(values.get('leveraged_net_weekly_change'))}）张"
        else:
            values = item.get('values', {})
            amounts = [values.get(f'change_{d}d', values.get(f'observed_sum_{d}d')) for d in WINDOWS]
            if item.get('unit') == 'fraction':
                text = ' / '.join('待确认' if v is None else f'{v*100:+.4f}%' for v in amounts)
            else:
                text = ' / '.join(fmt(v) for v in amounts)
                text += ' ' + {'USD_peg_units':'美元锚定单位'}.get(item.get('unit'),item.get('unit',''))
            text += '（1/7/14日）'
        if item.get('relation') == 'proxy':
            text += '；SPY代理参考'
        lines.append(heading + "：" + text)
    if 'equity_etf' in observations:
        lines.append('ETF申赎/持仓：尚缺公开稳定来源；股票Spot CVD：缺少主动成交方向。')
    if len(lines) == 2:
        lines.append('历史数据正在积累。')
    return lines
