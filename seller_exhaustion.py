"""ARK-definition reconstruction from anonymous BGeometrics daily inputs.

Stdlib-only reader/derivation. HTTP is imported only by the history worker.
No provider key, account access, fixed buy threshold or strategy decision.
"""
import copy
import math
import statistics
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from market_history import DAY

NAME = 'seller_exhaustion_daily'
BASE = 'https://bitcoin-data.com/v1/'
INPUTS = {'price': ('btc-price', 'btcPrice', 'USD/BTC'),
          'profit': ('supply-profit', 'supplyProfitBtc', 'BTC'),
          'supply': ('supply-current', 'supplyCurrent', 'BTC')}
ENDPOINTS = [BASE + item[0] for item in INPUTS.values()]
METRICS = {NAME}
METHOD = 'ark_definition_bgeometrics_log_returns_30d_population_v1'
REASONS = {'access_denied', 'rate_limited', 'source_error', 'invalid_response',
           'history_not_initialized', 'empty_response'}


class SourceUnavailable(RuntimeError):
    def __init__(self, reason, seconds=21600):
        super().__init__(reason)
        self.reason, self.seconds = reason, seconds


def finite(value):
    return type(value) in (int, float) and math.isfinite(value)


def parse(raw, kind, now_ms):
    if not isinstance(raw, list) or len(raw) > 20000:
        raise ValueError('invalid_response')
    rows, seen = [], set()
    field = INPUTS[kind][1]
    for item in raw:
        if not isinstance(item, dict) or not isinstance(item.get('d'), str):
            raise ValueError('invalid_response')
        day = datetime.strptime(item['d'], '%Y-%m-%d').replace(tzinfo=timezone.utc)
        if day.strftime('%Y-%m-%d') != item['d']:
            raise ValueError('invalid_response')
        start = int(day.timestamp() * 1000)
        raw_stamp = item.get('unixTs')
        try:
            if type(raw_stamp) not in (str, int, float) or len(str(raw_stamp)) > 64:
                raise ValueError('invalid_response')
            stamp = Decimal(str(raw_stamp))
            if not stamp.is_finite() or stamp != Decimal(start // 1000) or start in seen or start < 0:
                raise ValueError('invalid_response')
        except InvalidOperation as exc:
            raise ValueError('invalid_response') from exc
        seen.add(start)
        if start + DAY > now_ms:
            continue
        value = item.get(field)
        if value is not None:
            if isinstance(value, bool):
                raise ValueError('invalid_response')
            value = float(value)
            if not finite(value) or value < 0 or kind != 'profit' and value == 0:
                raise ValueError('invalid_response')
        # Null supersedes a previously available point, never zero-fills it.
        rows.append({'observed_ms': start + DAY - 1, 'source_as_of_ms': start,
                     'period_start_ms': start, 'period_end_ms': start + DAY, 'value': value})
    return sorted(rows, key=lambda row: row['observed_ms'])


def sync(store, kind, now_ms):
    """Refresh the free four-year series twice daily (six requests/day total).

    Each input has an independent job and failure state. Never retry in the
    hourly report or use paid/private endpoints when the free source fails.
    """
    from btc_collection_io import get
    path, _, unit = INPUTS[kind]
    key = 'seller_input:' + kind
    def fail(reason, seconds=21600):
        store.set_meta(key, {'reason': reason, 'checked_ms': now_ms})
        raise SourceUnavailable(reason, seconds)
    try:
        result = get(BASE + path, timeout=(5, 20), allow_redirects=False)
        if result.status_code in (401, 403):
            fail('access_denied', 86400)
        if result.status_code == 429:
            try:
                seconds = max(21600, min(86400, int(result.headers.get('Retry-After', '21600'))))
            except (TypeError, ValueError):
                seconds = 21600
            fail('rate_limited', seconds)
        if result.status_code != 200:
            fail('source_error')
        rows = parse(result.json(), kind, now_ms)
    except SourceUnavailable:
        raise
    except (ValueError, TypeError, KeyError, OverflowError):
        fail('invalid_response')
    except Exception:
        fail('source_error')
    if not rows:
        fail('empty_response')
    import time
    received = max(now_ms, int(time.time() * 1000))
    store.put('seller_input_' + kind, 'BTC', rows, {'endpoint_url': BASE + path,
              'access_scope': 'public', 'unit': unit}, received_ms=received)
    store.set_meta(key, {'checked_ms': received, 'reason': None})
    return 43200


def derive(rows):
    """All daily calculations are rederived from retained raw inputs."""
    by_day = {r['period_start_ms']: r for r in rows}
    output = []
    for original in rows:
        row = copy.deepcopy(original)
        inputs = row['inputs']
        profit = (inputs.get('profit') or {}).get('value')
        supply = (inputs.get('supply') or {}).get('value')
        share = profit / supply if profit is not None and supply and 0 <= profit <= supply else None
        prices = [(by_day.get(row['period_start_ms'] - i*DAY, {}).get('inputs', {}).get('price') or {}).get('value')
                  for i in reversed(range(31))]
        vol = None
        if all(p is not None and p > 0 for p in prices):
            returns = [math.log(b)-math.log(a) for a, b in zip(prices, prices[1:])]
            vol = statistics.pstdev(returns)  # 30 returns, unannualized, ddof=0.
        row.update(supply_in_profit=share, volatility_30d=vol,
                   value=share*vol if share is not None and vol is not None else None)
        output.append(row)
    return output


def values(rows):
    usable = [r for r in rows if r.get('value') is not None]
    if not usable:
        return {}
    last = usable[-1]
    by_date = {r['observed_ms']: r['value'] for r in usable}
    result = {'latest': last['value'], 'as_of_ms': last['observed_ms'],
              'supply_in_profit': last['supply_in_profit'], 'volatility_30d': last['volatility_30d']}
    for days in (1, 7, 30):
        before = by_date.get(last['observed_ms'] - days*DAY)
        result['change_' + str(days) + 'd'] = last['value'] - before if before is not None else None
    return result


def measurements(store, now_ms):
    by_day, issues = {}, {}
    for kind, (path, _, unit) in INPUTS.items():
        raw = store.rows('seller_input_' + kind, 'BTC', start_ms=max(0, now_ms//DAY*DAY-397*DAY),
                         end_ms=now_ms, known_ms=now_ms) if store else []
        state = store.get_meta('seller_input:' + kind, {}) if store else {}
        reason = state.get('reason')
        if reason in REASONS:
            issues[kind] = reason
        elif not raw:
            issues[kind] = 'history_not_initialized'
        for source in raw:
            start = source['period_start_ms']
            row = by_day.setdefault(start, {'observed_ms': start+DAY-1,
                'period_start_ms': start, 'period_end_ms': start+DAY, 'inputs': {}})
            row['inputs'][kind] = {'value': source['value'], 'retrieved_at_ms': source['retrieved_at_ms'],
                'source_as_of_ms': source['source_as_of_ms'], 'provenance': source['provenance']}
    rows = derive([by_day[t] for t in sorted(by_day)])
    valid = values(rows)
    status = 'ok' if valid and not issues else 'partial' if valid else 'unavailable'
    if valid and now_ms-valid['as_of_ms'] > 2*DAY:
        status = 'stale'
    return {NAME: {'series': NAME, 'symbol': 'BTC', 'target_asset': 'BTC', 'relation': 'direct',
        'unit': 'fraction', 'analysis_eligible': False, 'method': METHOD, 'status': status,
        'input_issues': issues, 'rows': rows, 'values': valid, 'calculated_at_ms': now_ms,
        'as_of_ms': valid.get('as_of_ms')}}


def problems(item, cutoff):
    if not isinstance(item, dict):
        return ['invalid_dataset']
    if (tuple(item.get(k) for k in ('series', 'symbol', 'unit', 'relation', 'target_asset', 'method')) !=
            (NAME, 'BTC', 'fraction', 'direct', 'BTC', METHOD) or item.get('analysis_eligible') is not False):
        return ['instrument_or_unit_mismatch']
    rows = item.get('rows')
    if not isinstance(rows, list) or len(rows) > 400:
        return ['invalid_rows']
    calculation = item.get('calculated_at_ms', cutoff)
    if not finite(calculation) or not 0 <= calculation <= cutoff:
        return ['invalid_calculation_time']
    if not isinstance(item.get('input_issues', {}), dict):
        return ['invalid_input_issues']
    result, previous = [], -1
    for row in rows:
        if not isinstance(row, dict):
            return ['invalid_rows']
        start = row.get('period_start_ms')
        if (not finite(start) or start <= previous or start < 0 or start % DAY
                or row.get('period_end_ms') != start+DAY or row.get('observed_ms') != start+DAY-1
                or start+DAY > calculation):
            return ['invalid_closed_daily_bucket']
        previous = start
        inputs = row.get('inputs')
        if not isinstance(inputs, dict) or inputs.keys()-INPUTS.keys():
            return ['invalid_input_sources']
        for kind, raw in inputs.items():
            if not isinstance(raw, dict):
                return ['invalid_input_sources']
            val, known = raw.get('value'), raw.get('retrieved_at_ms')
            if (val is not None and (not finite(val) or val < 0 or kind != 'profit' and val == 0)
                    or not finite(known) or not start+DAY <= known <= calculation
                    or raw.get('source_as_of_ms') != start):
                return ['invalid_input_value_or_time']
            p = raw.get('provenance')
            if not isinstance(p, dict):
                return ['source_identity_mismatch']
            path, _, unit = INPUTS[kind]
            if tuple(p.get(k) for k in ('endpoint_url', 'access_scope', 'unit')) != (BASE+path, 'public', unit):
                return ['source_identity_mismatch']
        profit = (inputs.get('profit') or {}).get('value')
        supply = (inputs.get('supply') or {}).get('value')
        if profit is not None and supply is not None and profit > supply:
            result.append('supply_ratio_invalid')
    computed = derive(rows)
    for raw, expected in zip(rows, computed):
        for field in ('value', 'supply_in_profit', 'volatility_30d'):
            a, b = raw.get(field), expected[field]
            if b is None:
                if a is not None:
                    return ['daily_calculation_mismatch']
            elif not finite(a) or not math.isclose(a, b, rel_tol=1e-10, abs_tol=1e-14):
                return ['daily_calculation_mismatch']
    valid = values(computed)
    if not valid:
        result.append('collecting')
    elif calculation-valid['as_of_ms'] > 2*DAY:
        result.append('source_stale')
    if rows and any(r.get('value') is None for r in computed[30:]):
        result.append('source_partial')
    if rows and any(len(r['inputs']) < 3 or any(v['value'] is None for v in r['inputs'].values()) for r in rows[-31:]):
        result.append('source_partial')
    if item.get('status') not in ('ok', 'partial', 'collecting', 'stale', 'unavailable'):
        return ['invalid_dataset_status']
    if item.get('status') != 'ok':
        result.append('source_' + item['status'])
    return sorted(set(result))


def project(item, cutoff, reasons=None):
    reasons = problems(item, cutoff) if reasons is None else reasons
    invalid = any(p not in ('collecting', 'source_partial', 'source_collecting', 'source_stale', 'source_unavailable')
                  for p in reasons)
    rows = []
    for row in ([] if invalid else item.get('rows', [])):
        clean = {k: row[k] for k in ('observed_ms', 'period_start_ms', 'period_end_ms')}
        clean['inputs'] = {kind: {**{k: raw[k] for k in ('value', 'retrieved_at_ms', 'source_as_of_ms')},
                         'provenance': {k: raw['provenance'][k] for k in ('endpoint_url', 'access_scope', 'unit')}}
                           for kind, raw in row['inputs'].items()}
        rows.append(clean)
    rows = derive(rows)
    sources = {}
    for kind, (path, _, unit) in INPUTS.items():
        samples = [r['inputs'][kind] for r in rows if kind in r['inputs']]
        if not samples:
            continue
        sid = NAME + ':' + kind
        sources[sid] = {'source_id': sid, 'endpoint_url': BASE+path, 'access_scope': 'public',
                       'instrument': 'BTC', 'unit': unit, 'origin_family': 'BGeometrics',
                       'retrieved_at_ms': max(r['retrieved_at_ms'] for r in samples)}
    valid = values(rows)
    ds = {'rows': rows, 'values': valid, 'unit': 'fraction', 'instrument': 'BTC',
          'source_ids': list(sources), 'relation': 'direct', 'target_asset': 'BTC',
          'analysis_eligible': False, 'method': METHOD, 'official_equivalence': 'unverified',
          'origin_family': 'BGeometrics', 'time_basis': 'closed_utc_day', 'publication_time_unknown': True,
          'status': 'invalid' if invalid else 'stale' if 'source_stale' in reasons else
                    'partial' if 'source_partial' in reasons else item.get('status', 'unavailable'),
          'as_of_ms': valid.get('as_of_ms'), 'calculated_at_ms': item.get('calculated_at_ms', cutoff),
          'quality': {'reasons': reasons},
          'input_issues': {k:v for k,v in (item.get('input_issues') if isinstance(item.get('input_issues'), dict) else {}).items() if k in INPUTS and v in REASONS},
          'field_units': {'value': 'fraction', 'supply_in_profit': 'fraction',
                          'volatility_30d': 'daily_log_return_std_unannualized'}}
    return {'datasets': {NAME: ds}, 'sources': sources}


def from_projection(ds):
    return {'series': NAME, 'symbol': ds.get('instrument'), 'unit': ds.get('unit'),
            **{k:ds.get(k) for k in ('target_asset', 'relation', 'method', 'status', 'rows',
                                     'analysis_eligible', 'calculated_at_ms', 'input_issues')}}


def format_lines(payload):
    item = (payload.get('public_history') or {}).get(NAME)
    if not item:
        return []
    checked = project(item, item.get('calculated_at_ms', 0))['datasets'][NAME]
    v = checked['values']
    if not v:
        return ['卖竭常数（免费复算）：数据不足，需同日获利供应及连续31日价格']
    stamp = datetime.fromtimestamp(v['as_of_ms']/1000, timezone.utc).strftime('%Y-%m-%d')
    delta = v.get('change_7d')
    text = f"卖竭常数（免费复算）{v['latest']:.5g}｜{stamp} UTC｜7日变化 " + (f'{delta:+.5g}' if delta is not None else '数据不足')
    if checked['status'] == 'stale':
        text += '｜数据延迟，仅作历史背景'
    elif checked['quality']['reasons']:
        text += '｜部分数据受限'
    return [text]
