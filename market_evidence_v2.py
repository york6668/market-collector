"""V2 public-history contract; V1 remains readable through btc_evidence.

Validation checks identity, time, units and arithmetic, never independent truth.
Projection only exposes registered measurements, not arbitrary provider text.
"""
import copy
import hashlib
import math
import re
from urllib.parse import urlsplit

import btc_evidence as base
import seller_exhaustion
from market_history import DAY, HOUR, flow_summary
from market_observations import derived_values
from market_sources import SPOT, FUTURES, OI, FUNDING, HL, STABLE, VIX, VXN, CFTC, STOCK_WS, BYBIT_WS

SCHEMAS = ("BTC_COLLECTOR_EVIDENCE_V2", "MARKET_COLLECTOR_EVIDENCE_V2")
NUMBERS = set("observed_ms open_time close_time open high low close volume quote_volume taker_buy_quote quantity notional_value rate supply market_value_usd oi mark_price funding day_volume value open_interest asset_manager_net leveraged_net long_quantity short_quantity long_estimated_notional short_estimated_notional count retrieved_at_ms source_as_of_ms price previous_close vw".split())
NUMBERS.update(('last_event_retrieved_ms', 'period_start_ms', 'period_end_ms'))
STRINGS = {"date", "adjustment_mode"}
COMMON = {"derivative_cvd_long", "derivative_oi_long", "funding_long"}
CRYPTO = {"spot_cvd_long", "stablecoin_supply_long", "hyperliquid_oi_long", "hyperliquid_funding_long", "bybit_liquidation_long"}
EQUITY = {"stock_1h_public", "stock_5m_public", "stock_price_public", "equity_volatility", "equity_positioning"}
GAPS = {"equity_etf": "public_stable_source_missing", "equity_spot_cvd": "aggressor_direction_missing"}


def required_measurements(asset):
    return COMMON | (CRYPTO if asset == "BTC" else EQUITY | GAPS.keys())


def optional_measurements(asset):
    return set(seller_exhaustion.METRICS) if asset == "BTC" else set()


def empty_measurement(name, asset, cutoff):
    from market_observations import _record
    contract = _contract(name, asset)
    if contract is None:
        return {"status": "unavailable", "reason": GAPS.get(name, "history_not_initialized"),
                "target_asset": asset, "rows": [], "values": {}, "analysis_eligible": False}
    series, symbol, unit, relation, _ = contract
    item = _record(series, symbol, [], unit, target=asset, relation=relation)
    item["calculated_at_ms"] = cutoff
    if series in ("spot_1h", "futures_1h"):
        item.update(flow_summary([], cutoff))
    elif series == "bybit_liquidation":
        end = cutoff // HOUR * HOUR
        item["windows"] = {f"{days}d": {"status": "collecting", "coverage": {
            "start_ms": end-days*DAY, "end_ms": end, "covered_ms": 0, "complete": False,
            "gaps": [[end-days*DAY, end]]}, "observed": {key: 0 for key in (
                "long_quantity", "short_quantity", "long_estimated_notional", "short_estimated_notional", "count")}}
            for days in (1, 7, 14)}
    return item


def _contract(name, asset):
    future = {"BTC": "BTCUSDT", "QQQ": "QQQUSDT", "VOO": "SPYUSDT"}[asset]
    derivative_relation = "proxy" if asset == "VOO" else "direct"
    contracts = {
        "derivative_cvd_long": ("futures_1h", future, "USDT", derivative_relation, [FUTURES]),
        "derivative_oi_long": ("derivative_oi", future, "USDT", derivative_relation, [OI]),
        "funding_long": ("regular_funding", future, "fraction", derivative_relation, [FUNDING])}
    if asset == "BTC":
        contracts.update({
            "spot_cvd_long": ("spot_1h", "BTCUSDT", "USDT", "direct", [SPOT]),
            "stablecoin_supply_long": ("stablecoin_supply", "peggedUSD", "USD_peg_units", "direct", [STABLE]),
            "hyperliquid_oi_long": ("hl_context", "BTC", "BTC", "direct", [HL]),
            "hyperliquid_funding_long": ("hl_funding", "BTC", "fraction", "direct", [HL]),
            "bybit_liquidation_long": ("bybit_liquidation", "BTCUSDT", "USDT", "direct", [BYBIT_WS])})
    else:
        contracts.update({
            "stock_1h_public": ("stock_1h", asset, "USD/share", "direct", [STOCK_WS]),
            "stock_5m_public": ("stock_5m", asset, "USD/share", "direct", [STOCK_WS]),
            "stock_price_public": ("stock_price", asset, "USD/share", "direct", [STOCK_WS]),
            "equity_volatility": ("volatility", "VIX" if asset == "VOO" else "VXN", "index_points", "reference", [VIX if asset == "VOO" else VXN]),
            "equity_positioning": ("cftc", "SP500" if asset == "VOO" else "NASDAQ100", "contracts", "reference", [CFTC])})
    if asset == "BTC":
        contracts[seller_exhaustion.NAME] = (seller_exhaustion.NAME, "BTC", "fraction", "direct", seller_exhaustion.ENDPOINTS)
    return contracts.get(name)


def _endpoint_valid(url, contract):
    if url in contract[4]:
        return True
    p = urlsplit(url or "")
    prefix = "/data/spot/monthly/klines/" if contract[0] == "spot_1h" else "/data/futures/um/monthly/klines/"
    return (contract[0] in ("spot_1h", "futures_1h") and p.scheme == "https" and p.netloc == "data.binance.vision"
            and p.path.startswith(prefix + contract[1] + "/1h/" + contract[1] + "-1h-") and p.path.endswith(".zip")
            and not p.query and not p.fragment and "/../" not in p.path)


def _rows(rows):
    return [{k: copy.deepcopy(v) for k, v in row.items() if k in NUMBERS or k in STRINGS or k == "provenance"}
            for row in rows]


def build_bundle(payload, report_text, report_id, generated_at, full_report_text=None):
    asset = (payload.get("instrument") or {}).get("asset_id", "BTC")
    cutoff = int(base._aware_time(generated_at).timestamp()*1000)
    measurements = copy.deepcopy(payload.get("public_history") or {})
    for name in required_measurements(asset):
        measurements.setdefault(name, empty_measurement(name, asset, cutoff))
    if asset != "BTC":
        from market_observations import _record
        measurements['stock_1h_public'] = _record('stock_1h', asset,
            list(payload.get('raw_signal_bars') or [])[-336:], 'USD/share', target=asset)
        market = payload.get('market') or {}
        source_time = market.get('quote_source_as_of_ms')
        retrieved = market.get('quote_retrieved_at')
        rows = []
        if market.get('price') is not None and retrieved is not None:
            rows = [{'observed_ms': source_time if source_time is not None else int(retrieved*1000),
                'price': market['price'], 'source_as_of_ms': source_time, 'retrieved_at_ms': int(retrieved*1000),
                'provenance': {'endpoint_url': market.get('quote_endpoint_url'), 'access_scope': 'public', 'unit': 'USD/share'}}]
        measurements['stock_price_public'] = _record('stock_price', asset, rows, 'USD/share', target=asset)
        measurements['stock_price_public']['as_of_ms'] = source_time
        measurements['stock_price_public']['time_basis'] = 'source_observation' if source_time is not None else 'received_at'
    for name, item in measurements.items():
        if name != seller_exhaustion.NAME:
            item['rows'] = _rows(item.get('rows') or [])
    body = {'schema': SCHEMAS[0 if asset == 'BTC' else 1], 'asset_id': asset,
            'role': 'auxiliary_observation', 'report_id': str(report_id), 'generated_at': generated_at,
            'text_sha256': base.text_hash(report_text), 'measurements': measurements,
            'base_evidence': base._build_btc_bundle(payload, report_text, report_id, generated_at, full_report_text) if asset == 'BTC' else None,
            'full_report_text': full_report_text}
    if asset != 'BTC':
        import market_evidence
        stock = market_evidence.build_bundle(payload, report_text, report_id, generated_at,
                                             full_report_text, public_stock=True)
        body.update({k: v for k, v in stock.items() if k not in ('schema', 'bundle_id')})
        body.pop('base_evidence', None)
    body['bundle_id'] = 'market-v2:' + hashlib.sha256(base.canonical_bytes(body)).hexdigest()
    return body


def _number(value):
    return type(value) in (float, int) and math.isfinite(value)


def _problems(name, item, asset, cutoff):
    if name == seller_exhaustion.NAME and asset == "BTC":
        return seller_exhaustion.problems(item, cutoff)
    contract = _contract(name, asset)
    if contract is None:
        return ['source_unavailable' if name in ('equity_etf','equity_spot_cvd','history') else 'unsupported_dataset']
    if not isinstance(item, dict):
        return ['invalid_dataset']
    if tuple(item.get(k) for k in ('series','symbol','unit','relation')) != contract[:4] or item.get('target_asset') != asset:
        return ['instrument_or_unit_mismatch']
    problems, rows = [], item.get('rows')
    calculation = item.get('calculated_at_ms', cutoff)
    if not _number(calculation) or not 0 <= calculation <= cutoff:
        return ['invalid_calculation_time']
    cutoff = calculation
    if not isinstance(rows, list):
        return ['invalid_rows']
    stamps = [r.get('observed_ms') for r in rows if isinstance(r,dict)]
    if len(stamps) != len(rows) or any(not _number(t) for t in stamps) or stamps != sorted(set(stamps)):
        return ['invalid_row_order']
    for row in rows:
        if not isinstance(row, dict):
            return ['invalid_row']
        if any(v is not None and not _number(v) for k,v in row.items() if k in NUMBERS):
            return ['non_numeric_observation']
        observed, known = row.get('observed_ms'), row.get('retrieved_at_ms')
        if not _number(observed) or not _number(known) or not 0 <= observed <= known <= cutoff:
            problems.append('invalid_observation_time')
        provenance = row.get('provenance') or {}
        if not _endpoint_valid(provenance.get('endpoint_url'), contract) or provenance.get('access_scope') != 'public' or provenance.get('unit') != contract[2]:
            problems.append('source_identity_mismatch')
        if contract[0] in ('stock_1h', 'stock_5m') and row.get('adjustment_mode') != 'UNKNOWN':
            problems.append('stock_adjustment_claim_invalid')
        required = {'derivative_oi': ('quantity','notional_value'), 'regular_funding': ('rate',),
            'hl_funding': ('rate',), 'hl_context': ('oi','mark_price'),
            'stablecoin_supply': ('supply',), 'volatility': ('value',),
            'cftc': ('asset_manager_net','leveraged_net','open_interest'), 'stock_price': ('price',)}.get(contract[0], ())
        if any(not _number(row.get(key)) for key in required):
            problems.append('missing_measurement')
        if contract[0] == 'stock_price' and (row.get('source_as_of_ms') is not None and
                row.get('source_as_of_ms') != observed):
            problems.append('invalid_source_time')
        if contract[0] == 'hl_context' and row.get('source_as_of_ms') is not None:
            problems.append('invalid_source_time')
    if item.get('analysis_eligible') is not False:
        problems.append('unexpected_strategy_eligibility')
    if contract[0] in ('spot_1h','futures_1h') and rows:
        try:
            stamps = [r['open_time'] for r in rows]
            if stamps != sorted(set(stamps)) or any(r['close_time'] != r['open_time']+HOUR-1 or
                not 0 <= r['taker_buy_quote'] <= r['quote_volume'] for r in rows):
                problems.append('invalid_flow_rows')
            expected = flow_summary(rows, cutoff)
            if item.get('windows') != expected['windows'] or item.get('curve') != expected['curve']:
                problems.append('flow_calculation_mismatch')
        except (KeyError, TypeError, ValueError):
            problems.append('invalid_flow_rows')
    if contract[0] in ('stock_1h','stock_5m','spot_1h','futures_1h'):
        for row in rows:
            try:
                if not 0 < row['low'] <= min(row['open'],row['close']) <= max(row['open'],row['close']) <= row['high'] or row['volume'] < 0:
                    problems.append('invalid_ohlcv')
                if contract[0].startswith('stock_'):
                    interval = HOUR if contract[0] == 'stock_1h' else 300000
                    if (row['open_time'] % interval or row['close_time'] != row['open_time'] + interval - 1
                            or row['observed_ms'] != row['close_time'] or row['close_time'] >= row['retrieved_at_ms']):
                        problems.append('invalid_closed_bar')
            except (KeyError,TypeError):
                problems.append('invalid_ohlcv')
    if not rows and contract[0] != 'bybit_liquidation':
        problems.append('collecting')
    if contract[0] == 'bybit_liquidation':
        end = cutoff // HOUR * HOUR
        for days in (1,7,14):
            window = (item.get('windows') or {}).get(f'{days}d') or {}
            coverage = window.get('coverage') or {}
            gaps = coverage.get('gaps')
            if (coverage.get('start_ms'),coverage.get('end_ms')) != (end-days*DAY,end) or not isinstance(gaps,list):
                problems.append('invalid_coverage')
                continue
            cursor, missing = end-days*DAY, 0
            for gap in gaps:
                if not isinstance(gap,list) or len(gap) != 2 or not all(_number(t) for t in gap):
                    problems.append('invalid_coverage')
                    break
                a,b = gap
                if not cursor <= a < b <= end:
                    problems.append('invalid_coverage')
                missing += b-a
                cursor=b
            if coverage.get('covered_ms') != days*DAY-missing or coverage.get('complete') != (not gaps):
                problems.append('invalid_coverage')
            expected_status = 'ok' if not gaps else 'partial' if coverage.get('covered_ms') else 'collecting'
            if window.get('status') != expected_status:
                problems.append('invalid_coverage')
            selected = [r for r in rows if end-days*DAY <= r['observed_ms'] < end]
            for key in ('long_quantity','short_quantity','long_estimated_notional','short_estimated_notional','count'):
                if any(not _number(r.get(key)) or r[key] < 0 for r in selected) or (window.get('observed') or {}).get(key) != sum(r.get(key,0) for r in selected):
                    problems.append('invalid_liquidation_total')
    if rows:
        age_limit = {'stablecoin_supply':2*DAY, 'volatility':5*DAY, 'cftc':12*DAY,
                     'regular_funding':12*HOUR, 'stock_price':60000}.get(contract[0],2*HOUR)
        # Equity closures are expected; price history is still ineligible until
        # adjustment is verified. Freshness never grants strategy eligibility.
        if contract[0] in ('stock_1h', 'stock_5m'):
            from market_assets import latest_expected_close
            if rows[-1]['observed_ms'] < latest_expected_close(cutoff):
                problems.append('source_stale')
        elif cutoff - rows[-1]['observed_ms'] > age_limit:
            problems.append('source_stale')
    if item.get('status') != 'ok':
        problems.append('source_' + str(item.get('status','missing')))
    return sorted(set(problems))


def validate_bundle(bundle, report_text=None):
    result = {'status': 'rejected', 'reasons': [], 'dataset_reasons': {},
              'role': 'auxiliary_observation', 'independent_verified_sources': 0}
    try:
        asset = bundle.get('asset_id')
        if asset not in ('BTC','QQQ','VOO') or bundle.get('schema') != SCHEMAS[0 if asset == 'BTC' else 1]:
            raise ValueError('invalid_schema_or_asset')
        body = {k:v for k,v in bundle.items() if k != 'bundle_id'}
        encoded = base.canonical_bytes(body)
        if len(encoded) > base.MAX_BYTES or bundle.get('bundle_id') != 'market-v2:' + hashlib.sha256(encoded).hexdigest():
            raise ValueError('bundle_hash_or_size_mismatch')
        generated = base._aware_time(bundle.get('generated_at'))
        if not generated or bundle.get('role') != 'auxiliary_observation' or not bundle.get('report_id'):
            raise ValueError('invalid_report_identity')
        if not re.fullmatch('[0-9a-f]{64}', str(bundle.get('text_sha256',''))):
            raise ValueError('invalid_text_hash')
        if report_text is not None and bundle.get('text_sha256') != base.text_hash(report_text):
            raise ValueError('text_hash_mismatch')
        measurements = bundle.get('measurements')
        if not isinstance(measurements, dict) or not measurements:
            raise ValueError('missing_measurements')
        for name in required_measurements(asset) | measurements.keys():
            if name not in measurements:
                result['dataset_reasons'][name] = ['missing_dataset']
                continue
            item = measurements[name]
            problems = _problems(name, item, asset, int(generated.timestamp()*1000))
            if problems:
                result['dataset_reasons'][name] = problems
        if asset == 'BTC':
            legacy = bundle.get('base_evidence')
            if not isinstance(legacy,dict) or legacy.get('schema') != base.SCHEMA:
                raise ValueError('invalid_base_evidence')
            if any(legacy.get(k) != bundle.get(k) for k in ('report_id','generated_at','text_sha256')):
                raise ValueError('base_identity_mismatch')
            check = base.validate_bundle(legacy, report_text)
            if check['status'] == 'rejected':
                raise ValueError('invalid_base_evidence')
            result['dataset_reasons'].update({'base:'+k:v for k,v in check['dataset_reasons'].items()})
        else:
            import market_evidence
            check = market_evidence.validate_bundle(bundle, report_text, public_stock=True)
            if check['status'] == 'rejected':
                raise ValueError('invalid_stock_evidence:' + ','.join(check['reasons']))
            result['dataset_reasons'].update(check['dataset_reasons'])
        result['status'] = 'pending_text' if report_text is None else 'degraded_auxiliary' if result['dataset_reasons'] else 'valid_auxiliary'
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError, RecursionError) as exc:
        result['reasons'].append(str(exc) if isinstance(exc,ValueError) else 'invalid_bundle_values')
    return result


def auxiliary_observation(bundle, report_text):
    validation = validate_bundle(bundle, report_text)
    if validation['status'] not in ('valid_auxiliary','degraded_auxiliary'):
        return None
    if bundle['asset_id'] == 'BTC':
        projected = base.auxiliary_observation(bundle['base_evidence'], report_text)
    else:
        import market_evidence
        projected = market_evidence.auxiliary_observation(bundle, report_text, public_stock=True)
    result = projected or {'datasets': {}, 'sources': {}}
    result = copy.deepcopy(result)
    cutoff = int(base._aware_time(bundle['generated_at']).timestamp()*1000)
    for name in required_measurements(bundle['asset_id']) | bundle['measurements'].keys():
        item = bundle['measurements'].get(name) or empty_measurement(name, bundle['asset_id'], cutoff)
        projected = project_measurement(name, item, bundle['asset_id'], cutoff,
            validation['dataset_reasons'].get(name, []))
        result['datasets'].update(projected['datasets'])
        result['sources'].update(projected['sources'])
    result.update(schema=bundle['schema'], role='auxiliary_observation', asset_id=bundle['asset_id'], bundle_id=bundle['bundle_id'],
                  report_id=bundle['report_id'], generated_at=bundle['generated_at'], validation=validation,
                  independent_verified_sources=0)
    return result


def project_measurement(name, item, asset, cutoff, problems=None):
    """Recompute one typed domain for both raw attachments and local exports."""
    if name == seller_exhaustion.NAME and asset == 'BTC':
        return seller_exhaustion.project(item, cutoff, problems)
    result = {'datasets': {}, 'sources': {}}
    if problems is None:
        problems = _problems(name, item, asset, cutoff)
    contract = _contract(name, asset)
    if contract is None:
        if name in GAPS or name == 'history':
            result['datasets'][name] = {'rows': [], 'values': {}, 'status': 'unavailable',
                'target_asset': asset, 'analysis_eligible': False,
                'quality': {'reasons': problems}, 'reason': GAPS.get(name, 'history_unavailable')}
        return result
    invalid = any(p not in ('collecting','source_partial','source_collecting','source_stale','source_unavailable') for p in problems)
    rows = [] if invalid else [{**{k:copy.deepcopy(v) for k,v in row.items() if k in NUMBERS
            or k == 'adjustment_mode' and v in ('UNKNOWN', 'ADJUSTED')
            or k == 'date' and isinstance(v, str) and re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', v)},
            'provenance': {k: row['provenance'][k] for k in ('endpoint_url', 'access_scope', 'unit')}}
            for row in item['rows']]
    refs = []
    if not invalid:
        for row in item['rows']:
            url = row['provenance']['endpoint_url']
            sid = name + ':' + hashlib.sha256(url.encode()).hexdigest()[:12]
            if sid not in refs:
                refs.append(sid)
                result['sources'][sid] = {'endpoint_url':url,'access_scope':'public',
                    'retrieved_at':base.iso_time(row['retrieved_at_ms']/1000),'source_id':sid,
                    'instrument':contract[1]}
    dataset = {'rows': rows, 'unit': contract[2], 'instrument': contract[1], 'source_ids':refs,
               'relation': contract[3], 'target_asset': asset, 'analysis_eligible': False,
               'status': 'invalid' if invalid else item.get('status', 'collecting'),
               'as_of_ms': rows[-1]['observed_ms'] if rows else None,
               'calculated_at_ms': item.get('calculated_at_ms', cutoff), 'quality': {'reasons': problems}}
    dataset['field_units'] = {
        'derivative_oi':{'quantity':'BTC' if contract[1]=='BTCUSDT' else 'contracts','notional_value':'USDT'},
        'hl_context':{'oi':'BTC','mark_price':'USD/BTC','funding':'fraction','day_volume':'USD'},
        'stablecoin_supply':{'supply':'USD_peg_units','market_value_usd':'USD'},
        'stock_1h':{'open':'USD/share','high':'USD/share','low':'USD/share','close':'USD/share','volume':'shares'},
        'bybit_liquidation':{'long_quantity':'BTC','short_quantity':'BTC',
            'long_estimated_notional':'USDT','short_estimated_notional':'USDT','count':'events'}
    }.get(contract[0], {})
    if contract[0] == 'stock_5m':
        dataset['field_units'] = {k: 'shares' if k == 'volume' else 'USD/share' for k in ('open','high','low','close','volume')}
    if contract[0] == 'stock_price':
        dataset['source_time_missing'] = not rows or rows[-1].get('source_as_of_ms') is None
        dataset['as_of_ms'] = rows[-1].get('source_as_of_ms') if rows else None
        dataset['time_basis'] = 'received_at' if dataset['source_time_missing'] else 'source_observation'
    end = item.get('calculated_at_ms') or cutoff
    if not invalid:
        if contract[0] in ('spot_1h','futures_1h'):
            dataset.update(flow_summary(rows,end))
        elif contract[0] in ('stablecoin_supply','hl_context','derivative_oi','volatility','cftc','regular_funding','hl_funding'):
            dataset['values']=derived_values(contract[0],rows,end)
            if contract[0] == 'regular_funding':
                dataset['coverage_basis']='observed_settlements_only_variable_interval'
            if contract[0] == 'hl_context':
                dataset['time_basis']='retrieved_at_source_timestamp_missing'
        elif contract[0] == 'bybit_liquidation':
            dataset['windows']={}
            for days in (1,7,14):
                window=item['windows'][f'{days}d']
                dataset['windows'][f'{days}d']={
                    'status':window['status'],
                    'coverage':{k:copy.deepcopy(window['coverage'][k]) for k in
                                ('start_ms','end_ms','covered_ms','complete','gaps')},
                    'observed':{k:window['observed'][k] for k in ('long_quantity','short_quantity',
                                'long_estimated_notional','short_estimated_notional','count')}}
            dataset['notional_basis']='bankruptcy_price_estimate'
    result['datasets'][name] = dataset
    return result
