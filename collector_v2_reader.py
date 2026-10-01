"""Receiver compatibility for V2; no network, credentials or strategy authority."""
import copy
import re

import btc_evidence as btc
import market_evidence_v2 as v2
import market_reader


def _codes(values):
    return sorted({code for code in values if isinstance(code, str)
                   and re.fullmatch(r"[a-z][a-z0-9_]{0,80}", code)}) if isinstance(values, list) else []


def _stock_gaps(result, reasons, asset):
    spec = market_reader.ASSETS[asset]
    for name in market_reader.DATASET_SOURCES.keys() | {'macro'}:
        if name in result['datasets']:
            continue
        reasons.setdefault(name, ['empty_measurements'])
        stock = name.startswith('stock_')
        result['datasets'][name] = {
            'instrument': spec['price_symbol'] if stock else 'GLOBAL' if name == 'macro' else spec['derivatives_symbol'],
            'relation': 'direct' if stock else 'shared' if name == 'macro' else spec['derivatives_relation'],
            'source_ids': [], 'rows': [], 'values': {}, 'analysis_eligible': False,
            'status': 'unavailable', 'quality': {'reasons': reasons[name]}}


def auxiliary_observation(bundle, report_text):
    result = v2.auxiliary_observation(bundle, report_text)
    if result is None:
        return None
    reasons = {}
    for name, codes in result['validation']['dataset_reasons'].items():
        name = name[5:] if name.startswith('base:') else name
        reasons[name] = _codes(codes)
    if bundle['asset_id'] != 'BTC':
        validation, stock = market_reader.inspect(bundle, report_text, public_stock=True)
        if stock is None:
            return None
        for name in market_reader.DATASET_SOURCES.keys() | {'macro'}:
            result['datasets'].pop(name, None)
        result['datasets'].update(stock['datasets'])
        result['sources'].update(stock['sources'])
        result['not_applicable'] = stock['not_applicable']
        for name, codes in validation['dataset_reasons'].items():
            reasons[name] = sorted(set(reasons.get(name, []) + codes))
        _stock_gaps(result, reasons, bundle['asset_id'])
    else:
        # The V1 projector normalizes unknown dataset names and strips prose.
        legacy = btc.auxiliary_observation(bundle['base_evidence'], report_text)
        base_names = set(bundle['base_evidence']['datasets'])
        reasons = {name: codes for name, codes in reasons.items() if name not in base_names}
        reasons.update(legacy['validation']['dataset_reasons'])
    result['validation']['dataset_reasons'] = {name: codes for name, codes in reasons.items() if codes}
    result['validation']['status'] = 'degraded_auxiliary' if result['validation']['dataset_reasons'] else 'valid_auxiliary'
    result.update(schema=bundle['schema'], requires_independent_verification=True,
                  requires_current_source_check=True)
    return result


def sanitize_observation(item):
    """Recheck typed exported domains with the producer's own V2 arithmetic."""
    asset = item.get('asset_id')
    schema = v2.SCHEMAS[0 if asset == 'BTC' else 1]
    quality = item.get('validation') or {}
    generated = btc._aware_time(item.get('generated_at'))
    if (asset not in ('BTC', 'QQQ', 'VOO') or item.get('schema') != schema
            or item.get('role') != 'auxiliary_observation'
            or type(item.get('independent_verified_sources')) is not int
            or item['independent_verified_sources'] != 0
            or item.get('requires_independent_verification') is not True
            or item.get('requires_current_source_check') is not True
            or quality.get('status') not in ('valid_auxiliary', 'degraded_auxiliary')
            or quality.get('role') != 'auxiliary_observation'
            or type(quality.get('independent_verified_sources')) is not int
            or quality['independent_verified_sources'] != 0 or quality.get('reasons') != []
            or generated is None or not isinstance(item.get('datasets'), dict)
            or not isinstance(item.get('sources'), dict)
            or not isinstance(quality.get('dataset_reasons'), dict)):
        raise ValueError('invalid_v2_projection')
    cutoff = int(generated.timestamp() * 1000)
    base_names = set(btc._DATASET_SOURCE_IDS) if asset == 'BTC' else set(market_reader.DATASET_SOURCES) | {'macro'}
    known = base_names | v2.required_measurements(asset)
    reasons = {name: _codes(codes) for name, codes in quality['dataset_reasons'].items()
               if name in known or name in ('__bundle__', '__sources__', '__unsupported__')}
    reasons = {name: codes for name, codes in reasons.items() if codes}
    base_item = copy.deepcopy(item)
    base_item['datasets'] = {name: ds for name, ds in item['datasets'].items() if name in base_names}
    if asset != 'BTC':
        result = market_reader.sanitize_observation(base_item, public_stock=True)
        for name, codes in result['dataset_reasons'].items():
            reasons[name] = sorted(set(reasons.get(name, []) + codes))
    else:
        datasets, sources = {}, {}
        for name, ds in base_item['datasets'].items():
            refs = ds.get('source_ids') if isinstance(ds, dict) else None
            if (not isinstance(refs, list) or not refs
                    or any(not isinstance(sid, str) or not btc._source_identity(sid, item['sources'].get(sid, {})) for sid in refs)):
                reasons[name] = sorted(set(reasons.get(name, []) + ['source_contract_mismatch']))
                continue
            datasets[name] = btc._project_dataset(name, ds, item['sources'])
            sources.update({sid: btc._project_source(sid, item['sources'][sid]) for sid in refs})
        result = {'asset_id': asset, 'generated_at': item['generated_at'], 'datasets': datasets, 'sources': sources}
    for name in v2.required_measurements(asset):
        ds = item['datasets'].get(name)
        contract = v2._contract(name, asset)
        if contract is None:
            raw = v2.empty_measurement(name, asset, cutoff)
        elif not isinstance(ds, dict):
            raw = v2.empty_measurement(name, asset, cutoff)
            reasons[name] = sorted(set(reasons.get(name, []) + ['missing_dataset']))
        else:
            if ((ds.get('instrument'), ds.get('unit'), ds.get('relation'), ds.get('target_asset')) !=
                    (contract[1], contract[2], contract[3], asset)
                    or ds.get('analysis_eligible') is not False):
                raise ValueError('invalid_v2_measurement_identity')
            rows = ds.get('rows')
            if not isinstance(rows, list) or len(rows) > 10000:
                raise ValueError('invalid_v2_measurement_rows')
            raw = {'series': contract[0], 'symbol': contract[1], 'unit': contract[2], 'relation': contract[3],
                   'target_asset': asset, 'analysis_eligible': False,
                   'status': ds.get('status') if ds.get('status') in ('ok', 'partial', 'collecting', 'stale', 'unavailable', 'invalid') else 'invalid',
                   'rows': rows, 'calculated_at_ms': ds.get('calculated_at_ms', cutoff)}
            for key in ('windows', 'curve'):
                if key in ds:
                    raw[key] = ds[key]
        problems = sorted(set(v2._problems(name, raw, asset, cutoff) + reasons.get(name, [])))
        projected = v2.project_measurement(name, raw, asset, cutoff, problems)
        result['datasets'].update(projected['datasets'])
        result['sources'].update(projected['sources'])
        if problems:
            reasons[name] = problems
    if item['datasets'].keys() - known:
        reasons['__unsupported__'] = ['unsupported_dataset']
    if asset != 'BTC':
        _stock_gaps(result, reasons, asset)
    result.update(schema=schema, dataset_reasons=reasons, analysis_eligible=False)
    return result
