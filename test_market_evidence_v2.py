"""Regression coverage for complete attachments, including closed-market delivery."""
import copy
import hashlib
import os
import tempfile
import unittest
from contextlib import closing
from unittest.mock import patch

import btc_evidence as evidence
import btc_push as bot
import market_assets as assets
import market_evidence
import market_evidence_v2 as v2
import market_history as history
import market_observations as observations
from market_sources import STOCK_WS, provenance
from btc_evidence_reader import EvidenceReader
from test_market_monitor import NOW, payload as equity_payload


class CompleteAttachmentTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = self.temp.name + '/history.sqlite3'
        for p in (patch.dict(os.environ, {'MARKET_HISTORY_DB': self.path, 'MARKET_EVIDENCE_V2_READER_READY': '0'}),
                  patch.object(bot, 'STATE_FILE', self.temp.name + '/state.json'),
                  patch.object(bot, 'BTC_PUSH_EVIDENCE_ENABLED', True),
                  patch.object(bot.time, 'time', return_value=NOW)):
            p.start(); self.addCleanup(p.stop)

    def payload(self, asset):
        value = equity_payload(asset)
        for row in value['raw_signal_bars']:
            row.update(adjustment_mode='UNKNOWN', endpoint_url=STOCK_WS, observed_ms=row['close_time'],
                       provenance=provenance(STOCK_WS, 'USD/share'))
        value['signal_data_quality'] = assets.stock_quality(value['raw_signal_bars'], NOW * 1000)
        value['market'].update(quote_endpoint_url=STOCK_WS, quote_source_as_of_ms=(NOW-1)*1000,
                              previous_close=101.)
        value['derivative_bars'] = [{'oi_timestamp': (NOW-1)*1000, 'oi': 100., 'oi_value': 12000.,
            'oi_retrieved_at_ms': NOW*1000, 'oi_endpoint_url': 'https://fapi.binance.com/futures/data/openInterestHist'}]
        end = int(NOW // 28800) * 28800000
        raw = [{'symbol': assets.INSTRUMENTS[asset].derivatives_symbol,
                'fundingTime': end-i*28800000, 'fundingRate': '.0001'} for i in range(31)]
        raw.append({'symbol': assets.INSTRUMENTS[asset].derivatives_symbol,
                    'fundingTime': end, 'fundingRate': '-.04', 'rateType': 'Special'})
        with assets.asset_scope(asset), patch.object(bot, 'binance_get', side_effect=[raw, [
                {'symbol': assets.INSTRUMENTS[asset].derivatives_symbol, 'fundingIntervalHours': 8}]]):
            value['public_records']['funding'] = bot.fetch_funding_record(assets.current().derivatives_symbol)
        value['public_records']['macro'] = {'status': 'ok', 'value': {'us10y': 4.2, 'history': [{
            'series_id': 'treasury_us10y', 'endpoint_url': 'https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml',
            'venue': 'US Treasury', 'retrieved_at': NOW-1, 'end_date': '2026-09-29', 'fields': ['us10y'],
            'rows': [{'date': '2026-09-29', 'value': 4.2}]}]}}
        value['public_records']['basis'] = {'status': 'ok', 'venue': 'Binance',
            'source': 'Binance premiumIndex', 'endpoint_url': 'https://fapi.binance.com/fapi/v1/premiumIndex',
            'source_as_of_ms': (NOW-1)*1000, 'retrieved_at': NOW, 'updated_at': NOW,
            'value': {'mark_price': 101., 'index_price': 100., 'basis_pct': 1.}}
        return value

    def bundle(self, asset, value=None):
        text = '📊 ' + asset + '｜辅助观察'
        value = value if value is not None else self.payload(asset)
        raw = bot.build_delivery_document(value, text, asset.lower()+':repair', NOW, text)
        self.assertIsNotNone(raw)
        return evidence.decode_bundle(raw), text

    def project(self, bundle, text):
        asset = bundle['asset_id']
        path = self.temp.name + '/reader-' + asset + '.sqlite3'
        parent = {'chat': {'id': 1}, 'message_id': 10, 'date': int(NOW), 'text': text}
        update = {'update_id': 1, 'message': {'chat': {'id': 1}, 'message_id': 11,
            'date': int(NOW), 'reply_to_message': parent,
            'document': {'file_id': asset, 'file_name': asset.lower()+'-evidence.json'}}}
        with closing(EvidenceReader(path, 1)) as reader:
            reader.ingest_update(update)
            reader.ingest_update(update)
            result = reader.process_pending(lambda _: evidence.encode_bundle(bundle))
            self.assertIn(result[0]['status'], ('valid_auxiliary', 'degraded_auxiliary'), result)
        with closing(EvidenceReader(path, 1)) as reader:
            records = reader.observations(latest_only=True, asset_id=asset)
            self.assertEqual(1, len(records))
            return records[0]

    def test_all_stock_domains_survive_encoding_reader_restart_and_projection(self):
        for asset in ('QQQ', 'VOO'):
            bundle, text = self.bundle(asset)
            self.assertEqual(market_evidence.STOCK_DATASETS, set(bundle['datasets']))
            for field in ('instrument', 'sources', 'quality', 'applicability', 'mechanical_observation'):
                self.assertIn(field, bundle)
            projected = self.project(bundle, text)
            self.assertTrue(market_evidence.STOCK_DATASETS <= projected['datasets'].keys())
            self.assertEqual(assets.INSTRUMENTS[asset].identity(), projected['instrument'])
            self.assertEqual(12000, projected['datasets']['derivative_oi']['rows'][0]['notional_value'])
            self.assertAlmostEqual(.03, projected['datasets']['funding']['values']['funding_24h_pct'])
            self.assertEqual(-.04, projected['datasets']['dividend_adjustments']['rows'][0]['rate'])
            self.assertEqual(4.2, projected['datasets']['macro']['rows'][0]['rows'][0]['value'])
            self.assertEqual(102, projected['datasets']['stock_quote']['values']['price'])
            self.assertEqual(1., projected['datasets']['basis']['values']['basis_pct'])
            self.assertEqual('UNKNOWN', projected['datasets']['stock_1h']['adjustment_mode'])
            self.assertTrue(projected['datasets']['stock_1h']['rows'])
            self.assertFalse(projected['datasets']['stock_1h']['analysis_eligible'])
            self.assertEqual('proxy' if asset == 'VOO' else 'direct', projected['datasets']['basis']['relation'])
            self.assertNotIn('mechanical_observation', projected)
            self.assertEqual(0, projected['independent_verified_sources'])

    def test_v2_is_default_for_all_assets_even_when_old_readiness_flag_is_off(self):
        for asset in ('BTC', 'QQQ', 'VOO'):
            bundle, text = self.bundle(asset, {'instrument': assets.INSTRUMENTS[asset].identity()})
            self.assertTrue(bundle['schema'].endswith('_V2'))
            projection = evidence.auxiliary_observation(bundle, text)
            self.assertIsNotNone(projection)
            self.assertTrue(v2.required_measurements(asset) <= projection['datasets'].keys())
            if asset != 'BTC':
                for name, reason in v2.GAPS.items():
                    self.assertEqual(reason, projection['datasets'][name]['reason'])
        with patch.object(bot, 'BTC_PUSH_EVIDENCE_ENABLED', False):
            self.assertIsNone(bot.build_delivery_document({}, '', 'disabled', NOW))

    def test_removed_measurement_is_reported_instead_of_disappearing(self):
        bundle, text = self.bundle('QQQ')
        del bundle['measurements']['equity_volatility']
        bundle['bundle_id'] = 'market-v2:' + hashlib.sha256(evidence.canonical_bytes(
            {k: v for k, v in bundle.items() if k != 'bundle_id'})).hexdigest()
        projected = evidence.auxiliary_observation(bundle, text)
        self.assertEqual(['missing_dataset'], projected['validation']['dataset_reasons']['equity_volatility'])
        self.assertEqual([], projected['datasets']['equity_volatility']['rows'])

    def test_unknown_adjustment_never_reaches_v1_and_missing_quote_time_is_preserved(self):
        value = self.payload('VOO')
        value['market']['quote_source_as_of_ms'] = None
        bundle, text = self.bundle('VOO', value)
        self.assertNotIn('base_evidence', bundle)
        projected = evidence.auxiliary_observation(bundle, text)
        self.assertIsNone(bundle['measurements']['stock_price_public']['as_of_ms'])
        self.assertIsNone(projected['datasets']['stock_price_public']['as_of_ms'])
        self.assertTrue(projected['datasets']['stock_quote']['source_time_missing'])

    def test_v2_does_not_accept_retired_rest_as_public_stock_source(self):
        value = self.payload('QQQ')
        value['market']['quote_endpoint_url'] = assets.EQUITY_BASE + 'v1/public/equity/market/get-info'
        bundle, text = self.bundle('QQQ', value)
        projected = evidence.auxiliary_observation(bundle, text)
        self.assertIn('source_contract_mismatch', projected['validation']['dataset_reasons']['stock_quote'])
        self.assertTrue(projected['datasets']['stock_quote']['measurement_rejected'])
        self.assertNotIn('stock_quote', projected['sources'])

    def test_closed_query_with_no_valid_price_report_always_enqueues_attachment(self):
        for asset in ('QQQ', 'VOO'):
            value = self.payload(asset)
            for state in ({}, {'price_history_origin': 'binance_public_ws', 'latest_observation_report': {
                    'payload': value, 'generated_at': NOW-3600}}):
                before = copy.deepcopy(state)
                report_id = asset + ':' + str(bool(state))
                with assets.asset_scope(asset), patch.object(bot, 'pump_pending_deliveries'):
                    bot.send_cached_equity_query(state, {'open': False, 'phase': 'CLOSED'}, report_id, None)
                    sent = bot.get_delivery_store().get_report(report_id)
                self.assertNotEqual('skipped', sent['document_status'])
                self.assertNotIn('日/周辅助观察', sent['text'])
                bundle = evidence.decode_bundle(sent['document'])
                self.assertNotEqual('rejected', evidence.validate_bundle(bundle, sent['text'])['status'])
                self.assertEqual(before, state)

    def test_long_window_observations_do_not_change_original_body(self):
        for asset in ('QQQ', 'VOO'):
            value = self.payload(asset)
            for full in (False, True):
                before = bot.build_equity_report('2026-09-30 10:05 JST', value, full=full)
                value['public_history'] = {'equity_positioning': {'status': 'ok', 'values': {'asset_manager_net': 987654321}}}
                after = bot.build_equity_report('2026-09-30 10:05 JST', value, full=full)
                self.assertEqual(before, after)
                self.assertNotIn('日/周辅助观察', after)
                self.assertNotIn('987654321', after)
        from test_btc_push_rules import ShortReportPlainLanguageLayoutTest
        value = ShortReportPlainLanguageLayoutTest.payload()
        for builder in (bot.build_report_message, bot.build_full_report_message):
            before = builder('2026-09-30 10:05 JST', False, '2026-09-30', 1, value)
            value['public_history'] = {'spot_cvd_long': {'status': 'ok', 'windows': {}}}
            after = builder('2026-09-30 10:05 JST', False, '2026-09-30', 1, value)
            self.assertEqual(before, after)
            self.assertNotIn('日/周辅助观察', after)

    def test_forced_ineligible_report_keeps_raw_observation_cache(self):
        value = self.payload('QQQ')
        state = {'price_history_origin': 'binance_public_ws'}
        with assets.asset_scope('QQQ'), patch.object(bot, 'load_state', return_value=state), \
             patch.object(assets, 'market_session', return_value={'open': True, 'phase': 'REGULAR'}), \
             patch.object(bot, 'collect_report_payload', return_value=value), patch.object(bot, 'pump_pending_deliveries'):
            bot.send_push(cmd='FORCE_PUSH:QQQ', check_commands=False, report_id='qqq:forced-fixture')
            sent = bot.get_delivery_store().get_report('qqq:forced-fixture')
        self.assertNotEqual('skipped', sent['document_status'])
        self.assertIn('latest_observation_report', state)
        self.assertNotIn('latest_valid_report', state)
        self.assertFalse(state['latest_observation_report']['payload']['signal_data_quality']['usable'])

    def test_stock_warmup_reason_is_explained_once_without_empty_signal_sections(self):
        value = self.payload('QQQ')
        for full in (False, True):
            text = bot.build_equity_report('2026-09-30 10:05 JST', value, full=full)
            self.assertEqual(1, text.count('股票历史与复权口径尚待确认'))
            for repeated in ('数据不足', '不适用：', '🎯 现在怎么看', '📌 市场结构'):
                self.assertNotIn(repeated, text)
            self.assertIn('QQQUSDT', text)
            self.assertIn('普通Funding', text)
            self.assertIn('Special', text)

    def test_closed_five_minute_rows_survive_reader(self):
        end = int(NOW*1000)//300000*300000
        row = {'observed_ms': end-1, 'open_time': end-300000, 'close_time': end-1,
               'open': 100., 'high': 102., 'low': 99., 'close': 101., 'volume': 0., 'adjustment_mode': 'UNKNOWN'}
        with closing(history.HistoryStore(self.path)) as store:
            store.put('stock_5m', 'QQQ', [row], provenance(STOCK_WS, 'USD/share'), received_ms=NOW*1000)
        value = self.payload('QQQ')
        value['public_history'] = observations.collect('QQQ', self.path, NOW*1000)
        bundle, text = self.bundle('QQQ', value)
        projected = self.project(bundle, text)
        self.assertEqual(0, projected['datasets']['stock_5m_public']['rows'][0]['volume'])


if __name__ == '__main__':
    unittest.main()
