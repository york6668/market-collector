import copy
import json
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import Mock, patch

import btc_collection_io as io
import btc_push as bot
import market_assets as assets
import market_common as common
from btc_delivery import DeliveryFailure, DeliveryStore
from btc_evidence import build_bundle, encode_bundle
from btc_evidence_reader import EvidenceReader
import test_btc_push_rules as rule_tests
from test_market_monitor import NOW, payload as equity_payload


def macro_record():
    return {'status': 'ok', 'updated_at': NOW - 100, 'schema_version': bot.MACRO_CACHE_SCHEMA_REVISION,
            'source': 'Macro liquidity composite', 'publication_time_missing': True,
            'value': {'us10y': 4.2, 'real_yield_10y': 2., 'usd_index': 100.,
                      'source_date': {'us10y': '2026-09-29'}, 'history': [{'rows': [1, 2]}]}}


class CommonTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.now = [NOW]
        self.store = DeliveryStore(self.tmp.name + '/outbox.db', clock=lambda: self.now[0], retry_delay=10)
        self.addCleanup(self.store.close)
        self.addCleanup(io.clear_cache)
        io.clear_cache()
        patcher = patch.object(bot.news, 'fetch_events', return_value={'status': 'ok', 'events': []})
        self.news_fetch = patcher.start()
        self.addCleanup(patcher.stop)

    def test_equities_do_not_collect_or_consume_news(self):
        for asset in ('QQQ', 'VOO'):
            seen = {'旧新闻': NOW}
            state = {'news_seen': dict(seen)}
            value = equity_payload(asset, state=state)
            self.assertEqual([], value['news_titles'])
            self.assertEqual(seen, state['news_seen'])

    def test_concurrent_snapshot_survives_restart_and_does_not_share_mutable_objects(self):
        identity = (11, 'source', self.tmp.name)
        def fetch(seed):
            time.sleep(.02)
            return macro_record()
        fetch = Mock(side_effect=fetch)
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(lambda _: common.macro_snapshot(self.store, identity, {}, fetch, NOW), range(4)))
        fetch.assert_called_once()
        results[0]['value']['us10y'] = 999
        self.assertEqual(4.2, results[1]['value']['us10y'])
        io.clear_cache()
        reopened = DeliveryStore(self.store.path)
        try:
            result = common.macro_snapshot(reopened, identity, {}, Mock(side_effect=AssertionError), NOW + 20)
            self.assertEqual(4.2, result['value']['us10y'])
            self.assertEqual(NOW - 100, result['updated_at'])
        finally:
            reopened.close()

    def test_hour_and_source_changes_refresh_snapshot_and_preview_does_not_write(self):
        fetch = Mock(return_value=macro_record())
        for now, source in [(NOW, 'a'), (NOW + 3600, 'a'), (NOW + 3600, 'b')]:
            result = common.macro_snapshot(self.store, (source,), {}, fetch, now)
            self.assertEqual(common.hour_start(now), result['shared_hour'])
        self.assertEqual(3, fetch.call_count)
        before = list(self.store.db.iterdump())
        common.macro_snapshot(None, ('preview',), {}, fetch, NOW)
        self.assertEqual(before, list(self.store.db.iterdump()))

    def test_failed_refresh_keeps_shared_history_degraded_without_refreshing_source_time(self):
        old = macro_record()
        self.store.save_metadata('common:macro', {'identity': ['a'], 'hour': common.hour_start(NOW) - 3600, 'record': old})
        result = common.macro_snapshot(self.store, ('a',), {}, lambda _: {'reason': 'failed'}, NOW)
        self.assertEqual(old['value'], result['value'])
        self.assertEqual(old['updated_at'], result['updated_at'])
        self.assertFalse(bot.record_is_current(result))

    def test_initial_cache_seed_keeps_asset_files_unchanged(self):
        base = Path(self.tmp.name) / 'state.json'
        content = {'macro_cache': macro_record(), 'support_anchor': 123, 'snapshots': [{'keep': True}]}
        base.write_text(json.dumps(content))
        before = base.read_bytes()
        with patch.object(bot, 'STATE_FILE', str(base)), patch.object(bot.time, 'time', return_value=NOW):
            bot.initialize_common_macro(self.store)
        self.assertEqual(before, base.read_bytes())
        self.assertEqual(content['macro_cache'], self.store.load_metadata('common:macro')['record'])

    def test_common_schedule_is_hourly_and_expires_old_retry(self):
        saved = {'next_push': NOW - 100, 'retry_at': NOW - 10, 'retry_attempt': 1,
                 'report_id': common.report_id(NOW - 3600), 'market_open': True}
        schedule = common.CommonSchedule(saved, NOW)
        sent, expired = [], []
        kwargs = dict(send=lambda **kw: sent.append(kw['report_id']) or 'sent', pump=lambda: None,
                      expire=expired.append, save=lambda value: self.store.save_schedule('COMMON', value))
        schedule.tick(NOW, **kwargs)
        schedule.tick(NOW + 1, **kwargs)
        self.assertEqual([common.report_id(NOW)], sent)
        self.assertEqual([common.hour_start(NOW)] * 2, expired)
        restarted = common.CommonSchedule(self.store.load_schedule('COMMON'), NOW + 2)
        restarted.tick(NOW + 2, **kwargs)
        self.assertEqual(1, len(sent))

    def test_weekend_closes_stock_but_not_common(self):
        from test_market_monitor import stamp
        now = stamp('2026-10-03T12:00:00')
        self.assertFalse(assets.calendar_session(now)['open'])
        sent = Mock(return_value='sent')
        schedule = common.CommonSchedule(None, now, immediate=True)
        schedule.tick(now, send=sent, pump=lambda: None, expire=lambda _: None, save=lambda _: None)
        sent.assert_called_once()

    def patches(self, send):
        return (patch.object(bot, 'get_delivery_store', return_value=self.store),
                patch.object(bot.time, 'time', side_effect=lambda: self.now[0]),
                patch.object(bot, 'tg_send', send),
                patch.object(bot, 'fetch_macro_record', return_value={**macro_record(), 'shared_hour': common.hour_start(NOW)}))

    def test_common_once_and_query_do_not_touch_asset_state_or_recollect(self):
        send = Mock(side_effect=lambda _: {'message_id': 100 + send.call_count, 'date': int(self.now[0])})
        a, b, c, d = self.patches(send)
        with a, b, c, d as fetch, patch.object(bot, 'save_state', side_effect=AssertionError):
            self.assertEqual('sent', bot.send_common_push())
            self.assertEqual('sent', bot.send_common_push())
            fetch.assert_called_once()
            latest = self.store.load_metadata('common:latest_report')
            self.assertEqual(common.report_id(NOW), latest['report_id'])
            self.store.ingest_commands([{'update_id': 9, 'message': {'chat': {'id': 1}, 'text': '/macro'}}], 1, bot.parse_tg_command_text)
            self.assertEqual('sent', bot.safe_send_push(cmd=self.store.claim_command(), check_commands=False))
            self.assertEqual(2, send.call_count)
            self.assertIn('查询原报告', send.call_args.args[0])
            self.assertEqual(latest, self.store.load_metadata('common:latest_report'))
            fetch.assert_called_once()
        row = self.store.get_report('tg:9')
        self.assertEqual(('COMMON', 'skipped', 1), (row['asset_id'], row['document_status'], row['applied']))
        self.assertNotIn('COMMON', assets.INSTRUMENTS)
        self.assertIsNone(bot.parse_tg_command_text('/force COMMON'))

    def test_unknown_common_is_not_resent_and_other_assets_can_send(self):
        send = Mock(side_effect=DeliveryFailure('unknown'))
        a, b, c, d = self.patches(send)
        with a, b, c, d as fetch:
            self.assertEqual('unknown', bot.send_common_push())
            self.assertEqual('unknown', bot.send_common_push())
            send.assert_called_once(); fetch.assert_called_once()
            self.assertIsNone(self.store.load_metadata('common:latest_report'))
            for asset in assets.INSTRUMENTS:
                self.store.enqueue_report(asset + ':independent', asset, None, {'asset_id': asset})
                result = self.store.deliver_one(lambda _: {'message_id': 5, 'date': int(NOW)}, Mock(), asset_id=asset)
                self.assertEqual('sent', result['status'])

    def test_common_retry_only_resends_known_failure_without_recollecting(self):
        send = Mock(side_effect=[DeliveryFailure('connect_timeout'), {'message_id': 5, 'date': int(NOW + 11)}])
        a, b, c, d = self.patches(send)
        with a, b, c, d as fetch:
            self.assertEqual('queued', bot.send_common_push())
            self.now[0] += 11
            bot.pump_common_deliveries()
            self.assertEqual('sent', self.store.get_report(common.report_id(NOW))['summary_status'])
            self.assertEqual(2, send.call_count); fetch.assert_called_once()

    def test_common_old_pending_expires_but_unknown_is_preserved(self):
        old = common.report_id(NOW - 3600)
        self.store.enqueue_report(old, 'old', None, {'asset_id': 'COMMON'})
        self.store.enqueue_report(common.report_id(NOW), 'new', None, {'asset_id': 'COMMON'})
        self.store.expire_scheduled('COMMON', common.hour_start(NOW))
        # Even an old-hour report queued just after the boundary must expire.
        self.assertEqual('expired', self.store.get_report(old)['summary_status'])
        self.store.deliver_one(lambda _: (_ for _ in ()).throw(DeliveryFailure('unknown')), Mock(), asset_id='COMMON')
        self.store.expire_scheduled('COMMON')
        self.assertEqual('unknown', self.store.get_report(common.report_id(NOW))['summary_status'])

    @patch.object(bot.time, 'time', return_value=NOW)
    def test_asset_report_text_is_separate_and_macro_measurements_are_preserved(self, _clock):
        record = macro_record()
        common_text = bot.build_common_report(record, NOW)
        self.assertIn('公共宏观与事件简报', common_text)
        self.assertIn('US10Y', common_text)
        self.assertIn('2026-09-29', common_text)
        self.assertNotIn('比特币', common_text)
        for asset in assets.INSTRUMENTS:
            value = rule_tests.ShortReportPlainLanguageLayoutTest.payload() if asset == 'BTC' else equity_payload(asset)
            value['public_records']['macro'] = copy.deepcopy(record)
            before = copy.deepcopy(value)
            for build in (bot.build_report_message, bot.build_full_report_message):
                text = build('2026-09-30 15:00 JST', False, '2026-09-30', 1, value)
                self.assertIn('公共宏观与事件：', text)
                self.assertNotIn('宏观大环境', text)
                self.assertNotIn('US10Y', text)
                self.assertIn('市场结构', text)
            self.assertEqual(before, value)
            bundle = build_bundle(value, text, asset + ':test', '2026-09-30T14:05:00+00:00')
            self.assertIn('macro', bundle['datasets'])

    def test_interleaved_common_text_does_not_change_latest_asset_evidence(self):
        reader = EvidenceReader(self.tmp.name + '/reader.db', -1)
        try:
            messages, blobs = [], {}
            for index, asset in enumerate(assets.INSTRUMENTS):
                value = rule_tests.ShortReportPlainLanguageLayoutTest.payload() if asset == 'BTC' else equity_payload(asset)
                text = '📊 ' + asset + '｜辅助观察'
                mid = 10 * (index + 1)
                raw = encode_bundle(build_bundle(value, text, asset + ':evidence', '2026-09-30T14:05:00+00:00'))
                blobs[str(mid)] = raw
                messages += [{'message_id': mid, 'text': text},
                             {'message_id': mid + 1, 'text': '🏛 公共宏观简报'},
                             {'message_id': mid + 2, 'reply_to_message': {'message_id': mid},
                              'document': {'file_id': str(mid), 'file_name': asset.lower()+'-evidence.json', 'file_size': len(raw)}}]
            for message in messages:
                reader.ingest_update({'update_id': message['message_id'], 'channel_post': {
                    'date': int(NOW), 'chat': {'id': -1}, **message}})
            reader.process_pending(blobs.__getitem__)
            before = {asset: reader.observations(latest_only=True, asset_id=asset) for asset in assets.INSTRUMENTS}
            self.assertTrue(all(before.values()))
            reader.ingest_update({'update_id': 100, 'channel_post': {
                'message_id': 100, 'date': int(NOW), 'chat': {'id': -1}, 'text': '🏛 公共宏观简报 新一期'}})
            self.assertEqual(before, {asset: reader.observations(latest_only=True, asset_id=asset) for asset in assets.INSTRUMENTS})
        finally:
            reader.close()


if __name__ == '__main__':
    unittest.main()
