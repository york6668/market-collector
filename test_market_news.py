import copy
import json
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock, patch

import btc_collection_io as io
import btc_push as bot
import market_assets as assets
import market_common as common
import market_news as news
from btc_delivery import DeliveryFailure, DeliveryStore, ReceiptPersistenceError
from btc_presentation import utf16_length
from test_market_common import macro_record
from test_market_monitor import NOW, payload


def row(identity=1, title='美国公布9月CPI数据', **fields):
    return {'id': identity, 'title': title, 'content_text': title, 'type': 'live',
            'is_priced': False, 'display_time': NOW - identity, 'channels': ['us-stock-channel'],
            'uri': 'https://wallstreetcn.com/livenews/' + str(identity), **fields}


def events(count=1):
    rows = [row(i, f'公司{i}公布季度财报') for i in range(1, count + 1)]
    return {'status': 'ok', 'events': news.parse_events(rows, NOW)}


class EventSourceTest(unittest.TestCase):
    def tearDown(self):
        io.clear_cache()

    def test_real_recommendation_headlines_promotion_and_paid_content_are_excluded(self):
        # Titles observed in the retired feed; rows/flags below are synthetic.
        rejected = [
            '3只可能在10月IPO后纳入Anthropic的ETF',
            '这个市场信号在近90年的时间里准确率达到了95%。投资者现在需要做些什么来准备',
            '如果今年我只能在投资组合中添加1只ETF，这就是我会买的',
            '想要一个百万美元的投资组合？历史显示这个策略可以帮助你实现目标。',
            '在VOO接近历史高点时，是否比每股低于105美元的Lululemon股票更值得购买？',
            '该ETF在2026年上涨了20%。这是它可能有更多上涨空间的原因。',
            '扫码解锁：美联储宣布降息后的投资机会',
        ]
        rows = [row(i, title) for i, title in enumerate(rejected, 1)]
        rows += [row(20, is_priced=True), row(21, type='ad'),
                 row(22, content_text='美国公布CPI。点击领取投资课程'),
                 row(23, content_text='美国公布CPI。微信小程序了解更多。')]
        self.assertEqual([], news.parse_events(rows, NOW))

    def test_events_do_not_require_etf_names_and_headlineless_uses_first_paragraph(self):
        titles = ['美联储宣布降息25个基点', '美国8月PCE同比增长2.5%', '苹果公布第三季度财报',
                  '监管机构批准两家公司合并', '中东港口遇袭导致运输中断',
                  '谷歌公布季度财报，广告收入同比增长10%', '奈飞公布付费用户增长数据']
        rows = [row(i, title) for i, title in enumerate(titles, 1)]
        rows += [row(9, None, content_text='\n美国公布就业数据。\n第二段不作标题。')]
        actual = news.parse_events(rows, NOW)
        self.assertEqual(titles + ['美国公布就业数据。'], [e['title'] for e in actual])
        self.assertTrue(all(e['source'] == '华尔街见闻' for e in actual))

    def test_price_ticks_wrapups_forecasts_crypto_and_non_chinese_are_not_public_events(self):
        titles = ['标普500指数收跌0.17%', '美股期货走高，纳斯达克100指数期货上涨0.4%',
                  '早间要闻汇总：美国公布CPI数据', '提醒：日内请重点关注美联储利率决议',
                  'Counterpoint预测：苹果iPhone将售出600万部', 'SEC批准比特币ETF上市',
                  'Federal Reserve announces rate cut']
        self.assertEqual([], news.parse_events([row(i, t) for i, t in enumerate(titles, 1)], NOW))

    def test_source_time_required_and_cached_rows_are_rechecked_for_freshness(self):
        invalid = [None, True, '1790770000', float('nan'), float('inf'), NOW + 1, NOW - 86401]
        self.assertEqual([], news.parse_events([row(i, display_time=t) for i, t in enumerate(invalid, 1)], NOW))
        value = row(display_time=NOW - 86400)
        response = Mock(json=lambda: {'code': 20000, 'data': {'items': [value]}})
        with patch.object(io, 'get', return_value=response) as get:
            self.assertEqual(1, len(news.fetch_events(NOW)['events']))
            self.assertEqual([], news.fetch_events(NOW + 1)['events'])
            get.assert_called_once()

    def test_malformed_row_or_foreign_link_does_not_replace_original_source(self):
        rows = [None, row(uri='https://example.com/ads'), row(uri='https://wallstreetcn.com/livenews/2'),
                row(channels=['a-stock-channel']), row(is_priced=None), row(title=10),
                row(uri='https://[broken'), row(5, uri='https://wallstreetcn.com/livenews/5?ref=campaign')]
        value = news.parse_events(rows, NOW)
        self.assertEqual(['https://wallstreetcn.com/livenews/5'], [e['url'] for e in value])

    def test_concurrent_public_requests_share_five_minute_cache_with_source_key(self):
        def get(*args, **kwargs):
            time.sleep(.02)
            return Mock(json=lambda: {'code': 20000, 'data': {'items': [row()]}})
        with patch.object(io, 'get', side_effect=get) as fetch, patch.object(io.time, 'time', return_value=NOW):
            with ThreadPoolExecutor(3) as pool:
                values = list(pool.map(lambda _: news.fetch_events(NOW), range(3)))
            fetch.assert_called_once_with(news.EVENT_URL, params={'channel': 'us-stock-channel', 'client': 'pc', 'limit': 100}, timeout=(3, 8))
            values[0]['events'].clear()
            self.assertEqual(1, len(values[1]['events']))
        with patch.object(io, 'get', side_effect=get) as fetch, patch.object(io.time, 'time', return_value=NOW + 300):
            news.fetch_events(NOW + 300)
            fetch.assert_called_once()

    def test_failure_is_explicit_without_recommended_feed_fallback(self):
        for response in [TimeoutError(), {'code': 40000}, {'code': 20000, 'data': {'items': {}}}]:
            io.clear_cache()
            with patch.object(io, 'get', side_effect=response if isinstance(response, Exception) else None,
                              return_value=Mock(json=lambda: response)) as get:
                result = news.fetch_events(NOW)
                self.assertEqual('unavailable', result['status'])
                self.assertEqual([], result['events'])
                self.assertEqual([news.EVENT_URL], [call.args[0] for call in get.call_args_list])

    def test_btc_only_keeps_crypto_topics_and_existing_display_limit_and_dedupe(self):
        rejected = ['美联储宣布降息', '美元指数走高', '纳斯达克交易所公布新规则',
                    'ETF资金流入增加', '公司公布新合约', '半导体销量增长', '扫码领取比特币投资课程']
        accepted = ['比特币ETF公布持仓', '以太坊升级完成', 'Coinbase公布季度财报', '链上巨鲸转账']
        with patch.object(bot, 'fetch_blockbeats_titles', return_value=rejected + accepted), \
             patch.object(bot.time, 'time', return_value=NOW):
            actual = bot.get_chinese_news_titles()
            self.assertEqual(accepted, actual)
            state = {}
            self.assertEqual(3, len(bot.format_news_lines(bot.dedupe_news_titles(state, actual))))
            self.assertEqual([], bot.dedupe_news_titles(state, actual))


class EventDeliveryTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.now = [NOW]
        self.store = DeliveryStore(self.tmp.name + '/outbox.db', clock=lambda: self.now[0], retry_delay=10)
        self.addCleanup(self.store.close)
        self.send = Mock(side_effect=lambda _: {'message_id': 10 + self.send.call_count, 'date': int(self.now[0])})
        for target, name, kwargs in [
            (bot, 'get_delivery_store', {'return_value': self.store}),
            (bot.time, 'time', {'side_effect': lambda: self.now[0]}),
            (bot, 'tg_send', {'new': self.send}),
            (bot, 'fetch_macro_record', {'side_effect': lambda _: {**macro_record(), 'shared_hour': common.hour_start(self.now[0])}}),
            (news, 'fetch_events', {'return_value': events(4)}),
        ]:
            p = patch.object(target, name, **kwargs)
            value = p.start(); self.addCleanup(p.stop)
            if name == 'fetch_events': self.fetch = value

    def test_only_displayed_three_are_consumed_and_query_does_not_consume_or_fetch(self):
        self.assertEqual('sent', bot.send_common_push())
        first = self.send.call_args.args[0]
        self.assertEqual(3, first.count('https://wallstreetcn.com/livenews/'))
        self.assertEqual(6, len(self.store.load_metadata('common:news_seen')))
        self.assertNotIn('公司4', first)
        self.assertEqual('sent', bot.send_common_push(cmd='QUERY_PUSH:COMMON'))
        self.fetch.assert_called_once()
        self.now[0] += 3600
        bot.send_common_push()
        text = self.send.call_args.args[0]
        self.assertIn('公司4', text)
        self.assertNotIn('公司1', text)
        self.assertIn('来源显示时间', text)

    def test_unknown_survives_restart_and_is_not_selected_by_next_hour(self):
        self.send.side_effect = DeliveryFailure('unknown')
        self.assertEqual('unknown', bot.send_common_push())
        identity = common.report_id(NOW)
        self.assertIsNone(self.store.load_metadata('common:news_seen'))
        reopened = DeliveryStore(self.store.path, clock=lambda: self.now[0])
        try:
            reopened.recover()
            self.now[0] += 3600
            selected = common.select_events(reopened, events(4), self.now[0])
            self.assertEqual(['4'], [e['id'] for e in selected['events']])
            # Unknown is a delivery reservation, not a 48h confirmed-send cache.
            # A publisher refreshing the same story later cannot trigger a resend.
            refreshed = events(4)
            for event in refreshed['events']:
                event['display_time'] = NOW + 49 * 3600
            selected = common.select_events(reopened, refreshed, NOW + 49 * 3600)
            self.assertEqual(['4'], [e['id'] for e in selected['events']])
            with patch.object(bot, 'get_delivery_store', return_value=reopened):
                self.assertEqual('unknown', bot.send_common_push(report_id=identity))
            self.send.assert_called_once()
        finally:
            reopened.close()

    def test_confirmed_retry_uses_frozen_text_and_expired_failure_releases_events(self):
        self.send.side_effect = [DeliveryFailure('connect_timeout'), {'message_id': 50, 'date': int(NOW + 11)}]
        self.assertEqual('queued', bot.send_common_push())
        self.assertEqual(['4'], [e['id'] for e in common.select_events(self.store, events(4), NOW)['events']])
        self.fetch.return_value = events(1)
        self.now[0] += 11
        bot.send_common_push()
        self.assertEqual(self.send.call_args_list[0], self.send.call_args_list[1])
        self.fetch.assert_called_once()
        self.now[0] += 3600
        self.send.side_effect = DeliveryFailure('connect_timeout')
        self.fetch.return_value = events(5)
        bot.send_common_push()
        identity = common.report_id(self.now[0])
        self.now[0] += 3600
        self.store.expire_scheduled('COMMON', common.hour_start(self.now[0]))
        self.assertEqual('expired', self.store.get_report(identity)['summary_status'])
        self.assertEqual(['4', '5'], [e['id'] for e in common.select_events(self.store, events(5), self.now[0])['events']])

    def test_failed_terminal_report_stays_reserved_until_its_hour_expires(self):
        self.send.side_effect = DeliveryFailure('permanent')
        bot.send_common_push()
        self.assertEqual(['4'], [e['id'] for e in common.select_events(self.store, events(4), NOW)['events']])
        self.assertEqual(['1', '2', '3'], [e['id'] for e in common.select_events(self.store, events(4), NOW + 3600)['events']])

    def test_late_receipt_merges_seen_without_overwriting_newer_report(self):
        self.send.side_effect = DeliveryFailure('unknown')
        bot.send_common_push()
        row_value = self.store.get_report(common.report_id(NOW))
        old = json.loads(row_value['commit_payload'])
        self.now[0] += 3600
        self.send.side_effect = lambda _: {'message_id': 20, 'date': int(self.now[0])}
        bot.send_common_push()
        latest = self.store.load_metadata('common:latest_report')
        common.apply_receipt(self.store, row_value['report_id'], old, {'message_id': 19, 'date': int(NOW + 10)})
        self.assertEqual(latest, self.store.load_metadata('common:latest_report'))
        self.assertEqual([], common.select_events(self.store, events(4), self.now[0])['events'])
        before = self.store.load_metadata('common:news_seen')
        common.apply_receipt(self.store, row_value['report_id'], old, {'message_id': 19, 'date': int(NOW + 10)})
        self.assertEqual(before, self.store.load_metadata('common:news_seen'))

    def test_receipt_persistence_failure_reserves_events_and_recovers_without_resending(self):
        with patch.object(self.store, 'record_sent', side_effect=OSError('disk')):
            with self.assertRaises(ReceiptPersistenceError): bot.send_common_push()
        self.assertEqual(['4'], [e['id'] for e in common.select_events(self.store, events(4), NOW)['events']])
        bot.pump_common_deliveries()
        self.send.assert_called_once()
        self.assertEqual(6, len(self.store.load_metadata('common:news_seen')))

    def test_dedupe_48_hours_and_changed_id_same_title(self):
        bot.send_common_push()
        changed = events(1)
        changed['events'][0]['id'] = '99'
        self.assertEqual([], common.select_events(self.store, changed, NOW + 48 * 3600 - 1)['events'])
        self.assertEqual(1, len(common.select_events(self.store, changed, NOW + 48 * 3600)['events']))

    def test_news_failure_and_empty_are_distinct_and_do_not_block_macro(self):
        self.fetch.return_value = {'status': 'unavailable', 'events': []}
        self.assertEqual('sent', bot.send_common_push())
        self.assertIn('事件新闻暂不可用', self.send.call_args.args[0])
        self.assertIn('US10Y', self.send.call_args.args[0])
        self.now[0] += 3600
        self.fetch.return_value = {'status': 'ok', 'events': []}
        bot.send_common_push()
        self.assertIn('无新增事件', self.send.call_args.args[0])


class NewsReportTest(unittest.TestCase):
    def test_stock_short_full_and_cached_query_remove_retired_news_without_changing_state(self):
        for asset in ('QQQ', 'VOO'):
            value = payload(asset)
            value['news_titles'] = ['如果今年我只能买1只ETF，这就是我会买的']
            stamp = '2026-09-30 09:00 JST'
            for full in (False, True):
                text = bot.build_equity_report(stamp, value, full=full)
                self.assertNotIn('我会买', text)
                self.assertNotIn('📰', text)
                self.assertIn('公共宏观与事件', text)
            state = {'latest_valid_report': {'payload': value, 'text': '📰 我会买', 'full_text': '📰 我会买', 'generated_at': NOW - 86400}}
            before = copy.deepcopy(state)
            with tempfile.TemporaryDirectory() as root, assets.asset_scope(asset), \
                 patch.object(bot, 'STATE_FILE', root + '/state.json'), \
                 patch.object(bot, 'pump_pending_deliveries'), \
                 patch.object(bot, 'build_delivery_document', return_value=b'evidence') as document:
                bot.send_cached_equity_query(state, {'open': False}, 'cached:' + asset, None)
                row_value = bot.get_delivery_store().get_report('cached:' + asset)
                self.assertNotIn('我会买', row_value['text'])
                self.assertIn('最近观察报告', row_value['text'])
                document.assert_called_once()
                self.assertEqual(asset, document.call_args.args[0]['instrument']['asset_id'])
                self.assertEqual([], document.call_args.args[0]['raw_signal_bars'])
                self.assertNotIn('日/周辅助观察', row_value['text'])
                self.assertIn('旧接口历史不参与新分析', row_value['text'])
            self.assertEqual(before, state)

    def test_common_budget_preserves_all_selected_headlines_and_links(self):
        selected = events(3)
        for event in selected['events']:
            event['title'] += '已公布业绩数据' * 40
        with patch.object(bot, 'format_macro_block', return_value=['很长的宏观说明' * 600]):
            text = bot.build_common_report(macro_record(), NOW, selected)
        self.assertLessEqual(utf16_length(text), bot.TG_SAFE_LEN)
        for event in selected['events']:
            self.assertIn(bot.shorten_title(event['title']), text)
            self.assertIn(event['url'], text)


if __name__ == '__main__':
    unittest.main()
