import copy
import hashlib
import json
import os
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

import btc_collection_io as io
import btc_evidence as evidence
import btc_push as bot
import market_assets as assets
import market_evidence
from btc_collection import settlement_rows
from btc_delivery import DeliveryFailure, DeliveryStore
from btc_evidence_reader import EvidenceReader
from btc_model_engine import MarketModel
from btc_state_store import compact_state
from btc_structure_engine import analyze_structure, calc_delta_4h_from_bars
from market_scheduler import AssetSchedule


def stamp(local):
    return datetime.fromisoformat(local).replace(tzinfo=assets.ET).timestamp()


NOW = stamp('2026-09-30T10:05:00')


def stock_bars(asset='QQQ', now=NOW, hours=240):
    spec = assets.INSTRUMENTS[asset]
    closes = assets.expected_hour_closes(int(now * 1000) - hours * assets.HOUR, assets.latest_expected_close(now * 1000))
    return [{'open_time': close - assets.HOUR + 1, 'close_time': close, 'open': 100 + i / 100,
             'high': 101 + i / 100, 'low': 99 + i / 100, 'close': 100.1 + i / 100,
             'volume': 1000 + i, 'volume_unit': 'shares', 'price_currency': 'USD', 'calendar': 'US_EQUITY',
             'symbol': spec.price_symbol, 'adjustment_mode': 'ADJUSTED', 'session': 'ETH',
             'signal_source': 'Binance Stocks ' + spec.price_symbol + ' ADJUSTED 1h',
             'endpoint_url': assets.EQUITY_BASE + assets.CHART_PATH, 'retrieved_at_ms': int(now * 1000),
             **({'oi': 100 + i, 'oi_value': (100+i)*100, 'oi_timestamp': close + 1,
                 'oi_source_as_of_ms': close + 1, 'oi_retrieved_at_ms': int(now*1000),
                 'oi_endpoint_url': 'https://fapi.binance.com/futures/data/openInterestHist'} if asset == 'QQQ' else {})}
            for i, close in enumerate(closes)]


def receipt(rows, symbol='QQQ'):
    return {'retrieved_at': NOW, 'data': {'symbol': symbol, 'timeframe': '1H', 'adjustmentMode': 'ADJUSTED', 'bars': [
        {'t': row['open_time'], 'ct': row['close_time'], **{short:row[key] for short,key in
         [('o','open'),('h','high'),('l','low'),('c','close'),('v','volume')]}} for row in rows]}}


def payload(asset='QQQ', quote_failure=False, state=None):
    with assets.asset_scope(asset), patch.object(bot.time, 'time', return_value=NOW), \
         patch.object(bot,'get_signal_bars',return_value=stock_bars(asset)), \
         patch.object(bot,'get_display_market', side_effect=TimeoutError if quote_failure else None,
          return_value={'price':102.,'mcap':None,'chg24':None,'vol24':None,
          'source':'Binance Stocks EQ_'+asset,'quote_source_as_of_ms':None,'quote_source_time_missing':True,
          'quote_retrieved_at':NOW,'quote_endpoint_url':assets.EQUITY_BASE+'v1/public/equity/market/get-info',
          'session_volume_shares':12000}), \
         patch.object(bot,'get_current_derivatives_market',return_value={'oi_usd':12000,'oi_quantity':100,'mark_price':120.,
          'instrument':assets.INSTRUMENTS[asset].derivatives_symbol,'relation':assets.INSTRUMENTS[asset].derivatives_relation}), \
         patch.object(bot,'collect_public_snapshot',return_value={'ts':int(NOW),'public':{
          key: assets.not_applicable('unavailable_direction' if key=='spot_cvd' else 'crypto_only')
          for key in ('etf','spot_cvd','stablecoin','liquidation','macro','funding','basis')}}), \
         patch.object(assets,'market_session',return_value={'open':True,'phase':'REGULAR'}), \
         patch.object(bot,'get_news_titles',side_effect=AssertionError('equities do not collect news')):
        return bot.collect_report_payload(state if state is not None else {})


class MarketIdentityTest(unittest.TestCase):
    def tearDown(self):
        io.clear_cache()

    def test_context_paths_and_commands_are_independent(self):
        for asset in assets.INSTRUMENTS:
            with assets.asset_scope(asset):
                expected = '/tmp/state.json' if asset=='BTC' else '/tmp/state.'+asset.lower()+'.json'
                self.assertEqual(expected, assets.state_path('/tmp/state.json'))
                self.assertEqual(asset.lower()+':hour:3600' if asset!='BTC' else 'hour:3600', bot.scheduled_report_id(3601))
        for text, command in [('/btc','QUERY_PUSH'),('/qqq@bot','QUERY_PUSH:QQQ'),('/VOO','QUERY_PUSH:VOO'),
                              ('/force','FORCE_PUSH'),('/repair','REPAIR'),('/force voo','FORCE_PUSH:VOO'),('/repair QQQ','REPAIR:QQQ')]:
            self.assertEqual(command,bot.parse_tg_command_text(text))
        for text in ('/force SPY','/repair xxx','/voo extra','/force QQQ VOO'):
            self.assertIsNone(bot.parse_tg_command_text(text))

    def test_threaded_collection_preserves_context(self):
        with assets.asset_scope('QQQ'):
            result=io.collect_sources({'one':lambda:{'asset':assets.current().asset_id}},lambda *_:{})
        self.assertEqual('QQQ',result['one']['asset'])
        self.assertEqual('BTC',assets.current().asset_id)

    def test_shared_fetch_single_request_and_symbol_isolation(self):
        calls=[]
        def fetch():
            calls.append(1);time.sleep(.02);return {'value':1}
        with ThreadPoolExecutor(max_workers=3) as pool:
            results=list(pool.map(lambda _:io.cached_call(('source','macro',240),60,fetch),range(3)))
        self.assertEqual(1,len(calls))
        results[0]['value']=100
        self.assertEqual(1,results[1]['value'])
        self.assertEqual({'value':2},io.cached_call(('source','QQQ',240),60,lambda:{'value':2}))

    def test_one_instruments_threshold_does_not_change_other_models(self):
        with patch.dict(os.environ,{'QQQ_MODEL_TREND_PRICE_PCT':'10'}):
            results={asset:MarketModel(parameter=spec.parameter).classify_trend(2,3)
                     for asset,spec in assets.INSTRUMENTS.items()}
        self.assertEqual({'BTC':'UPTREND','QQQ':'RANGE','VOO':'UPTREND'},results)


class EquityCalendarTest(unittest.TestCase):
    def test_sessions_weekend_holiday_and_early_close(self):
        examples=[('2026-09-30T03:00:00','OVERNIGHT',True),('2026-09-30T08:00:00','PRE',True),
                  ('2026-09-30T10:00:00','REGULAR',True),('2026-09-30T17:00:00','POST',True),
                  ('2026-10-02T20:00:00','CLOSED',False),('2026-10-04T20:00:00','OVERNIGHT',True),
                  ('2026-09-06T20:00:00','CLOSED',False),('2026-09-07T19:59:00','CLOSED',False),
                  ('2026-09-07T20:00:00','OVERNIGHT',True),('2026-11-27T13:00:00','POST',True),
                  ('2026-11-27T17:00:00','CLOSED',False)]
        for value,phase,opened in examples:
            with self.subTest(value=value):
                session=assets.calendar_session(stamp(value));self.assertEqual((phase,opened),(session['phase'],session['open']))

    def test_dst_and_next_year_rules(self):
        for date, hour in [('2026-03-08T20:00:00',0),('2026-11-01T20:00:00',1)]:
            ts=stamp(date);self.assertEqual(hour,datetime.fromtimestamp(ts,timezone.utc).hour)
            self.assertTrue(assets.calendar_session(ts)['open'])
        self.assertTrue(assets.calendar_session(stamp('2027-12-31T10:00:00'))['open'])
        self.assertEqual('UNKNOWN',assets.calendar_session(stamp('2029-01-03T10:00:00'))['phase'])

    def test_live_halt_and_status_failure_pause(self):
        spec=assets.INSTRUMENTS['QQQ']
        records={'stock_price:QQQ':{'phase':'O','retrieved_at_ms':NOW*1000},
                 'stream:stocks':{'connected':True,'last_response_ms':NOW*1000},
                 'stock_tradability:QQQ':{'tradability':'BUY_SELL'},'stock_tradingStatus:QQQ':{}}
        with patch('market_history.read_meta',side_effect=lambda key,**kw:records.get(key,{})):
            self.assertTrue(assets.market_session(spec,NOW)['open'])
            records['stock_tradability:QQQ']['tradability']='SELL'
            records['stock_tradingStatus:QQQ']['status']='SHORT_SALE_RESTRICTED'
            self.assertTrue(assets.market_session(spec,NOW)['open'])
            records['stock_tradingStatus:QQQ']['status']='TRADING_HALTED'
            self.assertFalse(assets.market_session(spec,NOW)['open'])
            records['stock_tradingStatus:QQQ'].clear()
            records['stock_tradability:QQQ']['tradability']='NONE'
            self.assertFalse(assets.market_session(spec,NOW)['open'])
            records['stream:stocks']['connected']=False
            self.assertEqual('public_stream_not_ready',assets.market_session(spec,NOW)['reason'])

    def test_source_calendar_overrides_regular_day_and_preserves_dst(self):
        receipt={'retrieved_at':NOW,'data':{'days':[
            {'date':'2026-09-30','periods':[]},
            {'date':'2026-10-01','periods':[{'type':'OVERNIGHT_LATE',
              'startTime':int(stamp('2026-10-01T20:00:00')*1000),
              'endTime':int(stamp('2026-10-02T00:00:00')*1000)-1}]}]}}
        cal=assets.normalize_calendar(receipt)
        self.assertFalse(assets.calendar_session(NOW,cal)['open'])
        self.assertTrue(assets.calendar_session(stamp('2026-10-01T23:59:59'),cal)['open'])
        self.assertEqual([],assets.expected_hour_closes(int(NOW*1000)-3*assets.HOUR,int(NOW*1000),cal))
        with patch.object(assets,'fetch_calendar',return_value=cal),patch.object(assets,'equity_get') as get:
            self.assertFalse(assets.market_session(assets.INSTRUMENTS['QQQ'],NOW)['open'])
            get.assert_not_called()
        receipt['data']['days'][1]['periods'][0]['type']='UNRECOGNIZED'
        with self.assertRaisesRegex(ValueError,'calendar_period'):
            assets.normalize_calendar(receipt)

    def test_weekend_not_gap_natural_window_does_not_stretch(self):
        now=stamp('2026-09-28T23:05:00');rows=stock_bars(now=now)
        self.assertTrue(assets.stock_quality(rows,now*1000)['usable'])
        self.assertTrue(assets.stock_quality(rows,now*1000)['contiguous_4h'])  # Monday has traded since Sunday.
        now=stamp('2026-09-27T23:05:00');rows=stock_bars(now=now)
        self.assertTrue(assets.stock_quality(rows,now*1000)['usable'])
        self.assertIsNone(calc_delta_4h_from_bars(rows))
        self.assertTrue(analyze_structure(rows)['available'])
        rows.pop(-2)
        self.assertIn('trading_hour_missing',assets.stock_quality(rows,now*1000)['reasons'])

    def test_first_reopen_hour_is_not_fridays_current_four_hours(self):
        now=stamp('2026-09-27T20:05:00');rows=stock_bars(now=now)
        quality=assets.stock_quality(rows,now*1000)
        self.assertTrue(quality['usable'])
        self.assertFalse(quality['natural_window_current']);self.assertFalse(quality['contiguous_4h'])


class EquityPipelineTest(unittest.TestCase):
    def tearDown(self):
        io.clear_cache()

    def test_only_closed_bars_and_consistent_adjustment(self):
        rows=stock_bars(hours=24); future={**rows[-1],'open_time':int(NOW//3600)*assets.HOUR,'close_time':(int(NOW//3600)+1)*assets.HOUR-1}
        data={'ETH':receipt(rows+[future]),'OVERNIGHT':receipt([])}
        normalized=assets.normalize_stock_bars(data,assets.INSTRUMENTS['QQQ'],NOW*1000)
        self.assertEqual(len(rows),len(normalized));self.assertNotIn('quote_volume',normalized[-1])
        data['OVERNIGHT']['data']['adjustmentMode']='UNADJUSTED'
        with self.assertRaisesRegex(ValueError,'adjustment'):
            assets.normalize_stock_bars(data,assets.INSTRUMENTS['QQQ'],NOW*1000)

    def test_history_revision_invalidates_only_price_state(self):
        rows=stock_bars();state={'bars_1h':copy.deepcopy(rows),'market_structure_state':{'support_anchor':99},'market_pattern_history':[1],
              'snapshots':[1],'macro_cache':{'keep':True},'applied_report_ids':{'old':1}}
        changed=copy.deepcopy(rows);changed[0]['close']+=.01
        with patch.object(assets,'fetch_calendar',return_value=None), \
             patch.object(assets,'equity_get',side_effect=[receipt(changed),receipt([])]), patch.object(assets.time,'time',return_value=NOW):
            assets.fetch_stock_bars(state,assets.INSTRUMENTS['QQQ'])
        self.assertNotIn('market_structure_state',state);self.assertNotIn('snapshots',state)
        self.assertEqual({'keep':True},state['macro_cache']);self.assertEqual({'old':1},state['applied_report_ids'])
        self.assertEqual(1,state['price_history_revision'])

    def test_price_analysis_continues_without_cvd_and_proxy_does_not_become_voo_oi(self):
        qqq,voo=payload('QQQ'),payload('VOO')
        self.assertTrue(qqq['signal_data_quality']['usable']);self.assertTrue(voo['structure_state']['available'])
        self.assertIsNotNone(voo['levels']['short_support']);self.assertIsNone(voo['market']['oi_usd'])
        self.assertIsNone(voo['delta4h']['oi_4h_pct']);self.assertFalse(voo['model_state']['data_ready'])
        self.assertIsNone(voo['market']['quote_source_as_of_ms']);self.assertIsNone(voo['market']['current_quote_at_ms'])
        self.assertEqual('not_applicable',voo['public_records']['spot_cvd']['status'])
        self.assertNotIn('Spot CVD',voo['completeness']['missing'])
        for value in (qqq,voo):
            self.assertIn('price_trend',value['rule_state']['levels'])
            self.assertIsNotNone(value['delta4h']['price_4h_pct'])

    def test_reporting_preserves_instrument_and_precision(self):
        value=payload('VOO')
        text=bot.build_full_report_message('2026-09-30 23:05 JST',False,'2026-09-30',1,value,limit_length=False)
        self.assertIn('SPYUSDT：标普500衍生品参考',text);self.assertIn('102.00 USD/股',text)
        self.assertNotIn('ETF 日级背景数据不足',text)
        self.assertIn('报价源时间未提供',text)
        self.assertIn('自身ETF申赎/持仓：尚缺公开稳定来源',text)
        self.assertNotIn('统一 ADJUSTED',text)

    def test_regular_and_special_funding_separate(self):
        end=int(NOW//28800)*28800000
        raw=[{'fundingTime':end-i*28800000,'fundingRate':'0.0001','rateType':'Regular'} for i in range(31)]
        raw.append({'fundingTime':end-1000,'fundingRate':'-.04','rateType':'Special'})
        with assets.asset_scope('QQQ'),patch.object(bot.time,'time',return_value=NOW),patch.object(bot,'binance_get',side_effect=[raw,[{'symbol':'QQQUSDT','fundingIntervalHours':8}]]):
            rec=bot.fetch_funding_record('QQQUSDT')
        self.assertAlmostEqual(.01,rec['value']['funding_8h_pct']);self.assertAlmostEqual(.03,rec['value']['funding_24h_pct'])
        self.assertEqual(-.04,rec['value']['special_settlements'][0]['rate'])
        self.assertEqual(31,len(settlement_rows(raw)))

    def test_special_only_does_not_disappear_or_become_regular_funding(self):
        raw=[{'symbol':'SPYUSDT','fundingTime':int(NOW*1000)-1000,'fundingRate':'-.04','rateType':'Special'}]
        with assets.asset_scope('VOO'),patch.object(bot.time,'time',return_value=NOW), \
             patch.object(bot,'binance_get',side_effect=[raw,[{'symbol':'SPYUSDT','fundingIntervalHours':8}]]):
            rec=bot.fetch_funding_record('SPYUSDT')
        self.assertEqual('partial',rec['status']);self.assertIsNone(rec['value']['funding_8h_pct'])
        self.assertEqual(-.04,rec['value']['special_settlements'][0]['rate'])

    def test_fresh_special_does_not_make_old_regular_settlements_current(self):
        end=(int(NOW//28800)-2)*28800000
        raw=[{'fundingTime':end-i*28800000,'fundingRate':'.0001'} for i in range(31)]
        raw.append({'fundingTime':int(NOW*1000)-1000,'fundingRate':'-.04','rateType':'Special'})
        with assets.asset_scope('QQQ'),patch.object(bot.time,'time',return_value=NOW), \
             patch.object(bot,'binance_get',side_effect=[raw,[{'symbol':'QQQUSDT','fundingIntervalHours':8}]]):
            record=bot.fetch_funding_record('QQQUSDT')
        self.assertTrue(record['source_stale']);self.assertFalse(bot.record_is_current(record))
        self.assertEqual(1,len(record['value']['special_settlements']))

    def test_quote_failure_preserves_closed_bar_structure(self):
        value=payload('VOO',quote_failure=True)
        self.assertIsNone(value['market']['price']);self.assertTrue(value['signal_data_quality']['usable'])
        self.assertIsNotNone(value['levels']['short_support'])
        text=bot.build_report_message('2026-09-30 23:05 JST',False,'2026-09-30',1,value)
        self.assertIn('报价采集失败',text);self.assertIn('NA USD/股',text)

    def test_close_during_collection_does_not_queue_or_commit(self):
        with assets.asset_scope('QQQ'),patch.object(bot,'load_state',return_value={}), \
             patch.object(assets,'market_session',side_effect=[{'open':True},{'open':False}]), \
             patch.object(bot,'collect_report_payload',return_value={}),patch.object(bot,'save_state') as save, \
             patch.object(bot,'get_delivery_store') as outbox:
            self.assertEqual('market_closed',bot.send_push(dry_run=True,check_commands=False))
            save.assert_not_called();outbox.assert_not_called()

    def test_closed_bucket_is_frozen_and_reopen_counts_start_again(self):
        value=payload('QQQ');value['structure_state']['latest_close']=99
        value['levels'].update(short_support=100,rolling_short_support=100,short_resistance=120,trend_support=90,trend_resistance=130)
        with assets.asset_scope('QQQ'):
            bucket=bot.price_trend_bucket(value)
            previous={'schema_version':5,'bucket':bucket-50,'support_anchor':100,'support_anchor_phase':'watching',
                      'below_support_hours':1,'status':'breakdown_watch','support_state':'breakdown_watch'}
            state={bot.PRICE_TREND_STATE_KEY:previous}
            result=bot.update_price_trend_state(value,state,{'oi_24h_pct':-1,'funding_8h_pct':-.01},{}, {})
            self.assertEqual(1,result['below_support_hours']);self.assertEqual('watching',result['support_anchor_phase'])
            self.assertEqual(100,result['support_anchor'])
            value['structure_state']['latest_close']=200
            same=bot.update_price_trend_state(value,state,{'oi_24h_pct':1,'funding_8h_pct':.01},{}, {})
            self.assertEqual(result,same)

    def test_closed_queries_reuse_valid_report_without_advancing_state(self):
        cached=payload('VOO'); state={'latest_valid_report':{'payload':cached,'text':'📊 VOO old report','full_text':'old full','generated_at':NOW-86400}}
        with tempfile.TemporaryDirectory() as root,patch.object(bot,'STATE_FILE',root+'/state.json'),assets.asset_scope('VOO'), \
             patch.object(assets,'market_session',return_value={'open':False,'phase':'CLOSED'}), \
             patch.object(bot,'load_state',return_value=state),patch.object(bot,'collect_report_payload') as collect, \
             patch.object(bot,'tg_send',return_value={'message_id':11,'date':int(NOW)}),patch.object(bot,'BTC_PUSH_EVIDENCE_ENABLED',False):
            bot.send_push(cmd='QUERY_PUSH:VOO',check_commands=False,report_id='manual:voo')
            collect.assert_not_called()
            report=bot.get_delivery_store().get_report('manual:voo')
            self.assertIn('最近观察报告',report['text']);self.assertEqual('VOO',report['asset_id'])
            self.assertEqual('sent',report['summary_status'])


class MarketDeliveryScheduleTest(unittest.TestCase):
    def test_revision_commit_clears_old_prices_and_late_receipt_cannot_restore_them(self):
        with tempfile.TemporaryDirectory() as root,patch.object(bot,'STATE_FILE',root+'/state.json'), \
             assets.asset_scope('VOO'),patch.object(bot.time,'time',return_value=NOW):
            bot.save_state({'price_history_revision':0,'snapshots':[{'ts':NOW-7200,'price':100}],
                            'market_structure_state':{'support_anchor':90}})
            commit={'asset_id':'VOO','generated_at':NOW,'candidate':{'price_history_revision':1,
                    'market_structure_state':{'support_anchor':45}},'snapshot':{'ts':NOW,'price':50}}
            bot.apply_delivery_to_state('voo:new',commit,{'date':int(NOW)})
            saved=bot.load_state()
            self.assertEqual([50],[row['price'] for row in saved['snapshots']])
            old={**commit,'generated_at':NOW-3600,'candidate':{'price_history_revision':0,
                 'market_structure_state':{'support_anchor':90}},'snapshot':{'ts':NOW-3600,'price':100}}
            bot.apply_delivery_to_state('voo:late',old,{'date':int(NOW)+1})
            saved=bot.load_state()
            self.assertEqual(45,saved['market_structure_state']['support_anchor'])
            self.assertEqual([50],[row['price'] for row in saved['snapshots']])

    def test_state_commit_and_repair_are_asset_scoped(self):
        with tempfile.TemporaryDirectory() as root,patch.object(bot,'STATE_FILE',root+'/state.json'), \
             patch.object(bot.time,'time',return_value=NOW),patch.object(bot,'tg_send',return_value={'message_id':1,'date':int(NOW)}):
            for asset,anchor in [('BTC',90000),('QQQ',700),('VOO',600)]:
                bot.apply_delivery_to_state(asset+':test',{'asset_id':asset,'generated_at':NOW,
                     'candidate':{'market_structure_state':{'support_anchor':anchor}}},{'date':int(NOW)})
            originals={}
            for asset in assets.INSTRUMENTS:
                with assets.asset_scope(asset): originals[asset]=bot.load_state()
            self.assertNotEqual(originals['BTC']['applied_report_ids'],originals['QQQ']['applied_report_ids'])
            bot.send_push(cmd='REPAIR:QQQ',check_commands=False,report_id='manual:repair-qqq')
            for asset in ('BTC','VOO'):
                with assets.asset_scope(asset): self.assertEqual(originals[asset],bot.load_state())
            with assets.asset_scope('QQQ'):
                self.assertNotIn('market_structure_state',bot.load_state())

    def test_retry_progress_persisted_and_reopen_waits_for_next_hour(self):
        saved=[];s=AssetSchedule('QQQ',None,100,immediate=True,retries=1,retry_delay=15)
        send=Mock(side_effect=['retryable','sent','sent']);pump=Mock();expire=Mock()
        def tick(now,opened):
            return s.tick(now,session={'open':opened},send=send,pump=pump,expire=expire,
                          save=lambda value:saved.append(copy.deepcopy(value)),report_identity=lambda now:'qqq:hour:'+str(int(now)//3600*3600))
        tick(100,True);self.assertEqual(115,s.state['retry_at'])
        recovered=AssetSchedule('QQQ',copy.deepcopy(saved[-1]),110)
        self.assertEqual(1,recovered.state['retry_attempt'])
        tick(105,False);self.assertIsNone(s.state['retry_at']);self.assertEqual(1,send.call_count)
        tick(9000,False);tick(9500,True)
        self.assertEqual(1,send.call_count)
        tick(10800,True)
        self.assertEqual('qqq:hour:10800',send.call_args.kwargs['report_id']);self.assertEqual(2,send.call_count)

    def test_reopen_in_same_hour_does_not_create_another_report(self):
        schedule=AssetSchedule('QQQ',None,100,immediate=True)
        send=Mock(return_value='sent')
        kwargs=dict(send=send,pump=Mock(),expire=Mock(),save=Mock(),report_identity=lambda _: 'qqq:hour:0')
        schedule.tick(100,session={'open':True},**kwargs)
        schedule.tick(200,session={'open':False},**kwargs)
        schedule.tick(300,session={'open':True},**kwargs)
        self.assertEqual(['qqq:hour:0'],[call.kwargs['report_id'] for call in send.call_args_list])

    def test_slow_or_retryable_stock_delivery_does_not_block_other_asset(self):
        entered,release=threading.Event(),threading.Event();now=[100]
        with tempfile.TemporaryDirectory() as root:
            store=DeliveryStore(root+'/queue.db',clock=lambda:now[0],retry_delay=1)
            for asset in ('QQQ','VOO'):
                store.enqueue_report(asset.lower()+':hour:0','text',None,{'asset_id':asset})
            def stalled(_):
                entered.set();release.wait(3);raise DeliveryFailure('connect_timeout')
            with ThreadPoolExecutor(max_workers=2) as workers:
                qqq=workers.submit(store.deliver_one,stalled,Mock(),asset_id='QQQ')
                try:
                    self.assertTrue(entered.wait(1))
                    voo=workers.submit(store.deliver_one,lambda _: {'message_id':2},Mock(),asset_id='VOO')
                    self.assertEqual('sent',voo.result(timeout=1)['status'])
                finally:
                    release.set()
                self.assertEqual('pending',qqq.result(timeout=1)['status'])
            now[0]=200
            retry=store.deliver_one(lambda _: {'message_id':3},Mock(),asset_id='QQQ')
            self.assertEqual('sent',retry['status'])
            self.assertEqual(2,store.get_report('qqq:hour:0')['summary_attempts'])
            self.assertEqual(1,store.get_report('voo:hour:0')['summary_attempts']);store.close()

    def test_outbox_asset_filter_expiration_and_unknown(self):
        with tempfile.TemporaryDirectory() as root:
            store=DeliveryStore(root+'/queue.db',clock=lambda:100)
            for asset in ('BTC','QQQ','VOO'):
                store.enqueue_report(asset.lower()+':hour:0','text',b'{}',{'asset_id':asset})
            store.deliver_one(lambda _:(_ for _ in ()).throw(DeliveryFailure('unknown')),Mock(),asset_id='QQQ')
            store.expire_scheduled('QQQ');store.expire_scheduled('VOO')
            self.assertEqual('unknown',store.get_report('qqq:hour:0')['summary_status'])
            self.assertEqual('expired',store.get_report('voo:hour:0')['summary_status'])
            send=Mock(return_value={'message_id':1})
            store.deliver_one(send,Mock(),asset_id='BTC');send.assert_called_once()
            self.assertIsNone(store.deliver_one(send,Mock(),asset_id='QQQ'))
            store.close()

    def test_delivery_pump_expires_old_hours_and_resume_reports_but_keeps_manual_queries(self):
        with tempfile.TemporaryDirectory() as root:
            store = DeliveryStore(root + '/queue.db', clock=lambda: 4000)
            self.addCleanup(store.close)
            for asset in assets.INSTRUMENTS:
                with self.subTest(asset=asset), assets.asset_scope(asset), \
                     patch.object(bot, 'get_delivery_store', return_value=store), \
                     patch.object(bot.time, 'time', return_value=4000), \
                     patch.object(assets, 'market_session', return_value={'open': True}), \
                     patch.object(bot, 'apply_delivery_to_state'), patch.object(bot, 'log_line'), \
                     patch.object(bot, 'tg_send', return_value={'message_id': 1}) as send:
                    old, current = bot.scheduled_report_id(0), bot.scheduled_report_id(3600)
                    manual = 'manual:' + asset
                    for identity in (old, current, manual):
                        store.enqueue_report(identity, identity, None, {'asset_id': asset})
                    if asset != 'BTC':
                        store.enqueue_report(current + ':resume:3900', 'legacy', None, {'asset_id': asset})
                    bot.pump_pending_deliveries(max_parts=8)
                    self.assertEqual({current, manual}, {call.args[0] for call in send.call_args_list})
                    self.assertEqual('expired', store.get_report(old)['summary_status'])
                    send.reset_mock()
                    bot.pump_pending_deliveries(max_parts=8)
                    send.assert_not_called()

    def test_command_queue_can_claim_other_asset_while_one_running(self):
        with tempfile.TemporaryDirectory() as root:
            store=DeliveryStore(root+'/queue.db')
            updates=[{'update_id':i,'message':{'chat':{'id':1},'text':text}} for i,text in enumerate(['/qqq','/qqq','/voo','/btc'])]
            store.ingest_commands(updates,1,bot.parse_tg_command_text)
            self.assertEqual('QUERY_PUSH:QQQ',store.claim_command()['command'])
            self.assertEqual('QUERY_PUSH:VOO',store.claim_command()['command'])
            self.assertEqual('QUERY_PUSH',store.claim_command()['command'])
            self.assertIsNone(store.claim_command());store.close()

    def test_macro_event_compaction_preserves_business_and_latest_sequence(self):
        record={'source':'macro','source_as_of_ms':123,'updated_at':456,'value':{'us10y':4.2,'history':[{'rows':[1,2,3]}]}}
        state={'event_logs':[{'macro':copy.deepcopy(record),'conclusion':'keep'}], 'macro_cache':copy.deepcopy(record)}
        result=compact_state(state)
        self.assertEqual('keep',result['event_logs'][0]['conclusion'])
        self.assertEqual(4.2,result['event_logs'][0]['macro']['value']['us10y'])
        self.assertNotIn('history',result['event_logs'][0]['macro']['value'])
        self.assertEqual(record,result['macro_cache'])
        self.assertIn('history',state['event_logs'][0]['macro']['value'])


class MarketEvidenceTest(unittest.TestCase):
    def bundle(self,asset):
        text='📊 '+asset+' 09-30｜辅助观察'
        value=payload(asset)
        bundle=evidence.build_bundle(value,text,asset+':report',datetime.fromtimestamp(NOW,timezone.utc).isoformat())
        return bundle,text

    def rehash(self,bundle):
        bundle['bundle_id']='market:'+hashlib.sha256(evidence.canonical_bytes({k:v for k,v in bundle.items() if k!='bundle_id'})).hexdigest()

    def test_proxy_identity_and_numeric_only_projection(self):
        bundle,text=self.bundle('VOO')
        self.assertEqual('degraded_auxiliary',evidence.validate_bundle(bundle,text)['status'])
        result=evidence.auxiliary_observation(bundle,text)
        self.assertEqual('VOO',result['asset_id']);self.assertEqual('proxy',result['datasets']['basis']['relation'])
        self.assertEqual('SPYUSDT',result['datasets']['derivative_oi']['instrument'])
        self.assertNotIn('mechanical_observation',result)
        bundle['datasets']['basis']['instrument']='VOOUSDT';self.rehash(bundle)
        self.assertEqual('rejected',evidence.validate_bundle(bundle,text)['status'])

    def test_invalid_price_measurements_are_not_projected(self):
        bundle,text=self.bundle('QQQ')
        bundle['datasets']['stock_1h']['rows'][-1]['low']=100000
        self.rehash(bundle)
        result=evidence.auxiliary_observation(bundle,text)
        self.assertTrue(result['datasets']['stock_1h']['measurement_rejected'])
        self.assertEqual([],result['datasets']['stock_1h']['rows'])

    def test_interval_change_and_same_time_special_keep_regular_window(self):
        end=int(NOW//28800)*28800000
        regular=[{'symbol':'QQQUSDT','fundingTime':end-i*28800000,'fundingRate':'.0001'} for i in range(31)]
        regular.append({'symbol':'QQQUSDT','fundingTime':end-30*28800000+assets.HOUR,'fundingRate':'.0001'})
        raw=regular+[{'symbol':'QQQUSDT','fundingTime':end,'fundingRate':'-.04','rateType':'Special'}]
        with assets.asset_scope('QQQ'),patch.object(bot.time,'time',return_value=NOW), \
             patch.object(bot,'binance_get',side_effect=[raw,[{'symbol':'QQQUSDT','fundingIntervalHours':8}]]):
            record=bot.fetch_funding_record('QQQUSDT')
        value=payload('QQQ');value['public_records']['funding']=record
        text='📊 QQQ interval change';bundle=evidence.build_bundle(value,text,'qqq:test',datetime.fromtimestamp(NOW,timezone.utc).isoformat())
        validation=evidence.validate_bundle(bundle,text)
        self.assertNotEqual('rejected',validation['status'])
        self.assertEqual(['historical_interval_unproven_or_changed'],validation['dataset_reasons']['funding'])
        projection=evidence.auxiliary_observation(bundle,text)
        self.assertAlmostEqual(.03,projection['datasets']['funding']['values']['funding_24h_pct'])
        self.assertEqual('Special',projection['datasets']['dividend_adjustments']['rate_type'])

    def test_reader_interleaved_late_invalid_and_pending_per_asset(self):
        reader=EvidenceReader(':memory:',1);bundles={asset:self.bundle(asset) for asset in ('QQQ','VOO')}
        btc=evidence.build_bundle({},'📊 BTC old','btc:one',datetime.fromtimestamp(NOW,timezone.utc).isoformat())
        bundles['BTC']=(btc,'📊 BTC old')
        parents={};uid=0
        def parent(mid,text):
            return {'chat':{'id':1},'message_id':mid,'date':int(NOW),'text':text}
        def document(mid,parent_value,asset,raw):
            nonlocal uid
            uid+=1
            message={'chat':{'id':1},'message_id':mid,'date':int(NOW),'reply_to_message':parent_value,
                     'document':{'file_id':str(mid),'file_name':asset.lower()+'-evidence.json','mime_type':'application/json','file_size':len(raw)}}
            reader.ingest_update({'update_id':uid,'message':message});reader.process_pending(lambda _:raw)
        for i,asset in enumerate(('BTC','QQQ','VOO')):
            parents[asset]=parent(10+i*10,bundles[asset][1])
            document(11+i*10,parents[asset],asset,evidence.encode_bundle(bundles[asset][0]))
        for asset in bundles:
            self.assertEqual(asset,reader.observations(latest_only=True,asset_id=asset)[0]['asset_id'])
        # A late QQQ attachment for an older report does not outrank QQQ report 20.
        document(45,parent(15,bundles['QQQ'][1]),'QQQ',evidence.encode_bundle(bundles['QQQ'][0]))
        self.assertEqual(20,reader.observations(latest_only=True,asset_id='QQQ')[0]['transport']['text_message_id'])
        document(51,parent(50,'📊 QQQ new'),'QQQ',b'{"invalid":true}')
        self.assertEqual([],reader.observations(latest_only=True,asset_id='QQQ'))
        self.assertEqual('BTC',reader.observations(latest_only=True)[0]['asset_id'])
        self.assertEqual('VOO',reader.observations(latest_only=True,asset_id='VOO')[0]['asset_id'])
        uid+=1
        reader.ingest_update({'update_id':uid,'message':{'chat':{'id':1},'message_id':61,'date':int(NOW),
            'reply_to_message':parent(60,'📊 VOO pending'),
            'document':{'file_id':'pending','file_name':'voo-evidence.json','mime_type':'application/json'}}})
        self.assertEqual([],reader.observations(latest_only=True,asset_id='VOO'))
        self.assertEqual('BTC',reader.observations(latest_only=True)[0]['asset_id'])
        reader.close()


if __name__=='__main__':
    unittest.main()
