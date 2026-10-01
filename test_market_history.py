import copy
import hashlib
import io
import json
import os
import subprocess
import sys
import tempfile
import threading
import unittest
import zipfile
from contextlib import closing
from unittest.mock import Mock, patch

import btc_evidence as evidence
import market_assets as assets
import market_evidence_v2 as v2
import market_history as history
import market_observations as observations
import market_public as public
import market_streams as streams
from btc_evidence_reader import EvidenceReader

NOW = 1790769600000
END = NOW // history.HOUR * history.HOUR


def bar(opening, buy=6, total=10):
    return {'observed_ms': opening+history.HOUR-1, 'open_time': opening,
            'close_time': opening+history.HOUR-1, 'open': 100.,'high': 102.,'low': 99.,'close': 101.,
            'volume': 1., 'quote_volume': total, 'taker_buy_quote': buy}


class HistoryTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.temp.name, 'history.sqlite3')
        self.environment = patch.dict(os.environ, {'MARKET_HISTORY_DB': self.path})
        self.environment.start()
        self.clock = patch('time.time', return_value=(NOW+1000)/1000)
        self.clock.start()
        self.store = history.HistoryStore(self.path)

    def tearDown(self):
        self.store.close()
        self.environment.stop()
        self.clock.stop()
        self.temp.cleanup()

    def save_bars(self, missing=None):
        rows = [bar(t) for t in range(END-15*history.DAY, END, history.HOUR) if t != missing]
        self.store.put('spot_1h','BTCUSDT', rows, public.provenance(public.SPOT,'USDT'), received_ms=NOW)
        return rows

    def test_idempotence_revisions_and_point_in_time(self):
        row = bar(END-history.HOUR)
        p = public.provenance(public.SPOT, 'USDT')
        self.assertEqual(1, self.store.put('spot_1h','BTCUSDT',[row],p,received_ms=NOW))
        self.assertEqual(0, self.store.put('spot_1h','BTCUSDT',[row],p,received_ms=NOW+1))
        changed = {**row, 'close': 100.5}
        self.store.put('spot_1h','BTCUSDT',[changed],p,received_ms=NOW+100)
        self.assertEqual(101.,self.store.rows('spot_1h','BTCUSDT',known_ms=NOW)[0]['close'])
        self.assertEqual(100.5,self.store.rows('spot_1h','BTCUSDT',known_ms=NOW+100)[0]['close'])
        self.assertEqual([],self.store.rows('spot_1h','BTCUSDT',known_ms=NOW-1))
        self.assertEqual(2,self.store.db.execute('SELECT count(*) FROM observations').fetchone()[0])

    def test_transaction_rolls_back_invalid_future_observation(self):
        with self.assertRaises(ValueError):
            self.store.put('test','BTC',[{'observed_ms':NOW-1},{'observed_ms':NOW+1}],{},received_ms=NOW)
        self.assertEqual([],self.store.rows('test','BTC'))

    def test_reopen_retains_more_than_cache_window(self):
        self.save_bars()
        with closing(history.HistoryStore(self.path, readonly=True)) as reader:
            self.assertEqual(360,len(reader.rows('spot_1h','BTCUSDT',known_ms=NOW)))
        self.assertFalse(os.path.exists(self.path+'.absent'))
        self.assertEqual([],history.read_rows('x','y',path=self.path+'.absent'))

    def test_cvd_windows_zero_gaps_and_curve_origin(self):
        rows = self.save_bars()
        result = history.flow_summary(rows,NOW)
        self.assertEqual(48,result['windows']['1d']['net_quote'])
        self.assertEqual(672,result['windows']['14d']['net_quote'])
        self.assertEqual(rows[0]['open_time'],result['curve'][0]['start_ms'])
        zeros = [bar(r['open_time'],buy=5) for r in rows]
        self.assertEqual(0,history.flow_summary(zeros,NOW)['windows']['1d']['net_quote'])
        incomplete = history.flow_summary(rows[:-2]+rows[-1:],NOW)
        self.assertIsNone(incomplete['windows']['1d']['net_quote'])
        self.assertEqual(rows[-1]['open_time'],incomplete['curve'][-1]['start_ms'])

    def test_timestamp_units_unclosed_and_bad_buy(self):
        opening=END-history.HOUR
        row=[opening,100,102,99,101,1,END-1,10,1,0.06,6,0]
        micro=list(row);micro[0]*=1000;micro[6]*=1000
        self.assertEqual(public.klines([row],NOW),public.klines([micro],NOW))
        self.assertEqual([],public.klines([row],END-1))
        row[10]=11
        with self.assertRaises(ValueError): public.klines([row],NOW)

    def test_coverage_does_not_bridge_disconnect(self):
        self.store.cover('bybit_liquidation','BTCUSDT','a',END-history.DAY,END-5000)
        self.store.cover('bybit_liquidation','BTCUSDT','b',END-2000,END)
        result=self.store.coverage('bybit_liquidation','BTCUSDT',END-history.DAY,END)
        self.assertFalse(result['complete'])
        self.assertEqual([[END-5000,END-2000]],result['gaps'])

    def test_bybit_deduplicates_packet_not_identical_events(self):
        item={'T':NOW-1,'s':'BTCUSDT','S':'Buy','v':'2','p':'100'}
        msg={'topic':'allLiquidation.BTCUSDT','ts':NOW,'data':[item,item]}
        streams.bybit_message(self.store,msg,NOW)
        streams.bybit_message(self.store,msg,NOW+1)
        rows=self.store.rows('bybit_liquidation','BTCUSDT',known_ms=NOW+1)
        self.assertEqual(2,len(rows))
        self.assertEqual('long',rows[0]['side'])
        self.assertEqual(200,rows[0]['estimated_notional'])

    def test_silence_zero_only_with_full_coverage(self):
        empty=observations.collect('BTC',self.path,NOW)['bybit_liquidation_long']
        self.assertEqual('collecting',empty['status'])
        self.store.cover('bybit_liquidation','BTCUSDT','a',END-14*history.DAY,END)
        full=observations.collect('BTC',self.path,NOW)['bybit_liquidation_long']
        self.assertEqual('ok',full['status'])
        self.assertEqual(0,full['windows']['1d']['observed']['count'])

    def test_warmup_and_funding_display_do_not_claim_false_zeros(self):
        public.archive_hyperliquid({'openInterest':'2','markPx':'100'},NOW,self.path)
        values=observations.collect('BTC',self.path,NOW)
        self.assertEqual('collecting',values['hyperliquid_oi_long']['status'])
        self.assertIn('暂无覆盖，无法汇总','\n'.join(observations.report_lines(values)))
        record={'status':'ok','symbol':'BTCUSDT','unit':'fraction','values':{'observed_sum_1d':0.000123}}
        self.assertIn('+0.0123%','\n'.join(observations.report_lines({'funding_long':record})))

    def test_stock_closed_bars_and_unknown_adjustment(self):
        msg={'e':'kline','s':'QQQ','k':{'t':END-history.HOUR,'ct':END-1,'s':'QQQ','i':'1h',
             'o':'100','h':'102','l':'99','c':'101','v':'2','x':False}}
        streams.stock_message(self.store,msg,NOW)
        self.assertEqual([],self.store.rows('stock_1h','QQQ'))
        msg['k']['x']=True
        streams.stock_message(self.store,msg,NOW)
        with assets.asset_scope('QQQ'):
            state={'bars_1h':[{'old':True}], 'latest_valid_report': {'text':'old'}, 'market_structure_state':{'anchor':123}}
            rows=assets.fetch_stock_bars(state)
            self.assertEqual('UNKNOWN',rows[0]['adjustment_mode'])
            self.assertEqual([{'old':True}],state['legacy_stock_history']['bars_1h'])
            self.assertNotIn('latest_valid_report',state)
            self.assertNotIn('market_structure_state',state)
            self.assertFalse(assets.stock_quality(rows,NOW)['usable'])
        with self.assertRaises(RuntimeError): assets.equity_get('retired')

    def test_stock_quote_does_not_invent_source_time(self):
        streams.stock_message(self.store,{'e':'price','rates':[{'s':'VOO','ac':'EQ_VOO','p':'100','t':None,'mp':'O'}]},NOW)
        with assets.asset_scope('VOO'), patch.object(assets.time,'time',return_value=NOW/1000):
            value=assets.stock_quote()
        self.assertIsNone(value['quote_source_as_of_ms'])
        self.assertTrue(value['quote_source_time_missing'])

    def test_stablecoin_depeg_does_not_change_supply_metric(self):
        rows=[{'observed_ms':END-d*history.DAY,'supply':100,'market_value_usd':50 if d==0 else 100} for d in range(20)]
        self.store.put('stablecoin_supply','peggedUSD',rows,public.provenance(public.STABLE,'USD_peg_units'),received_ms=NOW)
        result=observations.collect('BTC',self.path,NOW)['stablecoin_supply_long']
        self.assertEqual(0,result['values']['change_1d'])
        self.assertEqual('USD_peg_units',result['unit'])

    def test_cftc_not_visible_before_retrieval_and_proxy_stays_proxy(self):
        row={'observed_ms':END-3*history.DAY,'date':'2026-09-27','open_interest':100,
             'asset_manager_net':5,'leveraged_net':-3}
        self.store.put('cftc','SP500',[row],public.provenance(public.CFTC,'contracts'),received_ms=NOW)
        self.assertEqual([],observations.collect('VOO',self.path,NOW-1)['equity_positioning']['rows'])
        current=observations.collect('VOO',self.path,NOW)
        self.assertEqual(5,current['equity_positioning']['values']['asset_manager_net'])
        self.assertEqual('proxy',current['derivative_cvd_long']['relation'])
        self.assertNotIn('stablecoin_supply_long',current)

    def test_retry_after_persists_and_other_jobs_continue(self):
        good=Mock(return_value=21600)
        bad=Mock(side_effect=public.RetryLater(100))
        public.run_once(self.path,now_ms=NOW,jobs={'bad':bad,'good':good})
        public.run_once(self.path,now_ms=NOW+99000,jobs={'bad':bad,'good':good})
        self.assertEqual(1,bad.call_count)
        self.assertEqual(1,good.call_count)
        self.assertEqual(NOW+100000,self.store.get_meta('job:bad')['next_ms'])

    def test_archive_backfill_is_normalized_and_retained(self):
        opening=END-180*history.DAY
        raw=f'{opening*1000},100,102,99,101,1,{(opening+history.HOUR-1)*1000},10,1,0.06,6,0\n'
        buf=io.BytesIO()
        with zipfile.ZipFile(buf,'w') as archive: archive.writestr('bars.csv',raw)
        reply=Mock(status_code=200,content=buf.getvalue())
        with patch.object(public.http,'get',return_value=reply), patch.object(public,'response',return_value=Mock(json=lambda:[])):
            public.sync_klines(self.store,'BTCUSDT',spot=True,now_ms=NOW)
        rows=self.store.rows('spot_1h','BTCUSDT',known_ms=NOW+1000)
        self.assertEqual(opening,rows[0]['open_time'])

    def test_oi_pagination_recovers_older_data_and_revisions(self):
        changed = [False]
        def reply(url, params):
            first = (params['startTime']+history.HOUR-1)//history.HOUR*history.HOUR
            stamps = list(range(first, params['endTime']+1, history.HOUR))[-500:]
            return Mock(json=lambda:[{'symbol':'QQQUSDT','timestamp':t,'sumOpenInterest':'2',
                'sumOpenInterestValue':'201' if changed[0] else '200'} for t in stamps])
        with patch.object(public,'response',side_effect=reply):
            public.sync_oi(self.store,'QQQUSDT',NOW)
            rows = self.store.rows('derivative_oi','QQQUSDT',known_ms=NOW+1000)
            self.assertEqual(697,len(rows))
            self.assertEqual(END-29*history.DAY,rows[0]['observed_ms'])
            changed[0] = True
            public.sync_oi(self.store,'QQQUSDT',NOW)
        self.assertEqual(201,self.store.rows('derivative_oi','QQQUSDT',known_ms=NOW+1000)[0]['notional_value'])

    def test_old_empty_kline_window_does_not_starve_current_data(self):
        def reply(url, params):
            opening=END-history.HOUR
            raw=[[opening,100,102,99,101,1,END-1,10,1,0.6,6,0]]
            return Mock(json=lambda:raw if params['startTime'] <= opening <= params['endTime'] else [])
        with patch.object(public,'response',side_effect=reply), patch.object(public.http,'get',return_value=Mock(status_code=404)):
            public.sync_klines(self.store,'SPYUSDT',now_ms=NOW)
            first=self.store.get_meta('kline_scan:futures_1h:SPYUSDT')['cursor']
            public.sync_klines(self.store,'SPYUSDT',now_ms=NOW)
            second=self.store.get_meta('kline_scan:futures_1h:SPYUSDT')['cursor']
        self.assertGreater(second,first)
        self.assertEqual(END-1,self.store.rows('futures_1h','SPYUSDT',known_ms=NOW+1000)[-1]['close_time'])

    def test_stream_reconnect_preserves_silent_period_and_gap(self):
        clock=[NOW/1000]
        stop=threading.Event()
        class Socket:
            def __init__(self, second=False): self.index=0;self.second=second
            def send_frame(self, *args): pass
            def close(self): pass
            def read_frame(self):
                self.index+=1
                if self.index==1:
                    clock[0]+=5 if self.second else 0
                    return 1,json.dumps({'op':'subscribe','success':True}).encode()
                if self.index==2:
                    clock[0]+=10
                    if self.second: stop.set()
                    return 1,b'{"op":"pong"}'
                clock[0]+=5
                raise ConnectionError('fixture_disconnect')
        with patch.object(streams,'connect_websocket',side_effect=[Socket(),Socket(True)]), \
             patch.object(streams.time,'time',side_effect=lambda:clock[0]), \
             patch.object(streams.time,'monotonic',side_effect=lambda:clock[0]), patch.object(stop,'wait',return_value=False):
            streams.run_stream(self.path,'bybit',stop=stop)
        coverage=self.store.coverage('bybit_liquidation','BTCUSDT',NOW,NOW+30000)
        self.assertEqual(20000,coverage['covered_ms'])
        self.assertEqual([[NOW+10000,NOW+20000]],coverage['gaps'])
        self.assertFalse(self.store.get_meta('stream:bybit')['connected'])
        self.assertEqual([],self.store.rows('bybit_liquidation','BTCUSDT'))

    def test_stream_requires_subscription_ack(self):
        clock=[NOW/1000]
        stop=threading.Event()
        class Socket:
            def send_frame(self,*args): pass
            def close(self): stop.set()
            def read_frame(self):
                clock[0]+=21
                return 1,b'{"op":"pong"}'
        with patch.object(streams,'connect_websocket',return_value=Socket()), \
             patch.object(streams.time,'time',side_effect=lambda:clock[0]), \
             patch.object(streams.time,'monotonic',side_effect=lambda:clock[0]):
            streams.run_stream(self.path,'bybit',stop=stop)
        self.assertEqual(0,self.store.coverage('bybit_liquidation','BTCUSDT',NOW,NOW+21000)['covered_ms'])
        self.assertEqual('TimeoutError',self.store.get_meta('stream:bybit')['error'])

    def read_bundle(self, asset, bundle, text):
        raw=evidence.encode_bundle(bundle)
        with closing(EvidenceReader(self.temp.name+'/reader-'+asset+'.sqlite3',-9001,{7001})) as reader:
            parent={'message_id':1,'date':NOW//1000,'chat':{'id':-9001},'from':{'id':7001},'text':text}
            child={'message_id':2,'date':NOW//1000,'chat':{'id':-9001},'from':{'id':7001},
                'reply_to_message':{'message_id':1},'document':{'file_id':'f','file_unique_id':'u',
                'file_name':'market.json','file_size':len(raw)}}
            reader.ingest_update({'update_id':1,'channel_post':parent})
            reader.ingest_update({'update_id':2,'channel_post':child})
            result=reader.process_pending(lambda _:raw)
            self.assertIn(result[0]['status'],('valid_auxiliary','degraded_auxiliary'))
            return reader.observations(asset_id=asset,latest_only=True)[0]

    def test_v2_btc_reader_preserves_flow_and_completed_event_buckets(self):
        self.save_bars()
        stamp=END-history.HOUR+1000
        streams.bybit_message(self.store,{'topic':'allLiquidation.BTCUSDT','ts':stamp,
            'data':[{'T':stamp,'s':'BTCUSDT','S':'Sell','v':'2','p':'100'}]},stamp)
        data=observations.collect('BTC',self.path,NOW)
        text='BTC fixture 辅助观察'
        bundle=evidence.build_bundle({'public_history':data,'evidence_version':2},text,'btc:test',evidence.iso_time(NOW/1000))
        result=self.read_bundle('BTC',bundle,text)
        self.assertEqual(48,result['datasets']['spot_cvd_long']['windows']['1d']['net_quote'])
        self.assertEqual(200,result['datasets']['bybit_liquidation_long']['windows']['1d']['observed']['short_estimated_notional'])
        self.assertNotIn('invalid_observation_time',result['validation']['dataset_reasons'].get('bybit_liquidation_long',[]))

    def test_v2_reader_recalculates_cftc_and_keeps_unknown_adjustment(self):
        rows=[{'observed_ms':END-days*history.DAY,'date':'2026-09-22' if days==8 else '2026-09-29',
               'asset_manager_net':value,'leveraged_net':-value,'open_interest':100} for days,value in ((8,5),(1,8))]
        self.store.put('cftc','SP500',rows,public.provenance(public.CFTC,'contracts'),received_ms=NOW)
        row={**bar(END-history.HOUR),'adjustment_mode':'UNKNOWN','retrieved_at_ms':NOW,
             'provenance':public.provenance(public.STOCK_WS,'USD/share')}
        data=observations.collect('VOO',self.path,NOW)
        data['equity_positioning']['values']['asset_manager_net']=999999  # recompute from rows
        text='📊 VOO｜辅助观察'
        bundle=evidence.build_bundle({'instrument':assets.INSTRUMENTS['VOO'].identity(),
            'public_history':data,'raw_signal_bars':[row],'market':{},'evidence_version':2},text,'voo:test',evidence.iso_time(NOW/1000))
        result=self.read_bundle('VOO',bundle,text)
        self.assertEqual(3,result['datasets']['equity_positioning']['values']['asset_manager_net_weekly_change'])
        self.assertEqual(8,result['datasets']['equity_positioning']['values']['asset_manager_net'])
        self.assertEqual('UNKNOWN',result['datasets']['stock_1h_public']['rows'][0]['adjustment_mode'])
        self.assertFalse(result['datasets']['stock_1h_public']['analysis_eligible'])

    def test_v2_reader_import_is_offline_without_requests(self):
        code="import sys;sys.modules['requests']=None;import btc_evidence_reader,market_evidence_v2"
        result=subprocess.run([sys.executable,'-c',code],capture_output=True,text=True)
        self.assertEqual(0,result.returncode,result.stderr)

    def test_v2_stock_roundtrip_and_cross_asset_tamper(self):
        data=observations.collect('VOO',self.path,NOW)
        payload={'instrument':assets.INSTRUMENTS['VOO'].identity(),'public_history':data,'raw_signal_bars':[],
                 'market':{},'evidence_version':2}
        text='📊 VOO｜辅助观察'
        bundle=evidence.build_bundle(payload,text,'voo:test',evidence.iso_time(NOW/1000))
        self.assertEqual('MARKET_COLLECTOR_EVIDENCE_V2',bundle['schema'])
        self.assertNotEqual('rejected',evidence.validate_bundle(bundle,text)['status'])
        projection=evidence.auxiliary_observation(bundle,text)
        self.assertEqual(0,projection['independent_verified_sources'])
        changed=copy.deepcopy(bundle)
        changed['measurements']['derivative_cvd_long']['relation']='direct'
        changed['bundle_id']='market-v2:'+hashlib.sha256(evidence.canonical_bytes({k:v for k,v in changed.items() if k!='bundle_id'})).hexdigest()
        check=evidence.validate_bundle(changed,text)
        self.assertIn('instrument_or_unit_mismatch',check['dataset_reasons']['derivative_cvd_long'])


if __name__=='__main__': unittest.main()
