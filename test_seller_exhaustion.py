import copy
import hashlib
import json
import math
import os
import tempfile
import unittest
from contextlib import closing
from unittest.mock import Mock, patch

import btc_evidence as evidence
import collector_v2_reader as receiver
import market_evidence_v2 as v2
import market_observations
import market_public
import seller_exhaustion as sec
from market_history import DAY, HistoryStore
from btc_evidence_reader import EvidenceReader

END = 1790812800000
NOW = END + DAY + 3600000


def raw(kind, days=70):
    result = []
    for i in range(days):
        stamp = END - (days-1-i)*DAY
        value = {'price': 50000*math.exp(.02*math.sin(i/3)), 'profit': 12000000+i*100,
                 'supply': 20000000}[kind]
        result.append({'d': sec.datetime.fromtimestamp(stamp/1000, sec.timezone.utc).strftime('%Y-%m-%d'),
                       'unixTs': str(stamp//1000), sec.INPUTS[kind][1]: str(value)})
    return result


class SellerExhaustionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = self.temp.name+'/history.sqlite3'
        self.store = HistoryStore(self.path)
        self.addCleanup(self.store.close)
        self.clock = patch('time.time', return_value=NOW/1000)
        self.clock.start(); self.addCleanup(self.clock.stop)

    def collect(self, overrides=None):
        for kind in sec.INPUTS:
            data = (overrides or {}).get(kind, raw(kind))
            with patch('btc_collection_io.get', return_value=Mock(status_code=200, json=lambda: data)) as get:
                sec.sync(self.store, kind, NOW)
                self.assertNotIn('params', get.call_args.kwargs)
                self.assertFalse(get.call_args.kwargs['allow_redirects'])
        return sec.measurements(self.store, NOW)[sec.NAME]

    def bundle(self, item):
        text = '📊 BTC｜辅助观察'
        return v2.build_bundle({'public_history': {sec.NAME:item}}, text, 'btc:sec',
                              evidence.iso_time(NOW/1000)), text

    def test_formula_components_exact_calendar_changes_and_warmup(self):
        item = self.collect()
        rows = item['rows']
        self.assertTrue(all(r['value'] is None for r in rows[:30]))
        last = rows[-1]
        prices = [r['inputs']['price']['value'] for r in rows[-31:]]
        returns = [math.log(b/a) for a,b in zip(prices,prices[1:])]
        mean = sum(returns)/30
        expected = math.sqrt(sum((r-mean)**2 for r in returns)/30)
        self.assertAlmostEqual(expected,last['volatility_30d'])
        self.assertAlmostEqual(last['supply_in_profit']*expected,last['value'])
        self.assertAlmostEqual(last['value']-rows[-8]['value'],item['values']['change_7d'])
        self.assertEqual([],sec.problems(item,NOW))

    def test_missing_price_invalidates_only_affected_contiguous_windows(self):
        prices = raw('price'); del prices[-10]
        item = self.collect({'price':prices})
        self.assertIsNone(item['rows'][-1]['value'])
        self.assertLess(item['values']['as_of_ms'],END-9*DAY)
        self.assertIn('source_stale',sec.problems(item,NOW))
        self.assertTrue(item['rows'][-1]['supply_in_profit'])

    def test_null_revision_removes_value_without_zero_filling_or_rewriting_past(self):
        self.collect()
        revised = raw('profit');revised[-1][sec.INPUTS['profit'][1]]=None
        with patch('btc_collection_io.get',return_value=Mock(status_code=200,json=lambda:revised)):
            sec.sync(self.store,'profit',NOW+1000)
        self.assertIsNotNone(sec.measurements(self.store,NOW)[sec.NAME]['rows'][-1]['value'])
        self.assertIsNone(sec.measurements(self.store,NOW+1000)[sec.NAME]['rows'][-1]['value'])
        count=self.store.db.execute('select count(*) from observations').fetchone()[0]
        with patch('btc_collection_io.get',return_value=Mock(status_code=200,json=lambda:revised)):
            sec.sync(self.store,'profit',NOW+2000)
        self.assertEqual(count,self.store.db.execute('select count(*) from observations').fetchone()[0])

    def test_empty_denied_and_rate_limit_have_safe_persistent_reasons(self):
        for status,expected in ((403,'access_denied'),(429,'rate_limited'),(500,'source_error')):
            with patch('btc_collection_io.get',return_value=Mock(status_code=status,headers={})):
                with self.assertRaises(sec.SourceUnavailable) as e:sec.sync(self.store,'price',NOW)
                self.assertEqual(expected,e.exception.reason)
                self.assertGreaterEqual(e.exception.seconds,21600)
                self.assertEqual(expected,self.store.get_meta('seller_input:price')['reason'])
        with patch('btc_collection_io.get',return_value=Mock(status_code=200,json=lambda:[])):
            with self.assertRaisesRegex(sec.SourceUnavailable,'empty_response'):sec.sync(self.store,'price',NOW)

    def test_open_day_bad_date_bool_negative_and_nonfinite(self):
        data=raw('price');data.append({'d':'2026-10-03','unixTs':1790985600,'btcPrice':'1'})
        self.assertEqual(70,len(sec.parse(data,'price',NOW)))
        for key,value in [('unixTs','1'),('btcPrice',True),('btcPrice','nan'),('btcPrice','-1'),('btcPrice','0')]:
            bad=raw('price');bad[-1][key]=value
            with self.assertRaises(ValueError):sec.parse(bad,'price',NOW)

    def test_source_timestamp_numeric_notation_is_exact(self):
        for stamp in ('1.7908128E9', '1790812800.0', 1790812800.0):
            row = raw('price')[-1]
            row['unixTs'] = stamp
            self.assertEqual(END, sec.parse([row], 'price', NOW)[0]['period_start_ms'])
        for stamp in (True, 'NaN', 'Infinity', '1790812800.1', '1.790812801E9'):
            row = raw('price')[-1]
            row['unixTs'] = stamp
            with self.assertRaises(ValueError):
                sec.parse([row], 'price', NOW)

    def test_zero_volatility_is_measured_zero_not_missing(self):
        data=raw('price')
        for r in data:r['btcPrice']='100'
        item=self.collect({'price':data})
        self.assertEqual(0,item['values']['latest'])
        self.assertNotIn('collecting',sec.problems(item,NOW))

    def test_encoding_reader_restart_and_consumer_preserve_raw_components(self):
        item=self.collect();bundle,text=self.bundle(item)
        parent={'chat':{'id':1},'message_id':10,'date':NOW//1000,'text':text}
        update={'update_id':1,'message':{'chat':{'id':1},'message_id':11,'date':NOW//1000,
            'reply_to_message':parent,'document':{'file_id':'sec','file_name':'btc-evidence.json'}}}
        path=self.temp.name+'/reader.sqlite3'
        with closing(EvidenceReader(path,1)) as reader:
            reader.ingest_update(update);reader.ingest_update(update)
            reader.process_pending(lambda _:evidence.encode_bundle(bundle))
        with closing(EvidenceReader(path,1)) as reader:
            observed=reader.observations(latest_only=True)[0]
        clean=receiver.sanitize_observation(observed)
        ds=clean['datasets'][sec.NAME]
        self.assertEqual(item['values'],ds['values'])
        self.assertEqual(70,len(ds['rows']))
        self.assertEqual(3,len(ds['source_ids']))
        self.assertEqual('unverified',ds['official_equivalence'])
        self.assertFalse(ds['analysis_eligible'])
        self.assertEqual(0,observed['independent_verified_sources'])
        self.assertTrue(all('retrieved_at_ms' in v for v in ds['rows'][-1]['inputs'].values()))
        observed['datasets'][sec.NAME]['rows'][-1]['value']=999
        rejected=receiver.sanitize_observation(observed)
        self.assertEqual([],rejected['datasets'][sec.NAME]['rows'])
        self.assertIn('daily_calculation_mismatch',rejected['dataset_reasons'][sec.NAME])

    def test_invalid_sources_future_times_units_and_supply_ratio_are_rejected(self):
        original=self.collect()
        mutations=[lambda i:i.update(unit='percent'),
            lambda i:i.update(input_issues=[]),
            lambda i:i['rows'][-1]['inputs']['price'].update(provenance=[]),
            lambda i:i['rows'][-1]['inputs']['price'].update(retrieved_at_ms=NOW+1),
            lambda i:i['rows'][-1]['inputs']['profit']['provenance'].update(endpoint_url='https://evil.test'),
            lambda i:i['rows'][-1]['inputs']['profit'].update(value=999999999)]
        for change in mutations:
            item=copy.deepcopy(original);change(item)
            ds=sec.project(item,NOW)['datasets'][sec.NAME]
            self.assertEqual('invalid',ds['status']);self.assertFalse(ds['rows'])

    def test_old_v2_has_no_required_new_domain_and_equities_exclude_it(self):
        for asset in ('BTC','QQQ','VOO'):
            self.assertNotIn(sec.NAME,v2.required_measurements(asset))
        self.assertFalse(v2.optional_measurements('VOO'))
        self.assertIsNone(v2._contract(sec.NAME,'QQQ'))
        bundle,text=self.bundle(self.collect())
        del bundle['measurements'][sec.NAME]
        bundle['bundle_id']='market-v2:'+hashlib.sha256(evidence.canonical_bytes({k:v for k,v in bundle.items() if k!='bundle_id'})).hexdigest()
        out=evidence.auxiliary_observation(bundle,text)
        self.assertNotIn(sec.NAME,receiver.sanitize_observation(out)['datasets'])

    def test_worker_reuses_persistent_schedule_and_local_reports_never_fetch(self):
        with patch('btc_collection_io.get',return_value=Mock(status_code=200,json=lambda:raw('price'))) as get:
            jobs={'seller_input_price':lambda s,n:sec.sync(s,'price',n)}
            market_public.run_once(self.path,now_ms=NOW,jobs=jobs)
            market_public.run_once(self.path,now_ms=NOW+3600000,jobs=jobs)
            self.assertEqual(1,get.call_count)
            payload={'public_history':market_observations.collect('BTC',self.path,NOW)}
            self.assertIn('数据不足',sec.format_lines(payload)[0])
            self.assertEqual(1,get.call_count)

    def test_display_labels_reconstruction_original_date_and_stale_background(self):
        item=self.collect();line=sec.format_lines({'public_history':{sec.NAME:item}})[0]
        self.assertIn('免费复算',line);self.assertIn('2026-10-01',line)
        stale=sec.measurements(self.store,NOW+3*DAY)
        self.assertIn('仅作历史背景',sec.format_lines({'public_history':stale})[0])


if __name__=='__main__':unittest.main()
