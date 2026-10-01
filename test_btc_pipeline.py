import copy
import time
import unittest
from unittest.mock import Mock, patch

import btc_push as bot
import btc_collection_io as io


class PipelineReliabilityTest(unittest.TestCase):
    def tearDown(self):
        io.clear_cache()

    def test_unready_core_stops_before_auxiliary_requests(self):
        now=1788451201
        target=int(now//3600)*3600000-1
        for usable, close in ((False,target),(True,target-3600000)):
            with patch.object(bot.time,"time",return_value=now), patch.object(bot,"merge_latest_hyperliquid_state"), patch.object(bot,"get_signal_bars",return_value=[]), patch.object(bot,"build_signal_data_quality",return_value={"usable":usable,"latest_close_ms":close,"reasons":[]}), patch.object(bot,"get_display_market") as market, patch.object(bot,"collect_public_snapshot") as public:
                with self.assertRaises(bot.SignalDataNotReadyError):
                    bot.collect_report_payload({},require_usable_signal_data=True)
                market.assert_not_called();public.assert_not_called()
        with patch.object(bot.time,"time",return_value=now), patch.object(bot,"merge_latest_hyperliquid_state"), patch.object(bot,"get_signal_bars",return_value=[]), patch.object(bot,"build_signal_data_quality",return_value={"usable":True,"latest_close_ms":target,"reasons":[]}), patch.object(bot,"get_display_market",side_effect=RuntimeError("positive-control-reached")):
            with self.assertRaisesRegex(RuntimeError,"positive-control-reached"):
                bot.collect_report_payload({},require_usable_signal_data=True)

    def test_waiting_oi_reuses_only_closed_price_dependency(self):
        now=1788451201;hour=int(now//3600)*3600000;ready=[False]
        bars=[[hour-(9-i)*3600000,"100","101","99","100","10",hour-(8-i)*3600000-1,"1000",5,"5","500"] for i in range(9)]
        oi=[{"timestamp":hour-(8-i)*3600000,"sumOpenInterest":"10","sumOpenInterestValue":"1000"} for i in range(9)]
        def fetch(path,params):
            return bars if path.endswith("klines") else oi if ready[0] else []
        with patch.object(bot.time,"time",return_value=now), patch.object(bot,"HYPERLIQUID_MODE","shadow"), patch.object(bot,"binance_get",side_effect=fetch) as get:
            state={}
            bot.get_signal_bars(state);bot.get_signal_bars(state)
            ready[0]=True
            bot.get_signal_bars(state);bot.get_signal_bars(state)
        paths=[call.args[0] for call in get.call_args_list]
        self.assertEqual(1,paths.count("/fapi/v1/klines"))
        self.assertEqual(3,paths.count("/futures/data/openInterestHist"))

    def test_cap_cache_does_not_reuse_price(self):
        quotes=[{"price":101},{"price":102}]
        with patch.object(bot,"BTC_PUSH_MARKET_SOURCE","binance"), patch.object(bot,"get_binance_24h_market",side_effect=quotes), patch.object(bot,"get_btc_market",return_value=(90,1000,1,20)) as cg:
            first=bot.get_display_market();second=bot.get_display_market()
            cg.assert_called_once()
        self.assertEqual((101,102),(first["price"],second["price"]))
        self.assertEqual(first["mcap_retrieved_at"],second["mcap_retrieved_at"])
        self.assertTrue(second["mcap_cache_used"])
        with patch.object(bot,"get_binance_24h_market",side_effect=RuntimeError("down")), patch.object(bot,"get_btc_market",return_value=(303,2000,2,30)) as cg:
            self.assertEqual(303,bot.get_display_market()["price"])
            cg.assert_called_once()

    def test_source_time_age_uses_actual_settlement_interval(self):
        now=1788451200
        for interval in (1,4,8):
            limit=interval*3600+bot.HYPERLIQUID_CTX_STALE_SEC
            rec={"value":{"current_interval_hours":interval},"source_as_of_ms":(now-limit)*1000,"updated_at":now}
            self.assertIsNone(bot.source_age_reason("funding",rec,now))
            self.assertEqual("source_age_exceeded",bot.source_age_reason("funding",rec,now+1))
        for key,limit in (("spot_cvd",bot.SIGNAL_BAR_STALE_SEC),("basis",bot.HYPERLIQUID_CTX_STALE_SEC)):
            rec={"source_as_of_ms":(now-limit)*1000}
            self.assertIsNone(bot.source_age_reason(key,rec,now))
            self.assertEqual("source_age_exceeded",bot.source_age_reason(key,rec,now+1))
        rec={"source_as_of_ms":(now+.5)*1000}
        self.assertIsNone(bot.source_age_reason("basis",rec,now+.75))
        self.assertEqual("source_time_in_future",bot.source_age_reason("basis",rec,now))
        self.assertEqual("source_time_missing",bot.source_age_reason("basis",{},now))

    def test_new_fetch_does_not_refresh_old_funding(self):
        now=time.time()
        old=bot.make_data_record({"funding_8h_pct":1,"funding_24h_pct":3,"current_interval_hours":8},"Binance")
        old.update(source_as_of_ms=int((now-7*86400)*1000),updated_at=int(now))
        with patch.object(bot,"HYPERLIQUID_MODE","shadow"):
            record=bot.select_public_records({},{"public":{"funding":old}})["funding"]
        self.assertFalse(bot.record_is_current(record))
        self.assertEqual("source_age_exceeded",record["source_stale_reason"])
        self.assertFalse(bot.build_signals({}, {}, {"funding":record}, {})["funding_extreme"])

    def test_fear_index_requires_available_current_source_but_accepts_zero(self):
        now=time.time()
        good={"available":True,"value":0,"timestamp":now}
        self.assertTrue(bot.fear_greed_is_current(good,now))
        for update in ({"available":False,"value":99},{"timestamp":1,"value":99},{"timestamp":None,"value":99},{"timestamp":now+3600,"value":99}):
            record={**good,**update}
            self.assertFalse(bot.build_signals({}, {}, {},record)["fear_extreme"])
            self.assertIn("数据不足",bot.format_plain_short_fear_greed_line(record))

    def test_quote_source_time_controls_relation_not_fetch_time(self):
        from test_btc_push_rules import MarketTimeContextTest
        good=MarketTimeContextTest.payload()
        good["market"]["quote_retrieved_at"] += 0.000123
        self.assertTrue(bot.build_market_time_context(good)["relation_eligible"])
        for stamp in (None,good["market"]["current_quote_at_ms"]-86400000,good["market"]["current_quote_at_ms"]+1000):
            payload=copy.deepcopy(good);payload["market"]["quote_source_as_of_ms"]=stamp
            self.assertFalse(bot.build_market_time_context(payload)["relation_eligible"])

    def test_completeness_counts_metrics_including_zero_not_metadata(self):
        payload={"public_records":{"funding":{"value":{"funding_8h_pct":0,"funding_24h_pct":0}},
            "liquidation":{"value":dict.fromkeys(("long_4h_usd","short_4h_usd","long_24h_usd","short_24h_usd"),0)}}}
        missing=bot.build_completeness(payload)["missing"]
        self.assertNotIn("Funding",missing);self.assertNotIn("Liquidation",missing)
        payload["public_records"]["funding"]={"value":{"source_date":"2026-09-04"}}
        payload["public_records"]["liquidation"]["stale"]=True
        missing=bot.build_completeness(payload)["missing"]
        self.assertIn("Funding",missing);self.assertIn("Liquidation",missing)

    def test_quote_time_failure_cannot_claim_collection_complete(self):
        from test_btc_push_rules import ShortReportPlainLanguageLayoutTest
        payload=ShortReportPlainLanguageLayoutTest.payload()
        public=payload["public_records"]
        public["funding"]["value"]["funding_24h_pct"]=0
        public["liquidation"]={"value":dict.fromkeys(("long_4h_usd","short_4h_usd","long_24h_usd","short_24h_usd"),0)}
        public["etf"]["value"]["holdings_btc"]=0
        public["spot_cvd"]["value"].update(volume_4h_pct=0,volume_24h_pct=0)
        payload["market_time_context"]=bot.build_market_time_context(payload)
        payload["completeness"]=bot.build_completeness(payload)
        self.assertIn("采集自检通过","\n".join(bot.format_plain_short_data_block(payload)))
        payload["market"]["quote_source_as_of_ms"]-=86400000
        payload["market_time_context"]=bot.build_market_time_context(payload)
        payload["completeness"]=bot.build_completeness(payload)
        text="\n".join(bot.format_plain_short_data_block(payload))
        self.assertNotIn("采集自检通过",text)
        self.assertIn("Price",payload["completeness"]["missing"])
        self.assertTrue(any("current_quote_source_stale" in note for note in payload["completeness"]["snapshot_notes"]))

    def test_default_report_preserves_auxiliary_quality_and_full_attachment(self):
        from test_btc_push_rules import ShortReportPlainLanguageLayoutTest
        from btc_presentation import utf16_length
        payload=ShortReportPlainLanguageLayoutTest.payload()
        payload["news_titles"]=["长新闻"*2000]
        args=("2026-09-04 16:00 JST",False,"2026-09-04",1,payload)
        with patch.object(bot,"format_news_lines",return_value=payload["news_titles"]):
            short=bot.build_report_message(*args)
            full=bot.build_full_report_message(*args,limit_length=False)
        self.assertIn("辅助观察",short);self.assertIn("🤖 数据状态",short)
        self.assertLessEqual(utf16_length(short),bot.TG_SAFE_LEN)
        self.assertIn("📎 采集自检",full)
        self.assertIn("长新闻"*2000,full)
        funds="\n".join(bot.format_plain_short_funds_block(payload))
        self.assertNotIn("每8H支付",funds)
        self.assertNotIn("稳定币与ETF仍偏流出",funds)
        self.assertIn("稳定币供给变化",funds)


if __name__=="__main__":
    unittest.main()
