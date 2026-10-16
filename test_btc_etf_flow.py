"""Anonymous ETF flow collection, calendar and independent degradation."""
import copy
import json
import unittest
from unittest.mock import patch

import requests
import btc_etf
import btc_etf_flow as flow
import btc_push as bot
import btc_evidence
from test_btc_etf import page
from collector_replay_fixture import END_MS, flow_response

NOW = END_MS / 1000 + 10


class FlowTests(unittest.TestCase):
    def parse(self, data=None):
        return flow.parse_json(json.dumps(flow_response() if data is None else data))

    def fetch(self, data=None, html=None, failed=None):
        def get(url, **kwargs):
            if url == failed:
                raise requests.Timeout()
            if url == btc_etf.ENDPOINT:
                return page() if html is None else html
            self.assertEqual(flow.ENDPOINT, url)
            return json.dumps(flow_response() if data is None else data)
        with patch.object(bot, "public_get_text", side_effect=get), patch.object(bot.time, "time", return_value=NOW):
            return bot.fetch_etf_record({})

    def test_precision_usd_and_complete_windows(self):
        parsed = self.parse()
        for n in (1, 5, 30):
            self.assertAlmostEqual(n * 1.01234567, parsed["values"][f"flow_{n}d_usd"], places=8)
            self.assertEqual("ok", parsed["window_quality"][f"{n}d"]["status"])
        self.assertEqual(30, len(parsed["history"]))
        self.assertEqual(list(btc_etf.FUNDS), parsed["flow_scope"])

    def test_zero_and_negative_values_are_real_observations(self):
        for amount in (0, -12.125):
            parsed = self.parse(flow_response(amount=amount))
            self.assertEqual(amount, parsed["values"]["flow_1d_usd"])
            self.assertEqual(amount * 30, parsed["values"]["flow_30d_usd"])

    def test_missing_trading_day_and_partial_day_are_never_skipped(self):
        data = flow_response(count=40)
        del data["days"][-3]
        values = self.parse(data)["values"]
        self.assertIsNotNone(values["flow_1d_usd"])
        self.assertIsNone(values["flow_5d_usd"])
        self.assertIsNone(values["flow_30d_usd"])
        data = flow_response(count=40)
        data["days"][-1]["perEtfUsd"]["IBIT"] = None
        self.assertTrue(all(v is None for v in self.parse(data)["values"].values()))
        data = flow_response(count=29)
        values = self.parse(data)["values"]
        self.assertIsNotNone(values["flow_5d_usd"])
        self.assertIsNone(values["flow_30d_usd"])

    def test_fund_scope_invalid_values_and_total_mismatch(self):
        for change in (lambda row: row["perEtfUsd"].pop("BTC"),
                       lambda row: row["perEtfUsd"].update(UNKNOWN=0),
                       lambda row: row.update(netFlowUsd=99),
                       *(lambda row, v=v: row["perEtfUsd"].update(IBIT=v) for v in (True, None, "0", "-", "NaN"))):
            data = flow_response(); change(data["days"][-1])
            self.assertTrue(all(v is None for v in self.parse(data)["values"].values()))
        data = flow_response(amount=0.3)
        data["days"][-1]["perEtfUsd"].update(IBIT=0.1, FBTC=0.2)
        self.assertEqual(0.3, self.parse(data)["values"]["flow_1d_usd"])

    def test_schema_date_duplicates_unit_and_json_fail_closed(self):
        for change in (lambda d: d.update(units="US$m"), lambda d: d.update(updatedThrough=None),
                       lambda d: d.update(updatedThrough="2026-09-02"),
                       lambda d: d["days"].append(copy.deepcopy(d["days"][-1])),
                       lambda d: d["days"].reverse(), lambda d: d["days"][-1].update(date="bad")):
            data = flow_response(); change(data)
            with self.assertRaises(ValueError): self.parse(data)
        for text in ('{"units":"USD","units":"USD"}', '{"x":NaN}', 'challenge', '[]'):
            with self.assertRaises(ValueError): flow.parse_json(text)

    def test_weekend_holiday_unknown_calendar_and_early_session(self):
        self.assertEqual(["2026-09-03", "2026-09-04", "2026-09-08"], flow.trading_dates("2026-09-08", 3))
        data = flow_response(end="2026-09-08", count=5)
        data["days"].insert(-1, {**copy.deepcopy(data["days"][-1]), "date": "2026-09-07"})
        self.assertIsNone(self.parse(data)["values"]["flow_5d_usd"])
        with self.assertRaisesRegex(ValueError, "calendar_year_unverified"):
            flow.trading_dates("2029-01-02", 5)
        self.assertEqual("flow_session_not_closed", flow.freshness("2026-09-04", NOW)[0])

    def test_flow_and_holdings_fail_independently(self):
        complete = self.fetch()
        self.assertEqual("ok", complete["status"])
        self.assertIsNotNone(bot.current_record_value(complete, "flow_30d_usd"))
        self.assertIsNotNone(bot.current_record_value(complete, "holdings_btc"))
        complete["flow_record"]["expected_source_date"] = None
        self.assertFalse(any("待更新" in line for line in bot.format_etf_holdings_only(complete)))
        self.assertFalse(any("尚未更新" in note for note in bot.build_completeness({"public_records": {"etf": complete}})["snapshot_notes"]))
        no_flow = self.fetch(failed=flow.ENDPOINT)
        self.assertIsNotNone(bot.current_record_value(no_flow, "holdings_btc"))
        self.assertIsNone(bot.current_record_value(no_flow, "flow_1d_usd"))
        self.assertIn("ETF Flow", bot.build_completeness({"public_records": {"etf": no_flow}})["missing"])
        no_holding = self.fetch(failed=btc_etf.ENDPOINT)
        self.assertIsNone(bot.current_record_value(no_holding, "holdings_btc"))
        self.assertIsNotNone(bot.current_record_value(no_holding, "flow_1d_usd"))
        self.assertTrue(bot.build_etf_rule_state({"public_records": {"etf": no_holding}}, {})["eligible_for_rules"])
        future_flow = self.fetch(flow_response(end="2026-09-08"))
        self.assertEqual("future_flow_date", future_flow["flow_reason"])
        self.assertFalse(any("ETF流量历史" in line for line in bot.format_etf_holdings_only(future_flow)))
        bundle = btc_evidence.build_bundle({"public_records": {"etf": future_flow}}, "future flow", "test",
                                         btc_evidence.iso_time(NOW))
        self.assertNotIn("etf", btc_evidence.validate_bundle(bundle, "future flow")["dataset_reasons"])

    def test_staleness_background_and_source_dates_stay_separate(self):
        stale_flow = self.fetch(flow_response(end="2026-08-31"))
        self.assertIsNone(bot.current_record_value(stale_flow, "flow_1d_usd"))
        self.assertIsNotNone(bot.current_record_value(stale_flow, "holdings_btc"))
        text = "\n".join(bot.format_etf_holdings_only(stale_flow))
        self.assertIn("2026-08-31", text)
        self.assertIn("仅作背景", text)
        self.assertIn(flow.PAGE, text)
        stale_hold = self.fetch(html=page(date="August 1, 2026"))
        self.assertIsNone(bot.current_record_value(stale_hold, "holdings_btc"))
        self.assertIsNotNone(bot.current_record_value(stale_hold, "flow_1d_usd"))
        mismatch = self.fetch(flow_response(end="2026-09-02"))
        text = "\n".join(bot.format_etf_holdings_only(mismatch))
        self.assertIn("TFTC，截至 2026-09-02", text)
        self.assertIn(flow.PAGE, text)
        self.assertIn("Bitbo，截至 2026-09-03", text)
        self.assertIn("应更新至 2026-09-03", text)
        self.assertTrue(any("尚未更新至2026-09-03" in note for note in bot.build_completeness({"public_records": {"etf": mismatch}})["snapshot_notes"]))
        self.assertTrue(any("日期未对齐" in note for note in bot.build_completeness({"public_records": {"etf": mismatch}})["snapshot_notes"]))

    def test_revision_and_latest_flow_background_do_not_revive_old_values(self):
        first, revised = self.fetch(), self.fetch(flow_response(amount=-20))
        self.assertEqual(-20, bot.current_record_value(revised, "flow_1d_usd"))
        state = {"public_snapshots": [{"public": {"etf": first}}, {"public": {"etf": revised}}]}
        background = btc_etf.flow_background(state)
        self.assertEqual("TFTC", background["venue"])
        self.assertEqual(-20, background["value"]["flow_1d_usd"])
        self.assertFalse(background["analysis_eligible"])

    def test_consistency_compares_only_same_source_and_same_dated_window(self):
        previous = self.fetch(flow_response(amount=25000000))
        current = self.fetch(flow_response(amount=-25000000))
        state = {"public_snapshots": [{"ts": NOW - 3600, "public": {"etf": previous}}]}
        payload = {"public_records": {"etf": current}, "public_snapshot": {"ts": NOW, "public": {"etf": current}}}
        self.assertTrue(bot.build_etf_rule_state(payload, state)["consistency"]["flow_5d_inconsistent"])
        previous["flow_record"]["source_date"] = "2026-09-02"
        self.assertFalse(bot.build_etf_rule_state(payload, state)["consistency"]["flow_5d_inconsistent"])
        previous["flow_record"]["venue"] = "SoSoValue"
        self.assertFalse(bot.build_etf_rule_state(payload, state)["consistency"]["flow_5d_inconsistent"])


if __name__ == "__main__":
    unittest.main()
