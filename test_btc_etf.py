"""ETF collection and report regression cases; synthetic pages, no live state."""
import copy
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import requests
import btc_etf as etf
import btc_push as bot

sys.path.insert(0, str(Path(__file__).parent / "tests"))
from collector_replay_fixture import END_MS, holdings_only_record

NOW = END_MS / 1000 + 10


def page(rows=None, date="September 3, 2026"):
    if rows is None:
        rows = [{"name": symbol, "symbol": symbol, "btc": 1.00000001} for symbol in etf.FUNDS]
        rows.extend([{"name": "excluded", "symbol": "BITA", "btc": 17},
                     {"name": "delisted", "symbol": "DEFI", "btc": 0}])
    literal = json.dumps(rows).replace('"name":', 'name:').replace('"symbol":', 'symbol:').replace('"btc":', 'btc:')
    cell = f'<td class="td-last-updated" data-title="Last Updated">{date}</td>' if date is not None else ""
    return f"<table>{cell}</table><script>const data = {literal};</script>"


class AnonymousHoldingsTests(unittest.TestCase):
    def fetch(self, state=None, html=None):
        with patch.object(bot, "public_get_text", side_effect=lambda url, **kwargs: (html or page()) if url == etf.ENDPOINT else '{}') as get, \
             patch.object(bot, "public_post_json", side_effect=AssertionError("obsolete endpoint")), \
             patch.object(bot.time, "time", return_value=NOW):
            record = bot.fetch_etf_record(state or {})
            get.assert_any_call(etf.ENDPOINT, retry_read_timeout=True)
        return record

    def test_full_scope_precision_and_exclusions(self):
        result = etf.parse_page(page())
        self.assertEqual(12.00000012, result["holdings_btc"])
        self.assertEqual(list(etf.FUNDS), result["holdings_scope"])
        self.assertEqual("2026-09-03", result["source_date"])
        self.assertEqual(1.00000001, result["holdings_rows"][0]["holdings_btc"])

    def test_true_zero_is_complete(self):
        rows = [{"symbol": symbol, "btc": 0} for symbol in etf.FUNDS]
        record = self.fetch(html=page(rows))
        self.assertEqual(0, bot.current_record_value(record, "holdings_btc"))
        self.assertEqual("collecting", record["value"]["holding_7d_status"])

    def test_missing_duplicate_unknown_and_invalid_funds_fail_closed(self):
        rows = [{"symbol": symbol, "btc": 1} for symbol in etf.FUNDS]
        cases = [rows[:-1], rows + [rows[0]], rows + [{"symbol": "NEW", "btc": 1}]]
        cases += [rows[:-1] + [{"symbol": rows[-1]["symbol"], "btc": value}]
                  for value in (-1, None, True, "1", float("nan"), float("inf"))]
        for sample in cases:
            with self.subTest(sample=sample[-1]):
                with self.assertRaises(ValueError):
                    etf.parse_page(page(sample))
        with self.assertRaises(ValueError):
            etf.parse_page(page().replace("1.00000001", "1e308"))

    def test_page_changes_and_missing_or_conflicting_dates_are_visible(self):
        for html in (page().replace("const data", "let data"), page(date=None), page(date="bad"),
                     page() + page(), "<html>challenge</html>",
                     page() + '<td data-title="Last Updated">September 2, 2026</td>',
                     page().replace('symbol: "', 'unknown: "', 1)):
            with self.subTest(html=html[:40]):
                record = self.fetch(html=html)
                self.assertEqual("unavailable", record["status"])
                self.assertTrue(record["reason"])

    def test_source_date_freshness_and_future_dates(self):
        stale = self.fetch(html=page(date="August 1, 2026"))
        self.assertTrue(stale["source_stale"])
        self.assertIsNone(bot.current_record_value(stale, "holdings_btc"))
        future = self.fetch(html=page(date="September 5, 2026"))
        self.assertEqual("future_holdings_date", future["reason"])

    def test_http_failure_and_timeout_are_unavailable(self):
        response = requests.Response(); response.status_code = 403
        for failure, reason in ((requests.HTTPError(response=response), "http_403"),
                                (requests.Timeout(), "Timeout")):
            with patch.object(bot, "public_get_text", side_effect=failure):
                record = bot.fetch_etf_record({})
            self.assertEqual("unavailable", record["status"])
            self.assertEqual(reason, record["reason"])
            self.assertTrue(etf.is_holdings_source(record))

    def baseline(self, amount=1):
        record = holdings_only_record("2026-08-27", NOW - 7 * 86400, amount)
        return {"public_snapshots": [{"ts": NOW - 7 * 86400, "public": {"etf": record}}]}

    def test_seven_day_basis_uses_source_date_and_latest_known_revision(self):
        record = self.fetch(self.baseline())
        self.assertAlmostEqual(0.000001, record["value"]["holding_7d_pct"])
        self.assertEqual("ok", record["value"]["holding_7d_status"])
        self.assertEqual("2026-08-27", record["holding_7d_basis"]["source_date"])
        changes = ({"venue": "SoSoValue"}, {"endpoint_url": "https://example.com/"},
                   {"holdings_scope": ["IBIT"]}, {"holdings_rows": []},
                   {"stale": True}, {"source_stale": True}, {"snapshot_used": True},
                   {"status": "unavailable"})
        for change in changes:
            state = self.baseline(); state["public_snapshots"][0]["public"]["etf"].update(change)
            self.assertEqual("collecting", self.fetch(state)["value"]["holding_7d_status"])
        for delta in (-86400, 86400):
            state = self.baseline(); state["public_snapshots"][0]["ts"] += delta
            self.assertIsNotNone(self.fetch(state)["value"]["holding_7d_pct"])
        state = self.baseline()
        state["public_snapshots"][0]["public"]["etf"]["value"]["source_date"]["holdings"] = "2026-08-26"
        self.assertIsNone(self.fetch(state)["value"]["holding_7d_pct"])
        self.assertEqual("collecting", self.fetch(self.baseline(0))["value"]["holding_7d_status"])

    def test_old_flow_stays_dated_background_and_never_current(self):
        state = {"public_snapshots": [{"ts": NOW-100, "public": {"etf": {
            "venue": "SoSoValue", "value": {"flow_1d_usd": 0, "flow_5d_usd": -100,
                "flow_30d_usd": 1000, "source_date": {"flow_1d": "2026-08-30", "flow_5d": "2026-08-29", "flow_30d": "2026-08-28"}}}}}]}
        original = copy.deepcopy(state)
        record = self.fetch(state)
        self.assertEqual(original, state)
        for key in etf.FLOW_FIELDS:
            self.assertIsNone(record["value"][key])
        self.assertEqual(0, record["flow_background"]["value"]["flow_1d_usd"])
        self.assertFalse(record["flow_background"]["analysis_eligible"])
        state["public_snapshots"].append({"ts": NOW, "public": {"etf": record}})
        self.assertEqual(record["flow_background"], self.fetch(state)["flow_background"])
        text = "\n".join(bot.format_etf_holdings_only(record))
        self.assertIn("美元净流量缺失", text)
        self.assertIn("2026-08-29", text)
        self.assertIn("仅作背景", text)
        self.assertIn("12.00000012 BTC", text)
        self.assertIn("Bitbo，截至 2026-09-03", text)

    def test_failed_new_collection_never_revives_legacy_current_values(self):
        state = {"public_snapshots": [{"ts": NOW-100, "public": {"etf": {
            "status": "ok", "venue": "SoSoValue", "value": {"holdings_btc": 1, "flow_1d_usd": 7}}}}]}
        with patch.object(bot.time, "time", return_value=NOW):
            selected = bot.select_public_records(state, {"public": {"etf": self.fetch(html="challenge")}})["etf"]
        self.assertIsNone(bot.current_record_value(selected, "holdings_btc"))
        self.assertIsNone(bot.record_value(selected, "flow_1d_usd"))

    def test_holdings_current_flow_rules_insufficient_and_completeness_independent(self):
        record = self.fetch(self.baseline())
        rules = bot.build_etf_rule_state({"public_records": {"etf": record}}, {})
        self.assertEqual("data_insufficient", rules["status"])
        self.assertFalse(rules["eligible_for_rules"])
        self.assertIsNone(rules["flow_outflow_rule"])
        self.assertIsNotNone(rules["holding_7d_pct"])
        self.assertIn("流量数据不足", bot.plain_etf_flow_line(rules))
        missing = bot.build_completeness({"public_records": {"etf": record}})["missing"]
        self.assertIn("ETF Flow", missing)
        self.assertNotIn("ETF Holding", missing)
        block = "\n".join(bot.format_plain_short_data_block({"completeness": {
            "available": 11, "total": 12, "missing": ["ETF Flow"]}}))
        self.assertIn("核心 11/12", block)
        self.assertNotIn("时间没有对齐", block)


if __name__ == "__main__":
    unittest.main()
