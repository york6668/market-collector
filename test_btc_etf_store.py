"""Daily ETF persistence, restarts and failures through the real collector."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import requests
import btc_etf
import btc_etf_flow as flow
import btc_etf_store
import btc_push as bot
import btc_state_store
from test_btc_etf import page
from collector_replay_fixture import END_MS, flow_response, coinboss_response, holdings_only_record

NOW = END_MS / 1000 + 10


class ETFStoreTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = str(Path(self.directory.name) / "market.sqlite3")

    def fetch(self, *, now=NOW, state=None, failures=(), html=None, tftc=None, coinboss=None):
        def get(url, **kwargs):
            if url in failures:
                raise requests.ReadTimeout()
            if url == btc_etf.ENDPOINT:
                return page() if html is None else html
            if url == flow.ENDPOINT:
                return json.dumps(flow_response() if tftc is None else tftc)
            if url == flow.COINBOSS_ENDPOINT:
                return json.dumps(coinboss_response() if coinboss is None else coinboss)
            raise AssertionError(url)
        with patch.object(bot, "public_get_text", side_effect=get), patch.object(bot.time, "time", return_value=now):
            return bot.fetch_etf_record(state or {}, self.path)

    def test_restart_uses_daily_observations_without_hourly_snapshots_and_keeps_retrieval_time(self):
        first = self.fetch()
        current = self.fetch(now=NOW+3600, failures=(btc_etf.ENDPOINT, flow.ENDPOINT, flow.COINBOSS_ENDPOINT))
        self.assertEqual(first["value"], current["value"])
        self.assertEqual(NOW, current["flow_record"]["retrieved_at"])
        self.assertEqual(NOW, current["holdings_record"]["retrieved_at"])
        self.assertTrue(current["flow_record"]["cache_used"])
        self.assertTrue(current["holdings_record"]["cache_used"])
        self.assertEqual("ReadTimeout", current["flow_record"]["collection_reason"])
        notes = bot.build_completeness({"public_records": {"etf": current}})["snapshot_notes"]
        self.assertTrue(any("ETF流量本轮采集失败(ReadTimeout)" in n for n in notes))
        self.assertTrue(any("ETF持仓本轮采集失败(ReadTimeout)" in n for n in notes))
        self.assertIsNotNone(bot.current_record_value(current, "flow_30d_usd"))
        import btc_evidence
        bundle = btc_evidence.build_bundle({"public_records": {"etf": current}}, "ETF", "cache", btc_evidence.iso_time(NOW+3600))
        self.assertNotIn("etf", btc_evidence.validate_bundle(bundle, "ETF")["dataset_reasons"])

    def test_cached_data_is_reevaluated_after_weekend_and_never_refreshes_itself(self):
        self.fetch()
        current = self.fetch(now=NOW+5*86400, failures=(btc_etf.ENDPOINT, flow.ENDPOINT, flow.COINBOSS_ENDPOINT))
        self.assertIsNone(bot.current_record_value(current, "flow_1d_usd"))
        self.assertIsNone(bot.current_record_value(current, "holdings_btc"))
        self.assertEqual(NOW, current["flow_record"]["retrieved_at"])
        self.assertTrue(current["flow_record"]["source_stale"])
        self.assertTrue(current["holdings_record"]["source_stale"])

    def test_live_secondary_wins_over_same_dated_cached_primary(self):
        self.fetch()
        current = self.fetch(failures=(flow.ENDPOINT,))
        self.assertEqual("CoinBoss", current["flow_record"]["venue"])
        self.assertFalse(current["flow_record"].get("cache_used", False))
        self.assertEqual("ReadTimeout", current["flow_sources"][0]["collection_reason"])

    def test_schema_failure_does_not_revive_a_cached_current_value(self):
        self.fetch()
        current = self.fetch(tftc={}, coinboss={}, html="challenge")
        self.assertIsNone(bot.current_record_value(current, "flow_1d_usd"))
        self.assertIsNone(bot.current_record_value(current, "holdings_btc"))
        self.assertEqual("flow_unit_mismatch", current["flow_record"]["reason"])

    def test_holiday_uses_the_same_expected_session_for_flow_and_holdings(self):
        self.fetch()
        # Tuesday morning after US Labor Day: Friday remains the expected session.
        current = self.fetch(now=NOW+4*86400, failures=(btc_etf.ENDPOINT, flow.ENDPOINT, flow.COINBOSS_ENDPOINT))
        self.assertEqual("2026-09-04", current["flow_record"]["expected_source_date"])
        self.assertIsNotNone(bot.current_record_value(current, "flow_1d_usd"))
        self.assertIsNotNone(bot.current_record_value(current, "holdings_btc"))

    def test_revisions_are_dated_and_cannot_be_seen_before_their_retrieval(self):
        store = btc_etf_store.ETFStore(self.path)
        self.addCleanup(store.close)
        initial = btc_etf.parse_page(page())
        store.save_holdings(initial, NOW)
        revised = btc_etf.parse_page(page(rows=[{"symbol": s, "btc": 2} for s in btc_etf.FUNDS]))
        store.save_holdings(revised, NOW+3600)
        self.assertAlmostEqual(12.00000012, store.holdings("2026-09-03", NOW+100)["holdings_btc"])
        self.assertEqual(24, store.holdings("2026-09-03", NOW+3600)["holdings_btc"])
        store.save_holdings(revised, NOW+7200)
        self.assertEqual(2, store.history.db.execute("SELECT count(*) FROM observations WHERE series='etf_holdings'").fetchone()[0])

    def test_seven_day_basis_survives_compaction_and_a_different_retrieval_hour(self):
        baseline = holdings_only_record("2026-08-27", NOW-8*86400, amount=0.5)
        # Date labels cannot have been collected before that date.
        baseline["retrieved_at"] = NOW-6*86400
        state = {"public_snapshots": [{"ts": NOW-6*86400, "public": {"etf": baseline}}]}
        original = copy.deepcopy(state)
        record = self.fetch(state=btc_state_store.compact_state(state))
        self.assertEqual(original, state)
        self.assertAlmostEqual(100.000002, record["value"]["holding_7d_pct"])
        restarted = self.fetch()
        self.assertEqual(record["value"]["holding_7d_pct"], restarted["value"]["holding_7d_pct"])
        self.assertEqual("2026-08-27", restarted["holding_7d_basis"]["source_date"])

    def test_partial_source_response_remains_partial_in_cache_without_backfilling_missing_days(self):
        self.fetch()
        data = flow_response(); del data["days"][-3]
        partial = self.fetch(tftc=data, coinboss={})
        self.assertIsNone(bot.current_record_value(partial, "flow_5d_usd"))
        cached = self.fetch(failures=(flow.ENDPOINT,), coinboss={})
        self.assertEqual(partial["value"]["history"], cached["value"]["history"])
        self.assertIsNone(bot.current_record_value(cached, "flow_5d_usd"))
        self.assertIsNotNone(bot.current_record_value(cached, "flow_1d_usd"))

    def test_flow_revision_updates_the_same_source_only(self):
        self.fetch()
        revised = self.fetch(tftc=flow_response(amount=-3))
        self.assertEqual(-90, revised["value"]["flow_30d_usd"])
        cached = self.fetch(failures=(flow.ENDPOINT, flow.COINBOSS_ENDPOINT))
        self.assertEqual(-90, cached["value"]["flow_30d_usd"])
        self.assertEqual("TFTC", cached["flow_record"]["venue"])

    def test_persistence_failure_keeps_live_domains_and_exposes_the_gap(self):
        with patch.object(btc_etf_store.ETFStore, "save_holdings", side_effect=bot.sqlite3.OperationalError("disk full")):
            record = self.fetch()
        self.assertIsNotNone(bot.current_record_value(record, "flow_1d_usd"))
        self.assertIsNotNone(bot.current_record_value(record, "holdings_btc"))
        self.assertEqual("OperationalError", record["daily_store_reason"])
        self.assertTrue(any("ETF日级存储失败" in n for n in bot.build_completeness({"public_records": {"etf": record}})["snapshot_notes"]))


if __name__ == "__main__":
    unittest.main()
