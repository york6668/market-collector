import copy
import hashlib
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import btc_collection as collection
import btc_push as bot
import btc_evidence as evidence


class CollectionWindowTest(unittest.TestCase):
    def test_settlement_bucket_drift_preserves_raw_times_and_boundary_sum(self):
        hour = collection.HOUR_MS
        for spacing in (1, 2, 4, 8):
            for drift in (-100, 0, 80, 100):
                rows = [{"time": (1000 + i * spacing) * hour + (drift if i % 2 else 0),
                         "rate": 0 if i == 0 else 0.0001}
                        for i in range(240 // spacing + 1)]
                original = copy.deepcopy(rows)
                for hours in (8, 24, 240):
                    total, quality, selected = collection.settled_window(rows, hours, spacing)
                    self.assertTrue(quality["complete"])
                    self.assertAlmostEqual(hours / spacing * 0.01, total)
                    self.assertEqual(selected[0]["time"], quality["start_ms"])
                    self.assertEqual(selected[-1]["time"], quality["end_ms"])
                self.assertEqual(original, rows)

    def test_settlement_rejects_off_grid_missing_duplicate_changed_and_future_buckets(self):
        hour = collection.HOUR_MS
        rows = [{"time": (100 + i) * hour, "rate": 0} for i in range(25)]
        bad = [rows[:5] + rows[6:], rows[:5] + [dict(rows[5], time=rows[5]["time"]+1)] + rows[5:],
               rows[:5] + [dict(rows[5], time=rows[5]["time"]+hour)] + rows[6:]]
        for drift in (-101, 101, hour // 2):
            bad.append([dict(row, time=row["time"] + drift) for row in rows])
        for changed in bad:
            self.assertIsNone(collection.settled_window(changed, 24, 1)[0])
        # Both the raw source time and the comparison bucket must have elapsed.
        late = [dict(row, time=row["time"]-50) for row in rows]
        for cutoff in (rows[-1]["time"]-25, rows[-1]["time"]-75):
            self.assertIsNone(collection.settled_window(late, 24, 1, as_of_ms=cutoff)[0])
        self.assertTrue(collection.settled_window(late, 24, 1, as_of_ms=rows[-1]["time"])[1]["complete"])
        duplicate = collection.settlement_rows([
            {"fundingTime": row["time"], "fundingRate": "0"} for row in rows + [rows[5]]])
        self.assertIsNone(collection.settled_window(duplicate, 24, 1)[0])
        future = collection.settlement_rows([
            {"fundingTime": row["time"], "fundingRate": "0"} for row in rows],
            current_ms=rows[-1]["time"]-1)
        self.assertEqual(rows[-1]["time"], future[-1]["time"])
        self.assertIsNone(collection.settled_window(future, 24, 1)[0])

    def test_old_off_grid_row_does_not_poison_current_windows(self):
        hour = collection.HOUR_MS
        rows = [{"time": (1000+i*8)*hour, "rate": 0} for i in range(41)]
        rows[0]["time"] += 101
        for hours in (8, 24, 240):
            total, quality, selected = collection.settled_window(rows, hours, 8)
            self.assertEqual(0, total)
            self.assertTrue(quality["complete"])
            self.assertEqual(hours//8+1, len(selected))
        rows[-2]["time"] += 101
        self.assertIsNone(collection.settled_window(rows, 24, 8)[0])

    def test_settlement_actual_8h_and_24h_with_boundary(self):
        for interval in (1, 4, 8):
            with self.subTest(interval=interval):
                rows = [{"time": (i + 1) * interval * collection.HOUR_MS, "rate": 0.0001}
                        for i in range(24 // interval + 1)]
                for hours in (8, 24):
                    total, quality, _ = collection.settled_window(rows, hours, interval)
                    self.assertAlmostEqual(hours / interval * 0.01, total)
                    self.assertTrue(quality["complete"])

    def test_missing_boundary_changed_interval_and_missing_rate_are_incomplete(self):
        rows = [{"time": i * 8 * collection.HOUR_MS, "rate": 0.0} for i in range(1, 5)]
        for changed in (rows[1:], [dict(r, rate=None) if i == 2 else r for i, r in enumerate(rows)],
                        [dict(r, time=r["time"] - collection.HOUR_MS) if i == 2 else r
                         for i, r in enumerate(rows)]):
            self.assertIsNone(collection.settled_window(changed, 24, 8)[0])
        self.assertIsNone(collection.settled_window(rows, 24, 4)[0])
        self.assertEqual(0, collection.settled_window(rows, 24, 8)[0])

    def test_etf_complete_counts_dates_zero_and_missing(self):
        raw = [{"date": (datetime(2026, 8, 1) + timedelta(days=i)).strftime("%Y-%m-%d"),
                "totalNetInflow": i} for i in range(30)]
        rows, _ = collection.dated_flow_rows(list(reversed(raw)))
        self.assertEqual(135, collection.dated_flow_window(rows, 5)[0])
        for count in (4, 5, 29, 30):
            for window in (5, 30):
                value, coverage = collection.dated_flow_window(rows[:count], window)
                self.assertEqual(count >= window, coverage["complete"])
                self.assertEqual(value is not None, count >= window)
        rows[-1]["totalNetInflow"] = None
        self.assertIsNone(collection.dated_flow_window(rows, 5)[0])
        zeros, _ = collection.dated_flow_rows([dict(r, totalNetInflow=0) for r in raw])
        self.assertEqual(0, collection.dated_flow_window(zeros, 30)[0])

    def test_etf_duplicate_conflict_invalid_dates_do_not_fill_window(self):
        rows, quality = collection.dated_flow_rows([
            {"date": "2026-09-01", "totalNetInflow": 10},
            {"date": "2026-09-01", "totalNetInflow": 20},
            {"date": "2026-02-30", "totalNetInflow": 30},
        ])
        self.assertEqual(1, len(rows))
        self.assertIsNone(rows[0]["totalNetInflow"])
        self.assertEqual(1, quality["invalid_date_rows"])
        self.assertEqual(1, quality["duplicate_date_rows"])


class CollectionAdapterTest(unittest.TestCase):
    def test_hyperliquid_hourly_drift_uses_same_window_as_portable_validation(self):
        hour = collection.HOUR_MS
        raw = [{"time": (1000+i)*hour+(i*17)%81, "fundingRate": "0"} for i in range(24)]
        value = bot.build_hyperliquid_funding_value(raw, [], 1024*hour)
        self.assertEqual(0, value["funding_24h_pct"])
        self.assertTrue(value["window_quality"]["complete"])
        self.assertEqual([r["time"] for r in raw], [r["time"] for r in value["history"]])
        self.assertEqual(value["window_quality"], evidence.settlement_window(value["history"], 24, hour)[0])
        self.assertFalse(bot.funding_rows_contiguous([dict(r,time=r["time"]+hour//2) for r in value["history"]],24))

    def test_treasury_history_contains_completed_result_observations_only(self):
        now = datetime(2026, 9, 4, 12, tzinfo=timezone.utc).timestamp()
        raw = [
            {"auction_date": "2026-09-10", "announcemt_date": "2026-09-03", "bid_to_cover_ratio": "null", "offering_amt": "999"},
            {"auction_date": "2026-09-09", "bid_to_cover_ratio": "2", "offering_amt": "999"},
            {"auction_date": "2026-08-27", "bid_to_cover_ratio": "2.5", "offering_amt": "10",
             "maturity_date": "2056-08-15", "issue_date": "2026-09-15", "call_date": "null"},
            {"auction_date": "2026-09-03", "bid_to_cover_ratio": "0", "offering_amt": "0"},
            {"auction_date": "2026-09-03", "bid_to_cover_ratio": True, "offering_amt": "999"},
            {"auction_date": "2026-09-03", "bid_to_cover_ratio": "NaN", "offering_amt": "999"},
        ]
        original = copy.deepcopy(raw)
        with patch.object(bot, "fiscaldata_get_json", return_value={"data": raw}), patch.object(bot.time,"time",return_value=now):
            auction = bot.fetch_treasury_long_auction_value()
        self.assertEqual(original, raw)
        self.assertEqual(2, auction["auction_count"])
        self.assertEqual(10, auction["offering_recent_usd"])
        self.assertEqual(0, auction["latest_bid_to_cover"])
        history = auction["history"][0]
        self.assertEqual("2026-09-03", history["end_date"])
        self.assertIsNone(history["published_at"])
        self.assertEqual({"auction_date", "bid_to_cover_ratio", "offering_amt"}, set(history["rows"][0]))
        record = {"value": {"history": [history]}, "status": "ok"}
        bundle = evidence.build_bundle({"public_records": {"macro": record}}, "text", "test",
                                       datetime.fromtimestamp(now, timezone.utc).isoformat())
        self.assertEqual(["missing_publication_time"], evidence.validate_bundle(bundle,"text")["dataset_reasons"]["macro"])
        # Future observation dates still fail; contract dates are not exempted globally.
        for key in ("auction_date", "maturity_date"):
            bad = copy.deepcopy(bundle)
            bad["datasets"]["macro"]["rows"][0]["rows"][0][key] = "2056-08-15"
            bad["bundle_id"] = "btc:" + hashlib.sha256(evidence.canonical_bytes(
                {k:v for k,v in bad.items() if k != "bundle_id"})).hexdigest()
            self.assertIn("data_date_after_generation", evidence.validate_bundle(bad,"text")["dataset_reasons"]["macro"])

    def test_funding_fetch_dispatches_endpoints_and_retains_history(self):
        current_ms = 2000 * collection.HOUR_MS
        rows = [{"fundingTime": current_ms - (30-i)*8*collection.HOUR_MS, "fundingRate": "0.0001"}
                for i in range(31)]
        def get(path, params=None):
            if path == "/fapi/v1/fundingInfo":
                return [{"symbol": bot.BINANCE_SYMBOL, "fundingIntervalHours": 8}]
            self.assertEqual("/fapi/v1/fundingRate", path)
            return rows
        with patch.object(bot, "binance_get", side_effect=get), patch.object(bot.time, "time", return_value=current_ms/1000+1):
            record = bot.fetch_funding_record()
        self.assertAlmostEqual(0.01, record["value"]["funding_8h_pct"])
        self.assertAlmostEqual(0.03, record["value"]["funding_24h_pct"])
        self.assertEqual(31, len(record["value"]["history"]))
        self.assertTrue(record["value"]["window_quality"]["complete"])
        self.assertEqual("current_only", record["value"]["current_interval_metadata_scope"])

    def test_spot_240_closed_rows_and_future_candle_excluded(self):
        hour = collection.HOUR_MS
        raw = [[i*hour, "100", "102", "99", "101", "2", (i+1)*hour-1,
                "100", "1", "1", "60", "0"] for i in range(1, 242)]
        with patch.object(bot, "binance_spot_get", return_value=raw) as getter, patch.object(bot.time, "time", return_value=241*3600):
            record = bot.fetch_spot_cvd_record()
        rows = record["value"]["hourly_flow"]
        self.assertEqual(240, len(rows))
        self.assertTrue(record["history_window"]["complete"])
        self.assertEqual(241, getter.call_args.args[1]["limit"])
        self.assertEqual(241*hour-1, rows[-1]["close_time"])
        self.assertEqual(60, rows[-1]["taker_buy_quote"])

    def test_basis_and_quote_use_source_timestamp_and_preserve_oi_quantity(self):
        with patch.object(bot, "binance_get", return_value={"time": 1000, "markPrice": "100", "indexPrice": "100"}):
            self.assertEqual(1000, bot.fetch_basis_record()["source_as_of_ms"])
        with patch.object(bot, "binance_get", return_value={"markPrice": "100", "indexPrice": "0"}):
            record = bot.fetch_basis_record()
            self.assertIsNone(record["source_as_of_ms"])
            self.assertIsNone(record["value"]["basis_pct"])
        with patch.object(bot, "binance_spot_get", return_value={"lastPrice": "100", "closeTime": 1234}), patch.object(bot.time, "time", return_value=9):
            quote = bot.get_binance_24h_market()
        self.assertEqual(1234, quote["quote_source_as_of_ms"])
        self.assertEqual(9, quote["quote_retrieved_at"])
        with patch.object(bot, "HYPERLIQUID_MODE", "shadow"), patch.object(bot, "get_btc_oi", return_value=(7, 100, 700)):
            self.assertEqual(7, bot.get_current_derivatives_market({}, [], 100)["oi_btc"])

    def test_etf_fetch_history_date_is_independent_from_current_metrics(self):
        history = [{"date": (datetime(2026, 8, 1)+timedelta(days=i)).strftime("%Y-%m-%d"),
                    "totalNetInflow": 10} for i in range(30)]
        current = {"dailyNetInflow": {"value": 5, "lastUpdateDate": "2026-09-01"},
                   "totalBtcHoldings": {"value": 1, "lastUpdateDate": "2026-09-01"},
                   "totalNetAssets": {"value": 1000, "lastUpdateDate": "2026-09-01"}}
        def post(url, payload):
            return {"data": history if url.endswith("historicalInflowChart") else current}
        with patch.object(bot, "public_post_json", side_effect=post):
            record = bot.fetch_etf_record({})
        self.assertEqual(50, record["value"]["flow_5d_usd"])
        self.assertEqual(300, record["value"]["flow_30d_usd"])
        self.assertEqual("2026-08-30", record["source_date"]["flow_30d"])
        self.assertEqual("2026-09-01", record["source_date"]["flow_1d"])

    def test_etf_daily_fallback_exports_actual_source_and_retains_zero(self):
        history = [{"date": "2026-09-03", "totalNetInflow": 17}]
        for current_value, expected_value, expected_source, expected_date in (
                (None, 17, "history", "2026-09-03"),
                (0, 0, "current", "2026-09-04"),
                (5, 5, "current", "2026-09-04")):
            with self.subTest(current=current_value):
                current = {"dailyNetInflow": {"value": current_value, "lastUpdateDate": "2026-09-04"}}
                clock = [1788501600.0]
                def post(url, payload):
                    clock[0] += 1
                    return {"data": history if url.endswith("historicalInflowChart") else current}
                with patch.object(bot, "public_post_json", side_effect=post), \
                     patch.object(bot.time, "time", side_effect=lambda: clock[0]):
                    record = bot.fetch_etf_record({})
                self.assertEqual(expected_value, record["value"]["flow_1d_usd"])
                self.assertEqual(expected_source, record["flow_1d_source"])
                self.assertEqual(expected_date, record["source_date"]["flow_1d"])
                self.assertEqual(1788501601, record["current_retrieved_at"])
                self.assertEqual(1788501602, record["history_retrieved_at"])
                bundle = evidence.build_bundle({"public_records": {"etf": record}}, "test", "test",
                                               "2026-09-04T06:00:02+00:00")
                sid = "public_etf_history" if expected_source == "history" else "public_etf"
                other = "public_etf" if expected_source == "history" else "public_etf_history"
                self.assertEqual([sid], bundle["datasets"]["etf"]["field_sources"]["flow_1d_usd"])
                self.assertEqual(expected_date, bundle["sources"][sid]["source_dates"]["flow_1d"])
                self.assertNotIn("flow_1d", bundle["sources"][other]["source_dates"])

    def test_hyperliquid_funding_freshness_survives_primary_source_selection(self):
        now = 1788501600
        limit = 3600 + bot.HYPERLIQUID_CTX_STALE_SEC
        for age, expected_reason in ((0, None), (limit, None), (limit + 1, "source_age_exceeded"),
                                     (90 * 60, "source_age_exceeded"), (-1, "source_time_in_future")):
            with self.subTest(age=age):
                latest_ms = (now - age) * 1000
                history = [{"fundingRate": "0", "time": latest_ms - i * 3600000}
                           for i in range(23, -1, -1)]
                predictions = [[bot.HYPERLIQUID_COIN, [["HlPerp", {
                    "fundingRate": "0", "fundingIntervalHours": 1}]]]]
                with patch.object(bot, "BTC_PUSH_HYPERLIQUID_ENABLED", True), \
                     patch.object(bot, "hyperliquid_post_json", side_effect=[history, predictions]), \
                     patch.object(bot.time, "time", return_value=now), \
                     patch.object(bot, "now_ms", return_value=now * 1000):
                    record = bot.fetch_hyperliquid_funding_record()
                    self.assertEqual(expected_reason, record["source_stale_reason"])
                    self.assertEqual(expected_reason is None, bot.record_is_current(record))
                    # Selection must independently reject old cache records too.
                    record.pop("source_stale")
                    record.pop("source_stale_reason")
                    with patch.object(bot, "HYPERLIQUID_MODE", "primary"), \
                         patch.object(bot, "build_hyperliquid_oi_receipt", return_value={"primary_ready": True}):
                        selected = bot.select_public_records({}, {"public": {"hyperliquid_funding": record}})
                self.assertEqual(expected_reason is None, bot.record_is_current(selected["funding"]))
                self.assertEqual(expected_reason, selected["hyperliquid_funding"].get("source_stale_reason"))
                if expected_reason is None and latest_ms % 3600000 == 0:
                    self.assertEqual(0, selected["funding"]["value"]["funding_24h_pct"])
                elif latest_ms % 3600000 not in range(101):
                    self.assertIsNone(selected["funding"]["value"].get("funding_24h_pct"))

    def test_hyperliquid_basis_never_presents_receipt_as_source_time(self):
        ctx = {"received_at_ms": 100000, "mark_px": 100, "oracle_px": 100}
        with patch.object(bot, "BTC_PUSH_HYPERLIQUID_ENABLED", True), patch.object(bot, "ensure_hyperliquid_latest_ctx", return_value=ctx), patch.object(bot, "now_ms", return_value=100001):
            record = bot.fetch_hyperliquid_basis_record({})
        self.assertIsNone(record["source_as_of_ms"])
        self.assertEqual("received_at", record["time_basis"])
        self.assertTrue(record["source_time_missing"])

    def test_macro_preserves_series_provenance_cache_and_unknown_publication(self):
        today = datetime.now(timezone.utc).date()
        series = [{"date": (today-timedelta(days=i)).isoformat(), "value": 4.0}
                  for i in range(65, -1, -1)]
        with patch.object(bot, "fetch_yahoo_chart_series", return_value=series), \
             patch.object(bot, "fetch_treasury_xml_series", return_value=series), \
             patch.object(bot, "fetch_fred_series", return_value=series), \
             patch.object(bot, "fetch_fed_status_value", return_value={}), \
             patch.object(bot, "fetch_m2_liquidity_value", return_value={}), \
             patch.object(bot, "fetch_fedwatch_value", return_value={}), \
             patch.object(bot, "fetch_long_term_macro_value", return_value={}), \
             patch.object(bot, "fetch_treasury_buyback_value", return_value={}):
            state = {}
            record = bot.fetch_macro_record(state)
            self.assertEqual(record, bot.fetch_macro_record(state))
        self.assertTrue(record["value"]["history"])
        self.assertTrue(record["publication_time_missing"])
        self.assertIsNone(record["source_as_of_ms"])
        bundle = evidence.build_bundle({"public_records": {"macro": record}}, "test", "test",
                                       datetime.now(timezone.utc).isoformat())
        self.assertNotIn("public_macro", bundle["sources"])
        self.assertIn("macro_yahoo_us10y", bundle["datasets"]["macro"]["source_ids"])
        self.assertEqual(["macro_yahoo_us10y"], bundle["datasets"]["macro"]["field_sources"]["us10y"])
        check = evidence.validate_bundle(bundle, "test")
        self.assertIn("missing_publication_time", check["dataset_reasons"]["macro"])



    def test_macro_partial_yahoo_keeps_quote_and_maps_treasury_delta(self):
        today = datetime.now(timezone.utc).date()
        short = [{"date": (today-timedelta(days=i)).isoformat(), "value": 4.0}
                 for i in (1, 0)]
        long = [{"date": (today-timedelta(days=i)).isoformat(), "value": 5.0}
                for i in range(65, -1, -1)]
        with patch.object(bot, "fetch_yahoo_chart_series", return_value=short), \
             patch.object(bot, "fetch_treasury_xml_series", return_value=long), \
             patch.object(bot, "fetch_fred_series", return_value=long), \
             patch.object(bot, "fetch_fed_status_value", return_value={}), \
             patch.object(bot, "fetch_m2_liquidity_value", return_value={}), \
             patch.object(bot, "fetch_fedwatch_value", return_value={}), \
             patch.object(bot, "fetch_long_term_macro_value", return_value={}), \
             patch.object(bot, "fetch_treasury_buyback_value", return_value={}):
            record = bot.fetch_macro_record({})
        value = record["value"]
        self.assertEqual(4, value["us10y"])
        self.assertEqual("yahoo_us10y", value["field_sources"]["us10y"])
        self.assertEqual("yahoo_us10y", value["field_sources"]["us10y_1d_pp"])
        self.assertEqual("treasury_us10y", value["field_sources"]["us10y_30d_pp"])

    def test_fedwatch_manual_value_has_no_fabricated_publication_date(self):
        with patch.dict(bot.os.environ, {"BTC_PUSH_FED_POLICY_OVERRIDE": "Neutral"}):
            value = bot.fetch_fedwatch_value()
        self.assertIsNone(value["source_date"])
        self.assertIsNone(value["published_at"])
        self.assertIsNone(value["retrieved_at"])
        self.assertEqual("local_configuration", value["access_scope"])


if __name__ == "__main__":
    unittest.main()
