import copy
import hashlib
import unittest

import btc_evidence as evidence


class EvidenceContractTest(unittest.TestCase):
    def bundle(self):
        return evidence.build_bundle({}, "📊 辅助观察\n数据不足", "hour:1788500400",
                                     "2026-09-04T06:00:00+00:00")

    def rehash(self, bundle):
        body = {k: v for k, v in bundle.items() if k != "bundle_id"}
        bundle["bundle_id"] = "btc:" + hashlib.sha256(evidence.canonical_bytes(body)).hexdigest()

    def test_roundtrip_and_no_verified_claim(self):
        bundle = self.bundle()
        actual = evidence.decode_bundle(evidence.encode_bundle(bundle))
        validation = evidence.validate_bundle(actual, "📊 辅助观察\n数据不足")
        self.assertEqual(validation["status"], "degraded_auxiliary")
        observation = evidence.auxiliary_observation(actual, "📊 辅助观察\n数据不足")
        self.assertEqual(observation["independent_verified_sources"], 0)
        self.assertNotIn("mechanical_observation", observation)
        self.assertNotIn("evidence_claims", observation)

    def test_text_edit_and_body_tampering_rejected(self):
        bundle = self.bundle()
        self.assertIn("text_hash_mismatch", evidence.validate_bundle(bundle, "edited")["reasons"])
        bundle["report_id"] = "other"
        self.assertIn("bundle_hash_mismatch", evidence.validate_bundle(bundle)["reasons"])

    def test_unknown_schema_isolated(self):
        bundle = self.bundle()
        bundle["schema"] = "future"
        self.assertEqual(evidence.validate_bundle(bundle)["reasons"], ["unknown_schema"])

    def test_attachment_before_text_is_pending(self):
        validation = evidence.validate_bundle(self.bundle())
        self.assertEqual(validation["status"], "pending_text")
        self.assertIsNone(evidence.auxiliary_observation(self.bundle(), None))

    def test_duplicate_json_keys_and_nan_rejected(self):
        for raw in (b'{"a":1,"a":2}', b'{"a":NaN}'):
            with self.assertRaises(ValueError):
                evidence.decode_bundle(raw)

    def test_window_claim_recomputed(self):
        bundle = self.bundle()
        ds = bundle["datasets"]["futures_1h"]
        ds["window"].update(complete=True, actual_count=240)
        self.rehash(bundle)
        self.assertIn("window_mismatch", evidence.validate_bundle(bundle, "📊 辅助观察\n数据不足")["dataset_reasons"]["futures_1h"])

    def test_invalid_nested_values_do_not_crash(self):
        for change in ({"source_ids": [{}]}, {"quality": []},
                       {"window": {"complete": True, "timestamp_field": [], "interval_ms": "bad"}},
                       {"window": {"complete": True, "timestamp_field": "t", "interval_ms": "bad"}}):
            bundle = self.bundle()
            bundle["datasets"]["futures_1h"].update(change)
            self.rehash(bundle)
            self.assertNotEqual(evidence.validate_bundle(bundle, "📊 辅助观察\n数据不足")["status"], "valid_auxiliary")

    def test_spot_total_net_recovery_and_missing(self):
        rows = evidence._flow_rows([{"quote_volume": 100, "net_quote": 20},
                                    {"quote_volume": 0, "taker_buy_quote": 0},
                                    {"quote_volume": 100},
                                    {"quote_volume": 100, "taker_buy_quote": 101}])
        self.assertEqual(rows[0]["taker_buy_quote"], 60)
        self.assertEqual(rows[1]["net_taker_quote"], 0)
        self.assertIsNone(rows[2]["net_taker_quote"])
        self.assertIsNone(rows[3]["net_taker_quote"])




class EvidenceHardeningTest(unittest.TestCase):
    def valid(self):
        from pathlib import Path
        import sys
        sys.path.insert(0, str(Path(__file__).parent / "tests"))
        from collector_replay_fixture import protocol_fixture_directory
        root = protocol_fixture_directory()
        text = (root / "valid.txt").read_text(encoding="utf-8")
        bundle = evidence.decode_bundle((root / "valid.json").read_bytes())
        self.assertEqual("valid_auxiliary", evidence.validate_bundle(bundle, text)["status"])
        return bundle, text

    def validate_mutation(self, change):
        bundle, text = self.valid()
        change(bundle)
        body = {k: v for k, v in bundle.items() if k != "bundle_id"}
        bundle["bundle_id"] = "btc:" + hashlib.sha256(evidence.canonical_bytes(body)).hexdigest()
        result = evidence.validate_bundle(bundle, text)
        self.assertEqual(0, result["independent_verified_sources"])
        return result

    def test_source_failures_have_specific_reasons_from_valid_baseline(self):
        for field, value, reason in (
            ("endpoint_url", ["https://example.com"], "missing_public_endpoint"),
            ("endpoint_url", "https://127.0.0.1", "missing_public_endpoint"),
            ("canonical_origin", "binance.com", "canonical_origin_mismatch"),
            ("source_id", "other", "source_id_mismatch"),
            ("access_scope", "private", "invalid_access_scope"),
            ("retrieved_at", "garbage", "invalid_retrieved_at"),
            ("as_of", {"x": 1}, "invalid_as_of"),
            ("as_of", "2026-09-04T06:00:00", "invalid_as_of"),
            ("as_of", "2026-09-04T06:00:01+00:00", "source_time_order"),
            ("retrieved_at", "2026-09-05T06:00:00+00:00", "source_time_order"),
            ("source_dates", {"day": "2026-02-30"}, "invalid_source_dates"),
        ):
            with self.subTest(field=field, reason=reason):
                result = self.validate_mutation(lambda b: b["sources"]["futures_klines"].update({field: value}))
                self.assertIn(reason, result["dataset_reasons"]["futures_1h"])

    def test_empty_datasets_and_forged_single_window_degrade(self):
        empty = self.validate_mutation(lambda b: b.update(datasets={}))
        self.assertEqual("degraded_auxiliary", empty["status"])
        self.assertEqual(["empty_datasets"], empty["dataset_reasons"]["__bundle__"])
        result = self.validate_mutation(lambda b: b["datasets"]["futures_1h"].update(window={"complete": True}))
        self.assertIn("invalid_window", result["dataset_reasons"]["futures_1h"])
        def single(b):
            ds = b["datasets"]["spot_cvd"]
            ds["rows"] = ds["rows"][-1:]
            ds["window"] = evidence._window(ds["rows"], "close_time", 1, 3600000)
        result = self.validate_mutation(single)
        self.assertIn("window_contract_mismatch", result["dataset_reasons"]["spot_cvd"])

    def test_flow_and_oi_are_recomputed_and_true_zero_is_valid(self):
        for changes in ({"quote_volume": -1, "taker_buy_quote": 200},
                        {"net_taker_quote": 21}, {"taker_sell_quote": -1}):
            result = self.validate_mutation(lambda b: b["datasets"]["futures_1h"]["rows"][0].update(changes))
            self.assertIn("invalid_flow", result["dataset_reasons"]["futures_1h"])
        result = self.validate_mutation(lambda b: b["datasets"]["oi_1h"]["rows"][0].update(timestamp=10**400))
        self.assertIn("invalid_oi_alignment", result["dataset_reasons"]["oi_1h"])
        result = self.validate_mutation(lambda b: b["datasets"]["oi_1h"]["rows"][0].update(drift_ms=0))
        self.assertIn("invalid_oi_alignment", result["dataset_reasons"]["oi_1h"])
        zero = self.validate_mutation(lambda b: b["datasets"]["spot_cvd"]["rows"][0].update(
            quote_volume=0, taker_buy_quote=0, taker_sell_quote=0, net_taker_quote=0))
        self.assertEqual("valid_auxiliary", zero["status"])

    def test_bad_json_encodings_and_recursion_are_value_errors(self):
        for raw in ("{}".encode("utf-16"), b"["*1100+b"0"+b"]"*1100,
                    b'{"x":1e309}', b"\xef\xbb\xbf{}"):
            with self.subTest(raw_length=len(raw)), self.assertRaises(ValueError):
                evidence.decode_bundle(raw)

    def test_source_time_cannot_be_supplied_by_a_scheduled_event(self):
        result = self.validate_mutation(lambda b: b["sources"]["futures_klines"].update(
            as_of=None, source_dates={"nested": {"next_meeting_date": "2026-09-18"}}))
        self.assertIn("missing_source_time", result["dataset_reasons"]["futures_1h"])
        future = self.validate_mutation(lambda b: b["datasets"]["futures_1h"].update(
            values={"fed_next_meeting_date": "2026-09-18"}))
        self.assertEqual("valid_auxiliary", future["status"])

    def test_unknown_attachment_does_not_poison_next_valid_message(self):
        bundle, text = self.valid()
        unknown = copy.deepcopy(bundle)
        unknown["schema"] = "future"
        self.assertEqual(["unknown_schema"], evidence.validate_bundle(unknown, text)["reasons"])
        self.assertEqual("valid_auxiliary", evidence.validate_bundle(bundle, text)["status"])

    def test_current_quote_market_cap_and_fear_greed_are_separate_datasets(self):
        payload = {"market": {
            "price": 100, "mcap": 1000, "source": "Binance Spot 24hr", "mcap_source": "CoinGecko",
            "quote_endpoint_url": "https://api.binance.com/api/v3/ticker/24hr",
            "quote_retrieved_at": 1788501600, "quote_source_as_of_ms": 1788501599000,
            "mcap_endpoint_url": "https://api.coingecko.com/api/v3/coins/bitcoin",
            "mcap_retrieved_at": 1788501600, "mcap_source_as_of_ms": None,
        }, "fear_greed": {"available": True, "value": 50,
                          "endpoint_url": "https://api.alternative.me/fng/",
                          "retrieved_at": 1788501600, "source_as_of_ms": 1788500000000,
                          "history": [{"timestamp": 1788500000, "value": 50}]}}
        bundle = evidence.build_bundle(payload, "test", "test", "2026-09-04T06:00:10+00:00")
        self.assertEqual(100, bundle["datasets"]["market_current"]["values"]["price"])
        self.assertEqual(50, bundle["datasets"]["fear_greed"]["values"]["value"])
        self.assertIsNone(bundle["sources"]["market_cap"]["as_of"])
        validation = evidence.validate_bundle(bundle, "test")
        self.assertIn("missing_source_time", validation["dataset_reasons"]["market_cap"])

    def test_source_provider_and_row_time_must_match(self):
        for change, reason in (
            ({"endpoint_url": "https://unrelated.example/data", "canonical_origin": "unrelated.example"}, "source_contract_mismatch"),
            ({"endpoint_url": "https://fapi.binance.com/fapi/v1/premiumIndex"}, "source_contract_mismatch"),
            ({"venue": "Hyperliquid"}, "source_contract_mismatch"),
            ({"retrieved_at": "2026-08-01T00:00:00+00:00", "as_of": "2026-08-01T00:00:00+00:00"}, "observation_after_retrieval"),
            ({"as_of": "2026-09-04T04:00:00+00:00"}, "observation_after_as_of"),
        ):
            with self.subTest(change=change):
                result = self.validate_mutation(lambda b: b["sources"]["futures_klines"].update(change))
                self.assertIn(reason, result["dataset_reasons"]["futures_1h"])

    def funding(self, hl=True):
        bundle, text = self.valid()
        name = "hyperliquid_funding" if hl else "funding"
        sid = "public_" + name
        end = 1788501600000
        spacing, count = (3600000, 24) if hl else (8 * 3600000, 31)
        rows = [{"time": end - spacing * i, "rate": 0.0001} for i in reversed(range(count))]
        window = evidence._window(rows, "time", count, spacing)
        values = {"funding_8h_pct": 0.08 if hl else 0.01,
                  "funding_24h_pct": 0.24 if hl else 0.03,
                  "latest_funding_time_ms": end}
        if hl:
            values["funding_1h_pct"] = 0.01
        else:
            window["hours"] = 240
            values["latest_settled_rate_pct"] = 0.01
            values["settlement_windows"] = {"%dh" % h: evidence._window(
                rows[-(h // 8 + 1):], "time", h // 8 + 1, spacing) for h in (8, 24)}
        source = copy.deepcopy(bundle["sources"]["open_interest"])
        source.update(source_id=sid, venue="Hyperliquid" if hl else "Binance",
                      endpoint_url="https://api.hyperliquid.xyz/info" if hl else "https://fapi.binance.com/fapi/v1/fundingRate",
                      canonical_origin="api.hyperliquid.xyz" if hl else "fapi.binance.com")
        bundle["sources"][sid] = source
        bundle["datasets"][name] = {"source_ids": [sid], "rows": rows, "values": values,
                                     "window": window, "quality": {"status": "ok"}}
        self.seal(bundle)
        self.assertEqual("valid_auxiliary", evidence.validate_bundle(bundle, text)["status"])
        return bundle, text, name

    def seal(self, bundle):
        bundle["bundle_id"] = "btc:" + hashlib.sha256(evidence.canonical_bytes(
            {k: v for k, v in bundle.items() if k != "bundle_id"})).hexdigest()

    def test_funding_fixed_window_and_sum_against_positive_samples(self):
        for hl in (True, False):
            for mutation in ("single", "sum", "boundary", "timestamp_field"):
                with self.subTest(hl=hl, mutation=mutation):
                    bundle, text, name = self.funding(hl)
                    ds = bundle["datasets"][name]
                    if mutation == "single":
                        ds["rows"] = ds["rows"][-1:]
                        ds["window"] = evidence._window(ds["rows"], "time", 1, 1)
                    elif mutation == "sum":
                        ds["values"]["funding_24h_pct"] = 9999
                    elif mutation == "boundary":
                        ds["rows"] = ds["rows"][1:]
                        ds["window"] = evidence._window(ds["rows"], "time", len(ds["rows"]), ds["window"]["interval_ms"])
                    else:
                        ds["window"]["timestamp_field"] = "rate"
                    self.seal(bundle)
                    self.assertIn(name, evidence.validate_bundle(bundle, text)["dataset_reasons"])

    def test_funding_millisecond_drift_and_zero_are_valid_without_rewriting_times(self):
        for hl in (True, False):
            for drift in (-100, 80, 100):
                bundle, text, name = self.funding(hl)
                ds = bundle["datasets"][name]
                for i, row in enumerate(ds["rows"]):
                    row["time"] += drift if i % 2 else 0
                    row["rate"] = 0
                original = copy.deepcopy(ds["rows"])
                ds["window"] = evidence.settlement_window(ds["rows"], 24, 3600000)[0] if hl else evidence.settlement_window(ds["rows"], hours=240)[0]
                if not hl:
                    ds["window"]["hours"] = 240
                    ds["values"]["settlement_windows"] = {f"{h}h": evidence.settlement_window(ds["rows"], hours=h)[0] for h in (8,24)}
                for key in list(ds["values"]):
                    if key.startswith("funding_") or key == "latest_settled_rate_pct":
                        ds["values"][key] = 0
                ds["values"]["latest_funding_time_ms"] = ds["rows"][-1]["time"]
                bundle["sources"]["public_"+name]["as_of"] = evidence.iso_time(ds["rows"][-1]["time"]/1000)
                bundle["sources"]["public_"+name]["retrieved_at"] = evidence.iso_time((ds["rows"][-1]["time"]+1000)/1000)
                self.seal(bundle)
                result = evidence.validate_bundle(bundle,text)
                self.assertEqual("valid_auxiliary", result["status"], (hl, drift, result))
                self.assertEqual(original, ds["rows"])

    def test_funding_bad_buckets_and_not_yet_elapsed_bucket_remain_degraded(self):
        for hl in (True, False):
            for change in ("offset", "half_hour", "missing", "duplicate", "period", "future_raw", "future_bucket"):
                bundle, text, name = self.funding(hl)
                ds = bundle["datasets"][name]
                if change in ("offset", "half_hour"):
                    for row in ds["rows"]:
                        row["time"] += 101 if change == "offset" else 1800000
                elif change == "missing":
                    ds["rows"].pop(4)
                elif change == "duplicate":
                    ds["rows"].insert(4, dict(ds["rows"][4], time=ds["rows"][4]["time"]-1, rate=0))
                elif change == "period":
                    ds["rows"][4]["time"] += 3600000
                else:
                    end = ds["rows"][-1]["time"]
                    for row in ds["rows"]:
                        row["time"] -= 50
                    cutoff = end-25 if change == "future_bucket" else end-75
                    bundle["generated_at"] = evidence.iso_time(cutoff/1000)
                    source = bundle["sources"]["public_"+name]
                    source["retrieved_at"] = bundle["generated_at"]
                    source["as_of"] = evidence.iso_time(ds["rows"][-1]["time"]/1000)
                ds["window"] = evidence.settlement_window(ds["rows"], 24, 3600000)[0] if hl else evidence.settlement_window(ds["rows"], hours=240)[0]
                if not hl:
                    ds["window"]["hours"] = 240
                ds["values"]["latest_funding_time_ms"] = ds["rows"][-1]["time"]
                self.seal(bundle)
                self.assertIn(name, evidence.validate_bundle(bundle,text)["dataset_reasons"])

    def test_basis_is_point_measurement_and_recomputes_ratio(self):
        bundle, text = self.valid()
        source = copy.deepcopy(bundle["sources"]["open_interest"])
        source.update(source_id="public_basis", endpoint_url="https://fapi.binance.com/fapi/v1/premiumIndex")
        bundle["sources"]["public_basis"] = source
        ds = {"source_ids": ["public_basis"], "values": {"mark_price": 101, "index_price": 100, "basis_pct": 1}}
        bundle["datasets"]["basis"] = ds
        self.seal(bundle)
        self.assertEqual("valid_auxiliary", evidence.validate_bundle(bundle, text)["status"])
        ds["values"]["basis_pct"] = 9999
        ds["window"] = evidence._window([{"time": 1}], "time", 1, 1)
        self.seal(bundle)
        reasons = evidence.validate_bundle(bundle, text)["dataset_reasons"]["basis"]
        self.assertIn("basis_value_mismatch", reasons)
        self.assertIn("unexpected_window", reasons)

    def test_projection_drops_unknown_and_nested_claims_and_is_detached(self):
        bundle, text = self.valid()
        claim = {"decision": "BUY", "status": "VERIFIED", "independent_verified_sources": 4}
        ds = bundle["datasets"]["futures_1h"]
        ds["mechanical_observation"] = claim
        ds["rows"][0]["strategy"] = claim
        ds["rows"][0]["open"] = claim
        bundle["datasets"]["strategy_decision"] = {"source_ids": ["futures_klines"], "values": claim}
        bundle["sources"]["futures_klines"]["strategy"] = claim
        self.seal(bundle)
        projected = evidence.auxiliary_observation(bundle, text)
        encoded = evidence.canonical_bytes(projected)
        self.assertNotIn(b'"VERIFIED"', encoded)
        self.assertNotIn(b'"decision"', encoded)
        self.assertNotIn("strategy_decision", projected["datasets"])
        self.assertNotIn("strategy_decision", projected["validation"]["dataset_reasons"])
        self.assertNotIn("open", projected["datasets"]["futures_1h"]["rows"][0])
        before = projected["datasets"]["futures_1h"]["rows"][0]["quote_volume"]
        ds["rows"][0]["quote_volume"] = 123456
        self.assertEqual(before, projected["datasets"]["futures_1h"]["rows"][0]["quote_volume"])
        self.assertEqual(0, projected["independent_verified_sources"])

    def test_projection_revalidates_content_and_current_text(self):
        bundle, text = self.valid()
        old_validation = evidence.validate_bundle(bundle, text)
        self.assertIsNotNone(evidence.auxiliary_observation(bundle, text))
        self.assertIsNone(evidence.auxiliary_observation(bundle, text + " edited"))
        self.assertIsNone(evidence.auxiliary_observation(bundle, old_validation))
        bundle["datasets"]["futures_1h"]["rows"][0]["net_taker_quote"] = 123456
        self.assertIsNone(evidence.auxiliary_observation(bundle, text))

    def test_macro_date_provenance_and_projection_exclude_rules(self):
        bundle, text = self.valid()
        source = evidence._source("macro_WALCL", "WALCL", "https://fred.stlouisfed.org/graph/fredgraph.csv",
                                  None, "2026-09-04T06:00:00+00:00", None, {"observation": "2026-08-01"})
        self.assertEqual("FRED", source["venue"])
        bundle["sources"]["macro_WALCL"] = source
        bundle["datasets"]["macro"] = {"source_ids": ["macro_WALCL"], "values": {"m2_status": "VERIFIED"},
            "rows": [{"series_id": "WALCL", "rows": [{"date": "2026-09-03", "value": 1, "decision": "BUY"}]}]}
        self.seal(bundle)
        self.assertIn("observation_after_as_of", evidence.validate_bundle(bundle, text)["dataset_reasons"]["macro"])
        projected = evidence.auxiliary_observation(bundle, text)
        self.assertNotIn("m2_status", projected["datasets"]["macro"]["values"])
        self.assertEqual({"date": "2026-09-03", "value": 1}, projected["datasets"]["macro"]["rows"][0]["rows"][0])

    def test_degraded_bad_nested_shapes_can_still_be_projected(self):
        bundle, text = self.valid()
        bundle["datasets"]["futures_1h"].update(source_ids=None, rows=[None], values=[])
        self.seal(bundle)
        self.assertEqual("degraded_auxiliary", evidence.auxiliary_observation(bundle, text)["validation"]["status"])

    def test_unavailable_fear_greed_is_retained_for_audit(self):
        bundle = evidence.build_bundle({"fear_greed": {"available": False, "value": 12,
            "source_stale": True, "source_as_of_ms": 1788500000000}}, "text", "test",
            "2026-09-04T06:00:10+00:00")
        self.assertEqual(12, bundle["datasets"]["fear_greed"]["values"]["value"])
        reasons = evidence.validate_bundle(bundle, "text")["dataset_reasons"]["fear_greed"]
        self.assertIn("missing_data", reasons)
        self.assertIn("stale_or_cache_fallback", reasons)

    def test_invalid_measurements_degrade_and_are_not_projected_as_numbers(self):
        cases = (({"open": True}, "invalid_ohlc", "open"),
                 ({"close": None}, "invalid_ohlc", None),
                 ({"base_volume": -1}, "invalid_base_volume", "base_volume"),
                 ({"high": 98}, "invalid_ohlc", "high"),
                 ({"net_taker_quote": 999}, "invalid_flow", "net_taker_quote"))
        for mutation, reason, excluded in cases:
            with self.subTest(mutation=mutation):
                bundle, text = self.valid()
                bundle["datasets"]["futures_1h"]["rows"][0].update(mutation)
                self.seal(bundle)
                projected = evidence.auxiliary_observation(bundle, text)
                self.assertEqual("degraded_auxiliary", projected["validation"]["status"])
                self.assertIn(reason, projected["validation"]["dataset_reasons"]["futures_1h"])
                row = projected["datasets"]["futures_1h"]["rows"][0]
                if excluded:
                    self.assertNotIn(excluded, row)
                else:
                    self.assertIsNone(row["close"])
                if reason == "invalid_flow":
                    self.assertNotIn("flow_valid", row)
                self.assertEqual(mutation, {key: bundle["datasets"]["futures_1h"]["rows"][0][key]
                                             for key in mutation})
        bundle, text = self.valid()
        bundle["datasets"]["futures_1h"]["window"]["expected_count"] = True
        self.seal(bundle)
        projected = evidence.auxiliary_observation(bundle, text)
        self.assertIn("invalid_window", projected["validation"]["dataset_reasons"]["futures_1h"])
        self.assertNotIn("expected_count", projected["datasets"]["futures_1h"]["window"])

    def test_partial_ohlc_still_checks_every_available_price_pair(self):
        conflicts = (
            {"open": None, "high": 50, "low": 99, "close": 100},
            {"open": True, "high": 50, "low": 99, "close": 100},
            {"open": None, "high": 100, "low": 99, "close": 101},
            {"close": None, "high": 102, "low": 101, "open": 100},
            {"high": None, "low": 101, "open": 100, "close": 102},
            {"low": None, "high": 100, "open": 101, "close": 99},
        )
        for name in ("futures_1h", "spot_cvd"):
            for mutation in conflicts:
                with self.subTest(dataset=name, mutation=mutation):
                    bundle, text = self.valid()
                    bundle["datasets"][name]["rows"][0].update(mutation)
                    self.seal(bundle)
                    projected = evidence.auxiliary_observation(bundle, text)
                    self.assertIn("invalid_ohlc", projected["validation"]["dataset_reasons"][name])
                    row = projected["datasets"][name]["rows"][0]
                    for key, value in mutation.items():
                        if value is None:
                            self.assertIsNone(row[key])
                        else:
                            self.assertNotIn(key, row)
                    self.assertEqual(100, row["quote_volume"])

    def test_missing_ohlc_keeps_other_consistent_measurements(self):
        for missing in ("open", "high", "low", "close"):
            for absent in (False, True):
                bundle, text = self.valid()
                source = bundle["datasets"]["futures_1h"]["rows"][0]
                if absent:
                    del source[missing]
                else:
                    source[missing] = None
                self.seal(bundle)
                projected = evidence.auxiliary_observation(bundle, text)
                self.assertEqual("degraded_auxiliary", projected["validation"]["status"])
                row = projected["datasets"]["futures_1h"]["rows"][0]
                for key in ("open", "high", "low", "close"):
                    if key != missing or not absent:
                        self.assertEqual(source[key], row[key])
                    else:
                        self.assertNotIn(key, row)

    def test_oi_notional_alias_currency_and_true_zero(self):
        for mutation, reason in (({"notional_value": -99}, "invalid_oi_notional"),
                                 ({"notional_value": 99}, "oi_notional_alias_mismatch"),
                                 ({"notional_currency": "USD"}, "oi_notional_currency_mismatch"),
                                 ({"notional_value": True}, "invalid_oi_notional")):
            with self.subTest(mutation=mutation):
                bundle, text = self.valid()
                bundle["datasets"]["oi_1h"]["rows"][0].update(mutation)
                self.seal(bundle)
                projected = evidence.auxiliary_observation(bundle, text)
                self.assertIn(reason, projected["validation"]["dataset_reasons"]["oi_1h"])
                row = projected["datasets"]["oi_1h"]["rows"][0]
                self.assertNotIn("notional_value", row)
                self.assertNotIn("notional_usd", row)
                self.assertEqual(100, row["quantity_btc"])
        bundle, text = self.valid()
        bundle["datasets"]["oi_1h"]["rows"][0].update(quantity_btc=0, notional_value=0, notional_usd=0)
        bundle["datasets"]["spot_cvd"]["rows"][0].update(base_volume=0, quote_volume=0,
            taker_buy_quote=0, taker_sell_quote=0, net_taker_quote=0)
        self.seal(bundle)
        self.assertEqual("valid_auxiliary", evidence.validate_bundle(bundle, text)["status"])
        self.assertEqual(0, evidence.auxiliary_observation(bundle, text)["datasets"]["oi_1h"]["rows"][0]["notional_value"])

    def test_core_utc_hours_and_cross_dataset_alignment(self):
        for offset, reason in ((-1800000, "invalid_utc_hour_boundary"),
                               (-3600000, "core_hour_alignment_mismatch")):
            bundle, text = self.valid()
            ds = bundle["datasets"]["spot_cvd"]
            for row in ds["rows"]:
                row["open_time"] += offset
                row["close_time"] += offset
            ds["window"] = evidence._window(ds["rows"], "close_time", 240, 3600000)
            self.seal(bundle)
            self.assertIn(reason, evidence.validate_bundle(bundle, text)["dataset_reasons"]["spot_cvd"])

    def test_core_inclusive_and_exclusive_edges_share_hour_without_rewriting_oi(self):
        bundle, text = self.valid()
        oi_before = copy.deepcopy(bundle["datasets"]["oi_1h"]["rows"])
        ds = bundle["datasets"]["spot_cvd"]
        for i, row in enumerate(ds["rows"]):
            row["close_time"] += i % 2
        ds["window"] = evidence._window(ds["rows"], "close_time", 240, 3600000)
        bundle["sources"]["public_spot_cvd"]["as_of"] = evidence.iso_time(ds["rows"][-1]["close_time"] / 1000)
        self.seal(bundle)
        self.assertEqual("valid_auxiliary", evidence.validate_bundle(bundle, text)["status"])
        projected = evidence.auxiliary_observation(bundle, text)
        for original, actual in zip(oi_before, projected["datasets"]["oi_1h"]["rows"]):
            for key in ("timestamp", "close_time", "drift_ms"):
                self.assertEqual(original[key], actual[key])

    def test_each_missing_core_dataset_is_explicitly_degraded(self):
        for name in ("futures_1h", "oi_1h", "spot_cvd"):
            bundle, text = self.valid()
            del bundle["datasets"][name]
            self.seal(bundle)
            projected = evidence.auxiliary_observation(bundle, text)
            self.assertEqual("degraded_auxiliary", projected["validation"]["status"])
            self.assertEqual(["missing_dataset"], projected["validation"]["dataset_reasons"][name])
            self.assertNotIn(name, projected["datasets"])

    def etf_bundle(self, flow_source):
        from datetime import datetime, timedelta
        bundle, text = self.valid()
        rows = [{"date": (datetime(2026, 8, 4) + timedelta(days=i)).strftime("%Y-%m-%d"),
                 "totalNetInflow": i} for i in range(30)]
        record = {"status": "ok", "source": "SoSoValue", "venue": "SoSoValue",
            "endpoint_url": "https://api.sosovalue.xyz/openapi/v1/etf/us-btc-spot/currentEtfDataMetrics",
            "history_endpoint_url": "https://api.sosovalue.xyz/openapi/v1/etf/us-btc-spot/historicalInflowChart",
            "current_retrieved_at": 1788501598, "history_retrieved_at": 1788501600,
            "retrieved_at": 1788501600,
            "source_date": {"flow_1d": "2026-09-02", "flow_5d": "2026-09-02",
                            "flow_30d": "2026-09-02", "holdings": "2026-09-03"},
            "value": {"history": rows, "flow_1d_usd": 29 if flow_source == "history" else 0,
                      "flow_5d_usd": 135, "flow_30d_usd": 435}}
        if flow_source is not None:
            record["flow_1d_source"] = flow_source
        built = evidence.build_bundle({"public_records": {"etf": record}}, text, "etf-test", bundle["generated_at"])
        bundle["datasets"]["etf"] = built["datasets"]["etf"]
        for sid in ("public_etf", "public_etf_history"):
            bundle["sources"][sid] = built["sources"][sid]
        self.seal(bundle)
        return bundle, text

    def test_etf_provenance_current_zero_history_and_unknown_legacy(self):
        for origin, sid in (("current", "public_etf"), ("history", "public_etf_history")):
            bundle, text = self.etf_bundle(origin)
            self.assertEqual("valid_auxiliary", evidence.validate_bundle(bundle, text)["status"])
            projected = evidence.auxiliary_observation(bundle, text)
            self.assertEqual([sid], projected["datasets"]["etf"]["field_sources"]["flow_1d_usd"])
            self.assertEqual("2026-09-02", bundle["sources"][sid]["source_dates"]["flow_1d"])
            self.assertEqual(evidence.iso_time(1788501598), bundle["sources"]["public_etf"]["retrieved_at"])
            self.assertEqual(evidence.iso_time(1788501600), bundle["sources"]["public_etf_history"]["retrieved_at"])
        bundle, text = self.etf_bundle(None)
        result = evidence.validate_bundle(bundle, text)
        self.assertEqual(["daily_flow_source_unknown"], result["dataset_reasons"]["etf"])
        self.assertEqual([], bundle["datasets"]["etf"]["field_sources"]["flow_1d_usd"])
        bundle, text = self.etf_bundle("history")
        bundle["datasets"]["etf"]["values"]["flow_1d_usd"] = 999
        self.seal(bundle)
        self.assertIn("daily_flow_history_mismatch", evidence.validate_bundle(bundle, text)["dataset_reasons"]["etf"])


class BitboHoldingsContractTests(unittest.TestCase):
    def test_current_reader_nonobject_guard_is_preserved(self):
        for raw in ("null", "[]", "0", '"invalid"'):
            with self.assertRaisesRegex(ValueError, "evidence_bundle_must_be_object"):
                evidence.decode_bundle(raw)

    def bundle(self, amount=1.00000001, version=1):
        from pathlib import Path
        import sys
        sys.path.insert(0, str(Path(__file__).parent / "tests"))
        from collector_replay_fixture import make_payload, holdings_only_record, GENERATED_AT
        payload = make_payload()
        record = holdings_only_record(amount=amount)
        record["flow_background"] = {"value": {"flow_1d_usd": 99}, "source_date": {"flow_1d": "2026-08-20"}}
        payload["public_records"]["etf"] = record
        payload["evidence_version"] = version
        return evidence.build_bundle(payload, "test", "etf-test", GENERATED_AT)

    def rehash(self, bundle):
        bundle["bundle_id"] = "btc:" + hashlib.sha256(evidence.canonical_bytes(
            {key: value for key, value in bundle.items() if key != "bundle_id"})).hexdigest()

    def test_holdings_only_v1_v2_projection_does_not_require_flow_history(self):
        for version in (1, 2):
            for amount in (0, 1.00000001):
                bundle = self.bundle(amount, version)
                self.assertNotIn("etf", evidence.validate_bundle(bundle, "test")["dataset_reasons"])
                projection = evidence.auxiliary_observation(bundle, "test")
                ds = projection["datasets"]["etf"]
                self.assertEqual(["public_etf"], ds["source_ids"])
                self.assertAlmostEqual(amount * 12, ds["values"]["holdings_btc"])
                self.assertIsNone(ds["values"]["flow_1d_usd"])
                self.assertNotIn("public_etf_history", projection["sources"])
                self.assertNotIn("flow_background", ds)

    def test_invalid_holdings_and_attempted_flow_or_nav_degrade_etf(self):
        for key, value in (("flow_1d_usd", 0), ("flow_5d_usd", 1), ("flow_30d_usd", 1),
                           ("holdings_market_pct", 1), ("flow_1d_assets_pct", 0),
                           ("holdings_btc", 999), ("holdings_btc", -1), ("holdings_btc", True)):
            bundle = self.bundle(); bundle["datasets"]["etf"]["values"][key] = value
            self.rehash(bundle)
            validation = evidence.validate_bundle(bundle, "test")
            self.assertIn("etf", validation["dataset_reasons"])
            projection = evidence.auxiliary_observation(bundle, "test")
            self.assertIn("etf", projection["validation"]["dataset_reasons"])
        for change in ({"holdings_scope": ["IBIT"]}, {"holdings_rows": []}, {"rows": [{"date": "2026-09-03", "totalNetInflow": 1}]},
                       {"field_sources": {"holdings_btc": ["public_etf_history"]}}):
            bundle = self.bundle(); bundle["datasets"]["etf"].update(change); self.rehash(bundle)
            self.assertIn("etf", evidence.validate_bundle(bundle, "test")["dataset_reasons"])

    def test_missing_date_stale_and_cross_source_seven_day_are_rejected(self):
        for mutation in (lambda b: b["sources"]["public_etf"].update(source_dates={}),
                         lambda b: b["datasets"]["etf"]["quality"].update(source_stale=True),
                         lambda b: b["datasets"]["etf"]["values"].update(holding_7d_pct=1)):
            bundle = self.bundle(); mutation(bundle); self.rehash(bundle)
            self.assertIn("etf", evidence.validate_bundle(bundle, "test")["dataset_reasons"])
        from collector_replay_fixture import holdings_only_record
        baseline = holdings_only_record("2026-08-27", amount=1)
        basis = {key: baseline[key] for key in ("venue", "endpoint_url", "holdings_scope", "holdings_rows", "retrieved_at")}
        basis.update(source_date="2026-08-27", holdings_btc=12)
        bundle = self.bundle()
        ds = bundle["datasets"]["etf"]
        ds["values"]["holding_7d_pct"] = (ds["values"]["holdings_btc"] / 12 - 1) * 100
        ds["holding_7d_basis"] = basis
        self.rehash(bundle)
        self.assertNotIn("etf", evidence.validate_bundle(bundle, "test")["dataset_reasons"])
        for change in ({"venue": "SoSoValue"}, {"source_date": "2026-08-26"}, {"holdings_scope": ["IBIT"]}):
            mutated = copy.deepcopy(bundle)
            mutated["datasets"]["etf"]["holding_7d_basis"].update(change); self.rehash(mutated)
            self.assertIn("etf", evidence.validate_bundle(mutated, "test")["dataset_reasons"])


class AnonymousFlowEvidenceTests(unittest.TestCase):
    def bundle(self, version=1, no_holdings=False, missing_old_day=False):
        from pathlib import Path
        import sys
        sys.path.insert(0, str(Path(__file__).parent / "tests"))
        from collector_replay_fixture import make_payload, anonymous_etf_record, GENERATED_AT
        payload = make_payload()
        record = anonymous_etf_record()
        if no_holdings:
            record["value"].update(holdings_btc=None, holding_7d_pct=None)
            record["holdings_record"] = {"status": "unavailable"}
        if missing_old_day:
            del record["value"]["history"][0]
            record["value"]["flow_30d_usd"] = None
        payload["public_records"]["etf"] = record
        payload["evidence_version"] = version
        return evidence.build_bundle(payload, "flow-test", "anonymous-flow", GENERATED_AT)

    def rehash(self, bundle):
        bundle["bundle_id"] = "btc:" + hashlib.sha256(evidence.canonical_bytes(
            {key: value for key, value in bundle.items() if key != "bundle_id"})).hexdigest()

    def test_independent_sources_and_v1_v2_reader_projection(self):
        for version in (1, 2):
            for no_holdings in (False, True):
                bundle = self.bundle(version, no_holdings)
                validation = evidence.validate_bundle(bundle, "flow-test")
                self.assertNotIn("etf", validation["dataset_reasons"])
                result = evidence.auxiliary_observation(bundle, "flow-test")
                values = result["datasets"]["etf"]["values"]
                self.assertAlmostEqual(30.3703701, values["flow_30d_usd"])
                self.assertEqual("TFTC", result["sources"]["public_etf_history"]["venue"])
                if no_holdings:
                    self.assertNotIn("public_etf", result["sources"])
                    self.assertIsNone(values["holdings_btc"])
                else:
                    self.assertEqual("Bitbo", result["sources"]["public_etf"]["venue"])
                self.assertEqual(0, result["independent_verified_sources"])

    def test_partial_long_window_does_not_discard_holdings_or_daily_flow(self):
        bundle = self.bundle(missing_old_day=True)
        validation = evidence.validate_bundle(bundle, "flow-test")
        self.assertNotIn("etf", validation["dataset_reasons"])
        values = evidence.auxiliary_observation(bundle, "flow-test")["datasets"]["etf"]["values"]
        self.assertIsNone(values["flow_30d_usd"])
        self.assertIsNotNone(values["flow_5d_usd"])
        self.assertIsNotNone(values["holdings_btc"])

    def test_scope_date_units_source_and_window_forgery_are_rejected(self):
        for mutation in (
                lambda b: b["datasets"]["etf"].update(flow_scope=["IBIT"]),
                lambda b: b["datasets"]["etf"].update(units={"flow": "BTC", "holdings": "BTC"}),
                lambda b: b["datasets"]["etf"]["rows"][-1]["perEtfUsd"].pop("BTC"),
                lambda b: b["datasets"]["etf"]["rows"][-1].update(totalNetInflow=99),
                lambda b: b["datasets"]["etf"]["rows"].pop(-3),
                lambda b: b["datasets"]["etf"]["values"].update(flow_30d_usd=99),
                lambda b: b["datasets"]["etf"]["field_sources"].update(flow_1d_usd=["public_etf"]),
                lambda b: b["sources"]["public_etf_history"].update(venue="SoSoValue"),
                lambda b: b["sources"]["public_etf_history"]["source_dates"].update(flow_1d="2026-09-02"),
                lambda b: b["datasets"]["etf"]["values"].update(flow_1d_assets_pct=1)):
            bundle = self.bundle(); mutation(bundle); self.rehash(bundle)
            self.assertIn("etf", evidence.validate_bundle(bundle, "flow-test")["dataset_reasons"])
        bundle = self.bundle(missing_old_day=True)
        bundle["datasets"]["etf"]["values"]["flow_30d_usd"] = 1
        self.rehash(bundle)
        self.assertIn("incomplete_trading_day_samples", evidence.validate_bundle(bundle, "flow-test")["dataset_reasons"]["etf"])


if __name__ == "__main__":
    unittest.main()
