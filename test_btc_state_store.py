import copy
import hashlib
import json
import multiprocessing
import os
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

import btc_state_store as storage
from btc_state_store import StateStore, StateStorageError, MigrationRequiredError


def _append_worker(path, prefix, start):
    store = StateStore(path)
    start.wait(10)
    for index in range(8):
        def append(value):
            value.setdefault("events", []).append({"id": f"{prefix}-{index}", "ts_ms": index + 1})
            value["updated_at_ms"] = index + 1
            return value
        store.update_domain("liquidation_ws", append)


class StateStoreTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path = os.path.join(self.temporary.name, "state.json")
        self.store = StateStore(self.path)

    def legacy(self):
        record = {"source": "spot", "source_as_of_ms": 3000,
                  "value": {"cvd_4h_pct": 12, "hourly_flow": [{"close_time": 3000}]}}
        return {
            "liquidation_ws": {"stream": "stream-a", "updated_at_ms": 1000,
                               "events": [{"id": "old", "ts_ms": 1000}], "event_count_total": 1},
            "hyperliquid_derivatives": {"coin": "BTC", "updated_at_ms": 60000,
                                        "minute_samples": [{"minute_ms": 60000, "received_at_ms": 61000}]},
            "public_snapshots": [{"ts": 1, "public": {"spot_cvd": copy.deepcopy(record)}},
                                 {"ts": 2, "public": {"spot_cvd": copy.deepcopy(record)}}],
            "event_logs": [{"ts": 1, "market_time_context": {"source": "retained"},
                            "etf_rule_eligible": False, "etf_source_date": "2026-07-24",
                            "etf_rule_ineligible_reason": "source_stale",
                            "market_patterns": {"history": [1, 2]},
                            "public_snapshot": {"public": {"spot_cvd": copy.deepcopy(record)}}}],
            "report": {"public_records": {"spot_cvd": copy.deepcopy(record)}},
        }

    def write_legacy(self):
        value = self.legacy()
        value["event_logs"][0]["liquidation_ws"] = copy.deepcopy(value["liquidation_ws"])
        value["event_logs"][0]["hyperliquid_derivatives"] = copy.deepcopy(value["hyperliquid_derivatives"])
        value["report"]["liquidation_ws"] = copy.deepcopy(value["liquidation_ws"])
        with open(self.path, "w") as stream:
            json.dump(value, stream, indent=2)
        return value

    def test_read_legacy_is_read_only_and_writes_require_explicit_migration(self):
        original = self.write_legacy()
        before = os.listdir(self.temporary.name)
        self.assertEqual(original, self.store.load())
        self.assertEqual(before, os.listdir(self.temporary.name))
        with self.assertRaises(MigrationRequiredError):
            self.store.save(original)
        with self.assertRaises(MigrationRequiredError):
            self.store.update_domain("liquidation_ws", lambda value: value)
        with open(self.path) as stream:
            self.assertEqual(original, json.load(stream))

    def test_migration_preserves_backup_domain_values_and_business_receipts(self):
        original = self.write_legacy()
        with open(self.path, "rb") as stream:
            before = stream.read()
        receipt = self.store.migrate()
        with open(receipt["backup"], "rb") as stream:
            self.assertEqual(before, stream.read())
        loaded = self.store.load()
        for domain in storage.DOMAINS:
            self.assertEqual(original[domain], loaded[domain])
        with open(self.path) as stream:
            persisted = json.load(stream)
        self.assertNotIn("liquidation_ws", persisted)
        self.assertNotIn("events", persisted["report"]["liquidation_ws"])
        event = persisted["event_logs"][0]
        self.assertNotIn("events", event["liquidation_ws"])
        self.assertEqual(1, event["liquidation_ws"]["events_ref"]["count"])
        self.assertEqual("migration_backup", event["liquidation_ws"]["events_ref"]["resolution"])
        self.assertEqual(receipt["backup"], event["liquidation_ws"]["events_ref"]["archive"]["path"])
        self.assertEqual("event_logs[0].liquidation_ws.events", event["liquidation_ws"]["events_ref"]["json_path"])
        self.assertEqual(original["event_logs"][0]["market_patterns"], event["market_patterns"])
        for key in ("market_time_context", "etf_rule_eligible", "etf_source_date", "etf_rule_ineligible_reason"):
            self.assertEqual(original["event_logs"][0][key], event[key])
        self.assertFalse(self.store.migrate()["migrated"])
        self.assertEqual(1, len([name for name in os.listdir(self.temporary.name) if name.endswith(".bak")]))

    def test_interrupted_migration_reuses_backup_and_never_reverts_newer_sidecar(self):
        original = self.write_legacy()
        real_write = storage._atomic_write
        for failed_path in (self.store.domain_path("liquidation_ws"),
                            self.store.domain_path("hyperliquid_derivatives"), self.path):
            def fail(path, value):
                if path == failed_path:
                    raise OSError("injected write failure")
                real_write(path, value)
            with patch.object(storage, "_atomic_write", side_effect=fail):
                with self.assertRaises(OSError):
                    self.store.migrate()
            with open(self.path) as stream:
                self.assertEqual(original, json.load(stream))
            with self.assertRaises(MigrationRequiredError):
                self.store.update_domain("liquidation_ws", lambda value: value)
        newer = copy.deepcopy(original["liquidation_ws"])
        newer["events"].append({"id": "new", "ts_ms": 2000})
        newer["updated_at_ms"] = 2000
        self.store._write_domain("liquidation_ws", newer)
        self.store.migrate()
        self.assertEqual(["old", "new"], [row["id"] for row in self.store.load()["liquidation_ws"]["events"]])

    def test_corrupt_backup_stops_before_replacing_legacy(self):
        original = self.write_legacy()
        with open(self.path, "rb") as stream:
            digest = hashlib.sha256(stream.read()).hexdigest()
        with open(self.path + ".pre-split-v1." + digest[:16] + ".bak", "w") as stream:
            stream.write("broken")
        with self.assertRaises(StateStorageError):
            self.store.migrate()
        self.assertEqual(original, self.store.load())

    def test_latest_public_raw_is_retained_without_removing_historical_aggregates(self):
        original = self.legacy()
        original["public_snapshots"].append({"ts": 3, "public": {"spot_cvd": {"status": "unavailable"}}})
        result = storage.compact_state(original)
        snapshots = result["public_snapshots"]
        older = snapshots[0]["public"]["spot_cvd"]["value"]
        current = snapshots[1]["public"]["spot_cvd"]["value"]
        self.assertNotIn("hourly_flow", older)
        self.assertEqual(12, older["cvd_4h_pct"])
        self.assertEqual(1, older["hourly_flow_ref"]["count"])
        self.assertEqual([{ "close_time": 3000}], current["hourly_flow"])
        self.assertIn("hourly_flow", result["report"]["public_records"]["spot_cvd"]["value"])
        self.assertIn("hourly_flow", original["public_snapshots"][0]["public"]["spot_cvd"]["value"])

    def test_stale_report_save_preserves_both_new_collector_domains(self):
        self.store.save(self.legacy())
        stale = self.store.load()
        def event(value):
            value["events"].append({"id": "new", "ts_ms": 2000})
            value["updated_at_ms"] = 2000
            return value
        def sample(value):
            value["minute_samples"].append({"minute_ms": 120000, "received_at_ms": 121000})
            value["updated_at_ms"] = 121000
            return value
        self.store.update_domain("liquidation_ws", event)
        self.store.update_domain("hyperliquid_derivatives", sample)
        stale["report"]["revision"] = 99
        self.store.save(stale)
        result = self.store.load()
        self.assertEqual(["old", "new"], [row["id"] for row in result["liquidation_ws"]["events"]])
        self.assertEqual(2, len(result["hyperliquid_derivatives"]["minute_samples"]))
        self.assertEqual(99, result["report"]["revision"])

    def test_thread_callback_and_report_save_are_serialized(self):
        self.store.save(self.legacy())
        stale = self.store.load()
        entered, release, saved = threading.Event(), threading.Event(), threading.Event()
        def append(value):
            entered.set()
            if not release.wait(5):
                raise RuntimeError("test barrier timed out")
            value["events"].append({"id": "interleaved", "ts_ms": 2000})
            value["updated_at_ms"] = 2000
            return value
        writer = threading.Thread(target=lambda: self.store.update_domain("liquidation_ws", append))
        reporter = threading.Thread(target=lambda: (self.store.save(stale), saved.set()))
        writer.start()
        self.assertTrue(entered.wait(5))
        reporter.start()
        self.assertFalse(saved.wait(0.05))
        release.set()
        writer.join(5)
        reporter.join(5)
        self.assertTrue(saved.is_set())
        self.assertEqual(2, len(self.store.load()["liquidation_ws"]["events"]))

    def test_process_writers_preserve_every_event(self):
        self.store.save({})
        context = multiprocessing.get_context("spawn")
        start = context.Event()
        workers = [context.Process(target=_append_worker, args=(self.path, prefix, start)) for prefix in ("a", "b")]
        for worker in workers:
            worker.start()
        start.set()
        for worker in workers:
            worker.join(15)
            if worker.is_alive():
                worker.terminate()
                worker.join()
            self.assertEqual(0, worker.exitcode)
        events = self.store.load()["liquidation_ws"]["events"]
        self.assertEqual(16, len({event["id"] for event in events}))

    def test_domain_update_does_not_read_report_or_other_domain(self):
        self.store.save({"report": {"large": "x" * 100000}})
        reads = []
        real_read = storage._read
        def read(path, missing=None):
            reads.append(path)
            return real_read(path, missing)
        with patch.object(storage, "_read", side_effect=read):
            self.store.update_domain("liquidation_ws", lambda value: {"status": "connected"})
        self.assertEqual([self.store.domain_path("liquidation_ws")], reads)

    def test_missing_or_corrupt_new_sidecar_fails_closed(self):
        self.store.save({})
        os.unlink(self.store.domain_path("liquidation_ws"))
        with self.assertRaises(StateStorageError):
            self.store.load()
        with self.assertRaises(StateStorageError):
            self.store.save({})
        with open(self.store.domain_path("liquidation_ws"), "w") as stream:
            stream.write("not json")
        with self.assertRaises(StateStorageError):
            self.store.require_ready()

    def test_failed_replace_retains_previous_valid_domain(self):
        self.store.save(self.legacy())
        original = self.store.read_domain("liquidation_ws")
        with patch.object(storage.os, "replace", side_effect=OSError("disk unavailable")):
            with self.assertRaises(OSError):
                self.store.update_domain("liquidation_ws", lambda value: {"status": "new"})
        self.assertEqual(original, self.store.read_domain("liquidation_ws"))

    def test_source_change_and_reset_do_not_resurrect_old_events(self):
        self.store.save(self.legacy())
        stale = self.store.load()
        self.store.update_domain("liquidation_ws", lambda value: {
            "stream": "stream-b", "updated_at_ms": 2000, "events": [],
        })
        self.store.save(stale)
        self.assertEqual([], self.store.load()["liquidation_ws"]["events"])
        old = self.store.load()
        self.store.save({}, reset_collectors=True)
        self.store.save(old)
        self.assertEqual([], self.store.load()["liquidation_ws"].get("events", []))

    def test_report_writer_lock_does_not_block_collector_and_rejects_second_writer(self):
        self.store.save({})
        with self.store.writer_lock():
            self.store.update_domain("liquidation_ws", lambda value: {"status": "live"})
            with self.assertRaises(StateStorageError):
                with StateStore(self.path).writer_lock():
                    self.fail("second writer accepted")
        self.assertEqual("live", self.store.read_domain("liquidation_ws")["status"])

    def test_bot_wrappers_use_temporary_path_and_skip_repeated_minute_before_io(self):
        import btc_push as bot
        timestamp = int(time.time() * 1000)
        ctx = {"openInterest": "1000", "markPx": "64000", "oraclePx": "64010", "funding": "0.00001"}
        with patch.object(bot, "STATE_FILE", self.path), patch.object(bot, "BTC_PUSH_HYPERLIQUID_ENABLED", True):
            self.assertTrue(bot.persist_hyperliquid_ctx(ctx, timestamp))
            with patch.object(bot, "state_store", side_effect=AssertionError("duplicate minute touched storage")):
                self.assertFalse(bot.persist_hyperliquid_ctx(ctx, timestamp))
            fresh = bot.load_state()
            self.assertEqual(1, len(fresh["hyperliquid_derivatives"]["minute_samples"]))
            bot.save_state(fresh)
            self.assertEqual(1, len(bot.load_state()["hyperliquid_derivatives"]["minute_samples"]))

    def test_default_production_migration_rejects_old_daemon_and_scan_failure(self):
        import btc_push as bot
        production_path = os.path.expanduser("~/.btc_push_state_tokyo.json")
        listing = type("ProcessListing", (), {"stdout": "999999 python3 /synthetic/collector/btc_push.py\n"})()
        with patch.object(bot, "STATE_FILE", production_path), patch.object(bot.subprocess, "run", return_value=listing):
            with self.assertRaisesRegex(RuntimeError, "stopped BTC collectors"):
                bot.assert_migration_daemon_stopped()
        with patch.object(bot, "STATE_FILE", production_path), patch.object(bot.subprocess, "run", side_effect=OSError("ps unavailable")):
            with self.assertRaises(OSError):
                bot.assert_migration_daemon_stopped()
        with patch.object(bot, "STATE_FILE", self.path), patch.object(bot.subprocess, "run", side_effect=AssertionError("fixture scanned processes")):
            bot.assert_migration_daemon_stopped()


if __name__ == "__main__":
    unittest.main()
