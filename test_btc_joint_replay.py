"""Offline producer -> Telegram receipts -> reader -> auxiliary observation replay."""
import copy
import json
import os
import tempfile
import unittest
from pathlib import Path
from contextlib import ExitStack

from btc_delivery import DeliveryFailure, DeliveryStore
from btc_evidence import build_bundle, encode_bundle
from btc_evidence_reader import EvidenceReader


class JointReplayTest(unittest.TestCase):
    def test_full_fixture_attachment_retry_restart_and_out_of_order(self):
        root = Path(__file__).parent / "tests/fixtures/btc_evidence_v1"
        raw = (root / "valid.json").read_bytes()
        text = (root / "valid.txt").read_text(encoding="utf-8")
        bundle = json.loads(raw)
        now, calls = [1788451200.0], []
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            delivery = DeliveryStore(os.path.join(directory, "delivery.sqlite3"), clock=lambda: now[0])
            reader = EvidenceReader(os.path.join(directory, "reader.sqlite3"), -9001, {7001}, clock=lambda: now[0])
            stack.callback(lambda: delivery.close())
            stack.callback(lambda: reader.close())
            updates = []
            def message(mid):
                return {"message_id": mid, "date": int(now[0]), "chat": {"id": -9001}, "from": {"id": 7001}}
            def send_text(body):
                calls.append("text")
                updates.append({"update_id": 100, "channel_post": {**message(100), "text": body}})
                return {"message_id": 100, "date": int(now[0])}
            def unavailable(*args, **kwargs):
                calls.append("attachment_failed")
                raise DeliveryFailure("rate_limited", 2)
            def send_document(body, reply_to_message_id):
                calls.append("attachment")
                self.assertEqual(raw, body)
                self.assertEqual(100, reply_to_message_id)
                updates.append({"update_id": 101, "channel_post": {
                    **message(101), "document": {"file_id": "fixture", "file_unique_id": "fixture-v1",
                    "mime_type": "application/json", "file_size": len(body)},
                    "reply_to_message": {"message_id": reply_to_message_id}}})
                return {"message_id": 101, "date": int(now[0])}
            delivery.enqueue_report(bundle["report_id"], text, raw, {})
            delivery.deliver_one(send_text, send_document)
            delivery.deliver_one(send_text, unavailable)
            delivery.close()
            delivery = DeliveryStore(os.path.join(directory, "delivery.sqlite3"), clock=lambda: now[0])
            self.addCleanup(delivery.close)
            delivery.recover()
            now[0] += 31
            delivery.deliver_one(send_text, send_document)
            for update in reversed(updates):
                reader.ingest_update(update)
            reader.process_pending(lambda fid: raw)
            obs = reader.observations()
            self.assertEqual(["text", "attachment_failed", "attachment"], calls)
            self.assertEqual(1, len(obs))
            self.assertEqual("valid_auxiliary", obs[0]["validation"]["status"])
            self.assertEqual(0, obs[0]["independent_verified_sources"])
            self.assertEqual(100, obs[0]["transport"]["text_message_id"])
            self.assertEqual(101, obs[0]["transport"]["document_message_id"])
            for forbidden in ("claim_id", "decision", "positions", "mechanical_observation", "VERIFIED"):
                self.assertNotIn(forbidden, obs[0])
            for update in updates:
                self.assertEqual("duplicate", reader.ingest_update(update)["status"])
            self.assertEqual(1, len(reader.observations()))
            reader.close()
            # Cleanups must not close this already-closed instance twice.
            reader = EvidenceReader(os.path.join(directory, "reader.sqlite3"), -9001, {7001}, clock=lambda: now[0])
            self.assertEqual(1, len(reader.observations()))


if __name__ == "__main__":
    unittest.main()
