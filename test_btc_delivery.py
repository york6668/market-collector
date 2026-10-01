import copy
import json
import multiprocessing
import os
import sqlite3
import tempfile
import time
import unittest
from contextlib import ExitStack
from unittest.mock import Mock, patch

import requests

import btc_push as bot
import btc_runtime as runtime
from btc_delivery import DeliveryFailure, DeliveryStore, ReceiptPersistenceError, FailurePersistenceError
from btc_state_store import StateStore


def _recover_worker(path, started, finished):
    store = DeliveryStore(path)
    started.set()
    store.recover()
    finished.set()
    store.close()


class DeliveryStoreTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.now = 1000.0
        self.path = os.path.join(self.temp.name, "delivery.sqlite3")
        self.store = DeliveryStore(self.path, clock=lambda: self.now, retry_delay=10)
        self.text = Mock(return_value={"message_id": 11, "date": 1000})
        self.document = Mock(return_value={"message_id": 12, "date": 1001})

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def enqueue(self, report_id="hour:0", document=b'{"evidence":true}'):
        self.store.enqueue_report(report_id, "report", document, {"candidate": {"large": "x" * 10000}})

    def test_summary_and_document_receipts_apply_and_cleanup(self):
        self.enqueue()
        self.assertEqual("sent", self.store.deliver_one(self.text, self.document)["status"])
        apply = Mock()
        self.store.apply_pending(apply)
        self.assertEqual(1, apply.call_count)
        self.assertEqual("{}", self.store.get_report("hour:0")["commit_payload"])
        self.assertIsNotNone(self.store.get_report("hour:0")["document"])
        self.store.deliver_one(self.text, self.document)
        self.document.assert_called_once_with(b'{"evidence":true}', reply_to_message_id=11)
        self.assertIsNone(self.store.get_report("hour:0")["document"])
        self.store.apply_pending(apply)
        self.assertEqual(1, apply.call_count)
        self.assertIsNone(self.store.deliver_one(self.text, self.document))

    def test_known_not_delivered_retries_are_bounded_and_honor_retry_after(self):
        self.enqueue(document=None)
        self.text.side_effect = DeliveryFailure("rate_limited", retry_after=50)
        self.assertEqual("pending", self.store.deliver_one(self.text, self.document)["status"])
        self.assertEqual(1050, self.store.next_due_at())
        self.assertIsNone(self.store.deliver_one(self.text, self.document))
        self.now = 1050
        self.store.deliver_one(self.text, self.document)
        self.now = 1100
        self.assertEqual("failed", self.store.deliver_one(self.text, self.document)["status"])
        self.assertEqual(3, self.text.call_count)
        self.assertIsNone(self.store.next_due_at())

    def test_unknown_and_permanent_failures_are_not_retried(self):
        for reason, expected in (("unknown", "unknown"), ("permanent", "failed")):
            self.enqueue(reason, document=None)
            sender = Mock(side_effect=DeliveryFailure(reason))
            self.assertEqual(expected, self.store.deliver_one(sender, self.document)["status"])
            self.now += 1000
            self.assertIsNone(self.store.deliver_one(sender, self.document))
            sender.assert_called_once()

    def test_invalid_receipt_is_unknown_and_blocks_attachment(self):
        self.enqueue()
        self.text.return_value = None
        self.assertEqual("unknown", self.store.deliver_one(self.text, self.document)["status"])
        self.store.recover()
        self.assertIsNone(self.store.deliver_one(self.text, self.document))
        self.document.assert_not_called()

    def test_recovery_retires_resume_parts_and_preserves_receipts_and_unknowns(self):
        for asset in ('QQQ', 'VOO'):
            for index, status in enumerate(('pending', 'sending', 'sent', 'unknown', 'failed')):
                identity = f'{asset.lower()}:hour:0:resume:{index}'
                self.store.enqueue_report(identity, 'legacy', b'attachment', {'asset_id': asset})
                with self.store._transaction() as db:
                    db.execute('UPDATE reports SET summary_status=?, summary_receipt=? WHERE report_id=?',
                               (status, '{"message_id":11}', identity))
                    if status == 'unknown':
                        db.execute("UPDATE reports SET document_status='unknown' WHERE report_id=?", (identity,))
        self.store.recover()
        self.store.recover()
        for asset in ('QQQ', 'VOO'):
            for index, status in enumerate(('pending', 'sending', 'sent', 'unknown', 'failed')):
                with self.subTest(asset=asset, status=status):
                    report = self.store.get_report(f'{asset.lower()}:hour:0:resume:{index}')
                    self.assertEqual({'pending': 'expired', 'sending': 'unknown'}.get(status, status),
                                     report['summary_status'])
                    self.assertEqual('unknown' if status == 'unknown' else 'expired', report['document_status'])
                    self.assertEqual('{"message_id":11}', report['summary_receipt'])
                    self.assertEqual('legacy', report['text'])
                    self.assertEqual(b'attachment', report['document'])
            self.assertIsNone(self.store.deliver_one(self.text, self.document, asset_id=asset))
        self.text.assert_not_called()
        self.document.assert_not_called()

    def test_resume_report_cannot_be_claimed_without_startup_recovery(self):
        for asset in ('QQQ', 'VOO'):
            with self.subTest(asset=asset):
                identity = f'{asset.lower()}:hour:0:resume:300'
                self.store.enqueue_report(identity, 'legacy', b'attachment', {'asset_id': asset})
                self.assertIsNone(self.store.deliver_one(self.text, self.document, report_id=identity))
                report = self.store.get_report(identity)
                self.assertEqual('expired', report['summary_status'])
                self.assertEqual('expired', report['document_status'])
        self.text.assert_not_called()
        self.document.assert_not_called()

    def test_expiration_uses_report_hour_and_preserves_manual_and_terminal_reports(self):
        self.now = 3610
        for asset in ('BTC', 'QQQ', 'VOO', 'COMMON'):
            with self.subTest(asset=asset):
                prefix = 'hour:' if asset == 'BTC' else asset.lower() + ':hour:'
                identities = [prefix + str(hour) for hour in (0, 3600, -3600, -7200)]
                manual = 'manual:' + asset
                for identity in identities + [manual]:
                    self.store.enqueue_report(identity, 'text', b'attachment', {'asset_id': asset})
                with self.store._transaction() as db:
                    db.execute("UPDATE reports SET summary_status='unknown', document_status='unknown' "
                               "WHERE report_id=?", (identities[2],))
                    db.execute("UPDATE reports SET summary_status='sent', summary_receipt=? WHERE report_id=?",
                               ('{"message_id":11}', identities[3]))
                self.store.expire_scheduled(asset, 3600)
                self.assertEqual('expired', self.store.get_report(identities[0])['summary_status'])
                self.assertEqual('expired', self.store.get_report(identities[0])['document_status'])
                self.assertEqual('pending', self.store.get_report(identities[1])['summary_status'])
                self.assertEqual('pending', self.store.get_report(manual)['summary_status'])
                self.assertEqual('unknown', self.store.get_report(identities[2])['summary_status'])
                self.assertEqual('unknown', self.store.get_report(identities[2])['document_status'])
                self.assertEqual('sent', self.store.get_report(identities[3])['summary_status'])
                self.assertEqual('{"message_id":11}', self.store.get_report(identities[3])['summary_receipt'])
                self.assertEqual('expired', self.store.get_report(identities[3])['document_status'])

    def test_document_timeout_does_not_resend_summary(self):
        self.enqueue()
        self.store.deliver_one(self.text, self.document)
        self.document.side_effect = requests.ReadTimeout()
        self.assertEqual("unknown", self.store.deliver_one(self.text, self.document)["status"])
        self.store.recover()
        self.assertIsNone(self.store.deliver_one(self.text, self.document))
        self.text.assert_called_once()
        self.assertIsNotNone(self.store.get_report("hour:0")["document"])

    def test_receipt_save_failure_retries_only_local_write(self):
        self.enqueue(document=None)
        with patch.object(self.store, "record_sent", side_effect=sqlite3.OperationalError("disk unavailable")):
            with self.assertRaises(ReceiptPersistenceError):
                self.store.deliver_one(self.text, self.document)
        self.assertEqual("sending", self.store.get_report("hour:0")["summary_status"])
        self.assertIsNone(self.store.deliver_one(self.text, self.document))
        self.assertEqual("sent", self.store.get_report("hour:0")["summary_status"])
        self.text.assert_called_once()

    def test_process_loss_after_send_is_unknown(self):
        self.enqueue(document=None)
        with patch.object(self.store, "record_sent", side_effect=sqlite3.OperationalError("disk unavailable")):
            with self.assertRaises(ReceiptPersistenceError):
                self.store.deliver_one(self.text, self.document)
        other = DeliveryStore(self.path)
        try:
            other.recover()
            self.assertEqual("unknown", other.get_report("hour:0")["summary_status"])
            self.assertIsNone(other.deliver_one(self.text, self.document))
        finally:
            other.close()
        self.text.assert_called_once()

    def test_failure_status_write_recovery_retries_only_when_known_unsent(self):
        for reason in ("connect_timeout", "rate_limited", "unknown"):
            with self.subTest(reason=reason):
                report_id = "failure:" + reason
                self.enqueue(report_id, document=None)
                send = Mock(side_effect=DeliveryFailure(reason, retry_after=50))
                with patch.object(self.store, "record_failure", side_effect=sqlite3.OperationalError("temporary")):
                    with self.assertRaises(FailurePersistenceError):
                        self.store.deliver_one(send, self.document)
                self.assertEqual("sending", self.store.get_report(report_id)["summary_status"])
                self.assertIsNone(self.store.deliver_one(send, self.document))
                send.assert_called_once()
                expected = "unknown" if reason == "unknown" else "pending"
                self.assertEqual(expected, self.store.get_report(report_id)["summary_status"])
                self.assertEqual({}, self.store.pending_failures)
                if reason != "unknown":
                    self.now += 50
                    sent = Mock(return_value={"message_id": 91})
                    self.assertEqual("sent", self.store.deliver_one(sent, self.document)["status"])
                    sent.assert_called_once()

    def test_failure_write_committed_then_raised_does_not_extend_retry_or_replay(self):
        self.enqueue(document=None)
        real = self.store.record_failure
        def committed(claim, failure):
            real(claim, failure)
            raise sqlite3.OperationalError("after commit")
        with patch.object(self.store, "record_failure", side_effect=committed):
            with self.assertRaises(FailurePersistenceError):
                self.store.deliver_one(Mock(side_effect=DeliveryFailure("rate_limited", 50)), self.document)
        self.now += 20
        self.assertIsNone(self.store.deliver_one(self.text, self.document))
        self.assertEqual(1050, self.store.next_due_at())
        self.text.assert_not_called()
        self.assertEqual({}, self.store.pending_failures)

    def test_old_failure_attempt_cannot_overwrite_newer_success(self):
        self.enqueue(document=None)
        claim = self.store._claim_due()
        failure = DeliveryFailure("connect_timeout")
        self.store.record_failure(claim, failure)
        self.now += 10
        self.store.deliver_one(self.text, self.document)
        self.store.pending_failures[(claim["report_id"], claim["part"], claim["attempt"])] = (claim, failure)
        self.assertIsNone(self.store.deliver_one(self.text, self.document))
        report = self.store.get_report("hour:0")
        self.assertEqual(("sent", 2), (report["summary_status"], report["summary_attempts"]))
        self.text.assert_called_once()

    def test_lost_failure_receipt_on_restart_remains_unknown(self):
        self.enqueue(document=None)
        with patch.object(self.store, "record_failure", side_effect=sqlite3.OperationalError("temporary")):
            with self.assertRaises(FailurePersistenceError):
                self.store.deliver_one(Mock(side_effect=DeliveryFailure("connect_timeout")), self.document)
        self.store.close()
        self.store = DeliveryStore(self.path, clock=lambda: self.now)
        self.store.recover()
        self.assertEqual("unknown", self.store.get_report("hour:0")["summary_status"])
        self.assertIsNone(self.store.deliver_one(self.text, self.document))
        self.text.assert_not_called()

    def test_report_and_command_completion_commit_or_rollback_together(self):
        self.store.ingest_commands([{"update_id": 10, "message": {"chat": {"id": 7}, "text": "/btc"}}], 7, lambda _: "QUERY_PUSH")
        self.store.claim_command()
        real = self.store._finish_report_command
        def interrupted(db, update_id, report_id):
            real(db, update_id, report_id)
            raise sqlite3.OperationalError("before commit")
        with patch.object(self.store, "_finish_report_command", side_effect=interrupted):
            with self.assertRaises(sqlite3.OperationalError):
                self.store.enqueue_report("tg:10", "report", None, {}, command_id=10)
        self.assertIsNone(self.store.get_report("tg:10"))
        self.assertEqual("running", self.store.db.execute("SELECT status FROM commands").fetchone()[0])
        self.store.enqueue_report("tg:10", "report", None, {}, command_id=10)
        self.store.close()
        self.store = DeliveryStore(self.path, clock=lambda: self.now)
        self.store.recover(recover_commands=True)
        self.assertEqual("done", self.store.db.execute("SELECT status FROM commands").fetchone()[0])
        self.assertIsNone(self.store.claim_command())

    def test_existing_report_reconciles_running_command_after_local_failure(self):
        self.store.ingest_commands([{"update_id": n, "message": {"chat": {"id": 7}, "text": "/btc"}} for n in (10, 11)], 7, lambda _: "QUERY_PUSH")
        self.store.claim_command()
        self.store.enqueue_report("tg:10", "report", None, {})
        with patch.object(self.store, "_finish_report_command", side_effect=sqlite3.OperationalError("temporary")):
            with self.assertRaises(sqlite3.OperationalError):
                self.store.claim_command()
        self.assertEqual(11, self.store.claim_command()["update_id"])
        self.assertEqual("done", self.store.db.execute("SELECT status FROM commands WHERE update_id=10").fetchone()[0])
        self.assertEqual(0, self.store.get_report("tg:10")["summary_attempts"])

    def test_attachment_failure_write_recovery_keeps_summary_single(self):
        self.enqueue()
        self.store.deliver_one(self.text, self.document)
        self.document.side_effect = DeliveryFailure("rate_limited", 50)
        with patch.object(self.store, "record_failure", side_effect=sqlite3.OperationalError("temporary")):
            with self.assertRaises(FailurePersistenceError):
                self.store.deliver_one(self.text, self.document)
        self.store.recover()
        self.assertEqual("pending", self.store.get_report("hour:0")["document_status"])
        self.document.side_effect = None
        self.now += 50
        self.store.deliver_one(self.text, self.document)
        self.text.assert_called_once()
        self.assertEqual(2, self.document.call_count)
        self.assertEqual("sent", self.store.get_report("hour:0")["document_status"])

    def test_command_batch_offset_and_rollback(self):
        self.store.get_offset(100)
        updates = [{"update_id": n, "message": {"chat": {"id": 7}, "text": cmd}}
                   for n, cmd in ((100, "/btc"), (101, "/force"), (102, "/repair"))]
        self.assertEqual(3, self.store.ingest_commands(updates, 7, bot.parse_tg_command_text)["inserted"])
        self.assertEqual(103, self.store.get_offset())
        self.assertEqual(0, self.store.ingest_commands(updates, 7, bot.parse_tg_command_text)["inserted"])
        for n, cmd in ((100, "QUERY_PUSH"), (101, "FORCE_PUSH"), (102, "REPAIR")):
            item = self.store.claim_command()
            self.assertEqual((n, cmd), (item["update_id"], item["command"]))
            self.store.finish_command(n, {"report_id": f"tg:{n}"})
        self.assertIsNone(self.store.claim_command())
        broken = [{"update_id": 103, "message": {"chat": {"id": 7}, "text": "/btc"}}]
        with self.assertRaises(ValueError):
            self.store.ingest_commands(broken, 7, Mock(side_effect=ValueError("parse")))
        self.assertEqual(103, self.store.get_offset())
        self.assertIsNone(self.store.claim_command())

    def test_command_recovery_is_explicit(self):
        self.store.ingest_commands([{"update_id": 1, "message": {"chat": {"id": 7}, "text": "/btc"}}],
                                   7, bot.parse_tg_command_text)
        self.store.claim_command()
        self.store.recover()
        self.assertIsNone(self.store.claim_command())
        self.store.recover(recover_commands=True)
        self.assertEqual(1, self.store.claim_command()["update_id"])

    def test_idempotent_enqueue_remains_checkable_after_cleanup(self):
        self.enqueue(document=None)
        self.store.deliver_one(self.text, self.document)
        self.store.apply_pending(Mock())
        self.assertFalse(self.store.enqueue_report("hour:0", "report", None,
                                                   {"candidate": {"large": "x" * 10000}}))
        with self.assertRaises(ValueError):
            self.store.enqueue_report("hour:0", "changed", None, {})

    def test_worker_flock_prevents_recovering_another_process_inflight(self):
        self.enqueue(document=None)
        ctx = multiprocessing.get_context("spawn")
        started, finished = ctx.Event(), ctx.Event()
        child = ctx.Process(target=_recover_worker, args=(self.path, started, finished))

        def send(_):
            self.assertFalse(self.store.db.in_transaction)
            child.start()
            self.assertTrue(started.wait(10))
            self.assertFalse(finished.wait(0.15))
            # 其他 DB 事务可以在请求期间完成；独立 worker 锁不阻塞入队。
            self.store.enqueue_report("second", "another", None, {})
            return {"message_id": 11}

        try:
            self.store.deliver_one(send, self.document)
            child.join(10)
            self.assertFalse(child.is_alive())
            self.assertEqual(0, child.exitcode)
            self.assertTrue(finished.is_set())
            self.assertEqual("sent", self.store.get_report("hour:0")["summary_status"])
        finally:
            if child.is_alive():
                child.terminate()
                child.join()


class TelegramTransportTest(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(patch.object(runtime, "BOT_TOKEN", "test"))
        self.stack.enter_context(patch.object(runtime, "CHAT_ID_RAW", "7"))
        self.post = self.stack.enter_context(patch.object(runtime.requests, "post"))

    def test_send_returns_receipt_and_document_replies_to_message(self):
        self.post.return_value = Mock(status_code=200, json=Mock(return_value={"ok": True, "result": {"message_id": 1}}))
        self.assertEqual(1, runtime.tg_send("hello")["message_id"])
        self.assertEqual((5, 25), self.post.call_args.kwargs["timeout"])
        runtime.tg_send_document(b"{}", 1)
        request = self.post.call_args.kwargs
        self.assertEqual((30, 60), request["timeout"])
        self.assertEqual({"message_id": 1}, json.loads(request["data"]["reply_parameters"]))
        self.assertEqual("application/json", request["files"]["document"][2])

    def test_connect_timeout_only_and_read_timeout_unknown(self):
        for failure, reason in ((requests.ConnectTimeout(), "connect_timeout"),
                                (requests.ReadTimeout(), "unknown"),
                                (requests.ConnectionError(), "unknown")):
            for send in (lambda: runtime.tg_send("test"),
                         lambda: runtime.tg_send_document(b"{}", 1)):
                self.post.side_effect = failure
                with self.assertRaises(DeliveryFailure) as raised:
                    send()
                self.assertEqual(reason, raised.exception.reason)

    def test_explicit_rate_limit_and_permanent_rejection(self):
        for code, reason in ((429, "rate_limited"), (403, "permanent")):
            self.post.return_value = Mock(status_code=code, json=Mock(return_value={
                "ok": False, "error_code": code, "parameters": {"retry_after": 23}}))
            with self.assertRaises(DeliveryFailure) as raised:
                runtime.tg_send("test")
            self.assertEqual(reason, raised.exception.reason)
            if code == 429:
                self.assertEqual(23, raised.exception.retry_after)

    def test_invalid_success_response_is_unknown(self):
        for body in (None, {"ok": True}, {"ok": True, "result": {"message_id": True}}):
            self.post.return_value = Mock(status_code=200, json=Mock(return_value=body))
            with self.assertRaises(DeliveryFailure) as raised:
                runtime.tg_send("test")
            self.assertEqual("unknown", raised.exception.reason)


class DeliveryIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.stack = ExitStack()
        self.state_path = os.path.join(self.temp.name, "state.json")
        self.state = StateStore(self.state_path)
        self.state.save({"day_tokyo": bot.tokyo_day_str(), "day_push_count": 5})
        self.now = time.time()
        self.stack.enter_context(patch.object(bot.time, "time", side_effect=lambda: self.now))
        self.report_id = bot.scheduled_report_id(self.now)
        self.store = DeliveryStore(os.path.join(self.temp.name, "delivery.sqlite3"), clock=lambda: self.now)
        for name, value in (("STATE_FILE", self.state_path), ("LOG_FILE", os.path.join(self.temp.name, "test.log")),
                            ("BTC_PUSH_EVIDENCE_ENABLED", False), ("BTC_PUSH_CHECK_TG_COMMANDS", False)):
            self.stack.enter_context(patch.object(bot, name, value))
        self.stack.enter_context(patch.object(bot, "get_delivery_store", return_value=self.store))
        self.payload = {key: {} for key in ("market", "market_24h_delta", "fear_greed", "seven_day_comparison",
                                           "structure_state", "delta4h", "levels", "signals", "rule_state",
                                           "status_layer", "market_patterns", "public_records", "completeness")}
        self.payload["signal_data_quality"] = {"usable": True}
        self.collect = self.stack.enter_context(patch.object(bot, "collect_report_payload", side_effect=lambda state: copy.deepcopy(self.payload)))
        self.stack.enter_context(patch.object(bot, "build_report_message", return_value="report"))
        self.stack.enter_context(patch.object(bot, "build_full_report_message", return_value="full"))
        self.send = self.stack.enter_context(patch.object(bot, "tg_send", return_value={"message_id": 11, "date": int(self.now)}))
        self.document = self.stack.enter_context(patch.object(bot, "tg_send_document", return_value={"message_id": 12}))

    def tearDown(self):
        self.stack.close()
        self.store.close()
        self.temp.cleanup()

    def test_offset_reads_legacy_state_once(self):
        state=self.state.load();state["tg_update_offset"]=123;self.state.save(state)
        with patch.object(bot,"_tg_command_chat_id",return_value=7), patch.object(bot,"_fetch_tg_updates",return_value=[]), patch.object(bot,"load_state",wraps=bot.load_state) as read:
            self.assertIsNone(bot.poll_tg_command())
            self.assertIsNone(bot.poll_tg_command())
            self.assertEqual(1,read.call_count)
        self.assertEqual(123,self.store.get_offset())

    def test_old_delivery_preserves_newer_cache_and_merges_keys(self):
        now=int(self.now);version=bot.NEWS_TRANSLATION_CACHE_VERSION
        def collect(state):
            state["macro_cache"]={"schema_version":bot.MACRO_CACHE_SCHEMA_REVISION,"updated_at":now-120,"value":{"us10y":3}}
            state["news_seen"]={"shared":now-120,"old_only":now-120}
            state["news_title_cn_cache"]={"shared":{"ver":version,"ts":now-120,"cn":"旧翻译"}}
            return copy.deepcopy(self.payload)
        self.collect.side_effect=collect
        with patch.object(bot,"pump_pending_deliveries",return_value=[]):
            bot.send_push(check_commands=False,report_id=self.report_id)
        state=self.state.load()
        state["macro_cache"]={"schema_version":bot.MACRO_CACHE_SCHEMA_REVISION,"updated_at":now,"value":{"us10y":4}}
        state["news_seen"]={"shared":now,"new_only":now}
        state["news_title_cn_cache"]={"shared":{"ver":version,"ts":now,"cn":"新翻译"}}
        self.state.save(state)
        bot.pump_pending_deliveries()
        state=self.state.load()
        self.assertEqual(4,state["macro_cache"]["value"]["us10y"])
        self.assertEqual(now,state["news_seen"]["shared"])
        self.assertIn("old_only",state["news_seen"]);self.assertIn("new_only",state["news_seen"])
        self.assertEqual("新翻译",state["news_title_cn_cache"]["shared"]["cn"])

    def test_repair_ack_enqueue_retry_does_not_reset_again(self):
        self.store.ingest_commands([{"update_id":10,"message":{"chat":{"id":7},"text":"/repair"}}],7,bot.parse_tg_command_text)
        command=self.store.claim_command()
        with patch.object(self.store,"enqueue_report",side_effect=sqlite3.OperationalError("temporary")):
            self.assertEqual("queued",bot.safe_send_push(cmd=command,check_commands=False))
        self.assertIn("tg:10",self.state.load()["repair_markers"])
        self.assertIsNone(self.store.claim_command())
        self.now+=31
        command=self.store.claim_command()
        with patch.object(bot,"do_repair",wraps=runtime.do_repair) as repair:
            self.assertEqual("sent",bot.safe_send_push(cmd=command,check_commands=False))
            repair.assert_not_called()
        self.send.assert_called_once()
        self.assertEqual(5,self.state.load()["day_push_count"])
        self.assertEqual("done",self.store.db.execute("SELECT status FROM commands WHERE update_id=10").fetchone()[0])

    def test_command_local_retry_is_bounded_and_recovers_write_failure(self):
        self.store.ingest_commands([{"update_id":10,"message":{"chat":{"id":7},"text":"/repair"}}],7,bot.parse_tg_command_text)
        command=self.store.claim_command()
        with patch.object(self.store,"requeue_command",side_effect=sqlite3.OperationalError("temporary")):
            with self.assertRaises(sqlite3.OperationalError):
                self.store.defer_command(10,{"stage":"test"})
        self.assertIn(10,self.store.pending_command_requeues)
        self.assertIsNone(self.store.claim_command())
        self.assertEqual({},self.store.pending_command_requeues)
        self.now+=31
        self.assertEqual(10,self.store.claim_command()["update_id"])
        self.assertEqual("pending",self.store.requeue_command(10,{}))
        self.now+=31
        self.assertEqual(10,self.store.claim_command()["update_id"])
        self.assertEqual("failed",self.store.requeue_command(10,{}))
        self.assertIsNone(self.store.claim_command())

    def test_new_report_and_repair_complete_command_before_delivery(self):
        for update_id, text in ((21, "/btc"), (22, "/repair")):
            with self.subTest(command=text):
                self.store.ingest_commands([{"update_id": update_id, "message": {"chat": {"id": 7}, "text": text}}], 7, bot.parse_tg_command_text)
                command = self.store.claim_command()
                def send(body):
                    status = self.store.db.execute("SELECT status FROM commands WHERE update_id=?", (update_id,)).fetchone()[0]
                    self.assertEqual("done", status)
                    self.assertIsNotNone(self.store.get_report(f"tg:{update_id}"))
                    return {"message_id": update_id, "date": int(self.now)}
                self.send.side_effect = send
                self.assertEqual("sent", bot.safe_send_push(cmd=command, check_commands=False))

    def test_old_queued_command_completion_retries_without_collection_or_send(self):
        self.store.ingest_commands([{"update_id": n, "message": {"chat": {"id": 7}, "text": "/btc"}} for n in (10, 11)], 7, bot.parse_tg_command_text)
        command = self.store.claim_command()
        self.store.enqueue_report("tg:10", "existing report", None, {})
        with patch.object(self.store, "finish_report_command", side_effect=sqlite3.OperationalError("temporary")):
            self.assertEqual("queued", bot.safe_send_push(cmd=command, check_commands=False))
        self.assertEqual("queued", bot.safe_send_push(cmd=command, check_commands=False))
        self.assertEqual(11, self.store.claim_command()["update_id"])
        self.collect.assert_not_called()
        self.send.assert_not_called()
        self.document.assert_not_called()

    def test_repair_transaction_failure_retries_without_repeating_reset(self):
        self.store.ingest_commands([{"update_id": 10, "message": {"chat": {"id": 7}, "text": "/repair"}}], 7, bot.parse_tg_command_text)
        command = self.store.claim_command()
        with patch.object(self.store, "_finish_report_command", side_effect=sqlite3.OperationalError("temporary")):
            self.assertEqual("queued", bot.safe_send_push(cmd=command, check_commands=False))
        self.assertIsNone(self.store.get_report("tg:10"))
        self.assertIn("tg:10", self.state.load()["repair_markers"])
        self.now += 31
        command = self.store.claim_command()
        with patch.object(bot, "do_repair", wraps=runtime.do_repair) as repair:
            self.assertEqual("sent", bot.safe_send_push(cmd=command, check_commands=False))
            repair.assert_not_called()
        self.send.assert_called_once()

    def test_same_hour_retry_does_not_recollect_or_double_count(self):
        self.send.side_effect = [DeliveryFailure("connect_timeout"), {"message_id": 11, "date": int(self.now)}]
        self.assertEqual("queued", bot.send_push(check_commands=False, report_id=self.report_id))
        self.now += 31
        self.assertEqual("sent", bot.send_push(check_commands=False, report_id=self.report_id))
        self.assertEqual("sent", bot.send_push(check_commands=False, report_id=self.report_id))
        self.assertEqual(1, self.collect.call_count)
        self.assertEqual(2, self.send.call_count)
        self.assertEqual(6, self.state.load()["day_push_count"])
        self.assertEqual("{}", self.store.get_report(self.report_id)["commit_payload"])
        self.document.assert_not_called()

    def test_next_tick_rounds_up_to_the_intended_hour(self):
        with patch.object(bot.time, "time", return_value=5999.7), patch.object(bot, "seconds_until_next_hour_tokyo", return_value=1200):
            self.assertEqual(7200, bot.next_scheduled_push_ts())
            self.assertEqual("hour:7200", bot.scheduled_report_id(bot.next_scheduled_push_ts()))

    def test_invalid_mock_receipt_is_not_success(self):
        self.send.return_value = None
        self.assertEqual("unknown", bot.send_push(check_commands=False, report_id=self.report_id))
        bot.send_push(check_commands=False, report_id=self.report_id)
        self.assertEqual(5, self.state.load()["day_push_count"])
        self.send.assert_called_once()

    def test_query_and_force_batch_preserve_semantics(self):
        updates = [{"update_id": n, "channel_post": {"chat": {"id": 7}, "text": cmd}}
                   for n, cmd in ((1, "/btc"), (2, "/force"), (3, "/repair"))]
        with patch.object(bot, "_tg_command_chat_id", return_value=7), patch.object(bot, "_fetch_tg_updates", return_value=updates):
            for expected in (1, 2, 3):
                command = bot.poll_tg_command()
                self.assertEqual(expected, command["update_id"])
                bot.send_push(cmd=command, check_commands=False)
            self.assertIsNone(bot.poll_tg_command())
        self.assertEqual(4, self.store.get_offset())
        self.assertEqual(6, self.state.load()["day_push_count"])
        self.assertEqual(3, len(self.state.load()["applied_report_ids"]))
        self.assertEqual(2, self.collect.call_count)
        with self.store.lock:
            self.assertEqual(["done"] * 3, [r[0] for r in self.store.db.execute("SELECT status FROM commands ORDER BY update_id")])

    def test_apply_uses_latest_counter_and_preserves_collector_domain(self):
        with patch.object(bot, "pump_pending_deliveries", return_value=[]):
            bot.send_push(check_commands=False, report_id=self.report_id)
            bot.send_push(check_commands=False, report_id="manual:second")
        live = self.state.load()
        live["day_push_count"] = 20
        live["liquidation_ws"] = {"updated_at_ms": int(self.now * 1000), "marker": "latest"}
        self.state.save(live)
        bot.pump_pending_deliveries()
        live = self.state.load()
        self.assertEqual(22, live["day_push_count"])
        self.assertEqual("latest", live["liquidation_ws"]["marker"])

    def test_save_state_after_replace_failure_recovers_without_duplicate_send(self):
        save = bot.save_state
        def saved_then_failed(state, **kwargs):
            save(state, **kwargs)
            raise OSError("post-replace failure")
        with patch.object(bot, "save_state", side_effect=saved_then_failed):
            with self.assertRaises(OSError):
                bot.send_push(check_commands=False, report_id=self.report_id)
        self.assertEqual(6, self.state.load()["day_push_count"])
        bot.pump_pending_deliveries()
        self.assertEqual(6, self.state.load()["day_push_count"])
        self.assertEqual(1, self.store.get_report(self.report_id)["applied"])
        self.send.assert_called_once()

    def test_snapshot_delta_preserves_newer_history_inserted_while_queued(self):
        base = int(self.now) // 3600 * 3600 + 1
        state = self.state.load()
        bot.append_snapshot(state, {"ts": base - 3600, "marker": "before"})
        bot.append_public_snapshot(state, {"ts": base - 3600, "public": {}, "marker": "before"})
        self.state.save(state)
        def collect(candidate):
            bot.append_snapshot(candidate, {"ts": base, "marker": "queued"})
            bot.append_public_snapshot(candidate, {"ts": base, "public": {}, "marker": "queued"})
            return copy.deepcopy(self.payload)
        self.collect.side_effect = collect
        with patch.object(bot, "pump_pending_deliveries", return_value=[]):
            bot.send_push(check_commands=False, report_id=self.report_id)
        commit = json.loads(self.store.get_report(self.report_id)["commit_payload"])
        self.assertNotIn("snapshots", commit["candidate"])
        self.assertNotIn("public_snapshots", commit["candidate"])
        self.assertEqual(base, commit["snapshot"]["ts"])
        live = self.state.load()
        bot.append_snapshot(live, {"ts": base + 3600, "marker": "new"})
        bot.append_public_snapshot(live, {"ts": base + 3600, "public": {}, "marker": "new"})
        self.state.save(live)
        bot.pump_pending_deliveries()
        live = self.state.load()
        for key in ("snapshots", "public_snapshots"):
            self.assertEqual(["before", "queued", "new"], [row["marker"] for row in live[key]])

    def test_snapshot_delta_does_not_replace_newer_same_hour_sample(self):
        base = int(self.now) // 3600 * 3600 + 1
        state = {"snapshots": [{"ts": base + 10, "marker": "new"}],
                 "public_snapshots": [{"ts": base, "public": {}, "marker": "new"}]}
        bot._merge_delivery_snapshots(state, {
            "snapshot": {"ts": base, "marker": "old"},
            "public_snapshot": {"ts": base, "public": {}, "marker": "old"}})
        self.assertEqual("new", state["snapshots"][0]["marker"])
        self.assertEqual("new", state["public_snapshots"][0]["marker"])

    def test_repair_marker_survives_enqueue_failure_and_preserves_new_state(self):
        command = {"update_id": 10, "command": "REPAIR"}
        with patch.object(self.store, "enqueue_report", side_effect=sqlite3.OperationalError("disk")):
            with self.assertRaises(sqlite3.OperationalError):
                bot.send_push(cmd=command, check_commands=False)
        live = self.state.load()
        live["news_seen"] = {"new": int(self.now)}
        self.state.save(live)
        with patch.object(bot, "do_repair", wraps=runtime.do_repair) as repair:
            bot.send_push(cmd=command, check_commands=False)
            repair.assert_not_called()
        self.assertIn("new", self.state.load()["news_seen"])
        self.assertEqual(5, self.state.load()["day_push_count"])

    def test_old_candidate_does_not_revive_repair_caches(self):
        def collect(state):
            state["news_seen"] = {"old": int(self.now)}
            return copy.deepcopy(self.payload)
        self.collect.side_effect = collect
        with patch.object(bot, "pump_pending_deliveries", return_value=[]):
            bot.send_push(check_commands=False, report_id=self.report_id)
        bot.send_push(cmd={"update_id": 10, "command": "REPAIR"}, check_commands=False)
        self.assertEqual({}, self.state.load()["news_seen"])
        self.assertNotIn("report", self.state.load())

    def test_optional_attachment_hook_replies_and_cleans_document(self):
        with patch.object(bot, "BTC_PUSH_EVIDENCE_ENABLED", True), patch.object(bot, "build_delivery_document", return_value=b"{}"):
            bot.send_push(check_commands=False, report_id=self.report_id)
        self.document.assert_called_once_with(b"{}", reply_to_message_id=11)
        self.assertIsNone(self.store.get_report(self.report_id)["document"])

    def test_disabled_evidence_hook_does_not_build_an_attachment(self):
        self.assertIsNone(bot.build_delivery_document({}, "text", self.report_id, self.now))


if __name__ == "__main__":
    unittest.main()
