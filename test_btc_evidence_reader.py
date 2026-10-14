
import tempfile, unittest, copy, json, sqlite3, hashlib, shutil
from pathlib import Path
from unittest.mock import patch
from btc_evidence import build_bundle, encode_bundle, canonical_bytes, text_hash, validate_bundle
import btc_evidence_reader as candidate
class Replay(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.path=self.tmp.name+"/reader.sqlite3"
        self.reader=candidate.EvidenceReader(self.path,-9001,allowed_sender_ids={7001}, clock=lambda:1788451200)
        self.addCleanup(self.tmp.cleanup)
        self.addCleanup(lambda:self.reader.close())
        self.text="BTC fixture 辅助观察"
        self.bundle=build_bundle({},self.text,"hour:100","2026-09-04T00:00:00+00:00")
        self.raw=encode_bundle(self.bundle)
    def msg(self,mid,text=None,parent=None,edit=None,uid=None):
        m={"message_id":mid,"date":1788451200,"chat":{"id":-9001},"from":{"id":7001}}
        if text is not None:m["text"]=text
        if parent is not None:
            m["document"]={"file_id":"f"+str(mid),"file_unique_id":"u"+str(mid),"file_name":"btc.json","file_size":len(self.raw)}
            m["reply_to_message"]={"message_id":parent}
        if edit:
            m["edit_date"]=edit
        return {"update_id":uid or mid,"edited_channel_post" if edit else "channel_post":m}
    def test_ordered_and_duplicate(self):
        self.reader.ingest_update(self.msg(1,self.text))
        update=self.msg(2,parent=1)
        self.assertEqual("stored",self.reader.ingest_update(update)["status"])
        self.assertEqual("degraded_auxiliary",self.reader.process_pending(lambda _:self.raw)[0]["status"])
        self.assertEqual("duplicate",self.reader.ingest_update(update)["status"])
        self.reader.ingest_update(self.msg(3,parent=1))
        self.reader.process_pending(lambda _:self.raw)
        self.assertEqual(1,len(self.reader.observations()))
        obs=self.reader.observations()[0]
        self.assertEqual(0,obs["independent_verified_sources"])
        self.assertNotIn("mechanical_observation",obs)
    def test_holding_only_attachment_reaches_real_reader(self):
        import sys
        sys.path.insert(0, str(Path(__file__).parent / "tests"))
        from collector_replay_fixture import make_payload, holdings_only_record, GENERATED_AT
        for version in (1, 2):
            payload = make_payload()
            payload["public_records"]["etf"] = holdings_only_record()
            payload["evidence_version"] = version
            self.bundle = build_bundle(payload, self.text, "holding-only:%d" % version, GENERATED_AT)
            self.raw = encode_bundle(self.bundle)
            parent = version * 10
            self.reader.ingest_update(self.msg(parent, self.text))
            self.reader.ingest_update(self.msg(parent + 1, parent=parent))
            result = self.reader.process_pending(lambda _: self.raw)
            self.assertIn(result[0]["status"], ("valid_auxiliary", "degraded_auxiliary"))
            observation = self.reader.observations()[-1]
            ds = observation["datasets"]["etf"]
            self.assertAlmostEqual(12.00000012, ds["values"]["holdings_btc"])
            self.assertIsNone(ds["values"]["flow_1d_usd"])
            self.assertNotIn("public_etf_history", observation["sources"])
    def test_attachment_before_parent_and_restart(self):
        self.reader.ingest_update(self.msg(2,parent=1))
        self.assertEqual("pending_text",self.reader.process_pending(lambda _:self.raw)[0]["status"])
        self.reader.close()
        self.reader=candidate.EvidenceReader(self.path,-9001,allowed_sender_ids={7001})
        self.reader.ingest_update(self.msg(1,self.text))
        self.assertEqual(1,len(self.reader.observations()))
    def test_edit_invalidates_then_restores_without_download(self):
        self.reader.ingest_update(self.msg(1,self.text))
        self.reader.ingest_update(self.msg(2,parent=1))
        self.reader.process_pending(lambda _:self.raw)
        self.reader.ingest_update(self.msg(1,"changed",edit=1788451300,uid=3))
        self.assertEqual([],self.reader.observations())
        self.reader.ingest_update(self.msg(1,self.text,edit=1788451400,uid=4))
        self.assertEqual(1,len(self.reader.observations()))
        self.reader.ingest_update(self.msg(1,"older edit",edit=1788451301,uid=5))
        self.assertEqual(1,len(self.reader.observations()))
    def test_nested_reply_does_not_outrank_same_second_parent_edit(self):
        update=self.msg(2,parent=1,uid=3)
        update["channel_post"]["reply_to_message"]={
            "message_id":1,"date":1788451200,"chat":{"id":-9001},
            "from":{"id":7001},"text":self.text}
        self.reader.ingest_update(update)
        self.reader.process_pending(lambda _:self.raw)
        self.assertEqual(1,len(self.reader.observations()))
        edited=self.msg(1,"changed",edit=1788451200,uid=2)
        self.assertEqual("stored",self.reader.ingest_update(edited)["status"])
        self.assertEqual([],self.reader.observations())

    def test_bad_document_does_not_block_next(self):
        self.reader.ingest_update(self.msg(1,self.text))
        self.reader.ingest_update(self.msg(2,parent=1))
        self.reader.ingest_update(self.msg(3,parent=1))
        out=self.reader.process_pending(lambda fid:b"not json".ljust(len(self.raw),b" ") if fid=="f2" else self.raw)
        self.assertEqual(["rejected","degraded_auxiliary"],[x["status"] for x in out])
        self.assertEqual(1,len(self.reader.observations()))
    def test_download_failure_restarts_pending(self):
        self.reader.ingest_update(self.msg(1,self.text))
        self.reader.ingest_update(self.msg(2,parent=1))
        self.assertEqual("pending_download",self.reader.process_pending(lambda _:1/0)[0]["status"])
        self.reader.close()
        self.reader=candidate.EvidenceReader(self.path,-9001,allowed_sender_ids={7001})
        self.reader.clock=lambda:1788451231
        self.reader.process_pending(lambda _:self.raw)
        self.assertEqual(1,len(self.reader.observations()))
    def test_allowlist_before_download(self):
        update=self.msg(2,parent=1)
        update["channel_post"]["from"]["id"]=9999
        self.assertEqual("ignored",self.reader.ingest_update(update)["status"])
        self.assertEqual([],self.reader.process_pending(lambda _:self.fail("unauthorized download")))
    def test_unknown_schema_is_audit_only(self):
        self.reader.ingest_update(self.msg(1,self.text))
        bad=copy.deepcopy(self.bundle);bad["schema"]="FUTURE"
        raw=json.dumps(bad).encode()
        update=self.msg(2,parent=1)
        update["channel_post"]["document"]["file_size"]=len(raw)
        self.reader.ingest_update(update)
        out=self.reader.process_pending(lambda _:raw)
        self.assertIn("unknown_schema",out[0]["reasons"])
        self.assertEqual([],self.reader.observations())
    def test_partial_download_is_retried_without_poisoning_file_cache(self):
        self.reader.ingest_update(self.msg(1,self.text))
        self.reader.ingest_update(self.msg(2,parent=1))
        self.assertEqual("pending_download",self.reader.process_pending(lambda _:self.raw[:20])[0]["status"])
        self.assertEqual(0,self.reader.db.execute("SELECT count(*) FROM collector_blobs").fetchone()[0])
        self.reader.close()
        self.reader=candidate.EvidenceReader(self.path,-9001,{7001},clock=lambda:1788451231)
        calls=[]
        self.reader.process_pending(lambda fid:(calls.append(fid),self.raw)[1])
        self.assertEqual(["f2"],calls)
        self.assertEqual(1,len(self.reader.observations()))

    def test_legacy_text_is_retained(self):
        self.assertEqual("stored",self.reader.ingest_update(self.msg(1,self.text))["status"])
        self.assertEqual([],self.reader.observations())
        self.assertEqual(self.text,self.reader.db.execute("select text from collector_messages").fetchone()[0])

    def test_read_only_export_never_creates_or_mutates_database(self):
        missing = Path(self.tmp.name) / "absent.sqlite3"
        with self.assertRaises(sqlite3.OperationalError):
            candidate.EvidenceReader(missing, -9001, read_only=True)
        self.assertFalse(missing.exists())
        self.reader.ingest_update(self.msg(1, self.text))
        self.reader.ingest_update(self.msg(2, parent=1))
        self.reader.process_pending(lambda _: self.raw)
        expected = self.reader.observations()
        before = "\n".join(self.reader.db.iterdump())
        with_readonly = candidate.EvidenceReader(self.path, -9001, read_only=True)
        try:
            self.assertEqual(expected, with_readonly.observations())
            with self.assertRaises(sqlite3.OperationalError):
                with_readonly.db.execute("DELETE FROM collector_messages")
        finally:
            with_readonly.close()
        self.assertEqual(before, "\n".join(self.reader.db.iterdump()))

    def test_export_after_channel_change_does_not_adopt_prior_channel(self):
        self.reader.ingest_update(self.msg(1, self.text))
        self.reader.ingest_update(self.msg(2, parent=1))
        self.reader.process_pending(lambda _: self.raw)
        self.assertEqual(1, len(self.reader.observations()))
        other = candidate.EvidenceReader(self.path, -9002, read_only=True)
        try:
            self.assertEqual([], other.observations())
        finally:
            other.close()
class LatestObservations(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = str(Path(self.tmp.name) / "reader.sqlite3")
        self.reader = candidate.EvidenceReader(
            self.path, -9001, {7001},
            clock=lambda: 1788451200)
        self.addCleanup(self.reader.close)
        import sys
        sys.path.insert(0, str(Path(__file__).parent / "tests"))
        from collector_replay_fixture import protocol_fixture_directory
        fixtures = protocol_fixture_directory()
        self.base = json.loads((fixtures / "valid.json").read_text())
        self.text = (fixtures / "valid.txt").read_text()
        self.update_id = 0

    def bundle(self, report_id, *, missing=False, zero=False):
        bundle = copy.deepcopy(self.base)
        bundle["report_id"] = str(report_id)
        bundle["text_sha256"] = text_hash(self.text)
        if missing:
            spot = bundle["datasets"]["spot_cvd"]
            spot["rows"] = []
            spot["quality"].update(status="missing", reason="missing_spot")
        if zero:
            for name in ("futures_1h", "spot_cvd"):
                for row in bundle["datasets"][name]["rows"]:
                    row["taker_buy_quote"] = row["taker_sell_quote"] = row["quote_volume"] / 2
                    row["net_taker_quote"] = 0
        body = {k: v for k, v in bundle.items() if k != "bundle_id"}
        bundle["bundle_id"] = "btc:" + hashlib.sha256(canonical_bytes(body)).hexdigest()
        return bundle

    def ingest(self, mid, *, text=None, parent=None, raw=None, edit=False):
        self.update_id += 1
        message = {"message_id": mid, "date": 1788451200,
                   "chat": {"id": -9001}, "from": {"id": 7001}}
        if text is not None:
            message["text"] = text
        if parent is not None:
            digest = hashlib.sha256(raw).hexdigest()
            message["document"] = {"file_id": digest, "file_unique_id": digest,
                                   "file_name": "btc.json", "file_size": len(raw)}
            message["reply_to_message"] = {"message_id": parent}
        if edit:
            message["edit_date"] = 1788451200 + self.update_id
        update = {"update_id": self.update_id,
                  "edited_channel_post" if edit else "channel_post": message}
        self.assertEqual("stored", self.reader.ingest_update(update)["status"])

    def report(self, parent, document, bundle=None, *, pending=False):
        bundle = self.bundle(parent) if bundle is None else bundle
        raw = encode_bundle(bundle)
        self.ingest(parent, text=self.text)
        self.ingest(document, parent=parent, raw=raw)
        if not pending:
            self.reader.process_pending(lambda _: raw)
        return raw

    @staticmethod
    def output_bytes(observations):
        return len((json.dumps(observations, ensure_ascii=False, sort_keys=True) + "\n").encode("utf-8"))

    def test_new_missing_snapshot_preserves_quality_and_history_is_available(self):
        self.report(1, 2)
        self.assertEqual("valid_auxiliary", self.reader.observations()[0]["validation"]["status"])
        self.report(3, 4, self.bundle(3, missing=True))
        snapshot = self.reader.observations(latest_only=True)
        self.assertEqual(["3"], [x["report_id"] for x in snapshot])
        self.assertEqual("degraded_auxiliary", snapshot[0]["validation"]["status"])
        self.assertEqual([], snapshot[0]["datasets"]["spot_cvd"]["rows"])
        self.assertIn("spot_cvd", snapshot[0]["validation"]["dataset_reasons"])
        self.assertEqual(2, len(self.reader.observations()))

    def test_late_old_attachment_cannot_outrank_new_parent(self):
        self.report(10, 11)
        self.report(1, 99)
        self.assertEqual(10, self.reader.observations(latest_only=True)[0]["transport"]["text_message_id"])

    def test_pending_invalid_unknown_and_parent_edit_never_fall_back(self):
        self.report(1, 2)
        raw = self.report(3, 4, pending=True)
        self.assertEqual([], self.reader.observations(latest_only=True))
        self.reader.process_pending(lambda _: raw)
        self.assertEqual(1, len(self.reader.observations(latest_only=True)))
        self.ingest(3, text="edited parent", edit=True)
        self.assertEqual([], self.reader.observations(latest_only=True))
        self.assertEqual(1, len(self.reader.observations()))
        for schema, corrupt_hash in (("FUTURE", False), (self.base["schema"], True)):
            bundle = self.bundle(5)
            bundle["schema"] = schema
            if corrupt_hash:
                bundle["bundle_id"] = "btc:" + "0" * 64
            bad_raw = json.dumps(bundle).encode("utf-8")
            self.ingest(5, text=self.text, edit=schema == self.base["schema"])
            self.ingest(6, parent=5, raw=bad_raw, edit=schema == self.base["schema"])
            self.reader.process_pending(lambda _: bad_raw)
            self.assertEqual([], self.reader.observations(latest_only=True))
            self.assertEqual(1, len(self.reader.observations()))

    def test_latest_document_and_current_revision_win_with_real_zero(self):
        bundle = self.bundle(1, zero=True)
        self.assertEqual("valid_auxiliary", validate_bundle(bundle, self.text)["status"])
        raw = self.report(1, 2, bundle)
        self.ingest(3, parent=1, raw=raw)
        self.assertEqual([], self.reader.observations(latest_only=True))
        self.reader.process_pending(lambda _: raw)
        snapshot = self.reader.observations(latest_only=True)
        self.assertEqual(3, snapshot[0]["transport"]["document_message_id"])
        self.assertEqual(0, snapshot[0]["datasets"]["spot_cvd"]["rows"][0]["net_taker_quote"])
        self.assertEqual(1, len(self.reader.observations()))
        self.ingest(3, parent=1, raw=raw, edit=True)
        self.assertEqual([], self.reader.observations(latest_only=True))
        self.reader.process_pending(lambda _: self.fail("unchanged file must reuse its blob"))
        self.assertEqual(3, self.reader.observations(latest_only=True)[0]["transport"]["document_message_id"])

    def test_forty_reports_export_one_complete_snapshot_under_default_limit(self):
        for parent in range(1, 81, 2):
            self.report(parent, parent + 1)
        history = self.reader.observations()
        snapshot = self.reader.observations(latest_only=True)
        self.assertEqual(40, len(history))
        self.assertEqual(1, len(snapshot))
        self.assertEqual("79", snapshot[0]["report_id"])
        self.assertEqual(240, len(snapshot[0]["datasets"]["spot_cvd"]["rows"]))
        self.assertGreater(self.output_bytes(history), 4 * 1024 * 1024)
        self.assertLess(self.output_bytes(snapshot), 4 * 1024 * 1024)

    def test_byte_limit_counts_utf8_and_newline_without_database_changes(self):
        self.report(1, 2, self.bundle("中文报告"))
        snapshot = self.reader.observations(latest_only=True)
        exact = self.output_bytes(snapshot)
        before = "\n".join(self.reader.db.iterdump())
        self.assertEqual(snapshot, self.reader.observations(latest_only=True, max_output_bytes=exact))
        with self.assertRaisesRegex(ValueError, "observations_output_size_limit"):
            self.reader.observations(latest_only=True, max_output_bytes=exact - 1)
        self.assertEqual(before, "\n".join(self.reader.db.iterdump()))

    def test_single_oversized_snapshot_fails_without_changing_database(self):
        bundle = self.bundle("超" * (4 * 1024 * 1024 // 3))
        self.report(1, 2, bundle)
        before = "\n".join(self.reader.db.iterdump())
        with self.assertRaisesRegex(ValueError, "observations_output_size_limit"):
            self.reader.observations(latest_only=True)
        self.assertEqual(1, len(self.reader.observations()))
        self.assertEqual(before, "\n".join(self.reader.db.iterdump()))

    def test_snapshot_remains_scoped_to_configured_channel(self):
        self.report(1, 2)
        other = candidate.EvidenceReader(self.path, -9002, read_only=True)
        try:
            self.assertEqual([], other.observations(latest_only=True))
        finally:
            other.close()


class ReadonlySnapshot(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.path = self.root / "source.sqlite3"
        self.writer = sqlite3.connect(self.path, isolation_level=None)
        self.addCleanup(self.writer.close)
        self.writer.execute("PRAGMA journal_mode=WAL").fetchall()
        self.writer.execute("PRAGMA wal_autocheckpoint=0")
        self.writer.execute("CREATE TABLE sample(value INTEGER)")
        self.writer.execute("INSERT INTO sample VALUES (1)")

    def files(self, directory=None):
        return {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                for p in (directory or self.root).iterdir() if p.is_file()}

    def test_active_wal_latest_commit_is_visible_without_source_changes(self):
        self.writer.execute("INSERT INTO sample VALUES (2)")
        before = self.files()
        snapshot = candidate.open_readonly_snapshot(self.path)
        try:
            self.assertEqual([1, 2], [x[0] for x in snapshot.execute("SELECT value FROM sample")])
            with self.assertRaises(sqlite3.OperationalError):
                snapshot.execute("INSERT INTO sample VALUES (3)")
        finally:
            snapshot.close()
        self.assertEqual(before, self.files())

    def test_closed_wal_database_without_side_files_is_readable(self):
        self.writer.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchall()
        closed = self.root / "closed"; closed.mkdir()
        path = closed / "source.sqlite3"
        shutil.copyfile(self.path, path)
        before = self.files(closed)
        snapshot = candidate.open_readonly_snapshot(path)
        try:
            self.assertEqual(1, snapshot.execute("SELECT value FROM sample").fetchone()[0])
        finally:
            snapshot.close()
        self.assertEqual(before, self.files(closed))
        self.assertEqual({"source.sqlite3"}, set(before))

    def test_missing_database_is_not_created(self):
        missing = self.root / "absent" / "missing.sqlite3"
        before = self.files()
        with self.assertRaises(sqlite3.OperationalError):
            candidate.open_readonly_snapshot(missing)
        self.assertFalse(missing.parent.exists())
        self.assertEqual(before, self.files())

    def test_commit_during_copy_retries_and_includes_committed_wal(self):
        real_copy = shutil.copyfile
        attempts = []
        def copying(src, dst):
            result = real_copy(src, dst)
            if Path(src) == self.path:
                attempts.append(src)
                if len(attempts) == 1:
                    self.writer.execute("INSERT INTO sample VALUES (2)")
            return result
        with patch.object(candidate.shutil, "copyfile", side_effect=copying):
            snapshot = candidate.open_readonly_snapshot(self.path)
        try:
            self.assertEqual(2, len(attempts))
            self.assertEqual([1, 2], [x[0] for x in snapshot.execute("SELECT value FROM sample")])
        finally:
            snapshot.close()

    def test_continuously_changing_source_fails_after_three_attempts(self):
        real_copy = shutil.copyfile
        attempts = []
        def copying(src, dst):
            result = real_copy(src, dst)
            if Path(src) == self.path:
                attempts.append(src)
                self.writer.execute("INSERT INTO sample VALUES (2)")
            return result
        with patch.object(candidate.shutil, "copyfile", side_effect=copying):
            with self.assertRaisesRegex(sqlite3.OperationalError, "changed during copy"):
                candidate.open_readonly_snapshot(self.path)
        self.assertEqual(3, len(attempts))

    def test_rollback_journal_is_rejected_instead_of_ignored(self):
        Path(str(self.path) + "-journal").write_bytes(b"uncommitted journal fixture")
        before = self.files()
        with self.assertRaisesRegex(sqlite3.OperationalError, "stable WAL or clean"):
            candidate.open_readonly_snapshot(self.path)
        self.assertEqual(before, self.files())


if __name__ == "__main__":
    unittest.main()
