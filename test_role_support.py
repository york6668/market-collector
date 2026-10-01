import fcntl
import json
import os
from pathlib import Path
import plistlib
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

from btc_delivery import DeliveryStore, DeliveryFailure
from roles import role_support as support


class RoleSupportTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def value(self, paths=None):
        return {"role": "collector", "machine": "test-host", "python": sys.executable,
                "code_dir": str(self.root), "entrypoint": "btc_push.py", "arguments": [],
                "service_label": "test.market.collector", "log_dir": str(self.root / "logs"),
                "state_paths": paths or {}, "writer_locks": [], "environment": {}}

    def test_reference_roundtrip_requires_explicit_sources_and_empty_destination(self):
        (self.root / "main.py").write_text("print('reference')\n")
        (self.root / "private.json").write_text('{"BOT_TOKEN":"not-for-upload"}')
        bundle = self.root / "reference.json"
        support.package(self.root, ["main.py"], bundle, "collector")
        content = bundle.read_text()
        self.assertNotIn("not-for-upload", content)
        target = self.root / "rebuilt"
        support.unpack(bundle, target)
        self.assertEqual((target / "main.py").read_bytes(), (self.root / "main.py").read_bytes())
        with self.assertRaises(ValueError):
            support.unpack(bundle, target)

    def test_source_traversal_and_symlinks_are_rejected(self):
        bundle = self.root / "reference.json"
        for path in ("../escape", "/tmp/escape", "x/../escape", "x\\escape"):
            bundle.write_text(json.dumps({"format": "market-role-source-v1", "files": {path: "bad"}}))
            with self.assertRaises(ValueError):
                support.unpack(bundle, self.root / "rebuilt")
        (self.root / "link.py").symlink_to(__file__)
        with self.assertRaises(ValueError):
            support.package(self.root, ["link.py"], self.root / "new.json", "collector")

    def test_live_writer_lock_prevents_snapshot(self):
        state = self.root / "state.json"
        state.write_text('{"cursor":7}')
        lock = self.root / "writer.lock"
        with lock.open("w") as stream:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            value = self.value({"state": str(state)})
            value["writer_locks"] = [str(lock)]
            with self.assertRaises(BlockingIOError):
                support.snapshot(value, self.root / "backup")
        self.assertFalse((self.root / "backup").exists())

    def test_snapshot_captures_wal_and_restores_cursor_without_source_changes(self):
        database = self.root / "cursor.sqlite3"
        connection = sqlite3.connect(database)
        self.addCleanup(connection.close)
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA wal_autocheckpoint=0")
        connection.execute("CREATE TABLE progress(cursor INTEGER)")
        connection.execute("INSERT INTO progress VALUES (17)")
        connection.commit()
        source_bytes = {str(p): p.read_bytes() for p in self.root.glob("cursor.sqlite3*")}
        value = self.value({"reader": str(database)})
        backup = self.root / "backup"
        support.snapshot(value, backup)
        self.assertEqual(source_bytes, {str(p): p.read_bytes() for p in self.root.glob("cursor.sqlite3*")})
        restored = self.root / "restored/reader.sqlite3"
        support.restore(backup, self.value({"reader": str(restored)}))
        with sqlite3.connect(restored) as read:
            self.assertEqual(17, read.execute("SELECT cursor FROM progress").fetchone()[0])
        with self.assertRaises(ValueError):
            support.restore(backup, self.value({"reader": str(restored)}))

    def test_backup_detects_concurrent_json_change_and_leaves_no_partial_snapshot(self):
        state = self.root / "state.json"
        state.write_text('{"cursor":1}')
        original = support.shutil.copy2
        def changed(source, target):
            result = original(source, target)
            if Path(source) == state:
                state.write_text('{"cursor":2}')
            return result
        with patch.object(support.shutil, "copy2", side_effect=changed):
            with self.assertRaisesRegex(ValueError, "state changed"):
                support.snapshot(self.value({"state": str(state)}), self.root / "backup")
        self.assertFalse((self.root / "backup").exists())

    def test_context_directory_restore_keeps_baseline_and_pending_work(self):
        context = self.root / "context"
        context.mkdir()
        (context / "baseline.json").write_text('{"decision":"unchanged","pending":["review-next-hour"]}')
        (context / "completed.json").write_text('{"last_observed_hour":123}')
        value = self.value({"analysis_context": str(context), "optional": str(self.root / "missing")})
        value["role"] = "analysis"
        backup = self.root / "backup"
        support.snapshot(value, backup)
        target = self.root / "new-context"
        value["state_paths"] = {"analysis_context": str(target), "optional": str(self.root / "new-missing")}
        support.restore(backup, value)
        self.assertEqual((context / "baseline.json").read_bytes(), (target / "baseline.json").read_bytes())
        self.assertEqual((context / "completed.json").read_bytes(), (target / "completed.json").read_bytes())
        self.assertFalse((self.root / "new-missing").exists())

    def test_unknown_delivery_and_completed_report_survive_role_restore(self):
        database = self.root / "outbox.sqlite3"
        store = DeliveryStore(str(database), clock=lambda: 1000)
        store.enqueue_report("hour:0", "report", None, {})
        sender = Mock(side_effect=DeliveryFailure("unknown"))
        store.deliver_one(sender, Mock())
        store.enqueue_report("tg:17", "completed", None, {})
        store.deliver_one(Mock(return_value={"message_id": 19}), Mock())
        store.close()
        backup = self.root / "backup"
        support.snapshot(self.value({"outbox": str(database)}), backup)
        target = self.root / "new/outbox.sqlite3"
        support.restore(backup, self.value({"outbox": str(target)}))
        restored = DeliveryStore(str(target), clock=lambda: 2000)
        self.addCleanup(restored.close)
        restored.recover()
        self.assertEqual("unknown", restored.get_report("hour:0")["summary_status"])
        self.assertEqual("sent", restored.get_report("tg:17")["summary_status"])
        send = Mock()
        self.assertIsNone(restored.deliver_one(send, send))
        send.assert_not_called()

    def test_credentials_are_references_and_not_embedded_in_service(self):
        (self.root / "roles").mkdir()
        (self.root / "roles/role_support.py").write_text("# deployed helper")
        (self.root / "btc_push.py").write_text("# deployed collector")
        private = self.root / "private.json"
        private.write_text('{"BOT_TOKEN":"test-secret","CHAT_ID":"0"}')
        private.chmod(0o600)
        value = self.value()
        value["private_environment_file"] = str(private)
        path = self.root / "machine.json"
        path.write_text(json.dumps(value))
        support.render_service(path, self.root / "service.plist")
        plist = (self.root / "service.plist").read_bytes()
        self.assertNotIn(b"test-secret", plist)
        self.assertEqual(str(path), plistlib.loads(plist)["ProgramArguments"][-1])
        self.assertTrue(support.check(support.config(path))["ready"])
        private.chmod(0o644)
        self.assertFalse(support.check(support.config(path))["ready"])
        value["environment"] = {"BOT_TOKEN": "forbidden"}
        path.write_text(json.dumps(value))
        with self.assertRaises(ValueError):
            support.config(path)

    def test_state_symlink_and_role_mismatch_are_rejected(self):
        context = self.root / "context"
        context.mkdir()
        (context / "link").symlink_to(__file__)
        with self.assertRaises(ValueError):
            support.snapshot(self.value({"context": str(context)}), self.root / "backup")
        (context / "link").unlink()
        support.snapshot(self.value({"context": str(context)}), self.root / "backup")
        value = self.value({"context": str(self.root / "restored")})
        value["role"] = "analysis"
        with self.assertRaises(ValueError):
            support.restore(self.root / "backup", value)

    def test_internal_relative_and_absolute_context_links_remain_portable(self):
        context = self.root / "context"
        (context / "decisions/one").mkdir(parents=True)
        (context / "decisions/one/value.json").write_text('{"decision":"retained"}')
        (context / "current").symlink_to("decisions/one")
        (context / "display").symlink_to(context / "decisions/one/value.json")
        value = self.value({"context": str(context)})
        value["role"] = "analysis"
        support.snapshot(value, self.root / "backup")
        destination = self.root / "restored"
        value["state_paths"] = {"context": str(destination)}
        support.restore(self.root / "backup", value)
        self.assertEqual("decisions/one", os.readlink(destination / "current"))
        self.assertFalse(os.path.isabs(os.readlink(destination / "display")))
        (context / "decisions/one/value.json").unlink()
        self.assertEqual('{"decision":"retained"}', (destination / "current/value.json").read_text())
        self.assertEqual((destination / "display").read_text(), (destination / "current/value.json").read_text())

    def test_failed_restore_removes_partial_new_state(self):
        state = self.root / "state.json"
        state.write_text('{"cursor":1}')
        support.snapshot(self.value({"state": str(state)}), self.root / "backup")
        destination = self.root / "restored/state.json"
        original = support.shutil.copy2
        def interrupted(source, target):
            original(source, target)
            raise OSError("simulated interrupted copy")
        with patch.object(support.shutil, "copy2", side_effect=interrupted):
            with self.assertRaises(OSError):
                support.restore(self.root / "backup", self.value({"state": str(destination)}))
        self.assertFalse(destination.exists())
        self.assertEqual('{"cursor":1}', state.read_text())

    def test_backup_cannot_recurse_into_state_through_parent_symlink(self):
        context = self.root / "context"
        context.mkdir()
        alias = self.root / "alias"
        alias.symlink_to(context)
        with self.assertRaisesRegex(ValueError, "overlaps"):
            support.snapshot(self.value({"state": str(context)}), alias / "backup")
        self.assertEqual([], list(context.iterdir()))


if __name__ == "__main__":
    unittest.main()
