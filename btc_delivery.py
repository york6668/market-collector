"""持久命令队列与 Telegram 两段投递；未知结果永不自动重发。"""

import fcntl
import hashlib
import json
import math
import os
import sqlite3
import threading
import time
from market_assets import command_identity
from contextlib import contextmanager


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


class DeliveryFailure(Exception):
    def __init__(self, reason, retry_after=None):
        super().__init__(reason)
        self.reason, self.retry_after = reason, retry_after


class ReceiptPersistenceError(RuntimeError):
    """已有远端回执；只能补 record_sent，不能重发网络请求。"""
    def __init__(self, claim, receipt):
        super().__init__(f"receipt persistence failed: {claim['report_id']}/{claim['part']}")
        self.claim, self.receipt = claim, receipt


class FailurePersistenceError(RuntimeError):
    """发送结果已知；本进程只补写对应 attempt 的失败状态。"""
    def __init__(self, claim, failure):
        super().__init__(f"failure persistence failed: {claim['report_id']}/{claim['part']}")
        self.claim, self.failure = claim, failure


_registry_lock = threading.Lock()
_worker_locks = {}


class DeliveryStore:
    def __init__(self, path, *, clock=time.time, max_retries=2, retry_delay=30):
        self.path = os.path.abspath(os.path.expanduser(path))
        self.clock = clock
        self.max_retries = max(0, int(max_retries))
        self.retry_delay = max(1, float(retry_delay))
        self.lock = threading.RLock()
        self.pending_receipts = {}
        self.pending_failures = {}
        self.pending_command_requeues = {}
        with _registry_lock:
            self.worker_thread_lock = _worker_locks.setdefault(self.path, threading.RLock())
        self.db = sqlite3.connect(self.path, timeout=5, isolation_level=None,
                                  check_same_thread=False)
        os.chmod(self.path, 0o600)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS commands (
                update_id INTEGER PRIMARY KEY, command TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending', result TEXT);
            CREATE TABLE IF NOT EXISTS reports (
                report_id TEXT PRIMARY KEY, text TEXT NOT NULL, document BLOB,
                content_sha256 TEXT NOT NULL, commit_payload TEXT NOT NULL,
                created_at REAL NOT NULL,
                summary_status TEXT NOT NULL DEFAULT 'pending',
                document_status TEXT NOT NULL DEFAULT 'pending',
                summary_attempts INTEGER NOT NULL DEFAULT 0,
                document_attempts INTEGER NOT NULL DEFAULT 0,
                summary_retry_at REAL NOT NULL, document_retry_at REAL NOT NULL,
                summary_receipt TEXT, document_receipt TEXT,
                summary_error TEXT, document_error TEXT,
                applied INTEGER NOT NULL DEFAULT 0);
            CREATE INDEX IF NOT EXISTS commands_pending ON commands(status, update_id);
            CREATE INDEX IF NOT EXISTS reports_apply ON reports(summary_status, applied, created_at);
        """)
        if "asset_id" not in {row[1] for row in self.db.execute("PRAGMA table_info(reports)")}:
            self.db.execute("ALTER TABLE reports ADD COLUMN asset_id TEXT NOT NULL DEFAULT 'BTC'")

    @contextmanager
    def _transaction(self):
        with self.lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                yield self.db
                self.db.commit()
            except BaseException:
                self.db.rollback()
                raise

    @contextmanager
    def worker_lock(self, asset_id=None):
        # 与采集 state 锁完全分离；覆盖 claim、请求、回执及崩溃恢复。
        key = self.path + ("." + asset_id if asset_id else "")
        with _registry_lock:
            lock = _worker_locks.setdefault(key, threading.RLock())
        with lock:
            fd = os.open(key + ".worker.lock", os.O_CREAT | os.O_RDWR, 0o600)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX)
                yield
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
                os.close(fd)

    def get_offset(self, legacy_offset=0):
        with self._transaction() as db:
            row = db.execute("SELECT value FROM metadata WHERE key='tg_update_offset'").fetchone()
            if row is not None:
                return int(row[0])
            value = legacy_offset() if callable(legacy_offset) else legacy_offset
            offset = max(0, int(value or 0))
            db.execute("INSERT INTO metadata VALUES ('tg_update_offset', ?)", (str(offset),))
            return offset

    def ingest_commands(self, updates, allowed_chat_id, parse_command):
        """同一事务接收整批命令并推进 offset，不以最后一条覆盖前面的命令。"""
        with self._transaction() as db:
            row = db.execute("SELECT value FROM metadata WHERE key='tg_update_offset'").fetchone()
            offset = int(row[0]) if row else 0
            next_offset, inserted = offset, 0
            for item in updates:
                if not isinstance(item, dict):
                    continue
                update_id = item.get("update_id")
                if type(update_id) is not int or not offset <= update_id < 2**63 - 1:
                    continue
                next_offset = max(next_offset, update_id + 1)
                msg = item.get("message") or item.get("channel_post") or {}
                if not isinstance(msg, dict):
                    continue
                chat = msg.get("chat")
                if not isinstance(chat, dict) or chat.get("id") != allowed_chat_id:
                    continue
                text = msg.get("text")
                command = parse_command(text) if isinstance(text, str) else None
                try:
                    command_identity(command)
                except ValueError:
                    continue
                if command:
                    inserted += db.execute(
                        "INSERT OR IGNORE INTO commands(update_id,command) VALUES (?,?)",
                        (update_id, command)).rowcount
            db.execute("INSERT OR REPLACE INTO metadata VALUES ('tg_update_offset',?)", (str(next_offset),))
            return {"offset": next_offset, "inserted": inserted}

    def claim_command(self):
        for update_id, result in list(self.pending_command_requeues.items()):
            if not self.finish_report_command(update_id, f"tg:{update_id}"):
                self.requeue_command(update_id, result)
            self.pending_command_requeues.pop(update_id, None)
        with self._transaction() as db:
            # 修复旧版本入队后未完成的命令；只补本地状态，不重采或发送。
            self._reconcile_report_commands(db)
            running = {command_identity(row[0])[1] for row in db.execute("SELECT command FROM commands WHERE status='running'")}
            row = next((item for item in db.execute("SELECT * FROM commands WHERE status='pending' ORDER BY update_id")
                        if command_identity(item["command"])[1] not in running
                        and float(json.loads(item["result"] or "{}").get("retry_at") or 0) <= self.clock()), None)
            if row is None:
                return None
            result = json.loads(row["result"] or "{}")
            if float(result.get("retry_at") or 0) > self.clock():
                return None
            db.execute("UPDATE commands SET status='running' WHERE update_id=?", (row["update_id"],))
            return {**dict(row), "status": "running"}

    def requeue_command(self, update_id, result, max_retries=2, delay_sec=30):
        with self._transaction() as db:
            row = db.execute("SELECT result FROM commands WHERE update_id=? AND status='running'",
                             (update_id,)).fetchone()
            if row is None:
                return None
            previous = json.loads(row["result"] or "{}")
            retries = int(previous.get("prepare_retries") or 0) + 1
            status = "pending" if retries <= max_retries else "failed"
            result = {**result, "prepare_retries": retries,
                      "retry_at": self.clock() + max(1, delay_sec)}
            db.execute("UPDATE commands SET status=?,result=? WHERE update_id=? AND status='running'",
                       (status, _json(result), update_id))
            return status

    def defer_command(self, update_id, result):
        self.pending_command_requeues[update_id] = result
        status = self.requeue_command(update_id, result)
        self.pending_command_requeues.pop(update_id, None)
        return status

    def finish_command(self, update_id, result, status="done"):
        if status not in ("done", "failed", "unknown"):
            raise ValueError("invalid_command_status")
        with self._transaction() as db:
            return bool(db.execute(
                "UPDATE commands SET status=?,result=? WHERE update_id=? AND status='running'",
                (status, _json(result), update_id)).rowcount)

    @staticmethod
    def _finish_report_command(db, update_id, report_id):
        if type(update_id) is not int or update_id < 0 or report_id != f"tg:{update_id}":
            raise ValueError("command_report_identity_mismatch")
        db.execute("UPDATE commands SET status='done',result=? WHERE update_id=? "
                   "AND status IN ('pending','running')",
                   (_json({"report_id": report_id, "status": "queued"}), update_id))

    def finish_report_command(self, update_id, report_id):
        with self._transaction() as db:
            if db.execute("SELECT 1 FROM reports WHERE report_id=?", (report_id,)).fetchone() is None:
                return False
            self._finish_report_command(db, update_id, report_id)
            return True

    def _reconcile_report_commands(self, db):
        rows = db.execute("SELECT c.update_id,r.report_id FROM commands c JOIN reports r "
                          "ON r.report_id='tg:' || c.update_id "
                          "WHERE c.status IN ('pending','running')").fetchall()
        for row in rows:
            self._finish_report_command(db, row["update_id"], row["report_id"])

    def enqueue_report(self, report_id, text, document, commit_payload, *, command_id=None):
        if not isinstance(report_id, str) or not report_id or not isinstance(text, str) or not text:
            raise ValueError("missing_report_identity_or_text")
        if document is not None and not isinstance(document, (bytes, bytearray, memoryview)):
            raise TypeError("document_must_be_bytes")
        document = bytes(document) if document is not None else None
        encoded = _json(commit_payload)
        fingerprint = hashlib.sha256(_json({
            "text": text, "document_sha256": hashlib.sha256(document).hexdigest() if document is not None else None,
            "commit_payload": commit_payload,
        }).encode("utf-8")).hexdigest()
        with self._transaction() as db:
            old = db.execute("SELECT content_sha256 FROM reports WHERE report_id=?", (report_id,)).fetchone()
            if old:
                if old[0] != fingerprint:
                    raise ValueError("report_id_content_conflict")
                if command_id is not None:
                    self._finish_report_command(db, command_id, report_id)
                return False
            now = self.clock()
            db.execute(
                "INSERT INTO reports(report_id,text,document,content_sha256,commit_payload,created_at,"
                "document_status,summary_retry_at,document_retry_at,asset_id) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (report_id, text, document, fingerprint, encoded, now,
                 "pending" if document is not None else "skipped", now, now, commit_payload.get("asset_id", "BTC")))
            if command_id is not None:
                self._finish_report_command(db, command_id, report_id)
            return True

    def get_report(self, report_id):
        with self.lock:
            row = self.db.execute("SELECT * FROM reports WHERE report_id=?", (report_id,)).fetchone()
            return dict(row) if row is not None else None

    def recover(self, recover_commands=False):
        with self.worker_lock():
            self._flush_receipts()
            with self._transaction() as db:
                for part in ("summary", "document"):
                    db.execute(f"UPDATE reports SET {part}_status='unknown',{part}_error='process_interrupted' "
                               f"WHERE {part}_status='sending'")
                self._expire_resume_reports(db)
                # 调用方持有独立 report writer 锁后才恢复命令执行，不影响在途采集。
                if recover_commands:
                    self._reconcile_report_commands(db)
                    db.execute("UPDATE commands SET status='pending' WHERE status='running'")

    def _claim_due(self, asset_id=None, report_id=None):
        now = self.clock()
        with self._transaction() as db:
            self._expire_resume_reports(db, asset_id)
            row = db.execute(
                "SELECT * FROM reports WHERE ((summary_status='pending' AND summary_retry_at<=?) OR "
                "(summary_status='sent' AND document_status='pending' AND document_retry_at<=?)) "
                "AND (? IS NULL OR asset_id=?) AND (? IS NULL OR report_id=?) "
                "ORDER BY created_at,report_id LIMIT 1", (now, now, asset_id, asset_id, report_id, report_id)).fetchone()
            if row is None:
                return None
            part = "summary" if row["summary_status"] == "pending" else "document"
            attempt = row[f"{part}_attempts"] + 1
            db.execute(f"UPDATE reports SET {part}_status='sending',{part}_attempts=? WHERE report_id=?",
                       (attempt, row["report_id"]))
            return {**dict(row), "part": part, "attempt": attempt}

    @staticmethod
    def _part(claim):
        if claim["part"] not in ("summary", "document"):
            raise ValueError("invalid_delivery_part")
        return claim["part"]

    def record_sent(self, claim, receipt):
        if (not isinstance(receipt, dict) or type(receipt.get("message_id")) is not int
                or receipt["message_id"] <= 0):
            raise ValueError("invalid_telegram_receipt")
        part, encoded = self._part(claim), _json(receipt)
        with self._transaction() as db:
            changed = db.execute(
                f"UPDATE reports SET {part}_status='sent',{part}_receipt=?,{part}_error=NULL "
                f"WHERE report_id=? AND {part}_attempts=? AND {part}_status IN ('sending','unknown')",
                (encoded, claim["report_id"], claim["attempt"])).rowcount
            if part == "document" and changed:
                db.execute("UPDATE reports SET document=NULL WHERE report_id=?", (claim["report_id"],))
            if changed:
                return True
            row = db.execute(f"SELECT {part}_status,{part}_receipt,{part}_attempts "
                             "FROM reports WHERE report_id=?", (claim["report_id"],)).fetchone()
            return bool(row and tuple(row) == ("sent", encoded, claim["attempt"]))

    def record_failure(self, claim, failure):
        part, reason = self._part(claim), failure.reason
        retryable = reason in ("connect_timeout", "rate_limited")
        status = "failed" if retryable or reason == "permanent" else "unknown"
        retry_at = 0
        if retryable and claim["attempt"] <= self.max_retries:
            delay = min(900, self.retry_delay * 2 ** (claim["attempt"] - 1))
            try:
                wait = float(failure.retry_after)
                if math.isfinite(wait) and wait >= 0:
                    delay = max(delay, wait)
            except (ValueError, TypeError, OverflowError):
                pass
            status, retry_at = "pending", self.clock() + delay
        with self._transaction() as db:
            changed = db.execute(
                f"UPDATE reports SET {part}_status=?,{part}_retry_at=?,{part}_error=? "
                f"WHERE report_id=? AND {part}_attempts=? AND {part}_status='sending'",
                (status, retry_at, reason, claim["report_id"], claim["attempt"])).rowcount
        return status if changed else "unchanged"

    def _flush_receipts(self, asset_id=None):
        for key, (claim, receipt) in list(self.pending_receipts.items()):
            if asset_id and claim.get("asset_id", "BTC") != asset_id:
                continue
            try:
                if not self.record_sent(claim, receipt):
                    raise RuntimeError("receipt_claim_conflict")
            except Exception as exc:
                raise ReceiptPersistenceError(claim, receipt) from exc
            del self.pending_receipts[key]
        for claim, failure in list(self.pending_failures.values()):
            if asset_id and claim.get("asset_id", "BTC") != asset_id:
                continue
            self._persist_failure(claim, failure)

    def _persist_failure(self, claim, failure):
        key = (claim["report_id"], claim["part"], claim["attempt"])
        self.pending_failures[key] = (claim, failure)
        try:
            # record_failure 使用 attempt + sending 比较更新；旧结果不覆盖新尝试。
            status = self.record_failure(claim, failure)
        except Exception as exc:
            raise FailurePersistenceError(claim, failure) from exc
        del self.pending_failures[key]
        return status

    def deliver_one(self, send_text, send_document, *, asset_id=None, report_id=None):
        with self.worker_lock(asset_id):
            self._flush_receipts(asset_id)
            claim = self._claim_due(asset_id, report_id)
            if claim is None:
                return None
            # 此处没有 SQLite 事务、DB 锁或采集 state 锁。
            try:
                if claim["part"] == "summary":
                    receipt = send_text(claim["text"])
                else:
                    parent = json.loads(claim["summary_receipt"])
                    receipt = send_document(claim["document"], reply_to_message_id=parent["message_id"])
                if (not isinstance(receipt, dict) or type(receipt.get("message_id")) is not int
                        or receipt["message_id"] <= 0):
                    raise DeliveryFailure("unknown")
                # 回执中的文本/附件体不复制入 SQLite；保留身份与服务端日期。
                receipt = {k: receipt[k] for k in ("message_id", "date", "chat") if k in receipt}
            except DeliveryFailure as exc:
                status = self._persist_failure(claim, exc)
            except Exception:
                status = self._persist_failure(claim, DeliveryFailure("unknown"))
            else:
                self.pending_receipts[(claim["report_id"], claim["part"])] = (claim, receipt)
                self._flush_receipts(asset_id)
                status = "sent"
            return {"report_id": claim["report_id"], "part": claim["part"], "status": status}

    def apply_pending(self, apply_callback, limit=50, *, asset_id=None):
        with self.worker_lock(asset_id):
            self._flush_receipts(asset_id)
            with self.lock:
                rows = self.db.execute(
                    "SELECT report_id,commit_payload,summary_receipt FROM reports "
                    "WHERE summary_status='sent' AND applied=0 AND (? IS NULL OR asset_id=?) ORDER BY created_at,report_id LIMIT ?",
                    (asset_id, asset_id, max(1, int(limit)))).fetchall()
            for row in rows:
                # 主 state 已提交但本事务失败时，回调以 report_id 去重。
                apply_callback(row["report_id"], json.loads(row["commit_payload"]), json.loads(row["summary_receipt"]))
                with self._transaction() as db:
                    db.execute("UPDATE reports SET applied=1,commit_payload='{}' WHERE report_id=? "
                               "AND summary_status='sent'", (row["report_id"],))

    @staticmethod
    def _expire_resume_reports(db, asset_id=None):
        # Retire only unsent parts. Sent receipts and uncertain outcomes remain
        # historical evidence and must never become eligible for another send.
        for part in ("summary", "document"):
            db.execute(f"UPDATE reports SET {part}_status='expired', {part}_error='resume_report_retired' "
                       f"WHERE {part}_status='pending' AND asset_id IN ('QQQ','VOO') "
                       "AND report_id LIKE lower(asset_id) || ':hour:%:resume:%' "
                       "AND (? IS NULL OR asset_id=?)", (asset_id, asset_id))

    def expire_scheduled(self, asset_id, before=None):
        prefix = "hour:" if asset_id == "BTC" else asset_id.lower() + ":hour:"
        with self._transaction() as db:
            cutoff = f"CAST(substr(report_id, {len(prefix) + 1}) AS INTEGER)"
            for part in ("summary", "document"):
                reason = ("common_hour_expired" if asset_id == "COMMON" else
                          "scheduled_hour_expired" if before is not None else "stock_session_expired")
                db.execute(f"UPDATE reports SET {part}_status='expired', {part}_error=? "
                           f"WHERE asset_id=? AND report_id LIKE ? AND {part}_status='pending' "
                           f"AND (? IS NULL OR {cutoff}<?)",
                           (reason, asset_id, prefix + "%", before, before))

    def load_metadata(self, key):
        with self.lock:
            row = self.db.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
            return json.loads(row[0]) if row else None

    def unapplied_report_commits(self, asset_id):
        """Durable prepared content, including unknown sends awaiting a receipt."""
        with self.lock:
            rows = self.db.execute(
                "SELECT report_id,summary_status,created_at,commit_payload FROM reports "
                "WHERE asset_id=? AND applied=0", (asset_id,)).fetchall()
            return [{**dict(row), "commit_payload": json.loads(row["commit_payload"])} for row in rows]

    def save_metadata(self, key, value):
        with self._transaction() as db:
            db.execute("INSERT OR REPLACE INTO metadata VALUES (?,?)", (key, _json(value)))

    def load_schedule(self, asset_id):
        with self.lock:
            row = self.db.execute("SELECT value FROM metadata WHERE key=?", ("schedule:" + asset_id,)).fetchone()
            return json.loads(row[0]) if row else None

    def save_schedule(self, asset_id, value):
        with self._transaction() as db:
            db.execute("INSERT OR REPLACE INTO metadata VALUES (?,?)", ("schedule:" + asset_id, _json(value)))

    def next_due_at(self):
        with self.lock:
            return self.db.execute(
                "SELECT MIN(due) FROM (SELECT summary_retry_at AS due FROM reports WHERE summary_status='pending' "
                "UNION ALL SELECT document_retry_at FROM reports WHERE summary_status='sent' AND document_status='pending')"
            ).fetchone()[0]

    def close(self):
        with self.lock:
            self.db.close()
