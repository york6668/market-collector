"""Portable Telegram evidence intake; deployment adapter must retain its allowlist.

This module does not poll Telegram or write strategy/asset files. Downloads are
injected by the existing reader. Invalid attachments remain audit records and
cannot prevent independent messages from being ingested.
"""
import hashlib
import json
import os
import re
import shutil
import sqlite3
import stat
import tempfile
import threading
import time
from contextlib import closing, contextmanager
from pathlib import Path

from btc_evidence import (
    MAX_BYTES, decode_bundle, validate_bundle, auxiliary_observation,
)


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


def _snapshot_identity(path):
    try:
        info = path.stat()
    except FileNotFoundError:
        return None
    if not stat.S_ISREG(info.st_mode):
        raise sqlite3.OperationalError("snapshot source is not a regular file")
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def open_readonly_snapshot(path):
    """Return a query-only memory DB without opening the source through SQLite.

    Existing local stores use WAL. Copy the main DB and optional WAL during a
    stable stat interval; SQLite rebuilds SHM and recovers only in a private copy.
    A busy writer gets three bounded attempts, then an explicit retryable error.
    Rollback journals are unsupported: never expose uncommitted main-file pages.
    The returned snapshot excludes commits made after the successful copy.
    """
    source = Path(path).absolute()
    wal = Path(str(source) + "-wal")
    journal = Path(str(source) + "-journal")
    for _attempt in range(3):
        with tempfile.TemporaryDirectory(prefix="btc-readonly-snapshot-") as directory:
            snapshot = Path(directory) / "snapshot.sqlite3"
            try:
                before = tuple(_snapshot_identity(p) for p in (source, wal, journal))
                if before[0] is None:
                    raise sqlite3.OperationalError("snapshot source database is missing")
                if before[2] is not None:
                    raise sqlite3.OperationalError("snapshot requires a stable WAL or clean database")
                shutil.copyfile(source, snapshot)
                if before[1] is not None:
                    shutil.copyfile(wal, Path(str(snapshot) + "-wal"))
                after = tuple(_snapshot_identity(p) for p in (source, wal, journal))
                if before != after:
                    continue
            except FileNotFoundError:
                # Rotation/removal between stat and copy must not omit a WAL.
                continue
            with snapshot.open("rb") as stream:
                if stream.read(16) != b"SQLite format 3\x00":
                    raise sqlite3.OperationalError("snapshot source is not a SQLite database")
            memory = sqlite3.connect(":memory:", isolation_level=None, check_same_thread=False)
            try:
                # Only this private copy may recover WAL or create SQLite files.
                with closing(sqlite3.connect(snapshot, isolation_level=None)) as copied:
                    copied.backup(memory)
                memory.execute("PRAGMA query_only=ON")
                memory.row_factory = sqlite3.Row
                return memory
            except BaseException:
                memory.close()
                raise
    raise sqlite3.OperationalError("snapshot source changed during copy; retry later")


class EvidenceReader:
    def __init__(self, path, allowed_chat_id, allowed_sender_ids=None, clock=time.time,
                 *, read_only=False):
        self.allowed_chat_id = int(allowed_chat_id)
        self.allowed_sender_ids = (None if allowed_sender_ids is None
                                   else set(allowed_sender_ids))
        self.clock = clock
        self.lock = threading.RLock()
        if read_only:
            self.db = open_readonly_snapshot(path)
            return
        if path != ":memory:" and not os.path.exists(path):
            os.close(os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600))
        self.db = sqlite3.connect(path, timeout=5, isolation_level=None,
                                  check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS collector_updates(
                update_id INTEGER PRIMARY KEY, digest TEXT NOT NULL, status TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS collector_update_conflicts(
                update_id INTEGER NOT NULL, digest TEXT NOT NULL, content TEXT NOT NULL,
                PRIMARY KEY(update_id,digest));
            CREATE TABLE IF NOT EXISTS collector_validation_history(
                chat_id INTEGER NOT NULL, message_id INTEGER NOT NULL,
                revision TEXT NOT NULL, observed_at REAL NOT NULL, validation TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS collector_messages(
                chat_id INTEGER NOT NULL, message_id INTEGER NOT NULL,
                version_time INTEGER NOT NULL, update_id INTEGER NOT NULL,
                text TEXT, revision TEXT NOT NULL,
                PRIMARY KEY(chat_id,message_id));
            CREATE TABLE IF NOT EXISTS collector_revisions(
                chat_id INTEGER NOT NULL, message_id INTEGER NOT NULL,
                revision TEXT NOT NULL, content TEXT NOT NULL,
                PRIMARY KEY(chat_id,message_id,revision));
            CREATE TABLE IF NOT EXISTS collector_blobs(
                digest TEXT PRIMARY KEY, raw BLOB NOT NULL);
            CREATE TABLE IF NOT EXISTS collector_attachments(
                chat_id INTEGER NOT NULL, message_id INTEGER NOT NULL,
                revision TEXT NOT NULL, parent_id INTEGER,
                file_id TEXT NOT NULL, file_unique_id TEXT,
                declared_bytes INTEGER, received_at REAL NOT NULL,
                raw_digest TEXT, validation TEXT,
                download_status TEXT NOT NULL DEFAULT 'pending',
                download_error TEXT,
                download_attempts INTEGER NOT NULL DEFAULT 0,
                download_retry_at REAL NOT NULL DEFAULT 0,
                PRIMARY KEY(chat_id,message_id,revision));
        """)

    @contextmanager
    def _tx(self):
        with self.lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                yield self.db
                self.db.commit()
            except BaseException:
                self.db.rollback()
                raise

    def _allowed(self, message):
        chat = message.get("chat") or {}
        if not isinstance(chat, dict) or chat.get("id") != self.allowed_chat_id:
            return False
        if self.allowed_sender_ids is None:
            return True
        sender = message.get("from") or message.get("sender_chat") or {}
        return isinstance(sender, dict) and sender.get("id") in self.allowed_sender_ids

    def _save_message(self, db, message, update_id, nested=False):
        mid, stamp = message.get("message_id"), message.get("edit_date", message.get("date"))
        if type(mid) is not int or not 0 < mid < 2**63 - 1 or type(stamp) is not int or not 0 < stamp < 2**63 - 1:
            raise ValueError("invalid_message_identity")
        chat = self.allowed_chat_id
        old = db.execute("SELECT * FROM collector_messages WHERE chat_id=? AND message_id=?",
                         (chat, mid)).fetchone()
        # reply_to_message is a historical snapshot, never a later edit.
        if nested and old is not None:
            return False
        revision = hashlib.sha256(_json(message).encode("utf-8")).hexdigest()
        db.execute("INSERT OR IGNORE INTO collector_revisions VALUES (?,?,?,?)",
                   (chat, mid, revision, _json(message)))
        if old is not None and (stamp, update_id) <= (old["version_time"], old["update_id"]):
            return False
        body = message.get("text")
        body = body if isinstance(body, str) else None
        db.execute("INSERT OR REPLACE INTO collector_messages VALUES (?,?,?,?,?,?)",
                   (chat, mid, stamp, update_id, body, revision))
        document = message.get("document")
        if isinstance(document, dict) and isinstance(document.get("file_id"), str):
            filename = document.get("file_name") or ""
            if (document.get("mime_type") == "application/json" or
                    isinstance(filename, str) and filename.lower().endswith(".json")):
                parent = message.get("reply_to_message") or {}
                parent_id = parent.get("message_id") if isinstance(parent, dict) else None
                parent_id = parent_id if type(parent_id) is int and 0 < parent_id < 2**63 - 1 else None
                size = document.get("file_size")
                size = min(size, MAX_BYTES + 1) if type(size) is int and size >= 0 else None
                rejected = size is not None and size > MAX_BYTES
                validation = _json({"status": "rejected", "reasons": ["evidence_size_limit"],
                                    "independent_verified_sources": 0}) if rejected else None
                db.execute(
                    "INSERT OR IGNORE INTO collector_attachments "
                    "(chat_id,message_id,revision,parent_id,file_id,file_unique_id,"
                    "declared_bytes,received_at,validation,download_status) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (chat, mid, revision, parent_id, document["file_id"],
                     (document.get("file_unique_id") if isinstance(document.get("file_unique_id"), str) else None), size, self.clock(), validation,
                     "rejected" if rejected else "pending"))
        self._revalidate(db, parent_id=mid)
        return True

    def ingest_update(self, update):
        if not isinstance(update, dict) or type(update.get("update_id")) is not int:
            return {"status": "ignored", "reason": "invalid_update"}
        uid = update["update_id"]
        if not 0 <= uid < 2**63 - 1:
            return {"status": "ignored", "reason": "invalid_update"}
        try:
            digest = hashlib.sha256(_json(update).encode("utf-8")).hexdigest()
        except (TypeError, ValueError, OverflowError, RecursionError, UnicodeError):
            return {"status": "ignored", "reason": "invalid_update"}
        message = next((update.get(k) for k in (
            "message", "channel_post", "edited_message", "edited_channel_post")
            if isinstance(update.get(k), dict)), None)
        with self._tx() as db:
            old = db.execute("SELECT digest FROM collector_updates WHERE update_id=?", (uid,)).fetchone()
            if old:
                if old["digest"] != digest:
                    db.execute("INSERT OR IGNORE INTO collector_update_conflicts VALUES (?,?,?)",
                               (uid, digest, _json(update)))
                    return {"status": "update_id_conflict"}
                return {"status": "duplicate"}
            status = "ignored"
            if message and self._allowed(message):
                parent = message.get("reply_to_message")
                if isinstance(parent, dict) and self._allowed(parent):
                    try:
                        # A reply snapshot has no parent update_id; any same-second
                        # real parent update outranks this historical observation.
                        self._save_message(db, parent, -1, nested=True)
                    except (ValueError, TypeError):
                        pass
                try:
                    status = "stored" if self._save_message(db, message, uid) else "older_revision"
                except (ValueError, TypeError):
                    status = "invalid_message"
            db.execute("INSERT INTO collector_updates VALUES (?,?,?)", (uid, digest, status))
            return {"status": status}

    def _validation(self, db, attachment):
        if not attachment["raw_digest"]:
            return None
        raw = db.execute("SELECT raw FROM collector_blobs WHERE digest=?",
                         (attachment["raw_digest"],)).fetchone()
        try:
            bundle = decode_bundle(raw["raw"])
        except (ValueError, TypeError, RecursionError, OverflowError):
            return {"status": "rejected", "reasons": ["invalid_json"],
                    "independent_verified_sources": 0}
        parent = db.execute("SELECT text FROM collector_messages WHERE chat_id=? AND message_id=?",
                            (attachment["chat_id"], attachment["parent_id"])).fetchone()
        validation = validate_bundle(bundle, parent["text"] if parent else None)
        from market_reader import report_asset
        parent_asset = report_asset(parent["text"] if parent else None)
        if parent_asset and bundle.get("asset_id", "BTC") != parent_asset:
            validation = {"status": "rejected", "reasons": ["parent_asset_mismatch"],
                          "independent_verified_sources": 0}
        if attachment["parent_id"] is None:
            validation = {"status": "rejected", "reasons": ["missing_reply_relation"],
                          "independent_verified_sources": 0}
        return validation

    def _revalidate(self, db, parent_id=None, key=None):
        if key is not None:
            rows = db.execute("SELECT * FROM collector_attachments WHERE "
                              "chat_id=? AND message_id=? AND revision=?", key).fetchall()
        else:
            rows = db.execute("SELECT * FROM collector_attachments WHERE chat_id=? AND parent_id=?",
                              (self.allowed_chat_id, parent_id)).fetchall()
        for row in rows:
            validation = self._validation(db, row)
            if validation is not None:
                if row["validation"] != _json(validation):
                    db.execute("INSERT INTO collector_validation_history VALUES (?,?,?,?,?)",
                               (row["chat_id"], row["message_id"], row["revision"], self.clock(), _json(validation)))
                db.execute("UPDATE collector_attachments SET validation=? WHERE "
                           "chat_id=? AND message_id=? AND revision=?",
                           (_json(validation), row["chat_id"], row["message_id"], row["revision"]))

    def process_pending(self, download, limit=20):
        """Only Telegram file_id crosses this callback; no evidence URLs are fetched."""
        with self.lock:
            from market_reader import report_asset
            # A burst of historical/repeated reports must not postpone the
            # current three assets until the next hourly sync. Keep the same
            # bounded download budget and retain all historical pending work.
            rows = self.db.execute(
                "SELECT a.*,p.text AS parent_text FROM collector_attachments a JOIN collector_messages m "
                "ON a.chat_id=m.chat_id AND a.message_id=m.message_id AND a.revision=m.revision "
                "LEFT JOIN collector_messages p ON p.chat_id=a.chat_id AND p.message_id=a.parent_id "
                "WHERE a.chat_id=? AND a.download_status='pending' AND a.download_retry_at<=? "
                "ORDER BY a.received_at,a.message_id", (self.allowed_chat_id, self.clock())).fetchall()
            latest = {}
            for text_row in self.db.execute(
                    "SELECT message_id,text FROM collector_messages WHERE chat_id=? AND text IS NOT NULL "
                    "ORDER BY message_id DESC", (self.allowed_chat_id,)):
                symbol = report_asset(text_row['text'])
                if symbol and symbol not in latest:
                    latest[symbol] = text_row['message_id']
            priority = {}
            for row in rows:
                symbol = report_asset(row['parent_text'])
                if symbol and row['parent_id'] == latest.get(symbol):
                    if symbol not in priority or row['message_id'] > priority[symbol]['message_id']:
                        priority[symbol] = row
            first = list(priority.values())
            first_ids = {(r['message_id'], r['revision']) for r in first}
            rows = (first + [r for r in rows if (r['message_id'], r['revision']) not in first_ids])[:max(1, int(limit))]
        outcomes = []
        for row in rows:
            key = (row["chat_id"], row["message_id"], row["revision"])
            # Network work is outside transaction/lock; queued documents survive restarts.
            try:
                known = None
                if isinstance(row["file_unique_id"], str) and row["file_unique_id"]:
                    with self.lock:
                        known = self.db.execute(
                            "SELECT b.raw FROM collector_attachments a JOIN collector_blobs b "
                            "ON a.raw_digest=b.digest WHERE a.file_unique_id=? LIMIT 1",
                            (row["file_unique_id"],)).fetchone()
                declared = row["declared_bytes"]
                if known is not None and declared is not None and len(known["raw"]) != declared:
                    known = None
                raw = known["raw"] if known is not None else download(row["file_id"])
                if not isinstance(raw, bytes):
                    raise TypeError("document_not_bytes")
                if declared is not None and len(raw) != declared:
                    raise ValueError("document_size_mismatch")
            except Exception:
                with self._tx() as db:
                    db.execute("UPDATE collector_attachments SET download_error='download_failed',"
                               "download_attempts=download_attempts+1,download_retry_at=? "
                               "WHERE chat_id=? AND message_id=? AND revision=?",
                               (self.clock() + min(3600, 30 * 2 ** min(7, row["download_attempts"])), *key))
                outcomes.append({"message_id": row["message_id"], "status": "pending_download"})
                continue
            with self._tx() as db:
                if len(raw) > MAX_BYTES:
                    validation = {"status": "rejected", "reasons": ["evidence_size_limit"],
                                  "independent_verified_sources": 0}
                    db.execute("UPDATE collector_attachments SET validation=?,download_status='rejected' "
                               "WHERE chat_id=? AND message_id=? AND revision=?",
                               (_json(validation), *key))
                else:
                    digest = hashlib.sha256(raw).hexdigest()
                    db.execute("INSERT OR IGNORE INTO collector_blobs VALUES (?,?)", (digest, raw))
                    db.execute("UPDATE collector_attachments SET raw_digest=?,download_status='received',"
                               "download_error=NULL WHERE chat_id=? AND message_id=? AND revision=?",
                               (digest, *key))
                    self._revalidate(db, key=key)
                updated = db.execute("SELECT validation FROM collector_attachments WHERE "
                                     "chat_id=? AND message_id=? AND revision=?", key).fetchone()
                outcomes.append({"message_id": row["message_id"],
                                 **json.loads(updated["validation"])})
        return outcomes

    def observations(self, *, latest_only=False, max_output_bytes=None, asset="BTC", asset_id=None):
        """Export history, or the current attachment of the latest linked report.

        Snapshot selection precedes validation and blob lookup: pending or invalid
        evidence must not expose an older report as the current observation.
        The optional byte bound uses the reader CLI's JSON format, including its
        trailing newline, and fails without truncating datasets or changing state.
        """
        from market_reader import report_asset
        if asset_id is not None:
            if asset != "BTC" and asset != asset_id:
                raise ValueError("conflicting observation asset")
            asset = asset_id
        if asset not in ("BTC", "QQQ", "VOO", "all"):
            raise ValueError("unsupported observation asset")
        wanted = ("BTC", "QQQ", "VOO") if asset == "all" else (asset,)
        if max_output_bytes is None and latest_only:
            max_output_bytes = 4 * 1024 * 1024
        if max_output_bytes is not None and (
                type(max_output_bytes) is not int or max_output_bytes <= 0):
            raise ValueError("max_output_bytes must be a positive integer")
        with self.lock:
            # Select metadata first. Reading every historical blob would scale
            # with channel history instead of the three current assets.
            rows = self.db.execute(
                "SELECT a.*,p.text AS parent_text FROM collector_attachments a "
                "JOIN collector_messages m ON a.chat_id=m.chat_id AND a.message_id=m.message_id AND a.revision=m.revision "
                "LEFT JOIN collector_messages p ON p.chat_id=a.chat_id AND p.message_id=a.parent_id "
                "WHERE a.chat_id=? AND a.parent_id IS NOT NULL ORDER BY a.parent_id DESC,a.message_id DESC",
                (self.allowed_chat_id,)).fetchall()
            selected = {}
            if latest_only:
                # A newly posted text without its attachment also supersedes an
                # older usable report. Generic old BTC test/legacy texts remain supported.
                texts = self.db.execute(
                    "SELECT message_id,text FROM collector_messages WHERE chat_id=? AND text IS NOT NULL "
                    "ORDER BY message_id DESC", (self.allowed_chat_id,)).fetchall()
                for row in texts:
                    symbol = report_asset(row["text"])
                    if symbol in wanted and symbol not in selected:
                        selected[symbol] = {"parent_id": row["message_id"], "message_id": -1,
                                            "parent_text": row["text"], "raw_digest": None}
                for row in rows:
                    symbol = report_asset(row["parent_text"]) or ("BTC" if row["parent_text"] is not None else None)
                    if row["parent_text"] is None and row["raw_digest"]:
                        blob = self.db.execute("SELECT raw FROM collector_blobs WHERE digest=?", (row["raw_digest"],)).fetchone()
                        if blob:
                            try:
                                symbol = decode_bundle(blob["raw"]).get("asset_id", "BTC")
                            except (ValueError, TypeError):
                                pass
                    if symbol is None:
                        revision = self.db.execute(
                            "SELECT content FROM collector_revisions WHERE chat_id=? AND message_id=? AND revision=?",
                            (row['chat_id'], row['message_id'], row['revision'])).fetchone()
                        if revision:
                            name = (json.loads(revision['content']).get('document') or {}).get('file_name', '')
                            hint = re.match(r'^(BTC|QQQ|VOO)(?:[_. -]|$)', name, re.I) if isinstance(name, str) else None
                            symbol = hint.group(1).upper() if hint else None
                        # An unclassified pending document cannot supersede BTC
                        # merely because BTC was the historical default asset.
                    if symbol not in wanted:
                        continue
                    old = selected.get(symbol)
                    if old is None or (row["parent_id"], row["message_id"]) > (old["parent_id"], old["message_id"]):
                        selected[symbol] = dict(row)
                rows = [(symbol, selected.get(symbol)) for symbol in wanted]
            else:
                rows = [(report_asset(row["parent_text"]) or "BTC", dict(row)) for row in reversed(rows)]
            result, seen = [], set()
            for symbol, row in rows:
                if symbol not in wanted:
                    continue
                blob = self.db.execute("SELECT raw FROM collector_blobs WHERE digest=?", (row["raw_digest"],)).fetchone() if row and row["raw_digest"] else None
                observation, reasons = None, ["missing_report" if row is None else "attachment_unavailable"]
                if blob:
                    try:
                        bundle = decode_bundle(blob["raw"])
                        validation = validate_bundle(bundle, row["parent_text"])
                        reasons = validation["reasons"] or [validation["status"]]
                        # Revalidate retained bytes, not an old stored rejection.
                        observation = auxiliary_observation(bundle, row["parent_text"])
                        actual_asset = bundle.get("asset_id", "BTC")
                        if actual_asset != symbol:
                            observation, reasons = None, ["text_asset_mismatch"]
                    except (ValueError, TypeError):
                        reasons = ["invalid_attachment"]
                if observation is None:
                    if latest_only and asset == "all":
                        result.append({"role": "auxiliary_observation_gap", "asset_id": symbol,
                                       "reasons": reasons, "text_message_id": row["parent_id"] if row else None,
                                       "transport": {
                                           "text_message_id": row["parent_id"] if row else None,
                                           "document_message_id": row["message_id"] if row and row["message_id"] > 0 else None,
                                           "received_at": row.get("received_at") if row else None,
                                           "download_status": row.get("download_status", "missing_attachment") if row else "missing_report",
                                           "download_attempts": row.get("download_attempts") if row else None,
                                           "download_error": row.get("download_error") if row else None,
                                       },
                                       "independent_verified_sources": 0})
                    continue
                identity = (symbol, bundle["bundle_id"])
                if identity in seen:
                    continue
                seen.add(identity)
                observation["asset_id"] = symbol
                observation["transport"] = {
                    "chat_id": row["chat_id"], "text_message_id": row["parent_id"],
                    "document_message_id": row["message_id"], "received_at": row["received_at"],
                }
                observation["requires_independent_verification"] = True
                observation["requires_current_source_check"] = True
                result.append(observation)
        if max_output_bytes is not None:
            size = len((json.dumps(result, ensure_ascii=False, sort_keys=True) + "\n").encode("utf-8"))
            if size > max_output_bytes:
                raise ValueError(f"observations_output_size_limit: {size} > {max_output_bytes}")
        return result

    def close(self):
        with self.lock:
            self.db.close()
