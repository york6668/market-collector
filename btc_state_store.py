"""本机报告与采集状态的分域 JSON 存储；读取不迁移、不创建文件。

只有显式 migrate() 才转换旧状态。采集域采用独立原子替换，报告保存
在同一进程/文件锁内合并较新采样；网络操作不得放入存储回调。
"""

import copy
import fcntl
import hashlib
import json
import os
import tempfile
import threading
import time
from contextlib import contextmanager


STORAGE_REVISION = 1
DOMAINS = {
    "liquidation_ws": (".liquidation.json", "events", "ts_ms"),
    "hyperliquid_derivatives": (".hyperliquid.json", "minute_samples", "received_at_ms"),
}
_locks = {}
_ready_files = {}
_registry_lock = threading.Lock()


class StateStorageError(RuntimeError):
    pass


class MigrationRequiredError(StateStorageError):
    pass


def _bytes(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":")).encode("utf-8")


def _read(path, missing=None):
    try:
        with open(path, "r", encoding="utf-8") as stream:
            value = json.load(stream)
    except FileNotFoundError:
        return copy.deepcopy(missing)
    except (OSError, ValueError) as exc:
        raise StateStorageError(f"state unreadable: {path}") from exc
    if not isinstance(value, dict):
        raise StateStorageError(f"state must be an object: {path}")
    return value


def _atomic_write(path, value):
    directory = os.path.dirname(path) or "."
    fd, temporary = tempfile.mkstemp(prefix=os.path.basename(path) + ".", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(_bytes(value))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _sequence_receipt(rows, owner, archive=None, json_path=None):
    result = {
        "count": len(rows), "sha256": hashlib.sha256(_bytes(rows)).hexdigest(),
        "source": owner.get("source") or owner.get("stream"),
        "source_as_of_ms": owner.get("source_as_of_ms") or owner.get("as_of_ms") or owner.get("last_event_at_ms") or owner.get("last_sample_at_ms"),
        "updated_at": owner.get("updated_at"), "updated_at_ms": owner.get("updated_at_ms"),
        "resolution": "fingerprint_only",
    }
    if archive:
        result.update(resolution="migration_backup", archive=dict(archive), json_path=json_path)
    return result


def _compact_domain(value, domain, archive=None, json_path=None):
    if not isinstance(value, dict):
        return value
    sequence = DOMAINS[domain][1]
    rows = value.get(sequence)
    if not isinstance(rows, list):
        return copy.deepcopy(value)
    value = copy.deepcopy({key: item for key, item in value.items() if key != sequence})
    if isinstance(rows, list):
        value[sequence + "_ref"] = _sequence_receipt(rows, value, archive, json_path)
    return value


def _compact_public(records, keep=None, archive=None, json_path=None):
    """只处理明确的数据源原始时序；不触碰形态/结构的 history。"""
    if not isinstance(records, dict):
        return records
    result = {}
    for source, original in records.items():
        record = original
        if not isinstance(record, dict) or not isinstance(record.get("value"), dict):
            result[source] = copy.deepcopy(record)
            continue
        sequences = {key: rows for key, rows in record["value"].items()
                     if key in ("hourly_flow", "history") and isinstance(rows, list)
                     and (source, key) not in (keep or set())}
        record = copy.deepcopy({**record, "value": {key: item for key, item in record["value"].items()
                                                    if key not in sequences}})
        result[source] = record
        for key in ("hourly_flow", "history"):
            rows = sequences.get(key)
            if isinstance(rows, list):
                location = f"{json_path}.{source}.value.{key}" if json_path else None
                record["value"][key + "_ref"] = _sequence_receipt(rows, record, archive, location)
    return result


def compact_event_log(event, archive=None, json_path=None):
    original = event
    special = set(DOMAINS) | {"public_snapshot", "public_records", "macro"}
    event = copy.deepcopy({key: value for key, value in original.items() if key not in special})
    for domain in DOMAINS:
        if domain in original:
            location = f"{json_path}.{domain}.{DOMAINS[domain][1]}" if json_path else None
            event[domain] = _compact_domain(original[domain], domain, archive, location)
    snapshot = original.get("public_snapshot")
    if isinstance(snapshot, dict):
        event["public_snapshot"] = copy.deepcopy({key: value for key, value in snapshot.items() if key != "public"})
        event["public_snapshot"]["public"] = _compact_public(snapshot.get("public"), archive=archive,
                                            json_path=f"{json_path}.public_snapshot.public" if json_path else None)
    if isinstance(original.get("public_records"), dict):
        event["public_records"] = _compact_public(original["public_records"], archive=archive,
                                                  json_path=f"{json_path}.public_records" if json_path else None)
    if isinstance(original.get("macro"), dict):
        event["macro"] = _compact_public({"macro": original["macro"]}, archive=archive,
                                         json_path=json_path)["macro"]
    for key in special - event.keys():
        if key in original:
            event[key] = copy.deepcopy(original[key])
    return event


def compact_public_snapshots(snapshots, archive=None):
    """历史比较只读聚合值；每来源/序列保留最新一份原始数据供回退。"""
    result = [None] * len(snapshots or [])
    seen = set()
    for index in range(len(result) - 1, -1, -1):
        original = snapshots[index]
        snapshot = copy.deepcopy({key: value for key, value in original.items() if key != "public"})
        result[index] = snapshot
        records = original.get("public") or {}
        keep = set()
        for source, record in records.items():
            value = (record or {}).get("value") or {}
            for key in ("hourly_flow", "history"):
                identity = (source, key)
                if isinstance(value.get(key), list) and identity not in seen:
                    keep.add(identity)
                    seen.add(identity)
        snapshot["public"] = _compact_public(records, keep, archive, f"public_snapshots[{index}].public")
    return result


def compact_state(state, archive=None):
    result = copy.deepcopy({key: value for key, value in state.items()
                            if key not in DOMAINS and key not in ("event_logs", "public_snapshots")})
    if "event_logs" in state:
        result["event_logs"] = [compact_event_log(event, archive, f"event_logs[{index}]")
                                for index, event in enumerate(state["event_logs"] or [])]
    if "public_snapshots" in state:
        result["public_snapshots"] = compact_public_snapshots(state["public_snapshots"], archive)
    report = result.get("report")
    if isinstance(report, dict):
        for domain in DOMAINS:
            if domain in report:
                report[domain] = _compact_domain(report[domain], domain, archive,
                                                 f"report.{domain}.{DOMAINS[domain][1]}")
    result["storage_revision"] = STORAGE_REVISION
    return result


def merge_domain(domain, current, incoming):
    current, incoming = current or {}, incoming or {}
    if not incoming:
        return copy.deepcopy(current)
    if not current:
        return copy.deepcopy(incoming)
    newer, older = (incoming, current) if int(incoming.get("updated_at_ms") or 0) > int(current.get("updated_at_ms") or 0) else (current, incoming)
    # 换流、换币或显式重置之后，旧报告不能复活原采集历史。
    identity_changed = any(current.get(k) and incoming.get(k) and current[k] != incoming[k]
                           for k in ("stream", "coin"))
    if identity_changed:
        return copy.deepcopy(newer)
    reset_at = int(newer.get("reset_at_ms") or 0)
    if reset_at and int(older.get("updated_at_ms") or 0) <= reset_at:
        return copy.deepcopy(newer)
    result = copy.deepcopy(older)
    result.update(copy.deepcopy(newer))
    _, sequence, time_key = DOMAINS[domain]
    rows = {}
    for item in list(older.get(sequence) or []) + list(newer.get(sequence) or []):
        if domain == "liquidation_ws":
            key = item.get("id") or hashlib.sha256(_bytes(item)).hexdigest()
        else:
            key = int(item.get("minute_ms") or (int(item.get(time_key) or 0) // 60000) * 60000)
        previous = rows.get(key)
        if previous is None or int(item.get(time_key) or 0) >= int(previous.get(time_key) or 0):
            rows[key] = copy.deepcopy(item)
    result[sequence] = sorted(rows.values(), key=lambda row: int(row.get(time_key) or 0))
    if domain == "liquidation_ws":
        result["event_count_total"] = max(int(current.get("event_count_total") or 0),
                                          int(incoming.get("event_count_total") or 0))
    return result


class StateStore:
    def __init__(self, path):
        self.path = os.path.abspath(os.path.expanduser(path))
        with _registry_lock:
            self._thread_lock = _locks.setdefault(self.path, threading.RLock())

    def domain_path(self, domain):
        return self.path + DOMAINS[domain][0]

    @contextmanager
    def _locked(self, suffix=".lock", nonblocking=False):
        with self._thread_lock:
            fd = os.open(self.path + suffix, os.O_CREAT | os.O_RDWR, 0o600)
            try:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | (fcntl.LOCK_NB if nonblocking else 0))
                except BlockingIOError as exc:
                    raise StateStorageError("another report writer is active") from exc
                yield
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
                os.close(fd)

    @contextmanager
    def writer_lock(self):
        # 不持有线程锁等待网络；采集线程仍可独立更新侧车。
        fd = os.open(self.path + ".writer.lock", os.O_CREAT | os.O_RDWR, 0o600)
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise StateStorageError("another report writer is active") from exc
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def _main(self):
        return _read(self.path)

    def _remember_ready(self):
        stat = os.stat(self.path)
        _ready_files[self.path] = (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns)

    def _ready_unchanged(self):
        try:
            stat = os.stat(self.path)
        except FileNotFoundError:
            return False
        return _ready_files.get(self.path) == (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns)

    def require_ready(self):
        main = self._main()
        if main is not None and main.get("storage_revision") != STORAGE_REVISION:
            raise MigrationRequiredError("legacy state requires explicit --migrate-state before writes")
        if main is not None:
            for domain in DOMAINS:
                self._domain(domain, required=True)
            self._remember_ready()
        return main

    def _domain(self, domain, required=False):
        data = _read(self.domain_path(domain))
        if data is None:
            if required:
                raise StateStorageError(f"collector sidecar missing: {domain}")
            return {}
        if data.get("storage_revision") != STORAGE_REVISION or data.get("domain") != domain or not isinstance(data.get("value"), dict):
            raise StateStorageError(f"invalid collector sidecar: {domain}")
        return data["value"]

    def read_domain(self, domain):
        if os.path.exists(self.domain_path(domain)):
            return self._domain(domain)
        main = self._main() or {}
        if main.get("storage_revision") == STORAGE_REVISION:
            return self._domain(domain, required=True)
        return copy.deepcopy(main.get(domain) or {})

    def load(self):
        main = self._main()
        if main is None:
            return {}
        result = copy.deepcopy(main)
        for domain in DOMAINS:
            if main.get("storage_revision") == STORAGE_REVISION:
                result[domain] = self._domain(domain, required=True)
            elif os.path.exists(self.domain_path(domain)):
                result[domain] = merge_domain(domain, self._domain(domain), result.get(domain))
        return result

    def _write_domain(self, domain, value):
        _atomic_write(self.domain_path(domain), {
            "storage_revision": STORAGE_REVISION, "domain": domain, "value": value,
        })

    def _ensure_new(self):
        main = self._main()
        if main is not None:
            if main.get("storage_revision") != STORAGE_REVISION:
                raise MigrationRequiredError("legacy state requires explicit --migrate-state before writes")
            for domain in DOMAINS:
                self._domain(domain, required=True)
            self._remember_ready()
            return main
        for domain in DOMAINS:
            if not os.path.exists(self.domain_path(domain)):
                self._write_domain(domain, {})
        main = {"storage_revision": STORAGE_REVISION}
        _atomic_write(self.path, main)
        self._remember_ready()
        return main

    def update_domain(self, domain, callback):
        with self._locked():
            # 已有侧车时完全不读取低频报告。旧状态没有侧车，必须先迁移。
            if not self._ready_unchanged():
                self._ensure_new()
            value = self._domain(domain, required=True)
            updated = callback(copy.deepcopy(value))
            if updated is None:
                return False
            self._write_domain(domain, updated)
            return True

    def save(self, state, pruners=None, reset_collectors=False):
        with self._locked():
            main = self._ensure_new()
            for domain in DOMAINS:
                current = self._domain(domain, required=True)
                if reset_collectors:
                    value = {"reset_at_ms": int(time.time() * 1000), "updated_at_ms": int(time.time() * 1000)}
                else:
                    value = merge_domain(domain, current, state.get(domain))
                    if pruners and domain in pruners:
                        value = pruners[domain](value)
                if value != current:
                    self._write_domain(domain, value)
            result = compact_state(state)
            if main.get("storage_migration"):
                result["storage_migration"] = main["storage_migration"]
            _atomic_write(self.path, result)
            self._remember_ready()

    def migrate(self, normalize=None):
        with self._locked():
            original = self._main()
            if original is None:
                self._ensure_new()
                return {"migrated": False, "new_state": True}
            if original.get("storage_revision") == STORAGE_REVISION:
                self.require_ready()
                return {"migrated": False, "storage_revision": STORAGE_REVISION}
            with open(self.path, "rb") as stream:
                raw = stream.read()
            digest = hashlib.sha256(raw).hexdigest()
            backup = self.path + ".pre-split-v1." + digest[:16] + ".bak"
            if not os.path.exists(backup):
                fd = os.open(backup, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
                with os.fdopen(fd, "wb") as stream:
                    stream.write(raw)
                    stream.flush()
                    os.fsync(stream.fileno())
            with open(backup, "rb") as stream:
                if hashlib.sha256(stream.read()).hexdigest() != digest:
                    raise StateStorageError("migration backup checksum mismatch")
            normalized = normalize(copy.deepcopy(original)) if normalize else original
            for domain in DOMAINS:
                value = merge_domain(domain, self._domain(domain), normalized.get(domain))
                self._write_domain(domain, value)
            result = compact_state(normalized, archive={"path": backup, "sha256": digest})
            result["storage_migration"] = {"backup": backup, "sha256": digest, "legacy_bytes": len(raw)}
            _atomic_write(self.path, result)
            self._remember_ready()
            return {"migrated": True, "storage_revision": STORAGE_REVISION,
                    "backup": backup, "legacy_bytes": len(raw),
                    "state_bytes": os.path.getsize(self.path)}
