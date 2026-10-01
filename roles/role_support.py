#!/usr/bin/env python3
"""Local role packaging, configuration, service rendering and offline recovery.

No network, cloud dispatch or business decisions. Source bundles never include
runtime state or credentials. Backups remain local and require stopped writers.
"""
import argparse
import contextlib
import fcntl
import json
import os
from pathlib import Path, PurePosixPath
import plistlib
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile

VERSION = "2026.10.01.1"
NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,100}\Z")


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def write_json(path, value):
    path = Path(path)
    with path.open("x", encoding="utf-8") as stream:
        os.chmod(path, 0o600)
        json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.write("\n")


def local_path(value):
    path = Path(value).expanduser()
    if not path.is_absolute():
        raise ValueError("machine paths must be absolute")
    return path


def relative_path(value):
    path = PurePosixPath(value)
    if not value or path.is_absolute() or ".." in path.parts or "\\" in value or str(path) != value:
        raise ValueError("invalid relative path")
    return Path(*path.parts)


def config(path):
    value = read_json(path)
    if value.get("role") not in ("collector", "analysis"):
        raise ValueError("role must be collector or analysis")
    for key in ("machine", "python", "code_dir", "entrypoint", "service_label", "log_dir"):
        if not isinstance(value.get(key), str) or not value[key]:
            raise ValueError("missing machine configuration: " + key)
    for key in ("python", "code_dir", "log_dir"):
        local_path(value[key])
    relative_path(value["entrypoint"])
    if not NAME.fullmatch(value["service_label"]):
        raise ValueError("invalid service label")
    if not isinstance(value.get("arguments", []), list) or not all(isinstance(x, str) for x in value.get("arguments", [])):
        raise ValueError("arguments must be a string array")
    for name, item in value.get("state_paths", {}).items():
        if not NAME.fullmatch(name):
            raise ValueError("invalid state name")
        local_path(item)
    for item in value.get("writer_locks", []):
        local_path(item)
    if value.get("private_environment_file"):
        local_path(value["private_environment_file"])
    env = value.get("environment", {})
    if not isinstance(env, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in env.items()):
        raise ValueError("environment must contain strings")
    if any(re.search(r"TOKEN|SECRET|PASSWORD|API_KEY|CHAT_ID", k, re.I) for k in env):
        raise ValueError("credentials belong in private_environment_file")
    return value


def check(value):
    root = local_path(value["code_dir"])
    checks = {"python": local_path(value["python"]).is_file(),
              "entrypoint": (root / relative_path(value["entrypoint"])).is_file()}
    private = value.get("private_environment_file")
    if private:
        path = local_path(private)
        checks["private_environment_present"] = path.is_file()
        checks["private_environment_permissions"] = path.is_file() and not (path.stat().st_mode & 0o077)
    return {"role": value["role"], "machine": value["machine"], "checks": checks,
            "ready": all(checks.values()),
            "state_present": {name: local_path(p).exists() for name, p in value.get("state_paths", {}).items()}}


def package(root, files, destination, role):
    """An explicit source allowlist is required; never recursively package a host."""
    root = Path(root).resolve()
    contents = {}
    for name in sorted(set(files)):
        relative = relative_path(name)
        source = root / relative
        if source.is_symlink() or root not in source.resolve().parents:
            raise ValueError("source is outside the reference root")
        if any(part in (".strategy", ".git", "__pycache__", "node_modules") for part in relative.parts):
            raise ValueError("runtime data cannot be a source reference")
        if source.suffix not in (".py", ".md", ".txt", ".json", ".sh"):
            raise ValueError("unsupported source reference type")
        contents[name] = source.read_text(encoding="utf-8")
    if not contents:
        raise ValueError("empty source reference")
    write_json(destination, {"format": "market-role-source-v1", "version": VERSION,
                             "role": role, "files": contents})
    return {"role": role, "files": len(contents), "path": str(destination)}


def unpack(source, destination):
    data = read_json(source)
    if data.get("format") != "market-role-source-v1" or not isinstance(data.get("files"), dict):
        raise ValueError("unsupported source reference")
    files = [(relative_path(k), v) for k, v in data["files"].items()]
    if not all(isinstance(v, str) for _, v in files):
        raise ValueError("reference file content must be text")
    destination = Path(destination)
    if destination.exists():
        raise ValueError("rebuild destination must not exist")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="role-rebuild-", dir=destination.parent) as tmp:
        staged = Path(tmp) / "code"
        staged.mkdir(mode=0o700)
        for name, value in files:
            path = staged / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(value, encoding="utf-8")
        staged.rename(destination)
    return {"role": data.get("role"), "version": data.get("version"), "files": len(files), "path": str(destination)}


def tree_identity(path):
    if path.is_symlink():
        raise ValueError("state symlinks are unsupported")
    if not path.exists():
        return None
    nodes = [path] + (sorted(path.rglob("*")) if path.is_dir() else [])
    result = []
    for node in nodes:
        if node.is_symlink():
            target = node.resolve()
            if path.resolve() != target and path.resolve() not in target.parents:
                raise ValueError("state link escapes its configured root")
            stat = node.lstat()
            result.append((str(node.relative_to(path)), "link", os.readlink(node), stat.st_ino, stat.st_mtime_ns, stat.st_ctime_ns))
            continue
        if not (node.is_file() or node.is_dir()):
            raise ValueError("state contains a non-regular entry")
        stat = node.stat()
        result.append((str(node.relative_to(path)), stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns))
    return result


def copy_state_tree(source, destination):
    """Retain internal context pointers and make absolute internal links portable."""
    tree_identity(source)
    shutil.copytree(source, destination, symlinks=True)
    for original in source.rglob("*"):
        if original.is_symlink():
            copied = destination / original.relative_to(source)
            relative_target = original.resolve().relative_to(source.resolve())
            target = destination / relative_target
            copied.unlink()
            copied.symlink_to(os.path.relpath(target, copied.parent))


def snapshot(config_value, destination):
    """All configured writers (including Codex automations) must be stopped first."""
    destination = Path(destination).expanduser().absolute()
    if destination.exists():
        raise ValueError("backup destination must not exist")
    paths = {k: local_path(v) for k, v in config_value.get("state_paths", {}).items()}
    if not paths:
        raise ValueError("no state paths configured")
    for path in paths.values():
        if path.resolve() == destination.resolve() or path.resolve() in destination.resolve().parents:
            raise ValueError("backup destination overlaps state")
    with contextlib.ExitStack() as locks:
        for name in config_value.get("writer_locks", []):
            path = local_path(name)
            if path.exists():
                stream = locks.enter_context(path.open("r+"))
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # Include WAL siblings even if the main database is a single configured file.
        expanded = dict(paths)
        for key, path in paths.items():
            if path.is_file():
                for suffix in ("-wal", "-shm", "-journal"):
                    expanded[key + suffix] = Path(str(path) + suffix)
        before = {k: tree_identity(p) for k, p in expanded.items()}
        if any(p.exists() for k, p in expanded.items() if k.endswith("-journal")):
            raise ValueError("rollback journal exists; stop and recover writer before backup")
        destination.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="role-backup-", dir=destination.parent) as tmp:
            staged = Path(tmp) / "snapshot"
            staged.mkdir(mode=0o700)
            entries = {}
            for key, path in paths.items():
                if not path.exists():
                    entries[key] = None
                    continue
                target = staged / key
                if path.is_dir():
                    copy_state_tree(path, target)
                else:
                    shutil.copy2(path, target)
                    for suffix in ("-wal", "-shm"):
                        side = Path(str(path) + suffix)
                        if side.exists():
                            shutil.copy2(side, Path(str(target) + suffix))
                entries[key] = {"kind": "directory" if path.is_dir() else "file"}
            if before != {k: tree_identity(p) for k, p in expanded.items()}:
                raise ValueError("state changed during backup; stop all writers and retry")
            # SQLite recovery happens only in the copied data, preserving the source.
            for copied in list(staged.rglob("*")):
                if copied.is_symlink() or not copied.is_file():
                    continue
                with copied.open("rb") as stream:
                    sqlite_header = stream.read(16) == b"SQLite format 3\x00"
                if sqlite_header:
                    clean = copied.with_name(copied.name + ".backup-clean")
                    with contextlib.closing(sqlite3.connect(copied)) as src, contextlib.closing(sqlite3.connect(clean)) as dst:
                        src.backup(dst)
                        if dst.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                            raise ValueError("copied database failed integrity check")
                    for suffix in ("-wal", "-shm"):
                        Path(str(copied) + suffix).unlink(missing_ok=True)
                    clean.replace(copied)
            write_json(staged / "snapshot.json", {"format": "market-role-state-v1", "role": config_value["role"], "entries": entries})
            for entry in staged.rglob("*"):
                if not entry.is_symlink():
                    os.chmod(entry, 0o700 if entry.is_dir() else 0o600)
            staged.rename(destination)
    return {"role": config_value["role"], "path": str(destination), "entries": entries}


def restore(source, value):
    """Restore to unused configured locations; never merge with live state."""
    source = Path(source)
    data = read_json(source / "snapshot.json")
    if data.get("format") != "market-role-state-v1" or data.get("role") != value["role"]:
        raise ValueError("backup role or format mismatch")
    if set(data["entries"]) != set(value.get("state_paths", {})):
        raise ValueError("state mappings must preserve every entry")
    targets = {k: local_path(v) for k, v in value["state_paths"].items()}
    resolved = [p.resolve() for p in targets.values()]
    if any(a == b or a in b.parents or b in a.parents
           for index, a in enumerate(resolved) for b in resolved[index + 1:]):
        raise ValueError("restore destinations must be separate state locations")
    if any(p.exists() or p.is_symlink() for p in targets.values()):
        raise ValueError("restore destinations must not exist")
    for key, entry in data["entries"].items():
        if not NAME.fullmatch(key):
            raise ValueError("invalid state entry")
        if entry is not None:
            tree_identity(source / key)
            if not (source / key).exists():
                raise ValueError("backup state entry is missing")
    completed = []
    try:
        for key, entry in data["entries"].items():
            if entry is None:
                continue
            destination = targets[key]
            destination.parent.mkdir(parents=True, exist_ok=True)
            completed.append(destination)
            if entry["kind"] == "directory":
                copy_state_tree(source / key, destination)
            else:
                shutil.copy2(source / key, destination)
    except Exception:
        for path in completed:
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink(missing_ok=True)
        raise
    return {"role": value["role"], "restored": len(completed), "services_started": False}


def render_service(config_path, destination):
    value = config(config_path)
    root = local_path(value["code_dir"])
    helper = root / "roles/role_support.py"
    if not helper.is_file():
        raise ValueError("deployed role helper missing")
    logs = local_path(value["log_dir"])
    result = {"Label": value["service_label"],
              "ProgramArguments": [str(local_path(value["python"])), str(helper), "run", "--config", str(Path(config_path).absolute())],
              "WorkingDirectory": str(root), "RunAtLoad": True,
              "StandardOutPath": str(logs / "stdout.log"), "StandardErrorPath": str(logs / "stderr.log")}
    if value.get("service_interval_seconds"):
        result["StartInterval"] = int(value["service_interval_seconds"])
    else:
        result["KeepAlive"] = True
    with Path(destination).open("xb") as stream:
        os.chmod(destination, 0o600)
        plistlib.dump(result, stream)
    return {"path": str(destination), "label": result["Label"], "installed": False}


def run(value):
    environment = os.environ.copy()
    environment.update(value.get("environment", {}))
    if value.get("private_environment_file"):
        path = local_path(value["private_environment_file"])
        if path.stat().st_mode & 0o077:
            raise ValueError("private environment file must have owner-only permissions")
        private = read_json(path)
        if not isinstance(private, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in private.items()):
            raise ValueError("private environment must contain strings")
        if set(private) & set(value.get("environment", {})):
            raise ValueError("private environment cannot override public configuration")
        environment.update(private)
    root = local_path(value["code_dir"])
    os.chdir(root)
    python = str(local_path(value["python"]))
    os.execve(python, [python, str(root / relative_path(value["entrypoint"])), *value.get("arguments", [])], environment)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("check", "run", "status"):
        p = sub.add_parser(name)
        p.add_argument("--config", required=True)
    for name in ("snapshot", "restore", "render-service"):
        p = sub.add_parser(name)
        p.add_argument("--config", required=True)
        p.add_argument("path")
    p = sub.add_parser("package")
    p.add_argument("--root", required=True)
    p.add_argument("--role", required=True, choices=("collector", "analysis"))
    p.add_argument("--files", required=True, help="JSON array of explicit source paths; excludes runtime data")
    p.add_argument("path")
    p = sub.add_parser("unpack")
    p.add_argument("source")
    p.add_argument("destination")
    args = parser.parse_args()
    try:
        if args.command == "package":
            result = package(args.root, read_json(args.files), args.path, args.role)
        elif args.command == "unpack":
            result = unpack(args.source, args.destination)
        else:
            value = config(args.config)
            if args.command == "check":
                result = check(value)
            elif args.command == "run":
                run(value)
                return
            elif args.command == "status":
                run_result = subprocess.run(["launchctl", "print", "gui/{}/{}".format(os.getuid(), value["service_label"])], text=True, capture_output=True)
                result = {"registered": run_result.returncode == 0, "label": value["service_label"],
                          "state": [x.strip() for x in run_result.stdout.splitlines() if x.strip().startswith(("state =", "pid =", "last exit code ="))]}
            elif args.command == "snapshot":
                result = snapshot(value, args.path)
            elif args.command == "restore":
                result = restore(args.path, value)
            else:
                result = render_service(args.config, args.path)
        print(json.dumps(result, ensure_ascii=False))
        return 1 if args.command == "check" and not result["ready"] else 0
    except (ValueError, OSError, sqlite3.Error) as error:
        print(json.dumps({"error": type(error).__name__, "message": str(error)}, ensure_ascii=False), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
