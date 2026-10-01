#!/usr/bin/env python3
"""Export the independent collector's explicit source set into a new directory."""
import argparse
import json
import os
from pathlib import Path
import shutil
import tempfile

try:
    from .role_support import relative_path
except ImportError:
    from role_support import relative_path


ROOT = Path(__file__).resolve().parents[1]


def export(role, destination, root=ROOT):
    if role != 'collector':
        raise ValueError('this repository exports only the collector')
    root = Path(root).resolve()
    entries = json.loads((root / 'roles/collector-source-files.json').read_text())
    if not isinstance(entries, list) or not entries or not all(isinstance(x, str) for x in entries):
        raise ValueError('collector source set must be a non-empty path list')
    if len(entries) != len(set(entries)):
        raise ValueError('duplicate collector source path')
    destination = Path(destination).expanduser().absolute()
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(destination)
    sources = []
    for name in entries:
        relative = relative_path(name)
        source = root / relative
        if source.is_symlink() or root not in source.resolve().parents or not source.is_file():
            raise ValueError('source must be a regular repository file')
        sources.append((relative, source))
    destination.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix='.collector-source-', dir=destination.parent))
    try:
        for relative, source in sources:
            output = stage / relative
            output.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, output)
        if destination.exists() or destination.is_symlink():
            raise FileExistsError(destination)
        os.rename(stage, destination)
    finally:
        if stage.exists():
            shutil.rmtree(stage)
    return {'role': role, 'destination': str(destination), 'files': len(sources)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('role', choices=('collector',))
    parser.add_argument('destination')
    args = parser.parse_args()
    print(json.dumps(export(args.role, args.destination), ensure_ascii=False))


if __name__ == '__main__':
    main()
