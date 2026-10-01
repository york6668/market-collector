#!/usr/bin/env python3
"""Rebuild and test the collector without an analysis checkout or live state."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

from export_role import export


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--role', choices=('collector', 'all'), default='collector',
                        help='all is retained as an alias for the sole collector role')
    parser.parse_args()
    if sys.version_info < (3, 9):
        parser.error('Python 3.9 or later is required')
    environment = dict(os.environ, BOT_TOKEN='', CHAT_ID='', PYTHONDONTWRITEBYTECODE='1')
    environment.pop('PYTHONPATH', None)
    with tempfile.TemporaryDirectory(prefix='market-collector-rebuild-') as directory:
        target = Path(directory) / 'collector'
        exported = export('collector', target)
        result = subprocess.run([sys.executable, '-B', str(target / 'roles/offline_tests.py')],
                                cwd=target, env=environment)
    print('MARKET_COLLECTOR_REBUILD_RESULT=' + json.dumps({
        'role': 'collector', 'files': exported['files'], 'exit_code': result.returncode}, sort_keys=True))
    return 0 if result.returncode == 0 else 1


if __name__ == '__main__':
    raise SystemExit(main())
