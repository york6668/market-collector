#!/bin/sh
# Prepare only the independent collector's development and offline test environment.
set -eu

collector_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$collector_root"
collector_python=${COLLECTOR_PYTHON:-python3}
"$collector_python" -c 'import sys; assert sys.version_info >= (3, 9)'
"$collector_python" -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -B roles/validate_repository.py
