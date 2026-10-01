#!/usr/bin/env python3
"""Run the real role tests against temporary state with outbound sockets blocked."""
import os
from pathlib import Path
import socket
import sys
import tempfile
import unittest


_connect = socket.socket.connect
_connect_ex = socket.socket.connect_ex


def blocked_connect(sock, address):
    if sock.family in (socket.AF_INET, socket.AF_INET6):
        raise RuntimeError("role rehearsal blocks network access")
    return _connect(sock, address)


def blocked_connect_ex(sock, address):
    if sock.family in (socket.AF_INET, socket.AF_INET6):
        raise RuntimeError("role rehearsal blocks network access")
    return _connect_ex(sock, address)


# Spawned test workers also import this module and inherit the same restriction.
socket.socket.connect = blocked_connect
socket.socket.connect_ex = blocked_connect_ex
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def main():
    with tempfile.TemporaryDirectory(prefix="market-role-rehearsal-") as directory:
        # External machine paths must never escape the temporary test state.
        for name in ('QQQ_STATE_FILE', 'VOO_STATE_FILE', 'EQ_QQQ_STATE_FILE', 'EQ_VOO_STATE_FILE',
                     'MARKET_HISTORY_DB', 'BTC_PUSH_DELIVERY_DB', 'BTC_STATUS_RULES_FILE',
                     'QQQ_STATUS_RULES_FILE', 'VOO_STATUS_RULES_FILE'):
            os.environ.pop(name, None)
        os.environ.update({
            "BOT_TOKEN": "", "CHAT_ID": "", "BTC_PUSH_OLLAMA_ENABLED": "0",
            "BTC_PUSH_NEWS_TRANSLATE_WITH_OLLAMA": "0", "BTC_PUSH_IMMEDIATE_FIRST": "0",
            "STATE_FILE": str(Path(directory) / "state.json"),
            "BTC_PUSH_LOG_FILE": str(Path(directory) / "test.log"),
        })
        names = sys.argv[1:]
        suite = (unittest.defaultTestLoader.loadTestsFromNames(names) if names else
                 unittest.defaultTestLoader.discover(str(Path(__file__).resolve().parent.parent), pattern="test_*.py"))
        result = unittest.TextTestRunner(verbosity=1).run(suite)
        return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(main())
