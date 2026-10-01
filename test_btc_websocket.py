import base64
import hashlib
import json
import socket
import struct
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

import btc_websocket as ws
import btc_push as bot


def frame(opcode, payload=b"", fin=True):
    head = bytes([(0x80 if fin else 0) | opcode])
    if len(payload) < 126:
        return head + bytes([len(payload)]) + payload
    return head + b"\x7e" + struct.pack("!H", len(payload)) + payload


class FakeSocket:
    def __init__(self, chunks=()):
        self.chunks = list(chunks)
        self.sent = []
        self.closed = False
        self.timeouts = []

    def settimeout(self, timeout):
        self.timeouts.append(timeout)

    def sendall(self, data):
        self.sent.append(data)

    def recv(self, size):
        if not self.chunks:
            raise socket.timeout()
        item = self.chunks.pop(0)
        if isinstance(item, Exception):
            raise item
        if len(item) > size:
            self.chunks.insert(0, item[size:])
        return item[:size]

    def close(self):
        self.closed = True


class HandshakeSocket(FakeSocket):
    def __init__(self, extra=b"", alter=None):
        super().__init__()
        self.extra = extra
        self.alter = alter

    def sendall(self, data):
        super().sendall(data)
        key = data.split(b"Sec-WebSocket-Key: ", 1)[1].split(b"\r\n", 1)[0]
        accept = base64.b64encode(hashlib.sha1(key + ws.GUID.encode()).digest())
        response = (b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n"
                    b"Connection: keep-alive, Upgrade\r\nSec-WebSocket-Accept: " + accept + b"\r\n\r\n")
        if self.alter:
            response = self.alter(response)
        self.chunks = [response + self.extra]


class WebSocketTransportTests(unittest.TestCase):
    def connect(self, sock, **kwargs):
        with patch.object(ws.socket, "create_connection", return_value=sock):
            return ws.connect_websocket("ws://example.test:8080/feed?symbol=BTC", **kwargs)

    def test_handshake_keeps_first_frame_and_host_port(self):
        sock = HandshakeSocket(frame(1, b'{"ok":1}'))
        connection = self.connect(sock)
        self.assertEqual((1, b'{"ok":1}'), connection.read_frame())
        self.assertIn(b"Host: example.test:8080\r\n", sock.sent[0])

    def test_rejects_bad_handshake_and_closes_socket(self):
        alterations = [
            lambda data: data.replace(b"101 Switching", b"200 Switching"),
            lambda data: data.replace(b"Upgrade: websocket", b"Upgrade: h2"),
            lambda data: data.replace(b"keep-alive, Upgrade", b"keep-alive"),
            lambda data: data.replace(b"Sec-WebSocket-Accept: ", b"Sec-WebSocket-Accept: wrong"),
            lambda data: data.replace(b"\r\n\r\n", b"\r\nSec-WebSocket-Accept: duplicate\r\n\r\n"),
            lambda data: data.replace(b"\r\n\r\n", b"\r\nSec-WebSocket-Extensions: permessage-deflate\r\n\r\n"),
            lambda data: data.replace(b"\r\n\r\n", b"\r\nSec-WebSocket-Protocol: unexpected\r\n\r\n"),
            lambda data: data.replace(b"\r\n\r\n", b"\r\nSec-WebSocket-Protocol: a\r\nSec-WebSocket-Protocol: b\r\n\r\n"),
        ]
        for alter in alterations:
            with self.subTest(alter=alter):
                sock = HandshakeSocket(alter=alter)
                with self.assertRaises(ws.WebSocketError):
                    self.connect(sock)
                self.assertTrue(sock.closed)

    def test_header_limit_and_tls_failure_close(self):
        sock = HandshakeSocket()
        with self.assertRaises(ws.WebSocketError):
            self.connect(sock, max_header=20)
        self.assertTrue(sock.closed)
        sock = FakeSocket()
        with patch.object(ws.socket, "create_connection", return_value=sock), patch.object(
            ws.ssl, "create_default_context", side_effect=RuntimeError("TLS failed")
        ):
            with self.assertRaises(RuntimeError):
                ws.connect_websocket("wss://example.test/feed")
        self.assertTrue(sock.closed)

    def test_partial_header_extended_length_and_payload_survive_timeout(self):
        wire = frame(1, b"x" * 130)
        for split in (1, 3, 20):
            with self.subTest(split=split):
                sock = FakeSocket([wire[:split], socket.timeout(), wire[split:]])
                connection = ws.WebSocketConnection(sock)
                with self.assertRaises(socket.timeout):
                    connection.read_frame()
                self.assertEqual((1, b"x" * 130), connection.read_frame())

    def test_fragment_and_control_interleave_and_timeout(self):
        sock = FakeSocket([frame(1, b'\xe4', False) + frame(9, b"ping"),
                           socket.timeout(), frame(0, b'\xb8\xad', True)])
        connection = ws.WebSocketConnection(sock)
        self.assertEqual((9, b"ping"), connection.read_frame())
        with self.assertRaises(socket.timeout):
            connection.read_frame()
        self.assertEqual((1, "中".encode()), connection.read_frame())

    def test_rejects_invalid_framing_and_utf8(self):
        invalid = [b"\xc1\x00", b"\x81\x80", b"\x83\x00", frame(0, b"x"),
                   frame(9, b"x", False), b"\x89\x7e", b"\x81\x7e\x00\x01x",
                   b"\x81\x7f" + struct.pack("!Q", 1 << 63), frame(1, b"\xff"),
                   frame(8, b"x"), frame(8, struct.pack("!H", 1005)),
                   frame(1, b"a", False) + frame(1, b"b")]
        for wire in invalid:
            with self.subTest(wire=wire):
                with self.assertRaises((ws.WebSocketError, UnicodeDecodeError)):
                    ws.WebSocketConnection(FakeSocket([wire])).read_frame()

    def test_limits_checked_before_body_read(self):
        for kwargs, wire in [
            ({"max_frame": 5}, b"\x82\x06"),
            ({"max_message": 5}, frame(2, b"1234", False) + b"\x80\x02"),
        ]:
            with self.subTest(kwargs=kwargs), self.assertRaises(ws.WebSocketError):
                ws.WebSocketConnection(FakeSocket([wire]), **kwargs).read_frame()

    def test_trickling_frame_yields_at_total_deadline_and_resumes(self):
        clock = [0.0]
        sock = FakeSocket([b"\x81", b"\x02", b"o", b"k"])
        original = sock.recv
        def recv(size):
            clock[0] += 0.6
            return original(size)
        sock.recv = recv
        with patch.object(ws.time, "monotonic", side_effect=lambda: clock[0]):
            connection = ws.WebSocketConnection(sock, read_timeout=1)
            with self.assertRaises(socket.timeout):
                connection.read_frame()
            self.assertEqual((1, b"ok"), connection.read_frame())

    def test_client_frames_are_masked_and_control_bounded(self):
        sock = FakeSocket()
        connection = ws.WebSocketConnection(sock)
        with patch.object(ws.os, "urandom", return_value=b"1234"):
            connection.send_frame(9, b"hello")
        wire = sock.sent[0]
        self.assertEqual(b"\x89\x85", wire[:2])
        self.assertEqual(b"hello", bytes(value ^ wire[2 + i % 4] for i, value in enumerate(wire[6:])))
        with self.assertRaises(ws.WebSocketError):
            connection.send_frame(9, b"x" * 126)


class WebSocketLivenessTests(unittest.TestCase):
    def test_pings_do_not_extend_server_deadline(self):
        health = ws.WebSocketLiveness(0, 90)
        health.last_client_ping = 89
        with self.assertRaisesRegex(ws.WebSocketError, "server response"):
            health.check(90)

    def test_liquidation_silence_with_pongs_stays_healthy(self):
        health = ws.WebSocketLiveness(0, 90)
        for now in range(30, 1000, 30):
            health.last_server_response = now
            health.check(now)
        self.assertIsNone(health.last_valid_sample)

    def test_hyperliquid_pongs_do_not_replace_valid_context(self):
        health = ws.WebSocketLiveness(0, 90, sample_timeout=180)
        health.last_server_response = 180
        with self.assertRaisesRegex(ws.WebSocketError, "valid sample"):
            health.check(180)
        health.last_valid_sample = 179
        health.check(180)

    def test_poll_half_open_reconnects_without_claiming_server_response(self):
        clock = [0.0]
        connection = ws.WebSocketConnection(FakeSocket())
        connection.last_server_response_mono = 0
        health = ws.WebSocketLiveness(0, 90)
        with patch.object(bot.time, "monotonic", side_effect=lambda: clock[0]):
            for now in (30, 60):
                clock[0] = now
                self.assertEqual((None, True), bot.ws_poll(connection, health, 9, b"ping"))
            clock[0] = 90
            with self.assertRaisesRegex(ws.WebSocketError, "server response"):
                bot.ws_poll(connection, health, 9, b"ping")
        self.assertEqual(0, health.last_server_response)

    def test_status_and_rest_bootstrap_do_not_claim_ws_sample(self):
        ctx = {"openInterest": "2", "markPx": "100"}
        with tempfile.TemporaryDirectory() as folder, patch.object(bot, "STATE_FILE", str(Path(folder) / "state.json")), patch.object(
            bot, "BTC_PUSH_HYPERLIQUID_ENABLED", True
        ), patch.object(bot, "now_ms", return_value=180000):
            bot.persist_hyperliquid_ctx(ctx)
            state = bot.state_store().read_domain("hyperliquid_derivatives")
            self.assertNotIn("last_server_response_at_ms", state)
            self.assertNotIn("last_valid_sample_at_ms", state)
            bot.update_hyperliquid_status("client_ping")
            state = bot.state_store().read_domain("hyperliquid_derivatives")
            self.assertEqual(180000, state["last_client_ping_at_ms"])
            self.assertNotIn("last_server_response_at_ms", state)
            with self.assertRaises(ValueError):
                bot.persist_hyperliquid_ctx({"markPx": "NaN"}, from_websocket=True)
            bot.persist_hyperliquid_ctx(ctx, received_at_ms=240000, from_websocket=True)
            state = bot.state_store().read_domain("hyperliquid_derivatives")
            self.assertEqual(240000, state["last_valid_sample_at_ms"])

    def test_hyperliquid_bootstrap_cannot_keep_unresponsive_ws_alive(self):
        clock = [0.0]
        connection = ws.WebSocketConnection(FakeSocket())
        connection.last_server_response_mono = 0
        def read():
            clock[0] += 30
            raise socket.timeout()
        connection.read_frame = read
        statuses = []
        def status(value, reason=None):
            statuses.append((value, reason))
            if value == "error":
                bot.BTC_PUSH_HYPERLIQUID_ENABLED = False
        with patch.object(bot, "BTC_PUSH_HYPERLIQUID_ENABLED", True), patch.object(
            bot, "open_websocket", return_value=connection
        ), patch.object(bot, "fetch_hyperliquid_asset_ctx_rest", return_value={"valid": "REST"}), patch.object(
            bot, "persist_hyperliquid_ctx"
        ) as persist, patch.object(bot, "update_hyperliquid_status", side_effect=status), patch.object(
            bot.time, "monotonic", side_effect=lambda: clock[0]
        ), patch.object(bot, "log_line"):
            bot.run_hyperliquid_ws_collector()
        self.assertEqual(1, persist.call_count)
        self.assertTrue(connection.sock.closed)
        self.assertTrue(any(value == "error" and "server response overdue" in reason for value, reason in statuses))

    def test_same_minute_skipped_persistence_still_counts_valid_ws_ctx(self):
        clock = [0.0]
        connection = ws.WebSocketConnection(FakeSocket())
        connection.last_server_response_mono = 0
        message = {"channel": "activeAssetCtx", "data": {
            "coin": bot.HYPERLIQUID_COIN, "ctx": {"openInterest": "2", "markPx": "100"},
        }}
        def read():
            clock[0] += 30
            connection.last_server_response_mono = clock[0]
            if clock[0] == 600:
                bot.BTC_PUSH_HYPERLIQUID_ENABLED = False
            return 1, json.dumps(message).encode()
        connection.read_frame = read
        with patch.object(bot, "BTC_PUSH_HYPERLIQUID_ENABLED", True), patch.object(
            bot, "open_websocket", return_value=connection
        ), patch.object(bot, "update_hyperliquid_status") as status, patch.object(
            bot, "persist_hyperliquid_ctx", return_value=False
        ) as persist, patch.object(bot, "fetch_hyperliquid_asset_ctx_rest", return_value={}), patch.object(
            bot, "log_line"
        ), patch.object(bot.time, "monotonic", side_effect=lambda: clock[0]):
            bot.run_hyperliquid_ws_collector()
        self.assertEqual(21, persist.call_count)
        self.assertTrue(all(call.kwargs.get("from_websocket") for call in persist.call_args_list[1:]))
        self.assertNotIn("error", [call.args[0] for call in status.call_args_list])

    def test_nonfinite_liquidation_is_not_a_valid_sample(self):
        for qty in ("NaN", "inf", "0"):
            message = {"e": "forceOrder", "o": {"s": bot.BINANCE_SYMBOL, "S": "SELL", "z": qty, "ap": "100"}}
            self.assertIsNone(bot.liquidation_event_from_ws_message(message))

    def test_flapping_handshakes_keep_exponential_backoff(self):
        connection = ws.WebSocketConnection(FakeSocket([frame(8)]))
        waits = []
        def sleep(delay):
            waits.append(delay)
            if len(waits) == 3:
                bot.BTC_PUSH_LIQUIDATION_WS_ENABLED = False
        with patch.object(bot, "BTC_PUSH_LIQUIDATION_WS_ENABLED", True), patch.object(
            bot, "open_websocket", return_value=connection
        ), patch.object(bot, "ws_poll", return_value=((8, b""), False)), patch.object(
            bot, "update_liquidation_ws_status"
        ), patch.object(bot, "log_line"), patch.object(bot.time, "monotonic", return_value=0), patch.object(
            bot.time, "sleep", side_effect=sleep
        ), patch.object(bot.random, "uniform", return_value=1):
            bot.run_liquidation_ws_collector()
        self.assertEqual([1, 2, 4], waits)

    def test_liquidation_collector_accepts_pongs_without_events(self):
        clock = [0.0]
        connection = ws.WebSocketConnection(FakeSocket())
        connection.last_server_response_mono = 0
        def read():
            clock[0] += 30
            connection.last_server_response_mono = clock[0]
            if clock[0] == 600:
                bot.BTC_PUSH_LIQUIDATION_WS_ENABLED = False
            return 10, b"pong"
        connection.read_frame = read
        with patch.object(bot, "BTC_PUSH_LIQUIDATION_WS_ENABLED", True), patch.object(
            bot, "open_websocket", return_value=connection
        ), patch.object(bot, "update_liquidation_ws_status") as status, patch.object(
            bot, "append_liquidation_ws_event"
        ) as append, patch.object(bot, "log_line"), patch.object(
            bot.time, "monotonic", side_effect=lambda: clock[0]
        ):
            bot.run_liquidation_ws_collector()
        self.assertFalse(append.called)
        self.assertNotIn("error", [call.args[0] for call in status.call_args_list])

    def test_collectors_retry_when_recovery_status_write_fails(self):
        for kind in ("liquidation", "hyperliquid"):
            for failed_status in ("connecting", "error", "disconnected"):
                with self.subTest(collector=kind, failed_status=failed_status), ExitStack() as stack:
                    enabled = "BTC_PUSH_LIQUIDATION_WS_ENABLED" if kind == "liquidation" else "BTC_PUSH_HYPERLIQUID_ENABLED"
                    writer = "update_liquidation_ws_status" if kind == "liquidation" else "update_hyperliquid_status"
                    waits, connections = [], []
                    def opened(*args, **kwargs):
                        connection = ws.WebSocketConnection(FakeSocket())
                        connections.append(connection)
                        return connection
                    def status(value, reason=None):
                        if value == failed_status:
                            raise OSError("temporary state write failure")
                    def sleep(delay):
                        waits.append(delay)
                        if len(waits) == 2:
                            setattr(bot, enabled, False)
                    stack.enter_context(patch.object(bot, enabled, True))
                    stack.enter_context(patch.object(bot, writer, side_effect=status))
                    stack.enter_context(patch.object(bot, "open_websocket", side_effect=opened))
                    stack.enter_context(patch.object(bot, "ws_poll", return_value=((8, b""), False)))
                    stack.enter_context(patch.object(bot, "fetch_hyperliquid_asset_ctx_rest", return_value={}))
                    stack.enter_context(patch.object(bot, "persist_hyperliquid_ctx"))
                    stack.enter_context(patch.object(bot, "log_line"))
                    stack.enter_context(patch.object(bot.time, "monotonic", return_value=0))
                    stack.enter_context(patch.object(bot.time, "sleep", side_effect=sleep))
                    stack.enter_context(patch.object(bot.random, "uniform", return_value=1))
                    getattr(bot, f"run_{kind}_ws_collector")()
                    self.assertEqual([1, 2], waits)
                    self.assertTrue(all(connection.sock.closed for connection in connections))
                    self.assertEqual(0 if failed_status == "connecting" else 2, len(connections))

    def test_storage_recovery_reaches_next_connection_and_valid_sample(self):
        for kind in ("liquidation", "hyperliquid"):
            with self.subTest(collector=kind), ExitStack() as stack:
                enabled = "BTC_PUSH_LIQUIDATION_WS_ENABLED" if kind == "liquidation" else "BTC_PUSH_HYPERLIQUID_ENABLED"
                writer = "update_liquidation_ws_status" if kind == "liquidation" else "update_hyperliquid_status"
                attempt, connections, samples, waits = [0], [], [], []
                def opened(*args, **kwargs):
                    attempt[0] += 1
                    connection = ws.WebSocketConnection(FakeSocket())
                    connections.append(connection)
                    return connection
                def status(value, reason=None):
                    if attempt[0] == 1 and value != "connecting":
                        raise OSError("first connection cannot persist")
                def sample(*args, **kwargs):
                    if kwargs.get("from_websocket") or kind == "liquidation":
                        samples.append(True)
                        setattr(bot, enabled, False)
                    return True
                def pause(delay):
                    waits.append(delay)
                    if len(waits) > 3:
                        raise AssertionError("collector did not recover within three retries")
                stack.enter_context(patch.object(bot, enabled, True))
                stack.enter_context(patch.object(bot, writer, side_effect=status))
                stack.enter_context(patch.object(bot, "open_websocket", side_effect=opened))
                stack.enter_context(patch.object(bot, "ws_poll", return_value=((1, b"{}"), False)))
                stack.enter_context(patch.object(bot, "liquidation_event_from_ws_message", return_value={"id": "valid"}))
                stack.enter_context(patch.object(bot, "hyperliquid_asset_ctx_from_ws_message", return_value={"valid": True}))
                stack.enter_context(patch.object(bot, "append_liquidation_ws_event", side_effect=sample))
                stack.enter_context(patch.object(bot, "fetch_hyperliquid_asset_ctx_rest", return_value={}))
                stack.enter_context(patch.object(bot, "persist_hyperliquid_ctx", side_effect=sample))
                stack.enter_context(patch.object(bot, "log_line"))
                stack.enter_context(patch.object(bot.time, "monotonic", return_value=0))
                sleep = stack.enter_context(patch.object(bot.time, "sleep", side_effect=pause))
                stack.enter_context(patch.object(bot.random, "uniform", return_value=1))
                getattr(bot, f"run_{kind}_ws_collector")()
                self.assertEqual([True], samples)
                self.assertEqual(2, len(connections))
                self.assertTrue(all(connection.sock.closed for connection in connections))
                sleep.assert_called_once_with(1)


if __name__ == "__main__":
    unittest.main()
