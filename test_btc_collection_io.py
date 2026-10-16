import threading
import time
import unittest
from unittest.mock import Mock, patch

import requests
from urllib3 import poolmanager
from urllib3.exceptions import ReadTimeoutError
import btc_collection_io as io
from btc_presentation import render_report, utf16_length


class CollectionIOTest(unittest.TestCase):
    def tearDown(self):
        io.clear_cache()

    def test_connection_reuse_and_sanitized_source_timing(self):
        old = getattr(io._local, "session", None)
        fake = Mock()
        fake.request.return_value = Mock(status_code=200)
        try:
            io._local.session = fake
            with io.trace_requests() as trace:
                io.get("https://example.test/data?secret=never-log", timeout=2)
                io.get("https://example.test/data", timeout=2)
            self.assertEqual(2, fake.request.call_count)
            self.assertEqual("https://example.test/data", trace[0]["endpoint_url"])
            self.assertGreaterEqual(trace[0]["duration_ms"], 0)
            self.assertEqual(200, trace[0]["http_status"])
        finally:
            io._local.session = old

    def test_four_worker_bound_and_failure_isolation(self):
        barrier = threading.Barrier(4)
        lock, active, peak = threading.Lock(), [0], [0]
        def fetch():
            with lock:
                active[0] += 1
                peak[0] = max(peak[0], active[0])
            barrier.wait(timeout=2)
            with lock:
                active[0] -= 1
            return {"status": "ok"}
        fetchers = {str(i): fetch for i in range(8)}
        fetchers["bad"] = lambda: 1 / 0
        out = io.collect_sources(fetchers, lambda key, why: {"status": "unavailable", "reason": why})
        self.assertEqual(4, peak[0])
        self.assertEqual(9, len(out))
        self.assertEqual("ZeroDivisionError", out["bad"]["collector_metrics"]["failure"])

    def test_transport_observation_does_not_confuse_session_with_socket_reuse(self):
        pool = io._ObservedHTTPSPool("example.test")
        connection = io._ObservedHTTPSConnection("example.test")
        previous = getattr(io._local, "active_transport", None)
        try:
            io._local.active_transport = []
            with patch.object(io.HTTPSConnectionPool, "_get_conn", return_value=connection):
                pool._get_conn()
            self.assertEqual("new", connection._collection_event["connection_mode"])
            sock = Mock()
            sock.getpeername.return_value = ("192.0.2.10", 443)
            sock.getsockname.return_value = ("192.0.2.20", 1234)
            sock.getsockopt.side_effect = OSError("optional TCP state unavailable")
            connection.sock = sock
            connection._last_headers_at = time.monotonic() - 3600
            with patch.object(io.HTTPSConnectionPool, "_get_conn", return_value=connection):
                pool._get_conn()
            self.assertEqual("reused", connection._collection_event["connection_mode"])
            self.assertGreater(connection._collection_event["idle_since_headers_sec"], 3599)
            with patch.object(io.HTTPSConnection, "request", return_value=None):
                connection.request("GET", "/")
            with patch.object(io.HTTPSConnection, "getresponse", side_effect=requests.ReadTimeout()):
                with self.assertRaises(requests.ReadTimeout): connection.getresponse()
            self.assertEqual("response_headers", connection._collection_event["phase"])
            sock.getpeername.side_effect = OSError("socket closed after checkout")
            with patch.object(io.HTTPSConnectionPool, "_get_conn", return_value=connection):
                self.assertIs(connection, pool._get_conn())
        finally:
            connection.sock = None
            pool.close()
            io._local.active_transport = previous

    def test_adapter_observation_is_local_and_tls_failures_keep_the_actual_phase(self):
        original = poolmanager.pool_classes_by_scheme["https"]
        client = io._new_session()
        try:
            self.assertIs(original, poolmanager.pool_classes_by_scheme["https"])
            self.assertIs(io._ObservedHTTPSPool, client.get_adapter("https://example.test").poolmanager.pool_classes_by_scheme["https"])
            conn = io._ObservedHTTPSConnection("example.test")
            conn._collection_event = {"phase": "connection_checkout"}
            with patch.object(io.HTTPSConnection, "_new_conn", side_effect=OSError("connect failed")):
                with self.assertRaises(OSError): conn._new_conn()
            self.assertEqual("dns_tcp_connect", conn._collection_event["phase"])
            conn._collection_event["phase"] = "tls_handshake"
            with patch.object(io.HTTPSConnection, "connect", side_effect=requests.exceptions.SSLError()):
                with self.assertRaises(requests.exceptions.SSLError): conn.connect()
            self.assertEqual("tls_handshake", conn._collection_event["phase"])
        finally:
            client.close()

    def test_read_timeout_recovers_once_with_fresh_connection_and_retains_failure(self):
        for body_timeout in (False, True):
            with self.subTest(body_timeout=body_timeout):
                pooled, fresh = Mock(), Mock()
                response = requests.Response()
                response.status_code = 200
                response.headers["CF-Ray"] = "public-edge-id"
                def failed(method, url, hooks, **kwargs):
                    if body_timeout:
                        hooks["response"][0](response)
                        raise requests.ConnectionError(ReadTimeoutError(None, url, "body timeout"))
                    raise requests.ReadTimeout()
                def recovered(method, url, hooks, **kwargs):
                    hooks["response"][0](response)
                    return response
                pooled.request.side_effect = failed
                fresh.request.side_effect = recovered
                with patch.object(io, "session", return_value=pooled), \
                     patch.object(io, "_new_session", return_value=fresh), io.trace_requests() as trace:
                    self.assertIs(response, io.get("https://example.test/data", timeout=(3, 8),
                                                   retry_read_timeout=True))
                self.assertEqual(1, pooled.request.call_count)
                self.assertEqual(1, fresh.request.call_count)
                fresh.close.assert_called_once_with()
                pooled.close.assert_not_called()
                self.assertEqual(2, len(trace))
                self.assertEqual("response_body" if body_timeout else "response_headers", trace[0]["failure_phase"])
                self.assertEqual(200 if body_timeout else None, trace[0]["http_status"])
                self.assertEqual("fresh", trace[1]["session_mode"])
                self.assertEqual("unobserved", trace[1]["connection_mode"])
                self.assertEqual(2, trace[1]["attempt"])
                self.assertIsNone(trace[1]["failure"])
                self.assertIsNotNone(trace[1]["response_headers_ms"])
                self.assertEqual("public-edge-id", trace[1]["response_headers"]["CF-Ray"])

    def test_timeout_exhaustion_is_bounded_and_closes_recovery_session(self):
        pooled, fresh = Mock(), Mock()
        pooled.request.side_effect = requests.ReadTimeout()
        fresh.request.side_effect = requests.ReadTimeout()
        with patch.object(io, "session", return_value=pooled), \
             patch.object(io, "_new_session", return_value=fresh), io.trace_requests() as trace:
            with self.assertRaises(requests.ReadTimeout):
                io.get("https://example.test/data", retry_read_timeout=True, timeout=(3, 8))
        self.assertEqual(2, len(trace))
        self.assertTrue(all(row["failure"] == "ReadTimeout" for row in trace))
        self.assertEqual(1, fresh.request.call_count)
        fresh.close.assert_called_once_with()

    def test_other_failures_and_default_requests_are_not_retried(self):
        failures = (requests.ConnectTimeout(), requests.ConnectionError(), requests.exceptions.SSLError())
        for failure in failures:
            fake = Mock(); fake.request.side_effect = failure
            with patch.object(io, "session", return_value=fake), patch.object(io, "_new_session") as fresh:
                with self.assertRaises(type(failure)):
                    io.get("https://example.test/data", retry_read_timeout=True)
                fresh.assert_not_called()
        fake = Mock(); fake.request.side_effect = requests.ReadTimeout()
        with patch.object(io, "session", return_value=fake), patch.object(io, "_new_session") as fresh:
            with self.assertRaises(requests.ReadTimeout):
                io.get("https://example.test/data")
            fresh.assert_not_called()
            with self.assertRaisesRegex(ValueError, "requires_get"):
                io.post("https://example.test/data", retry_read_timeout=True)
            self.assertEqual(1, fake.request.call_count)
        response = requests.Response(); response.status_code = 403
        fake.request.side_effect = None; fake.request.return_value = response
        with patch.object(io, "session", return_value=fake), patch.object(io, "_new_session") as fresh:
            self.assertIs(response, io.get("https://example.test/data", retry_read_timeout=True))
            fresh.assert_not_called()

    def test_cache_expiry_does_not_mutate_original_or_fake_timestamp(self):
        io.cache_put("market_cap", {"cap": 7}, now=100)
        hit = io.cache_get("market_cap", 60, now=120)
        hit["value"]["cap"] = 99
        self.assertEqual(7, io.cache_get("market_cap", 60, now=120)["value"]["cap"])
        self.assertEqual(100, hit["retrieved_at"])
        self.assertIsNone(io.cache_get("market_cap", 60, now=160))
        self.assertIsNone(io.cache_get("market_cap", 60, now=99))

    def test_oversized_news_keeps_quality_and_essential_sections(self):
        lines = ["📊 BTC 辅助观察", "", "🎯 结论", "数据不足", "",
                 "📰 新闻", "🚀" * 3000, "", "🤖 数据状态", "OI 未对齐；不得用于当前判断"]
        report = render_report(lines, 400)
        self.assertLessEqual(utf16_length(report), 400)
        self.assertIn("数据不足", report)
        self.assertIn("OI 未对齐；不得用于当前判断", report)
        self.assertIn("新闻已按消息长度缩减", report)


if __name__ == "__main__":
    unittest.main()
