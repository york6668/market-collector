import threading
import time
import unittest
from unittest.mock import Mock, patch

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
