import copy
import os
import tempfile
import unittest
from unittest.mock import patch

import btc_push as bot
import btc_structure_engine as structure


def leverage_payload(oi_4h, oi_24h, oi_72h, funding_8h=0.002426, funding_24h=0.016883, basis=-0.05):
    return {
        "delta4h": {"oi_4h_pct": oi_4h},
        "structure_state": {
            "checks": {
                "oi_slow_confirmation": {
                    "delta_24h_pct": oi_24h,
                    "delta_72h_pct": oi_72h,
                }
            }
        },
        "public_records": {
            "funding": {"value": {"funding_8h_pct": funding_8h, "funding_24h_pct": funding_24h}},
            "basis": {"value": {"basis_pct": basis}},
        },
    }


def trend_payload(
    bucket,
    close,
    support,
    resistance=65589.7,
    trend_support=61806.0,
    trend_resistance=65589.7,
    rolling_support=None,
    patterns=None,
    support_test=None,
):
    payload = {
        "market": {"price": close},
        "structure_state": {"latest_close": close},
        "levels": {
            "short_support": support,
            "rolling_short_support": support if rolling_support is None else rolling_support,
            "short_resistance": resistance,
            "trend_support": trend_support,
            "trend_resistance": trend_resistance,
        },
        "public_snapshot": {"ts": bucket * 3600},
    }
    if support_test is not None:
        payload["levels"]["support_test"] = support_test
    if patterns is not None:
        payload["market_patterns"] = patterns
    return payload


DOWN_LEVERAGE = {"oi_24h_pct": -2.88, "funding_8h_pct": 0.002426}
DOWN_SPOT = {"cvd_4h_pct": -1.0}
UP_SPOT = {"cvd_4h_pct": 1.0}


def market_pattern_definition(key):
    definitions = bot.load_status_rules_config()["market_patterns"]["patterns"]
    return next(item for item in definitions if item["key"] == key)


def market_pattern_context(price, oi, futures_volume=50.0, spot_cvd=2.0, spot_volume=50.0, in_range=True):
    return {
        "meta": {"bucket": 100, "as_of_ms": 100 * 3600 * 1000},
        "price": {"change_4h_pct": price},
        "derivatives": {
            "oi_change_4h_pct": oi,
            "volume_change_4h_pct": futures_volume,
        },
        "spot": {
            "cvd_4h_pct": spot_cvd,
            "volume_change_4h_pct": spot_volume,
        },
        "structure": {"in_range": in_range},
    }


def synthetic_bars(count=40):
    hour_ms = 3600 * 1000
    return [
        {
            "open_time": index * hour_ms,
            "close_time": (index + 1) * hour_ms - 1,
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.0,
            "quote_volume": 100.0,
            "oi": 1000.0,
            "oi_value": 100000.0,
            "oi_timestamp": (index + 1) * hour_ms,
            "oi_drift_ms": 1,
            "oi_aligned": True,
        }
        for index in range(count)
    ]


class TelegramCommandPollingTest(unittest.TestCase):
    class Response:
        status_code = 200

        def __init__(self, payload):
            self.payload = payload

        def json(self):
            return self.payload

    def check_command(self, update_field, text):
        state = {}
        response = self.Response({
            "ok": True,
            "result": [{
                "update_id": 123,
                update_field: {
                    "chat": {"id": -1003964995888},
                    "text": text,
                },
            }],
        })
        with (
            patch.object(bot, "BTC_PUSH_CHECK_TG_COMMANDS", True),
            patch.object(bot, "BOT_TOKEN", "test-token"),
            patch.object(bot, "CHAT_ID_RAW", "-1003964995888"),
            patch.object(bot.requests, "get", return_value=response),
        ):
            command = bot.check_tg_command_and_update_offset(state)
        self.assertEqual(124, state["tg_update_offset"])
        return command

    def test_group_message_command_remains_supported(self):
        self.assertEqual("FORCE_PUSH", self.check_command("message", "/force"))

    def test_channel_post_commands_are_supported(self):
        commands = {
            "/btc": "QUERY_PUSH",
            "/force": "FORCE_PUSH",
            "/repair": "REPAIR",
        }
        for text, expected in commands.items():
            with self.subTest(text=text):
                self.assertEqual(expected, self.check_command("channel_post", text))


class OiClassificationTest(unittest.TestCase):
    def test_current_sample_is_short_contraction_with_high_72h_context(self):
        payload = leverage_payload(-0.40, -2.88, 5.32)
        leverage = bot.build_leverage_rule_state(payload)

        self.assertEqual("contraction", leverage["oi_short_term_status"])
        self.assertEqual("accumulating", leverage["oi_72h_context"])
        self.assertTrue(leverage["oi_contraction_rule"])
        self.assertFalse(leverage["oi_expansion_rule"])
        self.assertEqual("deleveraging", leverage["status"])
        self.assertEqual(
            "⚠ 杠杆短线降温，72H累计仓位仍偏高",
            bot.plain_leverage_signal_line(leverage),
        )

        payload["rule_state"] = {"leverage": leverage}
        status_layer = bot.build_status_layer(payload)
        self.assertEqual("短线降温，72H累计仍高", status_layer["leverage_environment"]["status"])

    def test_expansion_uses_only_4h_and_24h(self):
        result = bot.classify_oi_position(1.2, 2.0, -3.0)
        self.assertEqual("expansion", result["oi_short_term_status"])
        self.assertTrue(result["oi_expansion_rule"])
        self.assertFalse(result["oi_contraction_rule"])
        self.assertEqual("declining", result["oi_72h_context"])

    def test_mixed_short_windows_are_not_expansion_or_contraction(self):
        result = bot.classify_oi_position(1.2, -2.0, 3.0)
        self.assertEqual("mixed", result["oi_short_term_status"])
        self.assertTrue(result["oi_mixed_rule"])
        self.assertFalse(result["oi_expansion_rule"])
        self.assertFalse(result["oi_contraction_rule"])

    def test_stable_short_windows_keep_72h_as_context_only(self):
        result = bot.classify_oi_position(-0.4, 0.4, 4.1)
        self.assertEqual("stable", result["oi_short_term_status"])
        self.assertEqual("accumulating", result["oi_72h_context"])
        self.assertFalse(result["oi_expansion_rule"])
        self.assertFalse(result["oi_contraction_rule"])

    def test_missing_short_windows_are_insufficient(self):
        result = bot.classify_oi_position(None, None, 5.0)
        self.assertEqual("data_insufficient", result["oi_short_term_status"])
        self.assertIsNone(result["oi_expansion_rule"])
        self.assertIsNone(result["oi_contraction_rule"])


class SupportAnchorStateTest(unittest.TestCase):
    def update(
        self,
        state,
        bucket,
        close,
        support,
        spot=DOWN_SPOT,
        resistance=65589.7,
        rolling_support=None,
        patterns=None,
        leverage=DOWN_LEVERAGE,
    ):
        return bot.update_price_trend_state(
            trend_payload(
                bucket,
                close,
                support,
                resistance=resistance,
                rolling_support=rolling_support,
                patterns=patterns,
            ),
            state,
            leverage,
            spot,
            {},
        )

    def test_0900_to_1100_replay_keeps_anchor_and_confirms_breakdown(self):
        state = {}
        first = self.update(state, 100, 64721.5, 64752.0)
        self.assertEqual("watching", first["support_anchor_phase"])
        self.assertEqual(64752.0, first["support_anchor"])
        self.assertEqual(1, first["below_support_hours"])
        self.assertEqual("breakdown_watch", first["support_state"])

        second = self.update(state, 101, 64552.7, 64464.0)
        self.assertEqual("confirmed", second["support_anchor_phase"])
        self.assertEqual(64752.0, second["support_anchor"])
        self.assertEqual(64464.0, second["rolling_short_support"])
        self.assertEqual(2, second["below_support_hours"])
        self.assertEqual("breakdown_confirmed", second["support_state"])
        self.assertEqual(101, second["support_breakdown_confirmed_bucket"])
        self.assertEqual("confirmation_event", bot.trend_support_breakdown_phase(second))
        self.assertTrue(second["support_candidate_changed"])

        third = self.update(state, 102, 64698.7, 64362.2)
        self.assertEqual("confirmed", third["support_anchor_phase"])
        self.assertEqual(64752.0, third["support_anchor"])
        self.assertEqual(64362.2, third["rolling_short_support"])
        self.assertEqual(3, third["below_support_hours"])
        self.assertEqual("breakdown_confirmed", third["support_state"])
        self.assertEqual(101, third["support_breakdown_confirmed_bucket"])
        self.assertEqual("active_below_former_support", bot.trend_support_breakdown_phase(third))

    def test_delayed_confirmation_records_actual_confirmation_bucket_and_survives_retry(self):
        state = {}
        neutral_leverage = {"oi_24h_pct": 1.0, "funding_8h_pct": 0.004}
        first = self.update(
            state,
            100,
            64721.5,
            64752.0,
            leverage=neutral_leverage,
        )
        second = self.update(
            state,
            101,
            64552.7,
            64464.0,
            leverage=neutral_leverage,
        )
        confirmed = self.update(
            state,
            102,
            64300.0,
            64362.2,
            leverage=neutral_leverage,
            patterns={"spot_active_sell": {"state": "YES"}},
        )
        repeated = self.update(
            state,
            102,
            64300.0,
            64250.0,
            leverage=neutral_leverage,
            patterns={"spot_active_sell": {"state": "YES"}},
        )

        self.assertEqual("breakdown_watch", first["status"])
        self.assertEqual("breakdown_watch", second["status"])
        self.assertIsNone(second["support_breakdown_confirmed_bucket"])
        self.assertEqual("breakdown_confirmed", confirmed["status"])
        self.assertEqual(102, confirmed["support_breakdown_confirmed_bucket"])
        self.assertEqual("confirmation_event", bot.trend_support_breakdown_phase(confirmed))
        self.assertEqual(102, repeated["support_breakdown_confirmed_bucket"])
        self.assertEqual("confirmation_event", bot.trend_support_breakdown_phase(repeated))

    def test_same_hour_query_does_not_increment(self):
        state = {}
        self.update(state, 100, 64721.5, 64752.0)
        repeated = self.update(state, 100, 64721.5, 64464.0)
        self.assertEqual(1, repeated["below_support_hours"])
        self.assertEqual(64752.0, repeated["support_anchor"])

    def test_pressure_change_does_not_reset_support_anchor(self):
        state = {}
        self.update(state, 100, 64721.5, 64752.0)
        changed = self.update(state, 101, 64552.7, 64464.0, resistance=65000.0)
        self.assertTrue(changed["pressure_level_reset"])
        self.assertEqual(64752.0, changed["support_anchor"])
        self.assertEqual(2, changed["below_support_hours"])
        self.assertEqual("breakdown_confirmed", changed["support_state"])

    def test_recovery_requires_close_above_anchor_and_positive_cvd(self):
        state = {}
        self.update(state, 100, 64721.5, 64752.0)
        self.update(state, 101, 64552.7, 64464.0)

        no_recovery = self.update(state, 102, 64800.0, 64362.2, spot=DOWN_SPOT)
        self.assertEqual(64752.0, no_recovery["support_anchor"])
        self.assertEqual("confirmed", no_recovery["support_anchor_phase"])
        self.assertEqual("support_reclaim_watch", no_recovery["status"])
        self.assertEqual("support_reclaim_watch", no_recovery["support_state"])
        self.assertTrue(no_recovery["support_price_reclaimed_rule"])
        self.assertFalse(no_recovery["support_recovery_rule"])
        self.assertEqual(101, no_recovery["support_breakdown_confirmed_bucket"])

        back_below = self.update(state, 103, 64600.0, 64362.2, spot=DOWN_SPOT)
        self.assertEqual("breakdown_confirmed", back_below["status"])
        self.assertEqual(101, back_below["support_breakdown_confirmed_bucket"])
        self.assertEqual("active_below_former_support", bot.trend_support_breakdown_phase(back_below))

        recovered = self.update(state, 104, 64800.0, 64362.2, spot=UP_SPOT)
        self.assertIsNone(recovered["support_anchor"])
        self.assertEqual("inactive", recovered["support_anchor_phase"])
        self.assertTrue(recovered["support_recovery_rule"])
        self.assertTrue(recovered["support_reclaimed_rule"])
        self.assertEqual("support_holding", recovered["support_state"])
        self.assertIsNone(recovered["support_breakdown_confirmed_bucket"])

        new_watch = self.update(state, 105, 64000.0, 64362.2)
        new_confirmed = self.update(state, 106, 63900.0, 64200.0)
        self.assertEqual("breakdown_watch", new_watch["status"])
        self.assertIsNone(new_watch["support_breakdown_confirmed_bucket"])
        self.assertEqual("breakdown_confirmed", new_confirmed["status"])
        self.assertEqual(106, new_confirmed["support_breakdown_confirmed_bucket"])
        self.assertEqual("confirmation_event", bot.trend_support_breakdown_phase(new_confirmed))

    def test_price_reclaim_without_positive_cvd_has_distinct_state_and_consistent_outputs(self):
        state = {}
        self.update(
            state,
            100,
            63200.0,
            63263.5,
            rolling_support=63263.5,
            spot={"cvd_4h_pct": -7.24},
        )
        trend = self.update(
            state,
            101,
            63899.4,
            63263.5,
            rolling_support=63263.5,
            spot={"cvd_4h_pct": -0.081},
        )
        level_state = bot.apply_price_trend_to_level_state({}, trend)
        payload = trend_payload(
            101,
            63899.4,
            63263.5,
            rolling_support=63263.5,
        )
        payload["rule_state"] = {"levels": level_state}

        self.assertEqual("support_reclaim_watch", trend["status"])
        self.assertEqual("support_reclaim_watch", trend["support_state"])
        self.assertEqual("已收回，待资金确认", trend["support_state_text"])
        self.assertEqual(63263.5, trend["support_anchor"])
        self.assertTrue(trend["support_price_reclaimed_rule"])
        self.assertFalse(trend["support_recovery_rule"])
        self.assertTrue(level_state["support_reclaim_watch_rule"])

        current_view = bot.format_compact_current_view(payload)
        self.assertIn("价格已收回原短线支撑 63,264", current_view)
        self.assertIn("4H Spot CVD 尚未转正", current_view)
        self.assertNotIn("已失守", current_view)
        self.assertNotIn("下一潜在支撑", current_view)
        self.assertIn("价格已收回原短线支撑", bot.plain_breakout_line(level_state))
        self.assertIn("价格已收回锚点", bot.plain_action_line(payload))
        self.assertIn(
            "价格已回到原短线支撑上方",
            "\n".join(bot.format_final_view(payload)),
        )
        self.assertIn(
            "当前价格已收回原短线支撑",
            "\n".join(bot.format_market_patterns_block(payload)),
        )

        for text in (
            "\n".join(bot.format_compact_level_block(payload)),
            "\n".join(bot.format_level_block(payload)),
            "\n".join(bot.format_market_structure_raw_block(payload)),
        ):
            self.assertIn("已收回待确认锚点", text)
            self.assertIn("当前滚动支撑", text)
            self.assertNotIn("跌破观察锚点", text)
            self.assertNotIn("下一潜在支撑", text)

        structure = bot.build_market_pattern_current_structure(payload)
        self.assertEqual("support_reclaim_watch", structure["conclusion_key"])
        self.assertIn("价格已收回原短线支撑", structure["text"])

    def test_data_gap_preserves_active_anchor(self):
        state = {}
        self.update(state, 100, 64721.5, 64752.0)
        missing = bot.update_price_trend_state(
            trend_payload(101, None, 64464.0),
            state,
            DOWN_LEVERAGE,
            DOWN_SPOT,
            {},
        )
        self.assertFalse(missing["data_sufficient"])
        self.assertEqual(64752.0, missing["support_anchor"])
        self.assertEqual(1, missing["below_support_hours"])

    def test_near_support_is_not_reported_as_support_holding(self):
        state = {}
        result = self.update(state, 100, 64500.0, 64464.0)
        self.assertEqual("near_support", result["support_state"])
        self.assertEqual("接近支撑", result["support_state_text"])

    def test_compact_and_full_levels_show_current_and_former_support_roles(self):
        state = {}
        self.update(state, 100, 64721.5, 64752.0)
        trend = self.update(state, 101, 64552.7, 64464.0)
        payload = trend_payload(101, 64552.7, 64464.0)
        payload["rule_state"] = {
            "levels": {
                "price_trend": trend,
                "position_text": trend["position_text"],
                "pressure_state_text": trend["pressure_state_text"],
                "support_state_text": trend["support_state_text"],
            }
        }

        compact = "\n".join(bot.format_compact_level_block(payload))
        full = "\n".join(bot.format_level_block(payload))
        raw = "\n".join(bot.format_market_structure_raw_block(payload))
        for text in (compact, full, raw):
            self.assertIn("当前支撑", text)
            self.assertIn("原支撑（收回观察位）", text)
            self.assertIn("64,752", text)
            self.assertIn("64,464", text)
            self.assertNotIn("跌破观察锚点", text)

    def test_fixed_0701_to_1103_replay_freezes_old_support_and_keeps_confirmation(self):
        state = {}
        neutral_leverage = {"oi_24h_pct": 1.20, "funding_8h_pct": 0.004}

        first = self.update(
            state,
            200,
            63651.0,
            64811.0,
            rolling_support=63755.0,
            leverage=neutral_leverage,
        )
        self.assertEqual(5, first["schema_version"])
        self.assertEqual("breakdown_watch", first["status"])
        self.assertEqual(64811.0, first["support_anchor"])
        self.assertEqual(63755.0, first["rolling_short_support"])
        self.assertEqual("previous_closed_support", first["support_anchor_source"])

        second = self.update(
            state,
            201,
            63696.0,
            63755.0,
            rolling_support=63567.0,
            leverage=neutral_leverage,
            patterns={"spot_active_sell": {"state": "YES"}},
        )
        self.assertEqual("breakdown_confirmed", second["status"])
        self.assertEqual(64811.0, second["support_anchor"])
        self.assertEqual(2, second["below_support_hours"])
        self.assertIn("spot_active_sell", second["support_confirmation_sources"])
        self.assertEqual(201, second["support_breakdown_confirmed_bucket"])
        self.assertFalse(
            second["funding_confirmation"]["down_funds_confirmed_rule"]
        )
        self.assertTrue(
            second["funding_confirmation"]["down_pattern_confirmed_rule"]
        )

        missing = bot.update_price_trend_state(
            trend_payload(
                202,
                None,
                63567.0,
                rolling_support=63567.0,
            ),
            state,
            neutral_leverage,
            DOWN_SPOT,
            {},
        )
        self.assertFalse(missing["data_sufficient"])
        self.assertEqual("breakdown_confirmed", missing["status"])
        self.assertEqual(64811.0, missing["support_anchor"])
        self.assertIn("spot_active_sell", missing["support_confirmation_sources"])
        self.assertEqual(201, missing["support_breakdown_confirmed_bucket"])

        final = self.update(
            state,
            203,
            63178.0,
            63439.0,
            rolling_support=63021.0,
            leverage=neutral_leverage,
            patterns={
                "spot_active_sell": {"state": "YES"},
                "new_short_pressure": {"state": "YES"},
            },
        )
        payload = trend_payload(
            203,
            63178.0,
            63439.0,
            rolling_support=63021.0,
        )
        payload["rule_state"] = {
            "levels": {
                "price_trend": final,
                "position_text": final["position_text"],
                "pressure_state_text": final["pressure_state_text"],
                "support_state_text": final["support_state_text"],
            }
        }
        current_view = bot.format_compact_current_view(payload)
        structure = bot.build_market_pattern_current_structure(payload)
        self.assertEqual("breakdown_confirmed", final["status"])
        self.assertEqual(64811.0, final["support_anchor"])
        self.assertEqual(63021.0, final["rolling_short_support"])
        self.assertEqual(201, final["support_breakdown_confirmed_bucket"])
        self.assertIn("64,811", current_view)
        self.assertIn("63,021", current_view)
        self.assertNotIn("接近短线支撑，跌破尚未确认", current_view)
        self.assertEqual("breakdown_confirmed", structure["conclusion_key"])

    def test_market_levels_use_previous_complete_hour_for_decision_support(self):
        lows = [110.0, 108.0, 105.0, 100.0, 105.0, 104.0, 103.0, 102.0, 101.0, 80.0]
        bars = []
        for index, low in enumerate(lows):
            bars.append({
                "open_time": index * 3600 * 1000,
                "close_time": (index + 1) * 3600 * 1000 - 1,
                "low": low,
                "high": 120.0,
                "close": 85.0 if index == len(lows) - 1 else 102.0,
            })

        levels = bot.build_market_levels(bars, {})

        self.assertEqual(100.0, levels["short_support"])
        self.assertEqual(80.0, levels["rolling_short_support"])
        self.assertTrue(levels["short_breakdown_ok"])


class SupportTestStateTest(unittest.TestCase):
    @staticmethod
    def bars():
        hour_ms = 3600 * 1000
        values = [
            (64000.0, 64150.0),
            (63800.0, 63900.0),
            (63500.0, 63600.0),
            (63250.0, 63350.0),
            (63021.0, 63195.9),
            (63069.8, 63148.2),
            (63200.0, 63417.4),
        ]
        return [
            {
                "open_time": index * hour_ms,
                "close_time": (index + 1) * hour_ms - 1,
                "open": close + 30.0,
                "high": close + 150.0,
                "low": low,
                "close": close,
            }
            for index, (low, close) in enumerate(values)
        ]

    def test_63021_formation_and_63069_retest_are_distinct_closed_hours(self):
        bars = self.bars()

        formed = bot.build_support_test_state(bars[:5], 63021.0)
        tested = bot.build_support_test_state(bars, 63021.0)

        self.assertEqual("formed", formed["phase"])
        self.assertFalse(formed["completed_rule"])
        self.assertIsNone(formed["first_retest_bucket"])
        self.assertEqual("tested", tested["phase"])
        self.assertTrue(tested["completed_rule"])
        self.assertEqual(4, tested["formed_bucket"])
        self.assertEqual(5, tested["first_retest_bucket"])

    def test_later_close_below_anchor_invalidates_completed_test(self):
        bars = self.bars()
        hour_ms = 3600 * 1000
        bars.append({
            "open_time": 7 * hour_ms,
            "close_time": 8 * hour_ms - 1,
            "open": 63100.0,
            "high": 63200.0,
            "low": 62850.0,
            "close": 62900.0,
        })

        result = bot.build_support_test_state(bars, 63021.0)

        self.assertEqual("broken", result["phase"])
        self.assertFalse(result["completed_rule"])

    def test_support_change_or_expired_formation_does_not_reuse_old_test(self):
        self.assertEqual(
            "inactive",
            bot.build_support_test_state(self.bars(), 62900.0)["phase"],
        )
        expired = self.bars() + synthetic_bars(bot.MARKET_STRUCTURE_NEAR_SUPPORT_LOOKBACK_H + 1)
        for index, bar in enumerate(expired):
            bar["open_time"] = index * 3600 * 1000
            bar["close_time"] = (index + 1) * 3600 * 1000 - 1
        self.assertEqual(
            "inactive",
            bot.build_support_test_state(expired, 63021.0)["phase"],
        )


class MarketTimeContextTest(unittest.TestCase):
    CLOSED_AS_OF_MS = 1785304799999
    QUOTE_AT_MS = 1785304980000

    @classmethod
    def payload(
        cls,
        current_price=64058.0,
        current_source="Binance Spot 24hr",
        signal_usable=True,
        latest_close_ms=None,
        quote_at_ms=None,
        status="support_tested",
        support_test_anchor=63630.0,
        rolling_support=63630.0,
        support_anchor=None,
        resistance=64175.0,
        closed_price=64045.3,
        structure_bucket=200,
        breakdown_confirmed_bucket=None,
    ):
        latest_close_ms = cls.CLOSED_AS_OF_MS if latest_close_ms is None else latest_close_ms
        quote_at_ms = cls.QUOTE_AT_MS if quote_at_ms is None else quote_at_ms
        support_state = (
            "breakdown_confirmed"
            if status == "breakdown_confirmed"
            else "support_tested"
        )
        trend = {
            "status": status,
            "support_state": support_state,
            "pressure_state": "not_broken",
            "position_state": "mid_range",
            "position_text": "区间中部",
            "support_test_anchor": support_test_anchor,
            "support_anchor": support_anchor,
            "rolling_short_support": rolling_support,
            "above_pressure_hours": 0,
            "bucket": structure_bucket,
            "support_breakdown_confirmed_bucket": breakdown_confirmed_bucket,
            "data_sufficient": True,
            "levels": {"short_resistance": resistance},
        }
        payload = {
            "market": {
                "price": current_price,
                "source": current_source,
                "current_quote_at_ms": quote_at_ms,
                "quote_source_as_of_ms": quote_at_ms,
                "quote_retrieved_at": quote_at_ms / 1000,
            },
            "structure_state": {"latest_close": closed_price},
            "signal_data_quality": {
                "usable": signal_usable,
                "latest_close_ms": latest_close_ms,
                "source": "Binance Futures 1h Kline + OI Hist",
                "reasons": [] if signal_usable else ["oi_not_aligned"],
            },
            "levels": {
                "short_support": rolling_support,
                "rolling_short_support": rolling_support,
                "short_resistance": resistance,
            },
            "market_patterns": {
                "spot_active_sell": {"state": "NO"},
                "new_short_pressure": {"state": "NO"},
                "deleveraging_down": {"state": "NO"},
            },
            "rule_state": {
                "levels": {
                    "status": status,
                    "support_state": support_state,
                    "support_state_text": (
                        "原支撑失守持续"
                        if status == "breakdown_confirmed"
                        else "首次测试完成"
                    ),
                    "pressure_state": "not_broken",
                    "pressure_state_text": "未突破",
                    "position_state": "mid_range",
                    "position_text": "区间中部",
                    "price_trend": trend,
                },
                "macro": {},
                "leverage": {},
                "etf": {},
                "spot": {},
            },
            "public_records": {},
            "fear_greed": {},
            "seven_day_comparison": {},
        }
        context = bot.build_market_time_context(payload)
        context["summary_text"] = bot.build_market_time_summary(context)
        payload["market_time_context"] = context
        return payload

    def test_support_retest_and_current_quote_are_rendered_on_separate_times(self):
        payload = self.payload()
        context = payload["market_time_context"]
        expected = (
            "截至 15:00 JST，63,630 支撑已完成首次回测，上一完整小时收于 64,045，"
            "未跌破支撑；15:03 JST 现价 64,058，较支撑高 428（+0.67%），"
            "距 64,175 压力尚差 117（0.18%）；尚无连续两个完整小时收于压力上方，"
            "短线突破未确认。"
        )

        self.assertTrue(context["relation_eligible"])
        self.assertEqual("inside_range", context["current_relation"])
        self.assertAlmostEqual(428.0, context["support_distance_usd"])
        self.assertAlmostEqual(117.0, context["resistance_distance_usd"])
        self.assertEqual(expected, context["summary_text"])
        self.assertEqual(f"当前理解：{expected}", bot.format_compact_current_view(payload))
        self.assertEqual(
            expected,
            bot.build_market_pattern_current_structure(payload)["text"],
        )
        self.assertIn(f"市场结构: {expected}", bot.format_final_view(payload))
        self.assertIn(
            "时间对齐：上一完整小时 15:00 JST 收盘 64,045 | 推送现价 15:03 JST 64,058",
            bot.format_compact_level_block(payload),
        )
        self.assertNotIn("。。", "\n".join(bot.format_market_patterns_block(payload)))

        state = {}
        bot.append_success_log(state, "2026-07-29", payload, False)
        self.assertEqual(context, state["event_logs"][-1]["market_time_context"])

    def test_live_quote_above_resistance_does_not_confirm_closed_hour_breakout(self):
        payload = self.payload(current_price=64200.0)
        context = payload["market_time_context"]

        self.assertEqual("support_tested", payload["rule_state"]["levels"]["price_trend"]["status"])
        self.assertEqual("above_resistance", context["current_relation"])
        self.assertIn("暂时高于 64,175 压力 25（0.04%）", context["summary_text"])
        self.assertIn("等待完整 1H 收盘确认", context["summary_text"])
        self.assertIn("不提前改变完整小时状态", context["summary_text"])

    def test_live_quote_below_support_does_not_enter_breakdown_watch(self):
        payload = self.payload(current_price=63500.0)
        context = payload["market_time_context"]

        self.assertEqual("support_tested", payload["rule_state"]["levels"]["price_trend"]["status"])
        self.assertEqual("below_support", context["current_relation"])
        self.assertIn("暂时低于 63,630 支撑 130（0.20%）", context["summary_text"])
        self.assertIn("等待完整 1H 收盘确认", context["summary_text"])

    def test_completed_test_anchor_is_not_replaced_by_new_rolling_support(self):
        payload = self.payload(rolling_support=63526.0)
        context = payload["market_time_context"]

        self.assertEqual(63630.0, context["reference_support"])
        self.assertEqual("support_test_anchor", context["reference_support_kind"])
        self.assertEqual(63526.0, context["rolling_short_support"])
        self.assertIn("63,630 支撑已完成首次回测", context["summary_text"])

    def test_cross_provider_quote_keeps_price_but_blocks_level_relation(self):
        payload = self.payload(current_source="CoinGecko")
        context = payload["market_time_context"]

        self.assertFalse(context["relation_eligible"])
        self.assertEqual("source_mismatch", context["ineligible_reason"])
        self.assertEqual(64058.0, context["current_price"])
        self.assertIn("现价与结构价位来源不同", context["summary_text"])
        self.assertNotIn("距 64,175 压力", context["summary_text"])

    def test_binance_spot_quote_is_compatible_with_binance_futures_structure(self):
        payload = self.payload(current_source="Binance Spot 24hr")

        self.assertTrue(payload["market_time_context"]["relation_eligible"])
        self.assertEqual("inside_range", payload["market_time_context"]["current_relation"])

    def test_unaligned_or_regressed_time_degrades_without_new_structure_claim(self):
        unaligned = self.payload(signal_usable=False)
        self.assertEqual(
            "signal_data_unusable",
            unaligned["market_time_context"]["ineligible_reason"],
        )
        self.assertIn("上一完整小时数据未对齐", unaligned["market_time_context"]["summary_text"])

        regressed = self.payload(quote_at_ms=self.CLOSED_AS_OF_MS - 1000)
        self.assertEqual("time_regression", regressed["market_time_context"]["ineligible_reason"])
        self.assertIn("报价源时间早于上一完整小时", regressed["market_time_context"]["summary_text"])

        missing_quote = self.payload(current_price=None)
        self.assertEqual(
            "current_price_missing",
            missing_quote["market_time_context"]["ineligible_reason"],
        )
        self.assertIn("推送时现价数据不足", missing_quote["market_time_context"]["summary_text"])

        missing_closed = self.payload(latest_close_ms=0, closed_price=None)
        self.assertEqual(
            "closed_bar_missing",
            missing_closed["market_time_context"]["ineligible_reason"],
        )
        self.assertIn("截至上一完整小时，结构数据不足", missing_closed["market_time_context"]["summary_text"])

    def test_confirmed_closed_hour_status_remains_confirmed(self):
        payload = self.payload(
            current_price=64200.0,
            status="trend_confirmed",
            support_test_anchor=None,
        )
        payload["rule_state"]["levels"]["price_trend"]["pressure_state"] = "breakout_confirmed"
        context = bot.build_market_time_context(payload)
        context["summary_text"] = bot.build_market_time_summary(context)
        payload["market_time_context"] = context

        self.assertIn("64,175 压力处于突破确认", context["summary_text"])
        self.assertIn("短线突破已由完整小时确认", context["summary_text"])

    def test_breakdown_confirmation_event_is_not_repeated_in_later_hours(self):
        confirmed = self.payload(
            current_price=63488.0,
            status="breakdown_confirmed",
            support_test_anchor=None,
            support_anchor=64657.0,
            rolling_support=62400.0,
            resistance=64059.0,
            closed_price=63561.0,
            structure_bucket=201,
            breakdown_confirmed_bucket=201,
        )
        active = self.payload(
            current_price=63568.0,
            status="breakdown_confirmed",
            support_test_anchor=None,
            support_anchor=64657.0,
            rolling_support=63352.0,
            resistance=64059.0,
            closed_price=63588.0,
            structure_bucket=202,
            breakdown_confirmed_bucket=201,
        )

        self.assertEqual("confirmation_event", confirmed["market_time_context"]["breakdown_phase"])
        self.assertIn("64,657 原支撑本轮跌破确认", confirmed["market_time_context"]["summary_text"])
        confirmed_short = "\n".join(bot.format_plain_short_current_view_block(confirmed))
        self.assertEqual(1, confirmed_short.count("跌破确认"))

        context = active["market_time_context"]
        self.assertEqual("active_below_former_support", context["breakdown_phase"])
        self.assertEqual(63352.0, context["current_support"])
        self.assertIn("仍在 64,657 原支撑下方", context["summary_text"])
        self.assertIn("仍低于 64,657 原支撑", context["summary_text"])
        self.assertNotIn("等待完整 1H 收盘确认跌破", context["summary_text"])

        active_short = "\n".join(bot.format_plain_short_current_view_block(active))
        active_levels = "\n".join(bot.format_plain_short_level_block(active))
        active_full = "\n".join(bot.format_final_view(active))
        active_structure = bot.build_market_pattern_current_structure(active)["text"]
        self.assertNotIn("跌破已确认", active_short)
        self.assertNotIn("支撑（跌破确认）", active_levels)
        self.assertIn("原支撑失守状态仍在", active_short)
        self.assertIn("63,352 当前支撑", active_levels)
        self.assertIn("64,657 原支撑（收回观察位）", active_levels)
        self.assertIn("仍在 64,657 原支撑下方", active_full)
        self.assertIn("仍在 64,657 原支撑下方", active_structure)
        self.assertEqual("❌ 仍在原支撑下方", bot.plain_breakout_line(active["rule_state"]["levels"]))
        self.assertIn("原支撑失守状态仍在", bot.plain_action_line(active))
        self.assertIn("仍在原支撑下方", bot.format_market_stage_line(active))
        self.assertIn("价格仍在原支撑下方", bot.format_market_key_line(active))

    def test_confirmed_breakdown_without_lower_candidate_has_no_false_current_support(self):
        payload = self.payload(
            current_price=63568.0,
            status="breakdown_confirmed",
            support_test_anchor=None,
            support_anchor=64657.0,
            rolling_support=64657.0,
            resistance=65000.0,
            closed_price=63588.0,
            structure_bucket=202,
            breakdown_confirmed_bucket=201,
        )

        self.assertIsNone(payload["market_time_context"]["current_support"])
        self.assertIn(
            "当前支撑尚未形成",
            "\n".join(bot.format_plain_short_level_block(payload)),
        )

    def test_live_quote_above_former_support_waits_for_closed_hour_reclaim(self):
        payload = self.payload(
            current_price=64700.0,
            status="breakdown_confirmed",
            support_test_anchor=None,
            support_anchor=64657.0,
            rolling_support=63352.0,
            resistance=64059.0,
            closed_price=63588.0,
            structure_bucket=202,
            breakdown_confirmed_bucket=201,
        )
        context = payload["market_time_context"]

        self.assertEqual("temporarily_reclaimed_former_support", context["current_relation"])
        self.assertIn("暂时回到 64,657 原支撑上方", context["summary_text"])
        self.assertIn("等待完整 1H 收盘确认收回", context["summary_text"])

    def test_attached_multi_hour_replay_emits_one_confirmation_and_tracks_current_support(self):
        samples = [
            (63561.0, 63488.0, 62400.0),
            (63588.0, 63568.0, 62400.0),
            (63497.0, 63522.0, 63352.0),
            (63338.0, 63398.0, 63290.0),
            (63816.0, 63787.0, 63352.0),
            (63802.0, 63782.0, 63352.0),
            (63771.0, 63775.0, 63352.0),
            (64004.0, 63942.0, 63624.0),
            (63784.0, 63772.0, 63624.0),
            (63653.0, 63666.0, 63624.0),
            (63666.0, 63738.0, 63624.0),
        ]
        views = []
        level_blocks = []
        for offset, (closed_price, current_price, rolling_support) in enumerate(samples):
            payload = self.payload(
                current_price=current_price,
                status="breakdown_confirmed",
                support_test_anchor=None,
                support_anchor=64657.0,
                rolling_support=rolling_support,
                resistance=64059.0 if offset < 7 else 64240.0,
                closed_price=closed_price,
                structure_bucket=201 + offset,
                breakdown_confirmed_bucket=201,
            )
            views.append("\n".join(bot.format_plain_short_current_view_block(payload)))
            level_blocks.append("\n".join(bot.format_plain_short_level_block(payload)))

        self.assertEqual(1, sum(view.count("跌破确认") for view in views))
        self.assertTrue(all("支撑已经确认跌破" not in view for view in views))
        self.assertTrue(all("原支撑失守状态仍在" in view for view in views[1:]))
        self.assertIn("62,400 当前支撑", level_blocks[0])
        self.assertIn("63,352 当前支撑", level_blocks[2])
        self.assertIn("63,624 当前支撑", level_blocks[-1])
        self.assertTrue(all("64,657 原支撑（收回观察位）" in text for text in level_blocks))


class ShortReportPlainLanguageLayoutTest(unittest.TestCase):
    @staticmethod
    def payload():
        payload = MarketTimeContextTest.payload(
            current_price=64058.0,
            resistance=64175.0,
            closed_price=64045.3,
        )
        payload["market"].update({
            "chg24": 1.54,
            "oi_usd": 6.72e9,
        })
        payload["price_7d_pct"] = -2.37
        payload["fear_greed"] = {
            "available": True,
            "value": 29,
            "classification_cn": "恐惧",
            "timestamp": int(bot.time.time()),
        }
        payload["market_24h_delta"] = {"metrics": {"vol24": -12.65}}
        payload["delta4h"] = {
            "oi_4h_pct": 0.40,
            "vol_4h_pct": 24.51,
        }
        payload["structure_state"]["checks"] = {
            "oi_slow_confirmation": {
                "delta_24h_pct": -1.70,
                "delta_72h_pct": -2.71,
            }
        }
        payload["levels"]["trend_support"] = 62660.0
        payload["public_records"] = {
            "macro": {
                "status": "ok",
                "value": {
                    "us10y": 4.64,
                    "us10y_30d_pp": 0.27,
                    "real_yield_10y": 2.41,
                    "real_yield_10y_30d_pp": 0.23,
                    "usd_index": 101.34,
                    "usd_index_30d_pct": 0.22,
                    "m2_yoy_pct": 5.53,
                    "fed_balance_sheet_30d_pct": 0.16,
                    "fiscal_proxy": {
                        "status": "stressed",
                        "interest_revenue_ratio_pct": 30.0,
                    },
                    "treasury_buyback": {
                        "total_par_amt_accepted": 2_000_000_000,
                    },
                },
            },
            "funding": {"status": "ok", "value": {"funding_8h_pct": 0.010}},
            "basis": {"status": "ok", "value": {"basis_pct": -0.048}},
            "etf": {
                "status": "ok",
                "value": {
                    "flow_1d_usd": -50_000_000,
                    "flow_5d_usd": -458_000_000,
                    "flow_30d_usd": -2_000_000_000,
                    "source_date": {"flow_1d": "2026-07-28"},
                },
            },
            "spot_cvd": {
                "status": "ok",
                "value": {
                    "cvd_4h_pct": 6.34,
                    "cvd_24h_pct": 4.70,
                    "volume_4h_pct": 19.96,
                    "volume_24h_pct": -9.54,
                },
            },
            "stablecoin": {
                "status": "ok",
                "value": {
                    "flow_24h_usd": -725_000_000,
                    "flow_7d_usd": -508_000_000,
                },
            },
        }
        payload["rule_state"].update({
            "macro": {
                "environment": {
                    "high_real_rate_pressure_rule": True,
                    "dxy_stage_strength_rule": False,
                    "dxy_strong_rule": False,
                    "bands": {
                        "us10y": "偏高",
                        "real_yield": "偏高",
                        "dxy": "中性偏强",
                    },
                },
                "long_term_environment": {
                    "status": "exists_not_dominant",
                },
            },
        })
        pattern_items = [
            {
                "key": item["key"],
                "state": "NO",
                "evidence": {},
            }
            for item in bot.load_status_rules_config()["market_patterns"]["patterns"]
        ]
        for item in pattern_items:
            if item["key"] == "resistance_false_breakout":
                item["state"] = "YES"
                item["evidence"] = {"confirmed_bucket": 100}
        payload["market_patterns"] = {
            "items": pattern_items,
            "data_quality": {"bucket": 100},
        }
        payload["news_titles"] = [
            "MARA CEO：矿企未来核心竞争力将转向电力资源",
            "交易平台推出新的账户活动",
        ]
        payload["completeness"] = {
            "core_market": {"available": 12, "total": 12},
            "macro": {"available": 3, "total": 3},
        }
        return payload

    def test_short_report_keeps_modules_and_uses_plain_language(self):
        message = bot.build_report_message(
            now_tokyo="2026-07-29 15:03 JST",
            is_new_day=False,
            today="2026-07-29",
            push_count=1,
            payload=self.payload(),
        )

        self.assertTrue(message.startswith("📊 BTC 07-29 15:03 JST｜辅助观察\n"))
        expected_modules = [
            "公共宏观与事件：",
            "📍 资金",
            "📐 过去4小时发生了什么",
            "🎯 现在怎么看",
            "📌 市场结构",
            "📰 新闻",
            "🤖 数据状态",
        ]
        for module in expected_modules:
            with self.subTest(module=module):
                self.assertIn(module, message)

        self.assertIn("市场情绪：恐惧（29/100）", message)
        self.assertNotIn("🏛 宏观大环境", message)
        self.assertNotIn("美债10Y", message)
        self.assertIn("/macro", message)
        self.assertIn("合约未平仓：$6.72B | 4H +0.40% | 24H -1.70% | 72H -2.71%", message)
        self.assertIn("现货主动买卖：4H 主动买入占优6.34% | 24H 主动买入占优4.70%", message)
        self.assertIn("15:00 JST 收 64,045 | 63,630 支撑本次守住", message)
        self.assertIn("结论：已接近压力，但尚未确认突破", message)
        self.assertIn("63,630 支撑（已测试） | 64,175 压力（未突破）", message)
        self.assertIn("MARA CEO：矿企未来核心竞争力将转向电力资源", message)
        self.assertIn("采集自检通过（策略仍需独立核验）", message)
        self.assertNotIn("F&G", message)
        self.assertNotIn("Spot CVD", message)
        self.assertNotIn("Basis", message)
        self.assertLess(len(message), 1100)

    def test_compact_time_label_keeps_month_and_day(self):
        self.assertEqual(
            bot.compact_time_label("2026-08-03 15:03 JST"),
            "08-03 15:03 JST",
        )

    def test_macro_keeps_signed_thirty_day_changes(self):
        payload = self.payload()
        macro_value = payload["public_records"]["macro"]["value"]
        macro_value["us10y_30d_pp"] = -0.12
        macro_value["real_yield_10y_30d_pp"] = -0.08
        macro_value["usd_index_30d_pct"] = -0.30

        text = "\n".join(bot.format_plain_short_macro_block(payload))

        self.assertIn("美债10Y 4.64/4.2（30天 -0.12pp）", text)
        self.assertIn("实际利率 2.41/2（30天 -0.08pp）", text)
        self.assertIn("美元指数 101.34/100（30天 -0.30%）", text)

    def test_short_report_does_not_repeat_full_time_summary_in_pattern_module(self):
        message = bot.build_report_message(
            now_tokyo="2026-07-29 15:03 JST",
            is_new_day=False,
            today="2026-07-29",
            push_count=1,
            payload=self.payload(),
        )
        pattern_text = message.split("📐 过去4小时发生了什么", 1)[1].split(
            "🎯 现在怎么看",
            1,
        )[0]

        self.assertNotIn("15:00 JST", pattern_text)
        self.assertNotIn("当前窗口没有可用于组合的已确认形态", pattern_text)
        self.assertEqual(1, message.count("价格曾涨过近期高位"))
        self.assertLessEqual(message.count("63,630"), 2)
        self.assertLessEqual(message.count("64,175"), 2)

    def test_waiting_pattern_uses_compact_plain_language(self):
        payload = self.payload()
        for item in payload["market_patterns"]["items"]:
            item["state"] = "NO"
            item["evidence"] = {}
            if item["key"] == "volume_breakout":
                item["state"] = "WAIT"
                item["evidence"] = {
                    "anchor": 64557.0,
                    "remaining_confirm_bars": 2,
                }

        text = "\n".join(bot.format_plain_short_patterns_block(payload))

        self.assertIn(
            "放量冲高正在确认：观察 64,557，还需2个完整小时｜当前偏强，持续性未确认",
            text,
        )
        self.assertNotIn("合约成交额明显增加时", text)
        self.assertNotIn("正在观察能否继续保持", text)

    def test_live_price_crossing_level_still_waits_for_closed_hour(self):
        payload = self.payload()
        payload["market"]["price"] = 64200.0
        context = bot.build_market_time_context(payload)
        context["summary_text"] = bot.build_market_time_summary(context)
        payload["market_time_context"] = context

        text = "\n".join(bot.format_plain_short_current_view_block(payload))

        self.assertIn("暂高于 64,175 压力 25（0.04%）", text)
        self.assertIn("仍需等待完整小时收盘确认", text)
        self.assertIn("临时越过关键位", text)
        self.assertEqual(
            "support_tested",
            payload["rule_state"]["levels"]["price_trend"]["status"],
        )

    def test_unaligned_window_is_compact_and_does_not_add_structure_claim(self):
        payload = self.payload()
        payload["signal_data_quality"]["usable"] = False
        payload["signal_data_quality"]["reasons"] = ["oi_not_aligned"]
        for item in payload["market_patterns"]["items"]:
            item["state"] = "NA"
            item["reason"] = "signal_data_unusable"
            item["evidence"] = {"quality_reasons": ["oi_not_aligned"]}
        context = bot.build_market_time_context(payload)
        context["summary_text"] = bot.build_market_time_summary(context)
        payload["market_time_context"] = context

        pattern_text = "\n".join(bot.format_plain_short_patterns_block(payload))
        current_text = "\n".join(bot.format_plain_short_current_view_block(payload))

        self.assertIn("本次不判断4H形态", pattern_text)
        self.assertEqual(1, pattern_text.count("数据不足"))
        self.assertNotIn("• ", pattern_text)
        self.assertIn("本次不新增突破或跌破判断", current_text)
        self.assertNotIn("结论：支撑测试已完成", current_text)

    def test_news_module_remains_when_there_is_no_new_title(self):
        payload = self.payload()
        payload["news_titles"] = []

        message = bot.build_report_message(
            now_tokyo="2026-07-29 15:03 JST",
            is_new_day=False,
            today="2026-07-29",
            push_count=1,
            payload=payload,
        )

        self.assertIn("📰 新闻", message)
        self.assertIn("本小时无新增新闻", message)


class PostSupportConsolidationStateTest(unittest.TestCase):
    SUPPORT_TEST = {
        "anchor": 63021.0,
        "formed_bucket": 300,
        "first_retest_bucket": 301,
        "phase": "tested",
        "completed_rule": True,
        "buffer": 94.5315,
    }
    CLEAR_DOWN = {
        "spot_active_sell": {"state": "NO"},
        "new_short_pressure": {"state": "NO"},
        "deleveraging_down": {"state": "NO"},
    }
    NEUTRAL_LEVERAGE = {"oi_24h_pct": 1.0, "funding_8h_pct": 0.004}

    def update(self, state=None, bucket=302, close=63417.4, patterns=None, support_test=None):
        return bot.update_price_trend_state(
            trend_payload(
                bucket,
                close,
                63021.0,
                rolling_support=63021.0,
                support_test=self.SUPPORT_TEST if support_test is None else support_test,
                patterns=patterns or {},
            ),
            state if state is not None else {},
            self.NEUTRAL_LEVERAGE,
            DOWN_SPOT,
            {},
        )

    @staticmethod
    def render_payload(trend):
        levels = bot.apply_price_trend_to_level_state({}, trend)
        return {
            "market_patterns": {},
            "public_records": {},
            "rule_state": {"levels": levels},
        }

    def test_first_low_volume_hit_enters_watch_and_reports_easing(self):
        patterns = {
            **self.CLEAR_DOWN,
            "low_volume_range": {"state": "WAIT", "raw_hit": True},
            "spot_sell_pressure_change": {
                "available": True,
                "easing_rule": True,
                "previous_cvd_4h_pct": -2.19,
                "current_cvd_4h_pct": -1.17,
                "bucket_gap": 1,
            },
        }
        trend = self.update(patterns=patterns)
        payload = self.render_payload(trend)
        current_view = bot.format_compact_current_view(payload)
        structure = bot.build_market_pattern_current_structure(payload)
        conclusion = "\n".join(bot.format_market_patterns_block(payload))
        full_rules = "\n".join(bot.format_final_view(payload))

        self.assertEqual(5, trend["schema_version"])
        self.assertEqual("post_support_consolidation_watch", trend["status"])
        self.assertEqual("support_tested", trend["support_state"])
        self.assertEqual("consolidation_watch", trend["post_support_phase"])
        self.assertEqual(
            "当前理解：价格已完成对 63,021 附近支撑的首次测试；"
            "当前进入缩量整理观察，Binance 4H现货主动卖出占优幅度"
            "较上一可用整点缩小，但尚未形成反转。",
            current_view,
        )
        self.assertNotIn("接近短线支撑，跌破尚未确认", current_view)
        self.assertEqual(
            "post_support_consolidation_watch",
            structure["conclusion_key"],
        )
        self.assertIn("当前支撑首次测试后进入缩量整理观察", conclusion)
        self.assertNotIn("当前接近短线支撑，跌破尚未确认", conclusion)
        self.assertIn("支撑首次测试后进入缩量整理观察", full_rules)

    def test_three_consecutive_low_volume_hits_confirm_consolidation(self):
        trend = self.update(patterns={
            **self.CLEAR_DOWN,
            "low_volume_range": {
                "state": "YES",
                "raw_hit": True,
                "consecutive_hits": 3,
            },
        })
        current_view = bot.format_compact_current_view(self.render_payload(trend))

        self.assertEqual("post_support_consolidation_confirmed", trend["status"])
        self.assertIn("缩量整理已确认，方向仍未选择", current_view)
        self.assertNotIn("接近短线支撑", current_view)

    def test_down_pattern_return_has_priority_over_consolidation(self):
        trend = self.update(patterns={
            **self.CLEAR_DOWN,
            "spot_active_sell": {"state": "YES"},
            "low_volume_range": {"state": "YES", "raw_hit": True},
        })

        self.assertEqual("post_support_pressure_returned", trend["status"])
        self.assertIn(
            "关注是否再次测试或跌破支撑",
            bot.format_compact_current_view(self.render_payload(trend)),
        )

    def test_close_below_test_anchor_enters_existing_breakdown_watch(self):
        broken_test = {
            **self.SUPPORT_TEST,
            "phase": "broken",
            "completed_rule": False,
        }
        trend = self.update(
            close=62900.0,
            support_test=broken_test,
            patterns={
                **self.CLEAR_DOWN,
                "low_volume_range": {"state": "YES", "raw_hit": True},
            },
        )

        self.assertEqual("breakdown_watch", trend["status"])
        self.assertEqual(63021.0, trend["support_anchor"])
        self.assertEqual("completed_support_test", trend["support_anchor_source"])

    def test_missing_cvd_history_uses_data_insufficient_wording(self):
        trend = self.update(patterns={
            **self.CLEAR_DOWN,
            "low_volume_range": {"state": "WAIT", "raw_hit": True},
            "spot_sell_pressure_change": {
                "available": False,
                "easing_rule": None,
            },
        })
        current_view = bot.format_compact_current_view(self.render_payload(trend))

        self.assertIn("卖压是否继续衰减待确认", current_view)
        self.assertNotIn("幅度较上一可用整点缩小", current_view)

    def test_data_gap_preserves_support_test_phase(self):
        state = {}
        current = self.update(
            state=state,
            patterns={
                **self.CLEAR_DOWN,
                "low_volume_range": {"state": "WAIT", "raw_hit": True},
            },
        )
        missing = bot.update_price_trend_state(
            trend_payload(303, None, 63021.0, rolling_support=63021.0),
            state,
            self.NEUTRAL_LEVERAGE,
            DOWN_SPOT,
            {},
        )

        self.assertFalse(missing["data_sufficient"])
        self.assertEqual(current["status"], missing["status"])
        self.assertEqual(63021.0, missing["support_test_anchor"])
        self.assertEqual("consolidation_watch", missing["post_support_phase"])

    def test_completed_support_test_survives_new_rolling_candidate(self):
        state = {}
        tested = self.update(
            state=state,
            bucket=302,
            close=63417.4,
            patterns=self.CLEAR_DOWN,
        )
        new_formed_candidate = {
            "anchor": 63526.0,
            "formed_bucket": 303,
            "first_retest_bucket": None,
            "phase": "formed",
            "completed_rule": False,
            "buffer": None,
        }
        persisted = bot.update_price_trend_state(
            trend_payload(
                303,
                63927.5,
                63526.0,
                rolling_support=63526.0,
                support_test=new_formed_candidate,
                patterns=self.CLEAR_DOWN,
            ),
            state,
            self.NEUTRAL_LEVERAGE,
            UP_SPOT,
            {},
        )

        self.assertEqual("support_tested", tested["status"])
        self.assertEqual("support_tested", persisted["status"])
        self.assertEqual(63021.0, persisted["support_test_anchor"])
        self.assertEqual(300, persisted["support_test_formed_bucket"])
        self.assertEqual(301, persisted["support_test_first_retest_bucket"])
        self.assertEqual("tested", persisted["support_test_phase"])
        self.assertEqual("tested", persisted["post_support_phase"])

        untested_new_break = {
            **new_formed_candidate,
            "phase": "broken",
        }
        still_persisted = bot.update_price_trend_state(
            trend_payload(
                304,
                63400.0,
                63526.0,
                rolling_support=63526.0,
                support_test=untested_new_break,
                patterns=self.CLEAR_DOWN,
            ),
            state,
            self.NEUTRAL_LEVERAGE,
            UP_SPOT,
            {},
        )
        self.assertEqual("support_tested", still_persisted["status"])
        self.assertEqual(63021.0, still_persisted["support_test_anchor"])
        self.assertIsNone(still_persisted["support_anchor"])

        consolidation = bot.update_price_trend_state(
            trend_payload(
                305,
                63950.0,
                63526.0,
                rolling_support=63526.0,
                support_test=new_formed_candidate,
                patterns={
                    **self.CLEAR_DOWN,
                    "low_volume_range": {"state": "WAIT", "raw_hit": True},
                },
            ),
            state,
            self.NEUTRAL_LEVERAGE,
            UP_SPOT,
            {},
        )
        self.assertEqual("post_support_consolidation_watch", consolidation["status"])
        self.assertEqual(63021.0, consolidation["support_test_anchor"])

    def test_persisted_support_test_breaks_or_is_replaced_explicitly(self):
        state = {}
        self.update(
            state=state,
            bucket=302,
            close=63417.4,
            patterns=self.CLEAR_DOWN,
        )
        new_formed_candidate = {
            "anchor": 63526.0,
            "formed_bucket": 303,
            "first_retest_bucket": None,
            "phase": "formed",
            "completed_rule": False,
            "buffer": None,
        }
        broken = bot.update_price_trend_state(
            trend_payload(
                303,
                62900.0,
                63526.0,
                rolling_support=63526.0,
                support_test=new_formed_candidate,
                patterns=self.CLEAR_DOWN,
            ),
            state,
            self.NEUTRAL_LEVERAGE,
            DOWN_SPOT,
            {},
        )
        self.assertEqual("breakdown_watch", broken["status"])
        self.assertEqual(63021.0, broken["support_anchor"])
        self.assertEqual("completed_support_test", broken["support_anchor_source"])

        replacement_state = {}
        self.update(
            state=replacement_state,
            bucket=302,
            close=63417.4,
            patterns=self.CLEAR_DOWN,
        )
        new_tested_candidate = {
            "anchor": 63526.0,
            "formed_bucket": 303,
            "first_retest_bucket": 304,
            "phase": "tested",
            "completed_rule": True,
            "buffer": 95.0,
        }
        replaced = bot.update_price_trend_state(
            trend_payload(
                305,
                63700.0,
                63526.0,
                rolling_support=63526.0,
                support_test=new_tested_candidate,
                patterns=self.CLEAR_DOWN,
            ),
            replacement_state,
            self.NEUTRAL_LEVERAGE,
            UP_SPOT,
            {},
        )
        self.assertEqual("support_tested", replaced["status"])
        self.assertEqual(63526.0, replaced["support_test_anchor"])
        self.assertEqual(304, replaced["support_test_first_retest_bucket"])


class SpotSellPressureChangeTest(unittest.TestCase):
    @staticmethod
    def context(cvd, aligned=True):
        return {
            "meta": {"spot_aligned": aligned},
            "price": {"change_4h_pct": -0.2},
            "derivatives": {"volume_change_4h_pct": -20.0},
            "spot": {
                "cvd_4h_pct": cvd,
                "volume_change_4h_pct": -10.0,
            },
        }

    def test_negative_cvd_absolute_value_shrinks_within_two_hours(self):
        history = [{
            "bucket": 100,
            "metrics": {
                "spot_cvd_4h_pct": -2.19,
                "spot_aligned": True,
            },
        }]

        result = bot.build_spot_sell_pressure_change(
            history,
            102,
            self.context(-1.17),
        )

        self.assertTrue(result["available"])
        self.assertTrue(result["easing_rule"])
        self.assertEqual(2, result["bucket_gap"])

    def test_missing_stale_or_direction_mismatch_never_claims_easing(self):
        cases = [
            ([], 101, self.context(-1.17)),
            ([{"bucket": 98, "metrics": {"spot_cvd_4h_pct": -2.19, "spot_aligned": True}}], 101, self.context(-1.17)),
            ([{"bucket": 100, "metrics": {"spot_cvd_4h_pct": -2.19, "spot_aligned": False}}], 101, self.context(-1.17)),
            ([{"bucket": 100, "metrics": {"spot_cvd_4h_pct": 2.19, "spot_aligned": True}}], 101, self.context(-1.17)),
            ([{"bucket": 100, "metrics": {"spot_cvd_4h_pct": -2.19, "spot_aligned": True}}], 101, self.context(-1.17, aligned=False)),
        ]
        for history, bucket, context in cases:
            with self.subTest(history=history, bucket=bucket, context=context):
                result = bot.build_spot_sell_pressure_change(history, bucket, context)
                self.assertFalse(result["available"])
                self.assertIsNone(result["easing_rule"])


class MarketPatternQuadrantTest(unittest.TestCase):
    def test_price_oi_quadrants_are_mutually_exclusive(self):
        cases = [
            (-0.8, -1.5, "deleveraging_down"),
            (-0.8, 1.5, "new_short_pressure"),
            (0.8, 1.5, "leveraged_long_rise"),
            (0.8, -1.5, "short_covering_rise"),
        ]
        keys = [item[2] for item in cases]
        for price, oi, expected in cases:
            context = market_pattern_context(price, oi)
            states = {
                key: bot.evaluate_snapshot_market_pattern(market_pattern_definition(key), context)["state"]
                for key in keys
            }
            self.assertEqual("YES", states[expected])
            self.assertEqual(1, sum(state == "YES" for state in states.values()))

    def test_dead_zone_does_not_invent_a_quadrant(self):
        context = market_pattern_context(0.2, 0.3)
        for key in ("deleveraging_down", "new_short_pressure", "leveraged_long_rise", "short_covering_rise"):
            self.assertEqual(
                "NO",
                bot.evaluate_snapshot_market_pattern(market_pattern_definition(key), context)["state"],
            )

    def test_missing_price_is_data_insufficient(self):
        context = market_pattern_context(None, -1.5)
        self.assertEqual(
            "NA",
            bot.evaluate_snapshot_market_pattern(market_pattern_definition("deleveraging_down"), context)["state"],
        )


class MarketPatternSpotTest(unittest.TestCase):
    def test_spot_active_sell_requires_price_cvd_and_volume(self):
        definition = market_pattern_definition("spot_active_sell")
        self.assertEqual(
            "YES",
            bot.evaluate_snapshot_market_pattern(definition, market_pattern_context(-0.8, -1.2, spot_cvd=-4.5, spot_volume=270.0))["state"],
        )
        self.assertEqual(
            "NO",
            bot.evaluate_snapshot_market_pattern(definition, market_pattern_context(-0.8, -1.2, spot_cvd=-4.5, spot_volume=-10.0))["state"],
        )

    def test_missing_spot_data_is_not_no(self):
        definition = market_pattern_definition("spot_active_sell")
        result = bot.evaluate_snapshot_market_pattern(
            definition,
            market_pattern_context(-0.8, -1.2, spot_cvd=None, spot_volume=None),
        )
        self.assertEqual("NA", result["state"])


class MarketPatternStructureTest(unittest.TestCase):
    def structural(self, bars):
        config = bot.load_status_rules_config()["market_patterns"]
        return bot.evaluate_structural_market_patterns(
            bars,
            {"usable": True},
            config["patterns"],
            config,
        )

    def test_support_sweep_uses_prior_frozen_support(self):
        bars = synthetic_bars()
        bars[-1].update({"low": 98.0, "close": 99.5})
        result = self.structural(bars)["support_sweep"]
        self.assertEqual("YES", result["state"])
        self.assertEqual(99.0, result["evidence"]["anchor"])

    def test_false_breakout_uses_prior_frozen_resistance(self):
        bars = synthetic_bars()
        bars[-1].update({"high": 101.6, "close": 100.5})
        result = self.structural(bars)["false_breakout"]
        self.assertEqual("YES", result["state"])
        self.assertEqual(101.0, result["evidence"]["anchor"])

    def test_volume_breakout_requires_two_closed_bars(self):
        bars = synthetic_bars()
        bars[-2].update({"high": 101.8, "close": 101.6, "quote_volume": 400.0})
        bars[-1].update({"high": 101.9, "low": 100.8, "close": 101.7, "quote_volume": 400.0})
        result = self.structural(bars)["volume_breakout"]
        self.assertEqual("YES", result["state"])
        self.assertGreaterEqual(result["evidence"]["volume_ratio"], bot.STRUCTURE_VOLUME_FAST_MULTIPLIER)

    def delayed_support_reclaim(self, trigger_from_end, reclaim_from_end=None):
        config = bot.load_status_rules_config()["market_patterns"]
        prefix_required = max(
            bot.MARKET_STRUCTURE_SHORT_LOOKBACK_H,
            bot.MARKET_STRUCTURE_NEAR_SUPPORT_LOOKBACK_H,
            15,
        )
        required = prefix_required + int(config["structure_scan_hours"]) + 1
        bars = synthetic_bars(required + 1)
        trigger_index = len(bars) + int(trigger_from_end)
        bars[trigger_from_end].update({"low": 98.0, "close": 98.5})
        for index in range(trigger_index + 1, len(bars)):
            bars[index].update({"low": 98.4, "close": 98.5})
        if reclaim_from_end is not None:
            bars[reclaim_from_end].update({"low": 98.4, "close": 99.5})

        local_trigger_index = trigger_index - 1

        def support_for_prefix(prefix, *_args):
            return 99.0 if len(prefix) == local_trigger_index else 90.0

        with patch.object(bot, "calc_near_term_support_level", side_effect=support_for_prefix):
            return self.structural(bars)["support_sweep"]

    def test_support_reclaim_on_third_hour_is_confirmed(self):
        result = self.delayed_support_reclaim(-4, -1)
        self.assertEqual("YES", result["state"])
        self.assertEqual(
            3,
            result["evidence"]["confirmed_bucket"] - result["evidence"]["start_bucket"],
        )

    def test_support_candidate_expires_when_third_hour_closes_below_anchor(self):
        result = self.delayed_support_reclaim(-4)
        self.assertEqual("NO", result["state"])

    def test_support_reclaim_on_fourth_hour_is_not_backfilled(self):
        result = self.delayed_support_reclaim(-5, -1)
        self.assertEqual("NO", result["state"])


class ResistanceVolumeStallTest(unittest.TestCase):
    def fixture(self, extra_bars=0):
        bars = synthetic_bars(40 + extra_bars)
        touch_index = 37
        for index in range(36, 40):
            bars[index]["quote_volume"] = 200.0
        bars[touch_index].update({"high": 101.2, "close": 100.9})
        bars[touch_index + 1].update({"high": 101.15, "low": 100.7, "close": 100.95})
        bars[touch_index + 2].update({"high": 101.12, "low": 100.7, "close": 100.92})

        hourly_flow = []
        for index, bar in enumerate(bars):
            quote_volume = 100.0 if index >= 36 else 50.0
            hourly_flow.append({
                "close_time": bar["close_time"],
                "close": bar["close"],
                "quote_volume": quote_volume,
                "net_quote": quote_volume * 0.10,
            })
        spot_record = {
            "status": "ok",
            "value": {"hourly_flow": hourly_flow[-16:]},
        }
        return bars, spot_record

    def evaluate(self, bars, spot_record):
        config = bot.load_status_rules_config()["market_patterns"]
        return bot.evaluate_resistance_stall_market_pattern(
            bars,
            spot_record,
            {"usable": True},
            market_pattern_definition("resistance_volume_stall"),
            config,
        )

    def test_touch_waits_for_two_following_closed_hours(self):
        bars, spot_record = self.fixture()
        touch_only = self.evaluate(bars[:-2], spot_record)
        one_following = self.evaluate(bars[:-1], spot_record)

        self.assertEqual("WAIT", touch_only["state"])
        self.assertEqual(2, touch_only["evidence"]["remaining_confirm_bars"])
        self.assertEqual("WAIT", one_following["state"])
        self.assertEqual(1, one_following["evidence"]["remaining_confirm_bars"])

    def test_confirmation_freezes_touch_anchor_and_bucket(self):
        bars, spot_record = self.fixture(extra_bars=1)
        confirmed = self.evaluate(bars[:-1], spot_record)
        next_hour = self.evaluate(bars, spot_record)

        self.assertEqual("YES", confirmed["state"])
        self.assertEqual("YES", next_hour["state"])
        self.assertEqual(101.0, confirmed["evidence"]["anchor"])
        self.assertEqual(
            confirmed["evidence"]["confirmed_bucket"],
            next_hour["evidence"]["confirmed_bucket"],
        )
        self.assertLessEqual(confirmed["evidence"]["price_progress_pct"], 0.15)
        self.assertGreaterEqual(confirmed["evidence"]["futures_volume_ratio"], 1.3)

    def test_two_closed_breakout_hours_invalidate_stall(self):
        bars, spot_record = self.fixture(extra_bars=2)
        for index in (-2, -1):
            bars[index].update({"high": 102.2, "low": 101.8, "close": 102.0})
        result = self.evaluate(bars, spot_record)

        self.assertNotEqual("YES", result["state"])

    def test_missing_or_misaligned_hourly_spot_flow_is_data_insufficient(self):
        bars, spot_record = self.fixture()
        spot_record["value"]["hourly_flow"] = spot_record["value"]["hourly_flow"][-7:]
        result = self.evaluate(bars, spot_record)

        self.assertEqual("NA", result["state"])
        self.assertEqual("spot_hourly_flow_unavailable", result["reason"])

    def test_negative_post_touch_flow_does_not_claim_stall(self):
        bars, spot_record = self.fixture()
        for item in spot_record["value"]["hourly_flow"][-3:]:
            item["net_quote"] = -item["quote_volume"] * 0.10
        result = self.evaluate(bars, spot_record)

        self.assertEqual("NO", result["state"])

    def test_attached_23_to_02_replay_confirms_once_at_01(self):
        hour_ms = 3600 * 1000
        touch_index = 37
        touch_open_time = 1786280400000
        start_open_time = touch_open_time - touch_index * hour_ms
        bars = synthetic_bars(41)
        for index, bar in enumerate(bars):
            bar.update({
                "open_time": start_open_time + index * hour_ms,
                "close_time": start_open_time + (index + 1) * hour_ms - 1,
                "open": 64950.0,
                "high": 65163.0,
                "low": 64800.0,
                "close": 64950.0,
                "quote_volume": 70000000.0,
            })
        attached_bars = {
            36: (64921.7, 64928.5, 64884.3, 64928.5, 53763560.3034),
            37: (64928.4, 65245.5, 64928.4, 65194.9, 373725274.3270),
            38: (65195.0, 65279.9, 65118.4, 65237.3, 206624764.9155),
            39: (65237.3, 65240.9, 65177.0, 65207.7, 124416381.1039),
            40: (65207.8, 65243.1, 65147.2, 65204.6, 120889108.6343),
        }
        for index, values in attached_bars.items():
            bars[index].update(dict(zip(
                ("open", "high", "low", "close", "quote_volume"),
                values,
            )))

        spot_rows = (
            (1786265999999, 64824.16, 21343277.3953, 3284306.9568),
            (1786269599999, 64877.85, 9673240.7256, 2557936.6701),
            (1786273199999, 64959.86, 16204333.5397, 1367044.1979),
            (1786276799999, 64950.00, 9797293.4002, -1039390.0868),
            (1786280399999, 64960.11, 10837664.1917, 318937.9698),
            (1786283999999, 65219.78, 26510401.8542, 5087004.3957),
            (1786287599999, 65260.01, 18311637.3796, -906953.9484),
            (1786291199999, 65228.68, 23760272.5566, 1482537.0721),
            (1786294799999, 65232.14, 18880664.3197, -4645105.5019),
        )
        spot_record = {
            "status": "ok",
            "value": {
                "hourly_flow": [
                    {
                        "close_time": close_time,
                        "close": close,
                        "quote_volume": quote_volume,
                        "net_quote": net_quote,
                    }
                    for close_time, close, quote_volume, net_quote in spot_rows
                ],
            },
        }

        at_23 = self.evaluate(bars[:38], spot_record)
        at_00 = self.evaluate(bars[:39], spot_record)
        at_01 = self.evaluate(bars[:40], spot_record)
        at_02 = self.evaluate(bars, spot_record)

        self.assertEqual(
            ("WAIT", 2),
            (at_23["state"], at_23["evidence"]["remaining_confirm_bars"]),
        )
        self.assertEqual(
            ("WAIT", 1),
            (at_00["state"], at_00["evidence"]["remaining_confirm_bars"]),
        )
        self.assertEqual("YES", at_01["state"])
        self.assertEqual("YES", at_02["state"])
        self.assertEqual(65163.0, at_01["evidence"]["anchor"])
        self.assertAlmostEqual(7.53, at_01["evidence"]["spot_cvd_4h_pct"], places=2)
        self.assertAlmostEqual(39.29, at_01["evidence"]["spot_volume_change_4h_pct"], places=2)
        self.assertEqual(
            at_01["evidence"]["confirmed_bucket"],
            at_02["evidence"]["confirmed_bucket"],
        )


class SpotHourlyFlowReceiptTest(unittest.TestCase):
    def test_fetch_keeps_bounded_hourly_taker_flow_receipt(self):
        hour_ms = 3600 * 1000
        raw = []
        for index in range(49):
            raw.append([
                index * hour_ms,
                "100",
                "101",
                "99",
                "100.5",
                "1",
                (index + 1) * hour_ms - 1,
                "100",
                10,
                "0.6",
                "60",
                "0",
            ])

        with (
            patch.object(bot, "binance_spot_get", return_value=raw),
            patch.object(bot.time, "time", return_value=50 * 3600),
        ):
            record = bot.fetch_spot_cvd_record()

        hourly_flow = record["value"]["hourly_flow"]
        self.assertEqual(49, len(hourly_flow))
        self.assertEqual(100.5, hourly_flow[-1]["close"])
        self.assertEqual(20.0, hourly_flow[-1]["net_quote"])


class MarketPatternDataQualityTest(unittest.TestCase):
    def test_oi_change_uses_contract_quantity_not_notional(self):
        bars = synthetic_bars(9)
        for index in range(5, 9):
            step = index - 4
            bars[index]["oi"] = 1000.0 - step * 2.5
            bars[index]["oi_value"] = 100000.0 - step * 2500.0
        delta = bot.calc_delta_4h_from_bars(bars)
        self.assertAlmostEqual(-1.0, delta["oi_4h_pct"], places=6)
        self.assertAlmostEqual(-10.0, delta["oi_notional_4h_pct"], places=6)

    def test_notional_value_rising_cannot_reverse_contract_quantity_signal(self):
        bars = synthetic_bars(9)
        for index in range(5, 9):
            step = index - 4
            bars[index]["oi"] = 1000.0 - step * 3.0
            bars[index]["oi_value"] = 100000.0 + step * 2500.0
        delta = bot.calc_delta_4h_from_bars(bars)
        self.assertLess(delta["oi_4h_pct"], -1.0)
        self.assertGreater(delta["oi_notional_4h_pct"], 0.0)

        context = market_pattern_context(-0.8, delta["oi_4h_pct"])
        down_with_fewer_contracts = bot.evaluate_snapshot_market_pattern(
            market_pattern_definition("deleveraging_down"),
            context,
        )
        down_with_more_contracts = bot.evaluate_snapshot_market_pattern(
            market_pattern_definition("new_short_pressure"),
            context,
        )
        self.assertEqual("YES", down_with_fewer_contracts["state"])
        self.assertEqual("NO", down_with_more_contracts["state"])

    def test_stale_or_discontinuous_signal_bars_are_unusable(self):
        # 去掉工厂的 epoch 零起点，先证明同一组九根数据在正常状态下可用。
        bars = synthetic_bars(10)[1:]
        latest_close = bars[-1]["close_time"]
        fresh = bot.build_signal_data_quality(bars, latest_close + 1000)
        self.assertTrue(fresh["usable"])
        self.assertEqual([], fresh["reasons"])
        stale = bot.build_signal_data_quality(bars, latest_close + 3 * 3600 * 1000)
        self.assertFalse(stale["usable"])
        self.assertEqual(["latest_bar_stale"], stale["reasons"])
        broken = copy.deepcopy(bars)
        broken[-1]["open_time"] += 3600 * 1000
        broken[-1]["close_time"] += 3600 * 1000
        discontinuous = bot.build_signal_data_quality(broken, broken[-1]["close_time"] + 1000)
        self.assertFalse(discontinuous["usable"])
        self.assertEqual(["recent_bars_not_contiguous"], discontinuous["reasons"])

    def test_unusable_signal_reason_is_translated_exactly(self):
        config = bot.load_status_rules_config()["market_patterns"]
        result = bot.evaluate_structural_market_patterns(
            [],
            {"usable": False, "reasons": ["no_closed_bars", "oi_not_aligned"]},
            config["patterns"],
            config,
        )["support_sweep"]
        self.assertEqual(
            "缺少已经结束的完整小时数据；价格和未平仓合约数据的统计时间不一致",
            bot.market_pattern_reason_text(result),
        )

    def test_rolling_range_waits_for_three_consecutive_hours(self):
        definition = market_pattern_definition("low_volume_range")
        context = market_pattern_context(0.2, 0.3, futures_volume=-30.0, spot_volume=-40.0)
        raw = bot.evaluate_snapshot_market_pattern(definition, context)
        history = [
            {"bucket": 98, "raw_hits": {"low_volume_range": True}},
            {"bucket": 99, "raw_hits": {"low_volume_range": True}},
        ]
        result = bot.apply_rolling_market_pattern(
            raw,
            definition,
            history,
            100,
            bot.load_status_rules_config()["market_patterns"],
        )
        self.assertEqual("YES", result["state"])
        self.assertEqual(3, result["consecutive_hits"])

    def test_rolling_wait_clears_confirmation_bias(self):
        definition = market_pattern_definition("low_volume_range")
        context = market_pattern_context(0.2, 0.3, futures_volume=-30.0, spot_volume=-40.0)
        raw = bot.evaluate_snapshot_market_pattern(definition, context)
        self.assertEqual("YES", raw["state"])
        self.assertEqual("neutral", raw["surface_bias"])

        result = bot.apply_rolling_market_pattern(
            raw,
            definition,
            [],
            100,
            bot.load_status_rules_config()["market_patterns"],
        )
        self.assertEqual("WAIT", result["state"])
        self.assertIsNone(result["surface_bias"])


class MarketPatternOutputContractTest(unittest.TestCase):
    def build_payload(self, states=None, evidence=None, reasons=None):
        config = bot.load_status_rules_config()["market_patterns"]
        states = states or {}
        evidence = evidence or {}
        reasons = reasons or {}
        return {
            "market_patterns": {
                "as_of_ms": 100 * 3600 * 1000,
                "data_quality": {"bucket": 100},
                "items": [
                    bot.market_pattern_result(
                        item,
                        states.get(item["key"], "NO"),
                        evidence=evidence.get(item["key"]),
                        reason=reasons.get(item["key"]),
                    )
                    for item in config["patterns"]
                ]
            }
        }

    def test_internal_contract_keeps_eleven_fixed_keys(self):
        config = bot.load_status_rules_config()["market_patterns"]
        self.assertEqual(
            [
                "deleveraging_down",
                "new_short_pressure",
                "leveraged_long_rise",
                "short_covering_rise",
                "spot_led_rise",
                "spot_active_sell",
                "support_liquidity_sweep",
                "resistance_false_breakout",
                "resistance_volume_stall",
                "volume_breakout",
                "low_volume_range",
            ],
            [item["key"] for item in config["patterns"]],
        )
        self.assertEqual(
            [
                "negative",
                "negative",
                "positive",
                "positive",
                "positive",
                "negative",
                "positive",
                "negative",
                "neutral",
                "positive",
                "neutral",
            ],
            [item["surface_bias_on_confirmation"] for item in config["patterns"]],
        )
        result = bot.market_pattern_result(config["patterns"][0], "NO")
        self.assertTrue(result["required_inputs"])
        self.assertTrue(result["criteria"])
        self.assertIsNone(result["surface_bias"])
        self.assertFalse(result["is_forecast"])
        self.assertNotIn("legacy_label", result)

    def test_formatter_shows_plain_observations_and_surface_bias(self):
        payload = self.build_payload(
            states={
                "deleveraging_down": "YES",
                "spot_active_sell": "YES",
                "support_liquidity_sweep": "YES",
            },
            evidence={
                "deleveraging_down": {
                    "price.change_4h_pct": -0.8,
                    "derivatives.oi_change_4h_pct": -1.16,
                },
                "spot_active_sell": {
                    "price.change_4h_pct": -0.8,
                    "spot.cvd_4h_pct": -4.55,
                    "spot.volume_change_4h_pct": 271.81,
                },
                "support_liquidity_sweep": {
                    "anchor": 64005.6,
                    "trigger_low": 63843.1,
                    "confirmed_bucket": 99,
                    "confirmed_close": 64096.7,
                },
            },
        )
        lines = bot.format_market_patterns_block(payload)
        text = "\n".join(lines)
        self.assertNotIn("只使用已经结束的完整小时", text)
        self.assertNotIn("数据只来自 Binance 的 BTC 合约与现货", text)
        self.assertIn("价格下跌，未平仓合约量明显减少｜🔴 表面偏利空", text)
        self.assertIn("Binance 现货主动卖出明显多于买入", text)
        self.assertIn("价格曾跌破近期低位，随后重新回到该价位上方｜🟢 发生时表面偏利好", text)
        self.assertNotIn("不代表接下来一定上涨或下跌", text)

    def test_resistance_stall_reports_fact_without_claiming_sell_absorption(self):
        evidence = {
            "anchor": 65163.0,
            "confirmed_bucket": 99,
            "futures_volume_ratio": 2.66,
            "spot_cvd_4h_pct": 7.53,
            "price_progress_pct": 0.02,
        }
        payload = self.build_payload(
            states={"resistance_volume_stall": "YES"},
            evidence={"resistance_volume_stall": evidence},
        )
        text = "\n".join(bot.format_market_patterns_block(payload))
        short_text = "\n".join(bot.format_plain_short_patterns_block(payload))

        self.assertIn("阻力附近成交明显增加，但价格推进不足", text)
        self.assertIn("表面方向不明", text)
        self.assertIn("阻力附近成交明显增加，但价格推进不足", short_text)
        self.assertNotIn("卖单吸收", text + short_text)

    def test_resistance_stall_wait_text_keeps_observation_boundary(self):
        payload = self.build_payload(
            states={"resistance_volume_stall": "WAIT"},
            evidence={
                "resistance_volume_stall": {
                    "anchor": 65163.0,
                    "remaining_confirm_bars": 1,
                }
            },
        )
        text = "\n".join(bot.format_plain_short_patterns_block(payload))

        self.assertIn("阻力附近放量但推进不足正在确认", text)
        self.assertIn("还需1个完整小时", text)
        self.assertNotIn("卖压已确认", text)

    def test_formatter_does_not_emit_legacy_jargon(self):
        config = bot.load_status_rules_config()["market_patterns"]
        text = "\n".join(
            "\n".join(
                bot.format_market_patterns_block(
                    self.build_payload(states={item["key"]: "YES"})
                )
            )
            for item in config["patterns"]
        )
        for jargon in (
            "去杠杆下跌",
            "新增空头下压",
            "杠杆追多上涨",
            "空头回补上涨",
            "现货主导上涨",
            "支撑流动性清扫",
            "压力位假突破",
            "合约 OI",
            "Spot CVD",
            "闭合K线",
            "1H K线",
            "固定组合阈值",
            "前序支撑",
            "前序压力",
            "期现货缩量",
            "连续站稳",
        ):
            self.assertNotIn(jargon, text)

    def test_wait_state_explains_remaining_confirmation_bars(self):
        payload = self.build_payload(
            states={"support_liquidity_sweep": "WAIT"},
            evidence={
                "support_liquidity_sweep": {
                    "anchor": 64000.0,
                    "trigger_low": 63800.0,
                    "remaining_confirm_bars": 2,
                }
            },
        )
        text = "\n".join(bot.format_market_patterns_block(payload))
        self.assertIn("价格已经跌破近期低位 64,000", text)
        self.assertIn("还可观察2个完整小时", text)
        self.assertIn("当前偏弱，最终形态未确认", text)
        self.assertNotIn("发生时表面偏利好", text)
        self.assertNotIn("随后重新回到该价位上方", text)

    def test_unmatched_spot_combo_is_not_promoted_to_a_shape(self):
        evidence = {
            "price.change_4h_pct": -0.41,
            "spot.cvd_4h_pct": -7.24,
            "spot.volume_change_4h_pct": 116.34,
        }
        payload = self.build_payload(
            evidence={
                "spot_led_rise": {
                    **evidence,
                    "derivatives.oi_change_4h_pct": 0.21,
                },
                "spot_active_sell": evidence,
            }
        )
        text = "\n".join(bot.format_market_patterns_block(payload))
        self.assertIn("所有可判断的常见情况均未出现", text)
        self.assertNotIn("已观察到：", text)
        self.assertNotIn("固定组合阈值", text)
        self.assertNotIn("Spot CVD", text)

    def test_na_state_names_missing_inputs_instead_of_treating_as_no(self):
        missing_reason = "missing_or_unaligned:price.change_4h_pct,derivatives.oi_change_4h_pct"
        states = {
            key: "NA"
            for key in (
                "deleveraging_down",
                "new_short_pressure",
                "leveraged_long_rise",
                "short_covering_rise",
            )
        }
        reasons = {key: missing_reason for key in states}
        text = "\n".join(bot.format_market_patterns_block(self.build_payload(states=states, reasons=reasons)))
        self.assertIn("数据不足：", text)
        self.assertIn("价格变化、未平仓合约量变化缺失或统计时间不一致", text)
        self.assertIn("本次暂不判断", text)
        self.assertNotIn("表面偏利好", text)
        self.assertNotIn("表面偏利空", text)

    def test_direction_label_is_effective_only_after_confirmation(self):
        definition = market_pattern_definition("deleveraging_down")
        for state in ("NO", "WAIT", "NA"):
            result = bot.market_pattern_result(definition, state)
            self.assertIsNone(result["surface_bias"])
        result = bot.market_pattern_result(definition, "YES")
        self.assertEqual("negative", result["surface_bias"])
        self.assertEqual("🔴 表面偏利空", bot.market_pattern_bias_text(result))

    def test_missing_pattern_item_is_data_insufficient_not_no(self):
        payload = self.build_payload()
        payload["market_patterns"]["items"] = [
            item
            for item in payload["market_patterns"]["items"]
            if item["key"] != "low_volume_range"
        ]
        text = "\n".join(bot.format_market_patterns_block(payload))
        self.assertIn("持续平静状态：内部判断结果缺失", text)
        self.assertIn("本次暂不判断", text)

    def test_all_na_does_not_claim_that_shapes_are_absent(self):
        config = bot.load_status_rules_config()["market_patterns"]
        states = {item["key"]: "NA" for item in config["patterns"]}
        reasons = {item["key"]: "condition_unknown" for item in config["patterns"]}
        text = "\n".join(
            bot.format_market_patterns_block(
                self.build_payload(states=states, reasons=reasons)
            )
        )
        self.assertIn("数据不足：", text)
        self.assertNotIn("已观察到：", text)
        self.assertNotIn("未出现", text)
        self.assertNotIn("均未出现", text)

    def test_all_no_has_one_absence_conclusion(self):
        text = "\n".join(bot.format_market_patterns_block(self.build_payload()))
        self.assertEqual(1, text.count("所有可判断的常见情况均未出现"))
        self.assertNotIn("其余可判断的常见情况未出现", text)

    def test_current_config_is_static_truth_for_old_items(self):
        payload = self.build_payload(states={"deleveraging_down": "YES"})
        item = payload["market_patterns"]["items"][0]
        item.update({
            "label": "旧版错误文案",
            "category": "spot_flow",
            "surface_bias": "positive",
            "surface_bias_on_confirmation": "positive",
            "surface_bias_horizon": "event_time",
        })
        text = "\n".join(bot.format_market_patterns_block(payload))
        self.assertIn("价格下跌，未平仓合约量明显减少｜🔴 表面偏利空", text)
        self.assertNotIn("旧版错误文案", text)

    def test_duplicate_item_is_data_insufficient(self):
        payload = self.build_payload(states={"deleveraging_down": "YES"})
        payload["market_patterns"]["items"].append(
            dict(payload["market_patterns"]["items"][0])
        )
        text = "\n".join(bot.format_market_patterns_block(payload))
        self.assertIn("价格和未平仓合约量：内部判断结果重复", text)
        self.assertNotIn("价格下跌，未平仓合约量明显减少｜", text)

    def test_unknown_state_is_data_insufficient(self):
        payload = self.build_payload()
        payload["market_patterns"]["items"][0]["state"] = "BROKEN"
        text = "\n".join(bot.format_market_patterns_block(payload))
        self.assertIn("内部判断状态异常", text)
        self.assertNotIn("价格下跌，未平仓合约量明显减少｜", text)

    def test_exclusive_spot_conflict_does_not_show_two_directions(self):
        payload = self.build_payload(
            states={"spot_led_rise": "YES", "spot_active_sell": "YES"}
        )
        text = "\n".join(bot.format_market_patterns_block(payload))
        self.assertIn("现货主动买卖出现互相冲突的结果，本次不判断", text)
        self.assertNotIn("Binance 现货主动买入明显多于卖出｜", text)
        self.assertNotIn("Binance 现货主动卖出明显多于买入｜", text)

    def test_spot_buy_rule_discloses_combined_source_and_oi_condition(self):
        definition = market_pattern_definition("spot_led_rise")
        self.assertIn("perpetual price/OI", definition["source_scope"])
        self.assertIn("Spot BTCUSDT", definition["source_scope"])
        self.assertIn("未平仓合约量没有明显增加", definition["label"])

    def test_compact_funds_separates_oi_value_from_contract_quantity_change(self):
        payload = {
            "market": {"oi_usd": 6.54e9},
            "market_24h_delta": {"metrics": {"vol24": -25.3}},
            "delta4h": {"oi_4h_pct": -1.2, "vol_4h_pct": 87.5},
            "structure_state": {
                "checks": {
                    "oi_slow_confirmation": {
                        "delta_24h_pct": -4.9,
                        "delta_72h_pct": 3.9,
                    }
                }
            },
            "public_records": {
                "funding": {"value": {"funding_8h_pct": 0.008}},
                "basis": {"value": {"basis_pct": -0.032}},
                "spot_cvd": {
                    "value": {
                        "cvd_4h_pct": -7.24,
                        "cvd_24h_pct": 2.51,
                        "volume_4h_pct": 116.34,
                        "volume_24h_pct": -1.97,
                    }
                },
                "stablecoin": {
                    "value": {
                        "flow_24h_usd": 1_000_000_000,
                        "flow_7d_usd": -693_000_000,
                    }
                },
            },
        }
        lines = bot.format_compact_funds_block(payload)
        text = "\n".join(lines)
        self.assertIn("总价值 $6.54B", text)
        self.assertIn("数量变化：过去4小时减少1.20% | 24小时减少4.90% | 72小时增加3.90%", text)
        self.assertIn("看涨方向每8小时约支付0.008%给看跌方向", text)
        self.assertIn("合约价格比现货低0.032%", text)
        self.assertIn("过去4小时主动卖出占优7.24%", text)
        self.assertIn("24小时主动买入占优2.51%", text)
        self.assertIn("24小时净流入 1B", text)
        self.assertIn("7天净流出 693M", text)
        self.assertIn("合约成交额变化：", text)
        self.assertIn("现货成交额变化：", text)
        self.assertNotIn("OI 总量", text)
        self.assertNotIn("Funding", text)
        self.assertNotIn("Basis", text)
        self.assertNotIn("Spot CVD", text)


class MarketPatternConclusionTest(unittest.TestCase):
    def build_payload(self, states=None, evidence=None, reasons=None, bucket=100, trend=None):
        config = bot.load_status_rules_config()["market_patterns"]
        states = states or {}
        evidence = evidence or {}
        reasons = reasons or {}
        if trend is None:
            trend = {
                "status": "range_observe",
                "position_state": "mid_range",
                "support_state": "holding",
                "pressure_state": "not_broken",
                "data_sufficient": True,
            }
        return {
            "market_patterns": {
                "revision": config["revision"],
                "as_of_ms": int(bucket) * 3600 * 1000,
                "data_quality": {"bucket": int(bucket)},
                "items": [
                    bot.market_pattern_result(
                        item,
                        states.get(item["key"], "NO"),
                        evidence=evidence.get(item["key"]),
                        reason=reasons.get(item["key"]),
                    )
                    for item in config["patterns"]
                ],
            },
            "rule_state": {"levels": {"price_trend": trend}},
        }

    def conclusion(self, **kwargs):
        return bot.build_market_pattern_conclusion(self.build_payload(**kwargs))

    def test_contract_and_rules_revision(self):
        conclusion = self.conclusion()
        self.assertEqual(5, bot.load_status_rules_config()["market_patterns"]["revision"])
        self.assertEqual("btc_market_pattern_conclusion", conclusion["schema"])
        self.assertEqual(1, conclusion["revision"])
        self.assertEqual("rules", conclusion["source"])
        self.assertNotIn("global_bias", conclusion)

    def test_all_eleven_patterns_keep_fixed_time_domain_and_bias(self):
        expected = {
            "deleveraging_down": ("current_window", "negative"),
            "new_short_pressure": ("current_window", "negative"),
            "leveraged_long_rise": ("current_window", "positive"),
            "short_covering_rise": ("current_window", "positive"),
            "spot_led_rise": ("current_window", "positive"),
            "spot_active_sell": ("current_window", "negative"),
            "support_liquidity_sweep": ("latest_event", "positive"),
            "resistance_false_breakout": ("latest_event", "negative"),
            "volume_breakout": ("latest_event", "positive"),
            "low_volume_range": ("current_window", "neutral"),
        }
        for key, (domain, bias) in expected.items():
            with self.subTest(key=key):
                evidence = {key: {"confirmed_bucket": 99}} if domain == "latest_event" else {}
                result = self.conclusion(states={key: "YES"}, evidence=evidence)
                self.assertEqual(bias, result[domain]["bias"])
                self.assertIn(key, result[domain]["confirmed_keys"])

    def test_negative_window_positive_event_is_offsetting_near_support(self):
        payload = self.build_payload(
            states={
                "deleveraging_down": "YES",
                "spot_active_sell": "YES",
                "support_liquidity_sweep": "YES",
            },
            evidence={"support_liquidity_sweep": {"confirmed_bucket": 99}},
            trend={
                "status": "range_observe",
                "position_state": "near_lower",
                "support_state": "near_support",
                "pressure_state": "not_broken",
                "data_sufficient": True,
            },
        )
        result = bot.build_market_pattern_conclusion(payload)
        self.assertEqual("negative", result["current_window"]["bias"])
        self.assertEqual("positive", result["latest_event"]["bias"])
        self.assertEqual("offsetting", result["relation"])
        self.assertEqual("near_support", result["current_structure"]["conclusion_key"])
        self.assertIn("过去4小时已确认形态表面偏弱", result["text"])
        self.assertIn("与当前窗口已确认形态方向不同", result["text"])
        self.assertIn("窗口内并非单边延续", result["text"])
        self.assertIn("当前接近短线支撑，跌破尚未确认", result["text"])
        for forbidden in ("有人承接", "导致", "接下来一定"):
            self.assertNotIn(forbidden, result["text"])

    def test_historical_support_recovery_does_not_override_current_breakdown(self):
        result = self.conclusion(
            states={"spot_active_sell": "YES", "support_liquidity_sweep": "YES"},
            evidence={"support_liquidity_sweep": {"confirmed_bucket": 99}},
            trend={
                "status": "breakdown_confirmed",
                "position_state": "near_lower",
                "support_state": "breakdown_confirmed",
                "pressure_state": "not_broken",
                "data_sufficient": True,
            },
        )
        self.assertEqual("breakdown_confirmed", result["current_structure"]["conclusion_key"])
        self.assertIn("期间曾发生", result["text"])
        self.assertIn("当前仍在原支撑下方", result["text"])
        self.assertNotIn("跌破尚未确认", result["text"])

    def test_positive_window_negative_event_keeps_current_trend_confirmation(self):
        result = self.conclusion(
            states={
                "leveraged_long_rise": "YES",
                "spot_led_rise": "YES",
                "resistance_false_breakout": "YES",
            },
            evidence={"resistance_false_breakout": {"confirmed_bucket": 99}},
            trend={
                "status": "trend_confirmed",
                "position_state": "near_upper",
                "support_state": "holding",
                "pressure_state": "breakout_confirmed",
                "data_sufficient": True,
            },
        )
        self.assertEqual("positive", result["current_window"]["bias"])
        self.assertEqual("negative", result["latest_event"]["bias"])
        self.assertEqual("offsetting", result["relation"])
        self.assertIn("与当前窗口已确认形态方向不同", result["text"])
        self.assertIn("当前短线压力突破已确认", result["text"])

    def test_latest_event_uses_confirmed_bucket_and_keeps_older_event(self):
        result = self.conclusion(
            states={
                "support_liquidity_sweep": "YES",
                "resistance_false_breakout": "YES",
            },
            evidence={
                "support_liquidity_sweep": {"confirmed_bucket": 98},
                "resistance_false_breakout": {"confirmed_bucket": 99},
            },
        )
        self.assertEqual("negative", result["latest_event"]["bias"])
        self.assertEqual(99, result["latest_event"]["confirmed_bucket"])
        self.assertEqual(["resistance_false_breakout"], result["latest_event"]["confirmed_keys"])
        self.assertEqual(["support_liquidity_sweep"], result["latest_event"]["older_confirmed_keys"])

    def test_same_bucket_opposite_events_are_mixed(self):
        result = self.conclusion(
            states={
                "support_liquidity_sweep": "YES",
                "resistance_false_breakout": "YES",
            },
            evidence={
                "support_liquidity_sweep": {"confirmed_bucket": 99},
                "resistance_false_breakout": {"confirmed_bucket": 99},
            },
        )
        self.assertEqual("mixed", result["latest_event"]["bias"])
        self.assertEqual("mixed_events", result["relation"])
        self.assertIn("关键价位表现反复", result["text"])

    def test_missing_or_expired_event_time_is_partial_and_not_latest(self):
        for evidence in ({}, {"confirmed_bucket": 96}):
            with self.subTest(evidence=evidence):
                result = self.conclusion(
                    states={"spot_active_sell": "YES", "support_liquidity_sweep": "YES"},
                    evidence={"support_liquidity_sweep": evidence},
                )
                self.assertEqual([], result["latest_event"]["confirmed_keys"])
                self.assertIn("support_liquidity_sweep", result["latest_event"]["unavailable_keys"])
                self.assertIn("support_liquidity_sweep", result["latest_event"]["invalid_confirmed_keys"])
                self.assertEqual("PARTIAL", result["latest_event"]["integrity"])
                self.assertEqual("partial", result["data_quality"])
                self.assertIn("近期结构事件数据不足", result["text"])

    def test_wait_na_and_no_never_contribute_direction(self):
        result = self.conclusion(
            states={
                "deleveraging_down": "NA",
                "support_liquidity_sweep": "WAIT",
                "low_volume_range": "WAIT",
            }
        )
        self.assertEqual("none", result["current_window"]["bias"])
        self.assertEqual("none", result["latest_event"]["bias"])
        self.assertIn("low_volume_range", result["current_window"]["waiting_keys"])
        self.assertIn("support_liquidity_sweep", result["latest_event"]["waiting_keys"])

    def test_snapshot_wait_is_downgraded_to_unavailable(self):
        result = self.conclusion(states={"deleveraging_down": "WAIT"})
        self.assertEqual("NA", result["current_window"]["occurrence"])
        self.assertIn("deleveraging_down", result["current_window"]["unavailable_keys"])
        self.assertNotIn("deleveraging_down", result["current_window"]["waiting_keys"])

    def test_exclusive_cross_direction_and_neutral_mixes_fail_closed(self):
        cases = [
            {"spot_led_rise": "YES", "spot_active_sell": "YES"},
            {"deleveraging_down": "YES", "spot_led_rise": "YES"},
            {"spot_active_sell": "YES", "low_volume_range": "YES"},
        ]
        for states in cases:
            with self.subTest(states=states):
                result = self.conclusion(states=states)
                self.assertEqual("CONFLICT", result["current_window"]["integrity"])
                self.assertEqual("conflict", result["relation"])
                self.assertEqual("data_conflict", result["conclusion_key"])
                self.assertNotIn("整体偏强", result["text"])
                self.assertNotIn("整体偏弱", result["text"])

    def test_all_na_is_insufficient_and_all_no_does_not_infer_direction(self):
        keys = [item["key"] for item in bot.load_status_rules_config()["market_patterns"]["patterns"]]
        all_na = self.conclusion(states={key: "NA" for key in keys})
        self.assertEqual("data_insufficient", all_na["conclusion_key"])
        self.assertEqual("none", all_na["current_window"]["bias"])
        self.assertEqual("none", all_na["latest_event"]["bias"])
        self.assertNotIn("整体偏强", all_na["text"])
        self.assertNotIn("整体偏弱", all_na["text"])

        all_no = self.conclusion()
        self.assertEqual("none", all_no["relation"])
        self.assertEqual("NO", all_no["current_window"]["occurrence"])
        self.assertEqual("NO", all_no["latest_event"]["occurrence"])
        self.assertIn("当前窗口没有可用于组合的已确认形态", all_no["text"])

    def test_current_structure_mapping_and_stale_state_guard(self):
        expected = {
            "breakdown_confirmed": ("negative", "confirmed"),
            "breakdown_watch": ("negative", "watch"),
            "trend_confirmed": ("positive", "confirmed"),
            "pullback_watch": ("positive", "watch"),
            "breakout_hold": ("positive", "watch"),
            "breakout_attempt": ("positive", "watch"),
        }
        for status, (phase, confirmation) in expected.items():
            with self.subTest(status=status):
                result = self.conclusion(
                    trend={
                        "status": status,
                        "position_state": "mid_range",
                        "support_state": "holding",
                        "pressure_state": "not_broken",
                        "data_sufficient": True,
                    }
                )
                self.assertEqual(phase, result["current_structure"]["phase"])
                self.assertEqual(confirmation, result["current_structure"]["confirmation"])

        stale = self.conclusion(
            trend={
                "status": "breakdown_confirmed",
                "position_state": "near_lower",
                "support_state": "breakdown_confirmed",
                "pressure_state": "not_broken",
                "data_sufficient": False,
            }
        )
        self.assertEqual("insufficient", stale["current_structure"]["phase"])
        self.assertNotIn("当前短线支撑已确认跌破", stale["text"])
        self.assertNotIn("部分形态数据不足", stale["text"])

        unknown = self.conclusion(
            trend={
                "status": "BROKEN",
                "position_state": "near_lower",
                "support_state": "near_support",
                "pressure_state": "not_broken",
                "data_sufficient": True,
            }
        )
        self.assertEqual("insufficient", unknown["current_structure"]["phase"])
        self.assertNotIn("当前接近短线支撑", unknown["text"])

    def test_formatter_keeps_facts_before_conclusion(self):
        payload = self.build_payload(
            states={"spot_active_sell": "YES", "support_liquidity_sweep": "YES"},
            evidence={"support_liquidity_sweep": {"confirmed_bucket": 99}},
        )
        text = "\n".join(bot.format_market_patterns_block(payload))
        self.assertLess(text.index("已观察到："), text.index("综合结论："))
        self.assertIn("综合结论：", text)

    def test_formatter_hides_invalid_event_confirmation(self):
        payload = self.build_payload(
            states={"support_liquidity_sweep": "YES"},
            evidence={"support_liquidity_sweep": {"confirmed_bucket": 96}},
        )
        text = "\n".join(bot.format_market_patterns_block(payload))
        self.assertNotIn("价格曾跌破近期低位，随后重新回到该价位上方｜", text)
        self.assertIn("确认时间缺失或已过有效期", text)
        self.assertIn("近期结构事件数据不足", text)

    def test_formatter_hides_cross_category_direction_conflict(self):
        payload = self.build_payload(
            states={"deleveraging_down": "YES", "spot_led_rise": "YES"}
        )
        text = "\n".join(bot.format_market_patterns_block(payload))
        self.assertNotIn("价格下跌，未平仓合约量明显减少｜", text)
        self.assertNotIn("价格上涨，Binance 现货主动买入明显多于卖出｜", text)
        self.assertIn("当前窗口形态出现互相冲突的方向", text)
        self.assertIn("4H形态结果存在冲突", text)


class CanonicalRuntimeMigrationTest(unittest.TestCase):
    def test_legacy_state_keys_migrate_to_canonical_names(self):
        state = {
            "v4_public_snapshots": [{"ts": 1}],
            "v4_liquidation_ws": {"status": "connected"},
            "v5_macro_cache": {"updated_at": 1},
            "v5_market_structure_state": {"support_anchor": 64000},
            "report_v5": {"price": 64000},
        }

        migrated = bot.migrate_state_schema(state)

        self.assertEqual([{"ts": 1}], migrated["public_snapshots"])
        self.assertEqual({"status": "connected"}, migrated["liquidation_ws"])
        self.assertEqual({"updated_at": 1}, migrated["macro_cache"])
        self.assertEqual({"support_anchor": 64000}, migrated["market_structure_state"])
        self.assertEqual({"price": 64000}, migrated["report"])
        for legacy_key in (
            "v4_public_snapshots",
            "v4_liquidation_ws",
            "v5_macro_cache",
            "v5_market_structure_state",
            "report_v5",
        ):
            self.assertNotIn(legacy_key, migrated)

    def test_v2_price_trend_state_migrates_without_inventing_confirmation(self):
        state = {
            bot.PRICE_TREND_STATE_KEY: {
                "schema_version": 2,
                "status": "breakdown_confirmed",
                "support_anchor": 64811.0,
                "support_anchor_phase": "confirmed",
                "levels": {"short_support": 63755.0},
            }
        }

        migrated = bot.migrate_state_schema(state)[bot.PRICE_TREND_STATE_KEY]

        self.assertEqual(5, migrated["schema_version"])
        self.assertEqual("migrated_v2_anchor", migrated["support_anchor_source"])
        self.assertEqual(64811.0, migrated["support_anchor"])
        self.assertEqual(63755.0, migrated["rolling_short_support"])
        self.assertEqual([], migrated["support_confirmation_sources"])
        self.assertIsNone(migrated["support_breakdown_confirmed_bucket"])
        self.assertEqual("原支撑失守状态持续", migrated["status_text"])
        self.assertEqual("active_below_former_support", bot.trend_support_breakdown_phase(migrated))
        self.assertIsNone(migrated["support_test_anchor"])
        self.assertEqual("inactive", migrated["support_test_phase"])

        updated = bot.update_price_trend_state(
            trend_payload(
                300,
                63000.0,
                63755.0,
                rolling_support=63021.0,
                patterns={"spot_active_sell": {"state": "YES"}},
            ),
            state,
            {"oi_24h_pct": 1.0, "funding_8h_pct": 0.004},
            DOWN_SPOT,
            {},
        )
        self.assertEqual("breakdown_confirmed", updated["status"])
        self.assertEqual([], updated["support_confirmation_sources"])
        self.assertIsNone(updated["support_breakdown_confirmed_bucket"])
        self.assertEqual("active_below_former_support", bot.trend_support_breakdown_phase(updated))

    def test_v3_migration_preserves_anchor_without_inventing_support_test(self):
        state = {
            bot.PRICE_TREND_STATE_KEY: {
                "schema_version": 3,
                "status": "breakdown_watch",
                "support_anchor": 64811.0,
                "support_anchor_phase": "watching",
                "support_anchor_source": "previous_closed_support",
                "rolling_short_support": 63021.0,
                "support_confirmation_sources": [],
                "levels": {
                    "short_support": 64811.0,
                    "rolling_short_support": 63021.0,
                },
            }
        }

        migrated = bot.migrate_state_schema(state)[bot.PRICE_TREND_STATE_KEY]

        self.assertEqual(5, migrated["schema_version"])
        self.assertEqual(64811.0, migrated["support_anchor"])
        self.assertEqual("previous_closed_support", migrated["support_anchor_source"])
        self.assertEqual(63021.0, migrated["rolling_short_support"])
        self.assertIsNone(migrated["support_test_anchor"])
        self.assertIsNone(migrated["support_test_first_retest_bucket"])
        self.assertEqual("inactive", migrated["support_test_phase"])
        self.assertEqual("inactive", migrated["post_support_phase"])
        self.assertIsNone(migrated["support_breakdown_confirmed_bucket"])

    def test_v4_confirmed_state_migrates_as_existing_active_breakdown(self):
        state = {
            bot.PRICE_TREND_STATE_KEY: {
                "schema_version": 4,
                "status": "breakdown_confirmed",
                "support_state": "breakdown_confirmed",
                "support_anchor": 64657.0,
                "support_anchor_phase": "confirmed",
                "support_anchor_source": "previous_closed_support",
                "bucket": 500,
                "rolling_short_support": 63352.0,
                "levels": {
                    "short_support": 63352.0,
                    "rolling_short_support": 63352.0,
                },
            }
        }

        migrated = bot.migrate_state_schema(state)[bot.PRICE_TREND_STATE_KEY]

        self.assertEqual(5, migrated["schema_version"])
        self.assertIsNone(migrated["support_breakdown_confirmed_bucket"])
        self.assertEqual("原支撑失守状态持续", migrated["status_text"])
        self.assertEqual("原支撑失守持续", migrated["support_state_text"])
        self.assertEqual("active_below_former_support", bot.trend_support_breakdown_phase(migrated))


class CompactNumberFormattingTest(unittest.TestCase):
    def test_integer_trailing_zeros_are_preserved(self):
        self.assertEqual("250", bot.compact_number(250, 0))
        self.assertEqual("500", bot.compact_number(500, 0))
        self.assertEqual("+250M", bot.compact_money(250_000_000, 0))
        self.assertEqual("-240M", bot.compact_money(-240_000_000, 0))

    def test_decimal_trailing_zeros_are_trimmed_without_changing_value(self):
        self.assertEqual("+249.9M", bot.compact_money(249_900_000, 1))
        self.assertEqual("+1.25K", bot.compact_money(1_250, 2))
        self.assertEqual("+2.5B", bot.compact_money(2_500_000_000, 2))
        self.assertEqual("+1T", bot.compact_money(1_000_000_000_000, 2))
        self.assertEqual("0", bot.compact_money(0, 0))


class StablecoinCvdWordingTest(unittest.TestCase):
    def assert_wording(self, spot, expected):
        text = bot.plain_stable_signal_line(spot)
        self.assertEqual(expected, text)
        self.assertNotIn("短线资金持续流入", text)
        self.assertNotIn("导致", text)

    def test_sustained_supply_with_non_positive_cvd_is_only_potential_buying_power(self):
        for cvd in (-5.0, 0.0):
            with self.subTest(cvd=cvd):
                self.assert_wording(
                    {
                        "stablecoin_sustained_rule": True,
                        "cvd_4h_pct": cvd,
                    },
                    "⚠ 稳定币供给增加，但尚未转化为 BTC 现货买盘",
                )

    def test_sustained_supply_with_positive_or_missing_cvd_is_layered(self):
        self.assert_wording(
            {
                "stablecoin_sustained_rule": True,
                "cvd_4h_pct": 1.2,
            },
            "✅ 稳定币供给增加，BTC 4H 现货买盘同步转正",
        )
        self.assert_wording(
            {
                "stablecoin_sustained_rule": True,
                "cvd_4h_pct": None,
            },
            "⚠ 稳定币供给增加，是否转化为 BTC 买盘待确认",
        )

    def test_short_term_unconfirmed_and_missing_supply_are_explicit(self):
        self.assert_wording(
            {"stablecoin_short_term_rule": True},
            "⚠ 稳定币供给短期增加，7D 尚未确认",
        )
        self.assert_wording(
            {"stablecoin_24h_positive_rule": False},
            "❌ 稳定币供给增加未确认",
        )
        self.assert_wording(
            {},
            "⚪ 稳定币供给数据不足",
        )


class EtfFreshnessBoundaryTest(unittest.TestCase):
    @staticmethod
    def etf_record(**overrides):
        record = {
            "status": "ok",
            "source": "test",
            "value": {
                "flow_1d_usd": -240_000_000.0,
                "flow_5d_usd": -500_000_000.0,
                "flow_30d_usd": -2_000_000_000.0,
                "holding_7d_pct": 0.2,
                "source_date": {
                    "flow_1d": "2026-07-24",
                    "flow_30d": "2026-07-24",
                },
            },
        }
        record.update(overrides)
        return record

    @staticmethod
    def etf_payload(record):
        return {
            "market": {"price": 64000.0},
            "levels": {"support": 65000.0},
            "public_records": {
                "etf": record,
                "macro": {"status": "ok", "value": {}},
            },
            "public_snapshot": {"ts": 1000, "public": {}},
        }

    def test_fresh_etf_is_labeled_as_daily_background_only(self):
        record = self.etf_record()
        payload = self.etf_payload(record)
        state = bot.build_etf_rule_state(payload, {})

        self.assertTrue(state["eligible_for_rules"])
        self.assertTrue(state["flow_outflow_rule"])
        self.assertEqual("2026-07-24", state["source_date"])
        self.assertIn("最近交易日偏流出", bot.plain_etf_flow_line(state))
        self.assertIn("截至 2026-07-24", bot.plain_etf_flow_line(state))

        macro = bot.build_macro_rule_state(
            {
                "market": {"price": 64000.0},
                "levels": {"support": 65000.0},
                "public_records": {
                    "macro": {"value": {"us10y": 5.0}},
                },
            },
            state,
        )
        self.assertFalse(macro["rate_hike_rule"])
        self.assertEqual(1, macro["rate_hike_hit_count"])
        self.assertIsNone(macro["checks"]["btc_broke_with_etf_outflow"])

    def test_stale_etf_retains_raw_value_but_is_gated_everywhere(self):
        record = self.etf_record(
            source_stale=True,
            source_stale_reason="flow_1d:source_stale",
        )
        payload = self.etf_payload(record)
        etf_state = bot.build_etf_rule_state(payload, {})
        payload["rule_state"] = {"etf": etf_state}

        self.assertFalse(etf_state["eligible_for_rules"])
        self.assertEqual(-240_000_000.0, etf_state["raw_flow_1d_usd"])
        self.assertIsNone(etf_state["flow_1d_usd"])
        self.assertIsNone(etf_state["flow_outflow_rule"])
        self.assertIsNone(etf_state["mild_outflow_rule"])
        self.assertEqual(
            {
                "flow_1d_usd": None,
                "flow_5d_usd": None,
                "flow_30d_usd": None,
            },
            bot.build_status_context(payload)["etf"],
        )
        self.assertEqual(
            "数据不足",
            bot.build_status_layer(payload)["etf_funds"]["status"],
        )
        self.assertFalse(
            bot.build_signals({}, {}, {"etf": record}, {})["etf_outflow"]
        )
        self.assertIn("ETF 数据过期，仅作背景，不参与当前判断", bot.plain_etf_flow_line(etf_state))
        payload["rule_state"]["levels"] = {
            "price_trend": {
                "status": "range_observe",
                "position_state": "mid_range",
            }
        }
        self.assertEqual(
            "当前理解：短线仍在观察，等待价格和资金进一步确认。",
            bot.format_compact_current_view(payload),
        )

        raw_text = "\n".join(bot.format_compact_funds_block(payload))
        self.assertIn("ETF 1D -240M", raw_text)
        self.assertIn("截至 2026-07-24", raw_text)
        self.assertIn("ETF 数据过期，仅作背景，不参与当前判断", raw_text)

        event_state = {}
        event_payload = {
            "fear_greed": {},
            "seven_day_comparison": {},
            "rule_state": {"etf": etf_state},
            "public_records": {},
        }
        bot.append_success_log(event_state, "2026-07-28", event_payload, False)
        event = event_state["event_logs"][-1]
        self.assertFalse(event["etf_rule_eligible"])
        self.assertEqual("2026-07-24", event["etf_source_date"])
        self.assertEqual(
            "flow_1d:source_stale",
            event["etf_rule_ineligible_reason"],
        )

    def test_cached_fallback_is_not_rule_eligible(self):
        record = self.etf_record(stale=True, snapshot_used=True)
        state = bot.build_etf_rule_state(self.etf_payload(record), {})

        self.assertFalse(state["eligible_for_rules"])
        self.assertEqual("data_stale", state["status"])
        self.assertEqual("stale_snapshot", state["ineligible_reason"])
        self.assertIsNone(state["flow_outflow_rule"])

    def test_missing_etf_is_explicitly_insufficient(self):
        record = {"status": "unavailable", "value": {}, "reason": "no_snapshot"}
        state = bot.build_etf_rule_state(self.etf_payload(record), {})

        self.assertFalse(state["eligible_for_rules"])
        self.assertEqual("data_insufficient", state["status"])
        self.assertEqual("missing", state["ineligible_reason"])
        self.assertIsNone(state["flow_outflow_rule"])
        self.assertEqual("⚪ ETF 日级背景数据不足", bot.plain_etf_flow_line(state))


class StablecoinDataQualityTest(unittest.TestCase):
    def test_incomplete_history_below_coverage_floor_is_not_used_as_zero_flow(self):
        response = {
            "peggedAssets": [
                {
                    "pegType": "peggedUSD",
                    "circulating": {"peggedUSD": 980.0},
                    "circulatingPrevDay": {"peggedUSD": 900.0},
                    "circulatingPrevWeek": {"peggedUSD": 880.0},
                },
                {
                    "pegType": "peggedUSD",
                    "circulating": {"peggedUSD": 20.0},
                    "circulatingPrevDay": {},
                    "circulatingPrevWeek": {},
                },
            ]
        }

        with patch.object(bot, "public_get_json", return_value=response):
            record = bot.fetch_stablecoin_record()

        self.assertEqual("partial", record["status"])
        self.assertIsNone(record["value"]["flow_24h_usd"])
        self.assertIsNone(record["value"]["flow_7d_usd"])
        self.assertAlmostEqual(98.0, record["coverage"]["day_pct"])

    def test_small_positive_7d_flow_does_not_become_sustained_inflow(self):
        payload = {
            "public_records": {
                "spot_cvd": {"value": {"cvd_4h_pct": -4.55, "cvd_24h_pct": -4.71}},
                "stablecoin": {
                    "value": {
                        "total_usd": 309_000_000_000.0,
                        "flow_24h_usd": 282_000_000.0,
                        "flow_7d_usd": 26_000_000.0,
                    }
                },
            }
        }

        state = bot.build_spot_rule_state(payload)

        self.assertTrue(state["stablecoin_24h_positive_rule"])
        self.assertFalse(state["stablecoin_7d_positive_rule"])
        self.assertTrue(state["stablecoin_short_term_rule"])
        self.assertFalse(state["stablecoin_sustained_rule"])


class HyperliquidDualSourceTest(unittest.TestCase):
    HOUR_MS = 3600 * 1000

    @staticmethod
    def ctx(oi=1000.0, mark=64000.0, funding=0.00001, premium=-0.0002):
        return {
            "openInterest": str(oi),
            "markPx": str(mark),
            "oraclePx": str(mark + 10.0),
            "funding": str(funding),
            "premium": str(premium),
        }

    def ready_state(self, current_ms):
        state = {}
        last_close_ms = bot.latest_closed_hour_ms(current_ms)
        for offset in range(bot.HYPERLIQUID_PRIMARY_MIN_HOURS, -1, -1):
            close_ms = last_close_ms - offset * self.HOUR_MS
            bot.append_hyperliquid_minute_sample(
                state,
                self.ctx(oi=1000.0 + offset, mark=64000.0 + offset),
                received_at_ms=close_ms - 60 * 1000,
            )
        bot.append_hyperliquid_minute_sample(
            state,
            self.ctx(oi=1100.0, mark=64100.0),
            received_at_ms=current_ms - 1000,
        )
        return state

    def test_meta_response_uses_universe_index_not_fixed_position(self):
        payload = [
            {"universe": [{"name": "ETH"}, {"name": "BTC"}]},
            [self.ctx(oi=10.0), self.ctx(oi=321.5, mark=63000.0)],
        ]

        result = bot.hyperliquid_asset_ctx_from_meta_response(payload)

        self.assertEqual("321.5", result["openInterest"])
        with self.assertRaises(ValueError):
            bot.hyperliquid_asset_ctx_from_meta_response([{}, []])

    def test_ws_parser_accepts_only_target_asset_context(self):
        message = {
            "channel": "activeAssetCtx",
            "data": {"coin": "BTC", "ctx": self.ctx()},
        }

        self.assertEqual(self.ctx(), bot.hyperliquid_asset_ctx_from_ws_message(message))
        message["data"]["coin"] = "ETH"
        self.assertIsNone(bot.hyperliquid_asset_ctx_from_ws_message(message))

    def test_minute_samples_dedupe_and_alignment_is_not_after_close(self):
        state = {}
        close_ms = 100 * self.HOUR_MS - 1
        first_ms = close_ms - 50 * 1000
        replacement_ms = close_ms - 20 * 1000
        bot.append_hyperliquid_minute_sample(state, self.ctx(oi=1000.0), first_ms)
        bot.append_hyperliquid_minute_sample(state, self.ctx(oi=1001.0), replacement_ms)

        samples = state["hyperliquid_derivatives"]["minute_samples"]
        self.assertEqual(1, len(samples))
        aligned = bot.align_hyperliquid_sample_to_close(samples, close_ms)
        self.assertEqual(1001.0, aligned["open_interest_btc"])
        self.assertEqual(20 * 1000, aligned["oi_drift_ms"])
        after_close = bot.normalize_hyperliquid_ctx(self.ctx(), close_ms + 1)
        self.assertIsNone(bot.align_hyperliquid_sample_to_close([after_close], close_ms))
        too_old = bot.normalize_hyperliquid_ctx(self.ctx(), close_ms - 90 * 1000 - 1)
        self.assertIsNone(bot.align_hyperliquid_sample_to_close([too_old], close_ms))

    def test_73_contiguous_points_unlock_primary_and_keep_btc_notional_separate(self):
        current_ms = 1000 * self.HOUR_MS + 10 * 60 * 1000
        with patch.object(bot, "HYPERLIQUID_MODE", "primary"):
            state = self.ready_state(current_ms)
            receipt = bot.build_hyperliquid_oi_receipt(state, current_ms=current_ms)

        self.assertTrue(receipt["primary_ready"])
        self.assertTrue(receipt["ready_4h"])
        self.assertTrue(receipt["ready_24h"])
        self.assertEqual(73, receipt["continuous_points"])
        sample = receipt["aligned_samples"][-1]
        self.assertAlmostEqual(
            sample["open_interest_btc"] * sample["mark_px"],
            sample["open_interest_usd"],
        )

    def test_gap_out_of_order_and_stale_context_keep_primary_closed(self):
        current_ms = 1000 * self.HOUR_MS + 10 * 60 * 1000
        state = self.ready_state(current_ms)
        last_close_ms = bot.latest_closed_hour_ms(current_ms)
        missing_close_ms = last_close_ms - 3 * self.HOUR_MS
        samples = state["hyperliquid_derivatives"]["minute_samples"]
        state["hyperliquid_derivatives"]["minute_samples"] = list(reversed([
            item for item in samples
            if not 0 <= missing_close_ms - int(item["received_at_ms"]) <= 90 * 1000
        ]))

        receipt = bot.build_hyperliquid_oi_receipt(state, current_ms=current_ms)
        received_times = [
            item["received_at_ms"]
            for item in state["hyperliquid_derivatives"]["minute_samples"]
        ]
        stale_receipt = bot.build_hyperliquid_oi_receipt(
            state,
            current_ms=current_ms + (bot.HYPERLIQUID_CTX_STALE_SEC + 1) * 1000,
        )

        self.assertEqual(sorted(received_times), received_times)
        self.assertFalse(receipt["primary_ready"])
        self.assertEqual("warming_up", receipt["reason"])
        self.assertFalse(stale_receipt["latest_ctx_current"])
        self.assertEqual("ctx_stale_or_missing", stale_receipt["reason"])

    def test_persisted_hyperliquid_samples_survive_restart_merge(self):
        current_ms = bot.now_ms()
        original = {}
        bot.append_hyperliquid_minute_sample(original, self.ctx(), current_ms)

        with tempfile.TemporaryDirectory() as task_dir:
            state_file = os.path.join(task_dir, "state.json")
            with patch.object(bot, "STATE_FILE", state_file):
                bot.save_state(original)
                restarted = {}
                bot.merge_latest_hyperliquid_state(restarted)

        samples = restarted["hyperliquid_derivatives"]["minute_samples"]
        self.assertEqual(1, len(samples))
        self.assertEqual(bot.HYPERLIQUID_SOURCE, samples[0]["source"])
        self.assertEqual(current_ms, samples[0]["received_at_ms"])

    def test_primary_signal_bars_use_hyperliquid_quantity_without_binance_oi_fallback(self):
        current_ms = 1000 * self.HOUR_MS + 10 * 60 * 1000
        state = self.ready_state(current_ms)
        last_close_ms = bot.latest_closed_hour_ms(current_ms)
        bars = []
        for offset in range(bot.HYPERLIQUID_PRIMARY_MIN_HOURS, -1, -1):
            close_ms = last_close_ms - offset * self.HOUR_MS
            bars.append({
                "open_time": close_ms - self.HOUR_MS + 1,
                "close_time": close_ms,
                "open": 64000.0,
                "high": 64100.0,
                "low": 63900.0,
                "close": 64050.0,
                "quote_volume": 1000000.0,
                "source": "Binance Futures Kline",
            })

        with (
            patch.object(bot, "HYPERLIQUID_MODE", "primary"),
            patch.object(bot, "fetch_binance_klines", return_value=bars),
            patch.object(bot, "fetch_binance_oi_hist", side_effect=AssertionError("must not call")),
            patch.object(bot.time, "time", return_value=current_ms / 1000.0),
        ):
            merged = bot.get_signal_bars(state)
            quality = bot.build_signal_data_quality(merged, current_ms=current_ms)

        self.assertTrue(quality["usable"])
        self.assertIn("Hyperliquid", quality["source"])
        self.assertEqual(bot.HYPERLIQUID_SOURCE, merged[-1]["oi_source"])
        self.assertAlmostEqual(
            merged[-1]["oi"] * merged[-1]["oi_mark_px"],
            merged[-1]["oi_value"],
        )

    def test_primary_signal_quality_stays_unusable_during_hyperliquid_warmup(self):
        current_ms = 1000 * self.HOUR_MS + 10 * 60 * 1000
        state = self.ready_state(current_ms)
        state["hyperliquid_derivatives"]["minute_samples"] = state[
            "hyperliquid_derivatives"
        ]["minute_samples"][-10:]
        last_close_ms = bot.latest_closed_hour_ms(current_ms)
        bars = []
        for offset in range(8, -1, -1):
            close_ms = last_close_ms - offset * self.HOUR_MS
            bars.append({
                "open_time": close_ms - self.HOUR_MS + 1,
                "close_time": close_ms,
                "open": 64000.0,
                "high": 64100.0,
                "low": 63900.0,
                "close": 64050.0,
                "quote_volume": 1000000.0,
                "source": "Binance Futures Kline",
            })

        with (
            patch.object(bot, "HYPERLIQUID_MODE", "primary"),
            patch.object(bot, "fetch_binance_klines", return_value=bars),
            patch.object(bot, "fetch_binance_oi_hist", side_effect=AssertionError("must not call")),
            patch.object(bot.time, "time", return_value=current_ms / 1000.0),
        ):
            merged = bot.get_signal_bars(state)
            quality = bot.build_signal_data_quality(merged, current_ms=current_ms)

        self.assertFalse(quality["usable"])
        self.assertFalse(quality["hyperliquid_primary_ready"])
        self.assertIn("hyperliquid_oi_warmup", quality["reasons"])

    def test_primary_warmup_fails_closed_instead_of_using_binance_records(self):
        current_ms = 1000 * self.HOUR_MS + 10 * 60 * 1000
        state = self.ready_state(current_ms)
        state["hyperliquid_derivatives"]["minute_samples"] = state[
            "hyperliquid_derivatives"
        ]["minute_samples"][-10:]
        selected = {
            "funding": {"status": "ok", "value": {"funding_8h_pct": 0.01}, "stale": False},
            "basis": {"status": "ok", "value": {"basis_pct": 0.01}, "stale": False},
            "hyperliquid_funding": {"status": "unavailable", "value": {}, "reason": "timeout"},
            "hyperliquid_basis": {"status": "unavailable", "value": {}, "reason": "timeout"},
        }
        with (
            patch.object(bot, "HYPERLIQUID_MODE", "primary"),
            patch.object(bot.time, "time", return_value=current_ms / 1000.0),
        ):
            result = bot.apply_derivatives_source_mode(state, selected)

        self.assertEqual("unavailable", result["funding"]["status"])
        self.assertEqual("unavailable", result["basis"]["status"])
        self.assertEqual("ok", result["binance_funding_shadow"]["status"])

    def test_shadow_mode_never_replaces_active_binance_records(self):
        selected = {
            "funding": {"source": "Binance", "value": {"funding_8h_pct": 0.01}},
            "basis": {"source": "Binance", "value": {"basis_pct": 0.01}},
            "hyperliquid_funding": {"source": "Hyperliquid", "value": {"funding_8h_pct": 0.02}},
        }
        with patch.object(bot, "HYPERLIQUID_MODE", "shadow"):
            result = bot.apply_derivatives_source_mode({}, selected)

        self.assertEqual("Binance", result["funding"]["source"])
        self.assertNotIn("binance_funding_shadow", result)

    def test_actual_and_predicted_funding_windows_share_8h_equivalent_unit(self):
        start_ms = 100 * self.HOUR_MS
        history = [
            {"time": start_ms + index * self.HOUR_MS, "fundingRate": "0.00001", "premium": "0"}
            for index in range(24)
        ]
        predicted = [["BTC", [
            ["HlPerp", {"fundingRate": "0.0000125", "fundingIntervalHours": 1}],
            ["BinPerp", {"fundingRate": "0.0001", "fundingIntervalHours": 8}],
            ["BybitPerp", {"fundingRate": "0.00005", "fundingIntervalHours": 4}],
        ]]]

        value = bot.build_hyperliquid_funding_value(history, predicted)

        self.assertAlmostEqual(0.001, value["funding_1h_pct"])
        self.assertAlmostEqual(0.008, value["funding_8h_pct"])
        self.assertAlmostEqual(0.024, value["funding_24h_pct"])
        for venue in ("HlPerp", "BinPerp", "BybitPerp"):
            self.assertAlmostEqual(0.01, value["predicted_8h_equivalent_pct"][venue])

    def test_display_market_uses_binance_spot_ticker(self):
        with (
            patch.object(
                bot,
                "binance_spot_get",
                return_value={
                    "lastPrice": "64000",
                    "priceChangePercent": "1.2",
                    "quoteVolume": "123456789",
                },
            ) as spot_get,
            patch.object(bot, "get_btc_market", return_value=(63900.0, 1_000_000.0, 1.1, 9.0)),
            patch.object(bot, "BTC_PUSH_MARKET_SOURCE", "binance"),
        ):
            market = bot.get_display_market()

        self.assertEqual("Binance Spot 24hr", market["source"])
        self.assertEqual(64000.0, market["price"])
        spot_get.assert_called_once_with("/api/v3/ticker/24hr", {"symbol": bot.BINANCE_SYMBOL})

    def test_short_data_status_labels_hyperliquid_as_shadow_only(self):
        payload = {
            "completeness": {
                "core_market": {"available": 1, "total": 1, "missing": [], "pending": []},
                "macro": {"available": 1, "total": 1, "missing": [], "stale": []},
                "snapshot_notes": [],
            },
            "hyperliquid_derivatives": {"mode": "shadow", "continuous_hours": 12},
        }

        text = "\n".join(bot.format_plain_short_data_block(payload))

        self.assertIn("采集自检通过（策略仍需独立核验）", text)
        self.assertIn("Hyperliquid影子12/72H", text)


class MissingSignalDataSafetyTest(unittest.TestCase):
    def test_comparison_rules_treat_missing_threshold_as_unknown(self):
        for comparator in (
            bot.rule_ge,
            bot.rule_gt,
            bot.rule_le,
            bot.rule_lt,
            bot.rule_abs_le,
        ):
            self.assertIsNone(comparator(64000, None))

    def test_macro_rule_state_does_not_crash_without_support(self):
        result = bot.build_macro_rule_state(
            {
                "market": {"price": 64000},
                "levels": {"support": None},
                "public_records": {"macro": {"value": {}}},
            },
            {"flow_outflow_rule": None},
        )

        self.assertIsNone(result["checks"]["btc_broke_with_etf_outflow"])

    def test_scheduled_push_waits_when_signal_data_is_not_usable(self):
        payload = {
            "signal_data_quality": {
                "usable": False,
                "reasons": ["latest_bar_stale", "oi_not_aligned"],
            }
        }
        with (
            patch.object(bot, "state_store"),
            patch.object(bot, "get_delivery_store") as delivery,
            patch.object(bot, "load_state", return_value={}),
            patch.object(bot, "recycle_state"),
            patch.object(bot, "collect_report_payload", return_value=payload),
        ):
            delivery.return_value.get_report.return_value = None
            with self.assertRaises(bot.SignalDataNotReadyError):
                bot.send_push(
                    check_commands=False,
                    require_usable_signal_data=True,
                )


class ScheduledRetryTest(unittest.TestCase):
    class StopScheduler(Exception):
        pass

    def setUp(self):
        # 调度时钟测试不打开真实主 state、outbox，也不执行发送恢复。
        for name in ("state_store", "initialize_delivery", "pump_pending_deliveries"):
            active = patch.object(bot, name)
            active.start()
            self.addCleanup(active.stop)
        active = patch.object(bot, "delivery_next_due_at", return_value=None)
        active.start()
        self.addCleanup(active.stop)

    def test_retry_runs_after_signal_data_becomes_ready(self):
        clock = {"now": 0.0}
        send_results = [bot.PUSH_RESULT_RETRYABLE, bot.PUSH_RESULT_SENT]
        send_calls = []
        log_messages = []

        def fake_time():
            return clock["now"]

        def fake_sleep(seconds):
            if len(send_calls) >= 2:
                raise self.StopScheduler()
            clock["now"] += seconds

        def fake_send_push(**kwargs):
            send_calls.append(kwargs)
            return send_results[len(send_calls) - 1]

        with (
            patch.object(bot, "start_liquidation_ws_collector"),
            patch.object(bot, "start_hyperliquid_ws_collector"),
            patch.object(bot, "seconds_until_next_hour_tokyo", side_effect=[1, 3600]),
            patch.object(bot, "safe_send_push", side_effect=fake_send_push),
            patch.object(bot, "poll_tg_command", return_value=None),
            patch.object(bot, "log_line", side_effect=log_messages.append),
            patch.object(bot.time, "time", side_effect=fake_time),
            patch.object(bot.time, "sleep", side_effect=fake_sleep),
            patch.object(bot, "BTC_PUSH_SCHEDULE_RETRY_DELAY_SEC", 180),
            patch.object(bot, "BTC_PUSH_SCHEDULE_MAX_RETRIES", 2),
        ):
            with self.assertRaises(self.StopScheduler):
                bot.run_forever()

        self.assertEqual(2, len(send_calls))
        self.assertTrue(send_calls[0]["require_usable_signal_data"])
        self.assertTrue(send_calls[1]["require_usable_signal_data"])
        self.assertEqual(send_calls[0]["report_id"], send_calls[1]["report_id"])
        self.assertEqual(181.0, clock["now"])
        self.assertTrue(any("retry=1/2" in message for message in log_messages))
        self.assertTrue(any("retry succeeded" in message for message in log_messages))

    def test_retry_exhaustion_sends_data_insufficient_fallback(self):
        clock = {"now": 0.0}
        send_results = [
            bot.PUSH_RESULT_RETRYABLE,
            bot.PUSH_RESULT_RETRYABLE,
            bot.PUSH_RESULT_RETRYABLE,
            bot.PUSH_RESULT_SENT,
        ]
        send_calls = []
        log_messages = []

        def fake_time():
            return clock["now"]

        def fake_sleep(seconds):
            if len(send_calls) >= 4:
                raise self.StopScheduler()
            clock["now"] += seconds

        def fake_send_push(**kwargs):
            send_calls.append(kwargs)
            return send_results[len(send_calls) - 1]

        with (
            patch.object(bot, "start_liquidation_ws_collector"),
            patch.object(bot, "start_hyperliquid_ws_collector"),
            patch.object(bot, "seconds_until_next_hour_tokyo", side_effect=[1, 3600]),
            patch.object(bot, "safe_send_push", side_effect=fake_send_push),
            patch.object(bot, "poll_tg_command", return_value=None),
            patch.object(bot, "log_line", side_effect=log_messages.append),
            patch.object(bot.time, "time", side_effect=fake_time),
            patch.object(bot.time, "sleep", side_effect=fake_sleep),
            patch.object(bot, "BTC_PUSH_SCHEDULE_RETRY_DELAY_SEC", 180),
            patch.object(bot, "BTC_PUSH_SCHEDULE_MAX_RETRIES", 2),
        ):
            with self.assertRaises(self.StopScheduler):
                bot.run_forever()

        self.assertEqual(4, len(send_calls))
        self.assertTrue(all(
            call["require_usable_signal_data"] for call in send_calls[:3]
        ))
        self.assertFalse(send_calls[3]["require_usable_signal_data"])
        self.assertEqual(1, len({call["report_id"] for call in send_calls}))
        self.assertEqual(361.0, clock["now"])
        self.assertTrue(any("retries exhausted" in message for message in log_messages))
        self.assertTrue(any("fallback result=sent" in message for message in log_messages))


class CurrentRecordTruthfulnessTest(unittest.TestCase):
    def test_missing_raw_leverage_metrics_do_not_count_derived_labels(self):
        payload = leverage_payload(None, None, None, None, None, None)
        payload["rule_state"] = {"leverage": bot.build_leverage_rule_state(payload)}
        result = bot.build_status_layer(payload)["leverage_environment"]
        self.assertEqual("数据不足", result["status"])
        self.assertEqual(0, result["known_count"])
        self.assertEqual(6, result["input_count"])
        self.assertIsNone(result["score"])

        zero = leverage_payload(0, 0, 0, 0, 0, 0)
        zero["rule_state"] = {"leverage": bot.build_leverage_rule_state(zero)}
        result = bot.build_status_layer(zero)["leverage_environment"]
        self.assertEqual("稳定", result["status"])
        self.assertEqual(6, result["known_count"])
        self.assertEqual(0, result["score"])

    def test_current_record_gate_preserves_raw_zero_and_legacy_partial_records(self):
        for status in (None, "ok", "partial"):
            record = {"value": {"metric": 0.0}}
            if status is not None:
                record["status"] = status
            self.assertEqual(0.0, bot.current_record_value(record, "metric"))
            for flag in ("stale", "source_stale", "snapshot_used"):
                with self.subTest(status=status, flag=flag):
                    blocked = dict(record, **{flag: True})
                    self.assertIsNone(bot.current_record_value(blocked, "metric"))
                    self.assertEqual(0.0, bot.record_value(blocked, "metric"))
                    self.assertEqual("＊", bot.record_mark(blocked))
        for status in ("unavailable", "error"):
            self.assertEqual({}, bot.current_record_values({"status": status, "value": {"metric": 3}}))

    def test_stale_records_are_gated_in_rules_status_and_default_and_full_reports(self):
        fresh = ShortReportPlainLanguageLayoutTest.payload()
        self.assertIn("4H成交和买入回升", "\n".join(bot.format_plain_short_funds_block(fresh)))
        for flag in ("stale", "source_stale", "snapshot_used"):
            with self.subTest(flag=flag):
                payload = copy.deepcopy(fresh)
                for record in payload["public_records"].values():
                    record[flag] = True
                original_records = copy.deepcopy(payload["public_records"])
                etf = bot.build_etf_rule_state(payload, {})
                leverage = bot.build_leverage_rule_state(payload)
                spot = bot.build_spot_rule_state(payload)
                macro = bot.build_macro_rule_state(payload, etf)
                payload["rule_state"].update({"etf": etf, "leverage": leverage, "spot": spot, "macro": macro})
                cycle = bot.build_macro_cycle_state(payload, macro, etf, spot, {})
                self.assertIsNone(leverage["funding_8h_pct"])
                self.assertIsNone(leverage["basis_pct"])
                self.assertEqual("data_insufficient", spot["status"])
                self.assertIsNone(spot["stablecoin_sustained_rule"])
                self.assertEqual("data_insufficient", macro["status"])
                self.assertEqual("unknown", cycle["stage"])
                self.assertFalse(bot.build_signals({}, {}, payload["public_records"], {})["funding_extreme"])
                context = bot.build_status_context(payload)
                self.assertEqual(original_records["macro"]["value"], context["raw"]["macro"])
                self.assertIsNone(context["macro"]["us10y"])
                self.assertTrue(all(value is None for value in context["spot"].values()))
                self.assertEqual("数据不足", bot.build_status_layer(payload)["spot_funds"]["status"])

                funds = "\n".join(bot.format_plain_short_funds_block(payload))
                self.assertIn("-$50M", funds)
                self.assertIn("数据过期，仅作背景", funds)
                self.assertNotIn("4H成交和买入回升", funds)
                self.assertNotIn("稳定币与ETF仍偏流出", funds)
                self.assertNotIn("资金方向未一致", funds)
                for builder in (bot.build_report_message, bot.build_full_report_message):
                    message = builder("2026-09-04 12:00 JST", False, "2026-09-04", 1, payload)
                    self.assertIn("宏观输入不可用于当前确认", message)
                    self.assertNotIn("4H成交和买入回升", message)
                    self.assertNotIn("高真实利率压制仍在", message)
                    self.assertNotIn("Spot CVD 转正", message)
                common_text = bot.build_common_report(payload["public_records"]["macro"])
                self.assertIn("仅作历史参考", common_text)
                self.assertNotIn("高真实利率压制仍在", common_text)
                self.assertEqual(original_records, payload["public_records"])


class MissingNumericDataTest(unittest.TestCase):
    @staticmethod
    def spot_rows():
        hour = 3600 * 1000
        return [
            [index * hour, "100", "101", "99", "100", "1", (index + 1) * hour - 1,
             "100", 10, "0.6", "60", "0"]
            for index in range(1, 50)
        ]

    def test_nonfinite_inputs_are_missing_but_zero_remains_valid(self):
        formatters = (bot.format_pct_value, bot.format_pp_value, bot.format_rate_value,
                      bot.format_plain_number, bot.format_money, bot.compact_money,
                      bot.compact_number, bot.format_structure_price)
        for value in (float("nan"), float("inf"), float("-inf"), "NaN", "Infinity"):
            with self.subTest(value=value):
                self.assertIsNone(bot.safe_float(value))
                self.assertIsNone(structure._to_float(value))
                for formatter in formatters:
                    self.assertEqual("NA", formatter(value))
        self.assertEqual(0.0, bot.safe_float(0))
        self.assertEqual("0.00%", bot.format_pct_value(0))

    def test_missing_four_hour_volume_does_not_erase_valid_price_and_oi(self):
        for value in (None, -1, float("nan"), float("inf"), "bad"):
            with self.subTest(value=value):
                bars = synthetic_bars(10)[1:]
                bars[-1]["quote_volume"] = value
                result = bot.calc_delta_4h_from_bars(bars)
                self.assertIsNone(result["vol_4h_pct"])
                self.assertEqual(0.0, result["price_4h_pct"])
                self.assertEqual(0.0, result["oi_4h_pct"])
        bars = synthetic_bars(10)[1:]
        for bar in bars[-4:]:
            bar["quote_volume"] = 0.0
        self.assertEqual(-100.0, bot.calc_delta_4h_from_bars(bars)["vol_4h_pct"])

    def test_fast_and_slow_volume_distinguish_missing_zero_and_zero_baseline(self):
        fast = synthetic_bars(28)
        slow = synthetic_bars(192)
        for bars, window, check in (
            (fast, 4, lambda items: structure._volume_fast_check(items, 4, 24, 1.3)),
            (slow, 24, lambda items: structure._volume_slow_check(items, 24, 7, 1.1)),
        ):
            missing = copy.deepcopy(bars)
            missing[-1]["quote_volume"] = None
            self.assertEqual("NA", check(missing)["status"])
            zero = copy.deepcopy(bars)
            for bar in zero[-window:]:
                bar["quote_volume"] = 0.0
            self.assertEqual(0.0, check(zero)["ratio"])
            self.assertEqual("NO", check(zero)["status"])
            no_baseline = copy.deepcopy(bars)
            for bar in no_baseline[:-window]:
                bar["quote_volume"] = 0.0
            self.assertEqual("NA", check(no_baseline)["status"])

    def test_incomplete_slow_baseline_day_is_not_counted_as_zero_volume(self):
        bars = synthetic_bars(192)
        for day in range(5):
            bars[day * 24]["quote_volume"] = None
        result = structure._volume_slow_check(bars, 24, 7, 1.1)
        self.assertEqual("NA", result["status"])

    def test_invalid_spot_taker_data_cannot_become_active_selling(self):
        for field, value in ((7, None), (7, -1), (7, "NaN"), (10, None), (10, "bad"), (10, "101")):
            with self.subTest(field=field, value=value):
                rows = self.spot_rows()
                rows[-1][field] = value
                with patch.object(bot, "binance_spot_get", return_value=rows), patch.object(bot.time, "time", return_value=51 * 3600):
                    record = bot.fetch_spot_cvd_record()
                self.assertIsNone(record["value"]["cvd_4h_pct"])
                self.assertIsNone(record["value"]["hourly_flow"][-1]["net_quote"])
                self.assertEqual("partial", record["status"])
                if field == 7:
                    self.assertIsNone(record["value"]["volume_4h_pct"])

    def test_zero_spot_volume_is_not_missing_volume_or_sell_flow(self):
        rows = self.spot_rows()
        for row in rows[-4:]:
            row[7] = row[10] = "0"
        with patch.object(bot, "binance_spot_get", return_value=rows), patch.object(bot.time, "time", return_value=51 * 3600):
            record = bot.fetch_spot_cvd_record()
        self.assertEqual(-100.0, record["value"]["volume_4h_pct"])
        self.assertIsNone(record["value"]["cvd_4h_pct"])
        self.assertEqual(0.0, record["value"]["hourly_flow"][-1]["net_quote"])

    def test_twenty_four_hour_volume_requires_valid_complete_window(self):
        bars = synthetic_bars(49)[1:]
        bars[-1]["quote_volume"] = None
        self.assertNotIn("vol24", bot.build_exchange_market_24h_delta(bars, {})["metrics"])
        for bar in bars[-24:]:
            bar["quote_volume"] = 0.0
        self.assertEqual(-100.0, bot.build_exchange_market_24h_delta(bars, {})["metrics"]["vol24"])

    def test_nonfinite_oi_fails_signal_quality(self):
        bars = synthetic_bars(10)[1:]
        current_ms = bars[-1]["close_time"] + 1000
        self.assertTrue(bot.build_signal_data_quality(bars, current_ms)["usable"])
        bars[-1]["oi"] = float("nan")
        quality = bot.build_signal_data_quality(bars, current_ms)
        self.assertFalse(quality["usable"])
        self.assertEqual(["oi_not_aligned"], quality["reasons"])


if __name__ == "__main__":
    unittest.main()
