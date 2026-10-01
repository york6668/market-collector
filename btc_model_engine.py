TERM_META = {
    "Trend": {"cn": "趋势"},
    "Long-Term Trend": {"cn": "72h 长期趋势"},
    "Volatility": {"cn": "波动"},
    "Liquidity": {"cn": "流动性"},
    "TQ": {"cn": "趋势质量"},
    "Risk": {"cn": "风险"},
    "Execution": {"cn": "执行状态"},
    "Execution Stage": {"cn": "执行许可"},
}

VALUE_META = {
    "UPTREND": {"cn": "上升趋势"},
    "DOWNTREND": {"cn": "下降趋势"},
    "DELEVERAGING": {"cn": "去杠杆反弹"},
    "RANGE": {"cn": "区间整理"},
    "LOW_VOL": {"cn": "低波动"},
    "MID_VOL": {"cn": "中等波动"},
    "HIGH_VOL": {"cn": "高波动"},
    "EXPANDING": {"cn": "流动性扩张"},
    "STABLE": {"cn": "流动性稳定"},
    "CONTRACTING": {"cn": "流动性收缩"},
    "WEAK": {"cn": "弱趋势"},
    "NORMAL": {"cn": "普通趋势"},
    "STRONG": {"cn": "强趋势"},
    "EXTREME": {"cn": "过热趋势"},
    "LOW": {"cn": "低风险"},
    "MID": {"cn": "中风险"},
    "HIGH": {"cn": "高风险"},
    "EXECUTE": {"cn": "允许执行"},
    "WAIT": {"cn": "等待"},
    "YES": {"cn": "是"},
    "NO": {"cn": "否"},
}


class MarketModel:

    def __init__(self, rule_path=None, parameter=None):
        self.rule_path = rule_path
        self.parameter = parameter or (lambda name, default: default)

    @staticmethod
    def _abs(value):
        return abs(float(value or 0.0))

    def classify_trend(self, price_delta, oi_delta):
        if price_delta > self.parameter("MODEL_TREND_PRICE_PCT", 1.0) and oi_delta > self.parameter("MODEL_TREND_OI_PCT", 2.0):
            return "UPTREND"
        if price_delta < -self.parameter("MODEL_TREND_PRICE_PCT", 1.0) and oi_delta > self.parameter("MODEL_TREND_OI_PCT", 2.0):
            return "DOWNTREND"
        if price_delta > self.parameter("MODEL_TREND_PRICE_PCT", 1.0) and oi_delta < -self.parameter("MODEL_DELEVERAGING_OI_PCT", 6.0):
            return "DELEVERAGING"
        if self._abs(price_delta) < self.parameter("MODEL_RANGE_PRICE_PCT", 0.8) and self._abs(oi_delta) < self.parameter("MODEL_RANGE_OI_PCT", 1.5):
            return "RANGE"
        return "RANGE"

    def classify_volatility(self, price_delta):
        abs_price = self._abs(price_delta)
        if abs_price < self.parameter("MODEL_LOW_VOLATILITY_PCT", 0.8):
            return "LOW_VOL"
        if abs_price < self.parameter("MODEL_HIGH_VOLATILITY_PCT", 3.0):
            return "MID_VOL"
        return "HIGH_VOL"

    def classify_liquidity(self, vol_delta):
        if vol_delta > self.parameter("MODEL_VOLUME_EXPANSION_PCT", 5.0):
            return "EXPANDING"
        if vol_delta < -self.parameter("MODEL_VOLUME_CONTRACTION_PCT", 5.0):
            return "CONTRACTING"
        return "STABLE"

    def classify_tq(self, price_delta, oi_delta, vol_delta):
        if price_delta > self.parameter("MODEL_EXTREME_PRICE_PCT", 3.0) and oi_delta > self.parameter("MODEL_EXTREME_OI_PCT", 5.0):
            return "EXTREME"
        if price_delta > self.parameter("MODEL_TREND_PRICE_PCT", 1.0) and oi_delta > self.parameter("MODEL_TREND_OI_PCT", 2.0) and vol_delta > self.parameter("MODEL_VOLUME_EXPANSION_PCT", 5.0):
            return "STRONG"
        if price_delta > self.parameter("MODEL_TREND_PRICE_PCT", 1.0) and oi_delta > 0.0:
            return "NORMAL"
        if price_delta > self.parameter("MODEL_TREND_PRICE_PCT", 1.0) and oi_delta <= 0.0:
            return "WEAK"
        return "WEAK"

    def classify_risk(self, oi_delta, vol_delta):
        if oi_delta < -self.parameter("MODEL_HIGH_RISK_OI_PCT", 5.0) or vol_delta < -self.parameter("MODEL_HIGH_RISK_VOLUME_PCT", 10.0):
            return "HIGH"
        if oi_delta < 0.0 or vol_delta < 0.0:
            return "MID"
        return "LOW"

    def classify_execution(self, trend, volatility, liquidity, tq, risk):
        if (
            trend == "UPTREND"
            and volatility == "MID_VOL"
            and liquidity == "EXPANDING"
            and tq in ("STRONG", "EXTREME")
            and risk == "LOW"
        ):
            return "EXECUTE"
        return "WAIT"

    def classify_long_term_trend(self, snapshots):
        if not snapshots or len(snapshots) < 73:
            return "RANGE"

        window = snapshots[-73:]
        if any(b.get("ts", 0) - a.get("ts", 0) != 3600 for a, b in zip(window, window[1:])):
            return "RANGE"
        prices = [float(item.get("price", 0.0) or 0.0) for item in window]
        if any(price <= 0 for price in prices):
            return "RANGE"

        first = prices[0]
        last = prices[-1]
        seg1 = prices[:24]
        seg2 = prices[24:48]
        seg3 = prices[48:]
        highs = [max(seg1), max(seg2), max(seg3)]
        lows = [min(seg1), min(seg2), min(seg3)]

        if last > first and highs[0] < highs[1] < highs[2]:
            return "UPTREND"
        if last < first and lows[0] > lows[1] > lows[2]:
            return "DOWNTREND"
        return "RANGE"

    def execution_stage(self, execution):
        return "YES" if execution == "EXECUTE" else "NO"

    def default_state(self):
        execution = "WAIT"
        return {
            "trend": "RANGE",
            "long_term_trend": "RANGE",
            "volatility": "LOW_VOL",
            "liquidity": "STABLE",
            "tq": "WEAK",
            "risk": "MID",
            "execution": execution,
            "execution_stage": self.execution_stage(execution),
            "delta4h": None,
            "display": self.build_display_map({
                "trend": "RANGE",
                "long_term_trend": "RANGE",
                "volatility": "LOW_VOL",
                "liquidity": "STABLE",
                "tq": "WEAK",
                "risk": "MID",
                "execution": execution,
                "execution_stage": self.execution_stage(execution),
            }),
            "data_ready": False,
        }

    def build_display_map(self, state):
        return {
            "trend": self.format_factor("Trend", state["trend"]),
            "long_term_trend": self.format_factor("Long-Term Trend", state["long_term_trend"]),
            "volatility": self.format_factor("Volatility", state["volatility"]),
            "liquidity": self.format_factor("Liquidity", state["liquidity"]),
            "tq": self.format_factor("TQ", state["tq"]),
            "risk": self.format_factor("Risk", state["risk"]),
            "execution": self.format_factor("Execution", state["execution"]),
            "execution_stage": self.format_execution_stage(state["execution_stage"], state["execution"]),
        }

    def build_state_from_delta(self, delta4h, snapshots=None):
        if not delta4h or any(delta4h.get(key) is None for key in ("price_4h_pct", "oi_4h_pct", "vol_4h_pct")):
            state = self.default_state()
            state["delta4h"] = delta4h
            state["missing_inputs"] = [key for key in ("price_4h_pct", "oi_4h_pct", "vol_4h_pct") if not delta4h or delta4h.get(key) is None]
            if delta4h and delta4h.get("price_4h_pct") is not None:
                state["volatility"] = self.classify_volatility(delta4h["price_4h_pct"])
            if delta4h and delta4h.get("vol_4h_pct") is not None:
                state["liquidity"] = self.classify_liquidity(delta4h["vol_4h_pct"])
            state["long_term_trend"] = self.classify_long_term_trend(snapshots or [])
            state["display"] = self.build_display_map(state)
            return state

        price_delta = float(delta4h.get("price_4h_pct", 0.0) or 0.0)
        oi_delta = float(delta4h.get("oi_4h_pct", 0.0) or 0.0)
        vol_delta = float(delta4h.get("vol_4h_pct", 0.0) or 0.0)

        long_term_trend = self.classify_long_term_trend(snapshots or [])
        trend = self.classify_trend(price_delta, oi_delta)
        volatility = self.classify_volatility(price_delta)
        liquidity = self.classify_liquidity(vol_delta)
        tq = self.classify_tq(price_delta, oi_delta, vol_delta)
        risk = self.classify_risk(oi_delta, vol_delta)
        execution = self.classify_execution(trend, volatility, liquidity, tq, risk)
        if long_term_trend != "UPTREND":
            execution = "WAIT"
        execution_stage = self.execution_stage(execution)

        state = {
            "trend": trend,
            "long_term_trend": long_term_trend,
            "volatility": volatility,
            "liquidity": liquidity,
            "tq": tq,
            "risk": risk,
            "execution": execution,
            "execution_stage": execution_stage,
            "delta4h": {
                "price_4h_pct": price_delta,
                "oi_4h_pct": oi_delta,
                "vol_4h_pct": vol_delta,
            },
            "data_ready": True,
        }
        state["display"] = self.build_display_map(state)
        return state

    def format_factor(self, english_term, value):
        return f"{english_term}（{TERM_META[english_term]['cn']}）: {VALUE_META[value]['cn']}（{value}）"

    def format_execution_stage(self, stage_value, execution_value):
        return (
            f"Execution Stage（{TERM_META['Execution Stage']['cn']}）: "
            f"{stage_value}（{execution_value}）"
        )
