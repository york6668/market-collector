import math
from statistics import median


PHASE_META = {
    "DATA_INSUFFICIENT": "数据不足",
    "RANGE": "区间整理",
    "BREAKOUT_CANDIDATE": "突破候选",
    "BREAKOUT_CONFIRMED": "突破确认",
    "PULLBACK_WAIT": "等待回踩",
    "RETEST_CONFIRMED": "回踩确认",
    "INVALIDATED": "突破失效",
}


CHECK_META = {
    "breakout": "突破",
    "oi_fast_expansion": "OI4H短扩张",
    "oi_slow_confirmation": "OI24/72确认",
    "volume_fast_expansion": "量能4H放量",
    "volume_slow_confirmation": "量能24H确认",
    "pullback_confirmed": "回踩确认",
}


STATUS_META = {
    "YES": "满足",
    "NO": "未满足",
    "WAIT": "等待",
    "PARTIAL": "部分满足",
    "NA": "不可用",
}


def _to_float(value, default=None):
    try:
        if value is None:
            return default
        number = float(value)
        return number if math.isfinite(number) else default
    except Exception:
        return default


def _pct(now_value, prev_value):
    now = _to_float(now_value)
    prev = _to_float(prev_value)
    if now is None or prev is None or prev == 0:
        return None
    return (now - prev) / prev * 100.0


def _fmt_pct(value):
    if value is None:
        return "NA"
    return f"{value:+.2f}%"


def _clean_bars(bars):
    cleaned = []
    seen = set()
    for item in bars or []:
        try:
            open_time = int(item.get("open_time"))
        except Exception:
            continue
        if open_time in seen:
            continue
        seen.add(open_time)
        bar = dict(item)
        for key in ("open", "high", "low", "close", "quote_volume", "oi", "oi_value"):
            if key in bar:
                bar[key] = _to_float(bar.get(key))
        if not bar.get("close") or not bar.get("high") or not bar.get("low"):
            continue
        cleaned.append(bar)
    cleaned.sort(key=lambda x: int(x.get("open_time") or 0))
    return cleaned


def bars_are_contiguous(bars, interval_ms=3600 * 1000):
    bars = _clean_bars(bars)
    if len(bars) < 2:
        return bool(bars)
    expected = int(interval_ms)
    tolerance = 1000
    for previous, current in zip(bars, bars[1:]):
        delta = int(current.get("open_time") or 0) - int(previous.get("open_time") or 0)
        if abs(delta - expected) > tolerance:
            return False
    return True


def calc_atr(bars, period=14, end_index=None):
    bars = _clean_bars(bars)
    if end_index is None:
        end_index = len(bars) - 1
    end_index = min(int(end_index), len(bars) - 1)
    if end_index < period:
        return None

    true_ranges = []
    start = max(1, end_index - period + 1)
    for idx in range(start, end_index + 1):
        high = _to_float(bars[idx].get("high"))
        low = _to_float(bars[idx].get("low"))
        prev_close = _to_float(bars[idx - 1].get("close"))
        if high is None or low is None or prev_close is None:
            continue
        true_ranges.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    if not true_ranges:
        return None
    return sum(true_ranges) / float(len(true_ranges))


def _breakout_buffer(close_price, atr, buffer_pct, atr_mult):
    pct_buffer = close_price * buffer_pct
    atr_buffer = (atr or 0.0) * atr_mult
    return max(pct_buffer, atr_buffer)


def _resistance_level(bars, end_index, lookback):
    start = max(0, int(end_index) - int(lookback))
    window = bars[start:end_index]
    if bars and bars[-1].get("calendar") == "US_EQUITY" and end_index >= 0:
        boundary = bars[end_index]["open_time"]
        window = [row for row in bars[:end_index] if row["open_time"] >= boundary - lookback * 3600000]
    highs = [_to_float(item.get("high")) for item in window]
    highs = [value for value in highs if value is not None]
    if not highs:
        return None
    return max(highs)


def _latest_breakout_event(bars, lookback, buffer_pct, atr_mult, scan_bars):
    equity = bool(bars and bars[-1].get("calendar") == "US_EQUITY")
    if len(bars) < (3 if equity else lookback + 2):
        return None

    start = max(lookback + 1, len(bars) - int(scan_bars))
    if equity:
        start = 2
    event = None
    for idx in range(start, len(bars)):
        if equity and (bars[idx]["open_time"] < bars[-1]["open_time"] - scan_bars * 3600000
                       or not bars_are_contiguous(bars[idx - 1:idx + 1])):
            continue
        first_idx = idx - 1
        level = _resistance_level(bars, first_idx, lookback)
        if level is None:
            continue
        close_price = _to_float(bars[idx].get("close"))
        first_close = _to_float(bars[first_idx].get("close"))
        if close_price is None or first_close is None:
            continue
        atr = calc_atr(bars, end_index=idx)
        buffer_value = _breakout_buffer(close_price, atr, buffer_pct, atr_mult)
        threshold = level + buffer_value
        if first_close > threshold and close_price > threshold:
            event = {
                "index": idx,
                "level": level,
                "threshold": threshold,
                "buffer": buffer_value,
                "atr": atr,
                "bar": bars[idx],
            }
    return event


def _series_values(bars, key):
    return [_to_float(item.get(key)) for item in bars if _to_float(item.get(key)) is not None]


def _oi_fast_check(bars, window_h, min_delta_pct, min_rising_steps):
    recent = bars[-(int(window_h) + 1):]
    if (
        len(recent) < int(window_h) + 1
        or not bars_are_contiguous(recent)
        or any(_to_float(item.get("oi")) is None for item in recent)
    ):
        return {
            "ok": False,
            "status": "NA",
            "delta_pct": None,
            "rising_steps": 0,
            "compact": "NA",
            "detail": "OI短窗历史不足",
        }

    values = [_to_float(item.get("oi")) for item in recent]
    delta_pct = _pct(values[-1], values[0])
    rising_steps = sum(1 for idx in range(1, len(values)) if values[idx] > values[idx - 1])
    ok = delta_pct is not None and delta_pct >= min_delta_pct and rising_steps >= min_rising_steps
    if ok:
        status = "YES"
    elif rising_steps >= max(1, min_rising_steps - 1) or (delta_pct is not None and delta_pct >= min_delta_pct * 0.5):
        status = "PARTIAL"
    else:
        status = "NO"
    compact = f"{_fmt_pct(delta_pct)},{rising_steps}/{int(window_h)}"
    return {
        "ok": ok,
        "status": status,
        "delta_pct": delta_pct,
        "rising_steps": rising_steps,
        "window_h": int(window_h),
        "compact": compact,
        "detail": f"{int(window_h)}H {compact}",
    }


def _oi_slow_check(bars, day_h, trend_h, day_min_delta_pct, trend_min_delta_pct):
    trend_h = int(trend_h)
    day_h = int(day_h)
    recent = bars[-(trend_h + 1):]
    if (
        len(recent) < trend_h + 1
        or not bars_are_contiguous(recent)
        or any(_to_float(item.get("oi")) is None for item in recent)
    ):
        return {
            "ok": False,
            "status": "NA",
            "delta_24h_pct": None,
            "delta_72h_pct": None,
            "compact": "NA",
            "detail": "OI慢窗历史不足",
        }

    values = [_to_float(item.get("oi")) for item in recent]
    delta_day = _pct(values[-1], values[-day_h - 1]) if len(values) >= day_h + 1 else None
    delta_trend = _pct(values[-1], values[-trend_h - 1])
    ok = (
        delta_day is not None
        and delta_trend is not None
        and delta_day >= day_min_delta_pct
        and delta_trend >= trend_min_delta_pct
    )
    if ok:
        status = "YES"
    elif (
        delta_day is not None
        and delta_trend is not None
        and delta_day > 0.0
        and delta_trend > -1.0
    ):
        status = "PARTIAL"
    else:
        status = "NO"
    compact = f"24H {_fmt_pct(delta_day)}/72H {_fmt_pct(delta_trend)}"
    return {
        "ok": ok,
        "status": status,
        "delta_24h_pct": delta_day,
        "delta_72h_pct": delta_trend,
        "day_h": day_h,
        "trend_h": trend_h,
        "compact": compact,
        "detail": compact,
    }


def _volume_fast_check(bars, window_h, baseline_h, multiplier):
    window_h = int(window_h)
    baseline_h = int(baseline_h)
    if (len(bars) < window_h + baseline_h or
            bars[-1].get("calendar") == "US_EQUITY" and not bars_are_contiguous(bars[-(window_h + baseline_h):])):
        return {
            "ok": False,
            "status": "NA",
            "ratio": None,
            "compact": "NA",
            "detail": "成交量短窗历史不足",
        }

    confirm = [_to_float(bar_volume(item)) for item in bars[-window_h:]]
    baseline = [_to_float(bar_volume(item)) for item in bars[-(window_h + baseline_h):-window_h]]
    confirm = [value for value in confirm if value is not None and value >= 0]
    baseline = [value for value in baseline if value is not None and value >= 0]
    if len(confirm) < window_h or len(baseline) < baseline_h // 2:
        return {
            "ok": False,
            "status": "NA",
            "ratio": None,
            "compact": "NA",
            "detail": "成交量短窗历史不足",
        }

    avg_confirm = sum(confirm) / float(len(confirm))
    base = median(baseline)
    ratio = avg_confirm / base if base else None
    ok = ratio is not None and ratio >= multiplier
    if ratio is None:
        status = "NA"
    elif ok:
        status = "YES"
    elif ratio is not None and ratio >= 1.0:
        status = "PARTIAL"
    else:
        status = "NO"
    compact = f"{ratio:.2f}x" if ratio is not None else "NA"
    return {
        "ok": ok,
        "status": status,
        "ratio": ratio,
        "avg_confirm": avg_confirm,
        "baseline": base,
        "window_h": window_h,
        "baseline_h": baseline_h,
        "compact": compact,
        "detail": f"{window_h}H/24H中位数={compact}",
    }


def _volume_slow_check(bars, day_h, baseline_days, multiplier):
    day_h = int(day_h)
    baseline_days = int(baseline_days)
    required = day_h * (baseline_days + 1)
    if bars and bars[-1].get("calendar") == "US_EQUITY":
        # Closed intervals do not count as missing collection, but cannot be
        # stretched into complete natural-day volume samples either.
        return _equity_volume_slow_check(bars, day_h, baseline_days, multiplier)
    if len(bars) < required:
        return {
            "ok": False,
            "status": "NA",
            "ratio": None,
            "compact": "NA",
            "detail": "成交量慢窗历史不足",
        }

    recent = bars[-day_h:]
    baseline = bars[-required:-day_h]
    recent_sum = _volume_sum(recent)
    day_sums = []
    for idx in range(0, len(baseline), day_h):
        chunk = baseline[idx:idx + day_h]
        if len(chunk) == day_h:
            day_sums.append(_volume_sum(chunk))
    day_sums = [value for value in day_sums if value is not None]
    if recent_sum is None or len(day_sums) < max(3, baseline_days // 2):
        return {
            "ok": False,
            "status": "NA",
            "ratio": None,
            "compact": "NA",
            "detail": "成交量慢窗历史不足",
        }

    base = median(day_sums)
    ratio = recent_sum / base if base else None
    ok = ratio is not None and ratio >= multiplier
    if ratio is None:
        status = "NA"
    elif ok:
        status = "YES"
    elif ratio is not None and ratio >= 0.95:
        status = "PARTIAL"
    else:
        status = "NO"
    compact = f"{ratio:.2f}x" if ratio is not None else "NA"
    return {
        "ok": ok,
        "status": status,
        "ratio": ratio,
        "recent_sum": recent_sum,
        "baseline": base,
        "day_h": day_h,
        "baseline_days": baseline_days,
        "compact": compact,
        "detail": f"24H/7D日中位数={compact}",
    }


def _pullback_check(bars, event, atr_mult_touch, atr_mult_fail):
    if not event:
        return {
            "ok": False,
            "status": "NA",
            "detail": "尚未确认突破",
            "invalidated": False,
        }

    level = _to_float(event.get("level"))
    atr = _to_float(event.get("atr"), 0.0) or 0.0
    if level is None:
        return {
            "ok": False,
            "status": "NA",
            "detail": "突破位不可用",
            "invalidated": False,
        }

    after = bars[int(event["index"]) + 1:]
    if not after:
        return {
            "ok": False,
            "status": "WAIT",
            "detail": "突破刚确认，等待回踩",
            "invalidated": False,
        }

    fail_line = level - atr * atr_mult_fail
    touch_line = level + atr * atr_mult_touch
    confirm_line = level + max((event.get("buffer") or 0.0) * 0.25, atr * 0.1)

    invalidated = any((_to_float(item.get("close")) or 0.0) < fail_line for item in after)
    if invalidated:
        return {
            "ok": False,
            "status": "NO",
            "detail": "收盘跌破突破位容错线",
            "invalidated": True,
            "fail_line": fail_line,
        }

    touch_index = None
    for idx, item in enumerate(after):
        low = _to_float(item.get("low"))
        if low is not None and low <= touch_line:
            touch_index = idx
            break

    if touch_index is None:
        return {
            "ok": False,
            "status": "WAIT",
            "detail": "突破后尚未回踩到确认区",
            "invalidated": False,
            "touch_line": touch_line,
        }

    confirmed = any((_to_float(item.get("close")) or 0.0) > confirm_line for item in after[touch_index:])
    if confirmed:
        return {
            "ok": True,
            "status": "YES",
            "detail": "回踩确认区后重新站上",
            "invalidated": False,
            "touch_line": touch_line,
        }

    return {
        "ok": False,
        "status": "WAIT",
        "detail": "已回踩，等待重新站上",
        "invalidated": False,
        "touch_line": touch_line,
    }


def bar_volume(bar):
    return bar.get("volume") if bar.get("volume_unit") == "shares" else bar.get("quote_volume")


def _equity_volume_slow_check(bars, day_h, baseline_days, multiplier):
    end = bars[-1]["close_time"]
    samples = []
    for offset in range(baseline_days + 1):
        boundary = end - offset * day_h * 3600000
        rows = [row for row in bars if boundary - day_h * 3600000 < row["close_time"] <= boundary]
        samples.append(_volume_sum(rows) if len(rows) == day_h and bars_are_contiguous(rows) else None)
    baseline = [value for value in samples[1:] if value is not None]
    base = median(baseline) if len(baseline) >= max(3, baseline_days // 2) else None
    ratio = samples[0] / base if samples[0] is not None and base else None
    return {"ok": ratio is not None and ratio >= multiplier,
            "status": "NA" if ratio is None else "YES" if ratio >= multiplier else "NO",
            "ratio": ratio, "recent_sum": samples[0], "baseline": base, "day_h": day_h,
            "baseline_days": baseline_days, "volume_unit": "shares", "complete_day_samples": len(baseline),
            "compact": f"{ratio:.2f}x" if ratio is not None else "NA",
            "detail": "自然24H/7D内完整日窗口中位数；休市窗口不补样本"}


def _volume_sum(bars):
    """完整窗口的成交额；缺失和异常保留不可用语义，合法零值仍参与合计。"""
    values = [_to_float(bar_volume(item)) for item in bars]
    if not values or any(value is None or value < 0 for value in values):
        return None
    return sum(values)


def calc_delta_4h_from_bars(bars):
    bars = _clean_bars(bars)
    if len(bars) < 9 or not bars_are_contiguous(bars[-9:]):
        return None

    now = bars[-1]
    prev = bars[-5]
    recent_vol = _volume_sum(bars[-4:])
    prev_vol = _volume_sum(bars[-8:-4])
    return {
        "price_4h_pct": _pct(now.get("close"), prev.get("close")),
        "oi_4h_pct": _pct(now.get("oi"), prev.get("oi")),
        "oi_notional_4h_pct": _pct(now.get("oi_value"), prev.get("oi_value")),
        "vol_4h_pct": _pct(recent_vol, prev_vol),
        "window_sec": int((now.get("close_time") or now.get("open_time") or 0) - (prev.get("close_time") or prev.get("open_time") or 0)) // 1000,
        "prev_ts": int(prev.get("close_time") or prev.get("open_time") or 0) // 1000,
        "source": now.get("signal_source") if now.get("calendar") == "US_EQUITY" else "binance_futures_1h",
    }


def bars_to_model_snapshots(bars):
    out = []
    for item in _clean_bars(bars):
        close_time = int(item.get("close_time") or item.get("open_time") or 0)
        out.append({
            "ts": close_time // 1000,
            "price": item.get("close"),
            "oi_usd": item.get("oi_value"),
            "vol24": bar_volume(item),
        })
    return out


def analyze_structure(
    bars,
    lookback=72,
    buffer_pct=0.0015,
    buffer_atr_mult=0.2,
    oi_fast_window_h=4,
    oi_fast_min_delta_pct=1.0,
    oi_fast_min_rising_steps=3,
    oi_slow_day_h=24,
    oi_slow_trend_h=72,
    oi_slow_day_min_delta_pct=1.5,
    oi_slow_trend_min_delta_pct=0.0,
    volume_fast_window_h=4,
    volume_fast_baseline_h=24,
    volume_fast_multiplier=1.3,
    volume_slow_day_h=24,
    volume_slow_baseline_days=7,
    volume_slow_multiplier=1.1,
    pullback_atr_mult=0.5,
    fail_atr_mult=0.75,
    scan_bars=48,
):
    bars = _clean_bars(bars)
    equity = bool(bars and bars[-1].get("calendar") == "US_EQUITY")
    required_bars = max(
        int(lookback) + 2,
        int(oi_slow_trend_h) + 1,
        int(volume_slow_day_h) * (int(volume_slow_baseline_days) + 1),
        30,
    )
    if equity:
        required_bars = 30
    source = (bars[-1].get("signal_source") if equity else "Binance Futures 1h Kline + OI Hist")
    if len(bars) < required_bars:
        return {
            "available": False,
            "phase": "DATA_INSUFFICIENT",
            "execution": "WAIT",
            "execution_stage": "NO",
            "source": source,
            "reason": "股票1h价格历史不足" if equity else "Binance 1h K线/OI历史不足",
            "checks": {},
            "missing": [
                "breakout",
                "oi_fast_expansion",
                "oi_slow_confirmation",
                "volume_fast_expansion",
                "volume_slow_confirmation",
                "pullback_confirmed",
            ],
            "windows": {"required_bars": required_bars, "loaded_bars": len(bars)},
        }

    from market_assets import history_contiguous
    if not (history_contiguous(bars[-required_bars:]) if equity else bars_are_contiguous(bars[-required_bars:])):
        return {
            "available": False,
            "phase": "DATA_INSUFFICIENT",
            "execution": "WAIT",
            "execution_stage": "NO",
            "source": source,
            "reason": "Binance 1h K线时间不连续",
            "checks": {},
            "missing": [
                "breakout",
                "oi_fast_expansion",
                "oi_slow_confirmation",
                "volume_fast_expansion",
                "volume_slow_confirmation",
                "pullback_confirmed",
            ],
            "windows": {"required_bars": required_bars, "loaded_bars": len(bars)},
        }

    current_atr = calc_atr(bars)
    current_close = _to_float(bars[-1].get("close"))
    current_level = _resistance_level(bars, len(bars) - 2, lookback)
    current_buffer = _breakout_buffer(current_close, current_atr, buffer_pct, buffer_atr_mult)
    current_threshold = current_level + current_buffer if current_level is not None else None

    latest_close = _to_float(bars[-1].get("close"))
    prev_close = _to_float(bars[-2].get("close"))
    latest_above = current_threshold is not None and latest_close is not None and latest_close > current_threshold
    breakout_now = (
        current_threshold is not None
        and (not equity or bars_are_contiguous(bars[-2:]))
        and latest_close is not None
        and prev_close is not None
        and latest_close > current_threshold
        and prev_close > current_threshold
    )

    breakout_event = _latest_breakout_event(bars, lookback, buffer_pct, buffer_atr_mult, scan_bars)
    oi_fast_check = _oi_fast_check(
        bars,
        oi_fast_window_h,
        oi_fast_min_delta_pct,
        oi_fast_min_rising_steps,
    )
    oi_slow_check = _oi_slow_check(
        bars,
        oi_slow_day_h,
        oi_slow_trend_h,
        oi_slow_day_min_delta_pct,
        oi_slow_trend_min_delta_pct,
    )
    volume_fast_check = _volume_fast_check(
        bars,
        volume_fast_window_h,
        volume_fast_baseline_h,
        volume_fast_multiplier,
    )
    volume_slow_check = _volume_slow_check(
        bars,
        volume_slow_day_h,
        volume_slow_baseline_days,
        volume_slow_multiplier,
    )
    pullback_check = _pullback_check(bars, breakout_event, pullback_atr_mult, fail_atr_mult)

    breakout_ok = breakout_event is not None
    if breakout_ok:
        breakout_status = "YES"
        breakout_detail = f"最近{int(scan_bars)}H内曾连续2根1H收盘站上{int(lookback)}H阻力+buffer"
    elif latest_above:
        breakout_status = "PARTIAL"
        breakout_detail = "仅最新1H收盘站上，等待连续确认"
    else:
        breakout_status = "NO"
        breakout_detail = f"未连续站上{int(lookback)}H阻力+buffer"

    invalidated = bool(pullback_check.get("invalidated"))
    checks = {
        "breakout": {
            "ok": breakout_ok,
            "status": breakout_status,
            "detail": breakout_detail,
            "compact": f"{int(lookback)}H",
        },
        "oi_fast_expansion": oi_fast_check,
        "oi_slow_confirmation": oi_slow_check,
        "volume_fast_expansion": volume_fast_check,
        "volume_slow_confirmation": volume_slow_check,
        "pullback_confirmed": pullback_check,
    }

    flow_confirmed = all(
        checks[key].get("ok")
        for key in (
            "oi_fast_expansion",
            "oi_slow_confirmation",
            "volume_fast_expansion",
            "volume_slow_confirmation",
        )
    )
    if invalidated:
        phase = "INVALIDATED"
    elif breakout_event and pullback_check.get("ok") and flow_confirmed:
        phase = "RETEST_CONFIRMED"
    elif breakout_event and pullback_check.get("ok"):
        phase = "BREAKOUT_CONFIRMED"
    elif breakout_event:
        phase = "PULLBACK_WAIT"
    elif latest_above or breakout_now:
        phase = "BREAKOUT_CANDIDATE"
    else:
        phase = "RANGE"

    execution = "EXECUTE" if phase == "RETEST_CONFIRMED" else "WAIT"
    missing = [key for key, item in checks.items() if not item.get("ok")]

    level = breakout_event.get("level") if breakout_event else current_level
    threshold = breakout_event.get("threshold") if breakout_event else current_threshold
    return {
        "available": True,
        "phase": phase,
        "phase_cn": PHASE_META.get(phase, phase),
        "execution": execution,
        "execution_stage": "YES" if execution == "EXECUTE" else "NO",
        "source": source,
        "bars": len(bars),
        "lookback_h": lookback,
        "breakout_level": level,
        "breakout_threshold": threshold,
        "atr_14": current_atr,
        "latest_close": latest_close,
        "checks": checks,
        "missing": missing,
        "windows": {
            "breakout_h": int(lookback),
            "oi_fast_h": int(oi_fast_window_h),
            "oi_slow_day_h": int(oi_slow_day_h),
            "oi_slow_trend_h": int(oi_slow_trend_h),
            "volume_fast_h": int(volume_fast_window_h),
            "volume_fast_baseline_h": int(volume_fast_baseline_h),
            "volume_slow_day_h": int(volume_slow_day_h),
            "volume_slow_baseline_days": int(volume_slow_baseline_days),
            "atr_period": 14,
            "loaded_bars": len(bars),
        },
        "reason": "突破链路完成" if execution == "EXECUTE" else " / ".join(CHECK_META.get(key, key) for key in missing),
    }
