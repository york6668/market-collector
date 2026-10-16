#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import copy
import json
import html
import hashlib
import math
import os
import re
import random
import shutil
import socket
import subprocess
import sys
import threading
import time
import traceback
import warnings
import uuid
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from market_scheduler import AssetSchedule

import btc_collection_io as public_io
import market_assets as assets
import market_history
import market_observations
import seller_exhaustion
import btc_etf
import btc_etf_flow
import market_public
import market_streams
import market_common as common
import market_news as news
from btc_presentation import render_report, utf16_length
from datetime import datetime, timedelta
from io import StringIO
from statistics import median

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

warnings.filterwarnings(
    "ignore",
    message=r"urllib3 v2 only supports OpenSSL.*",
)

import requests

from btc_state_store import (
    DOMAINS,
    StateStore,
    compact_state,
    compact_event_log,
    compact_public_snapshots,
    merge_domain,
)
from btc_delivery import DeliveryStore, ReceiptPersistenceError
from btc_websocket import connect_websocket, WebSocketLiveness

from btc_model_engine import MarketModel, VALUE_META
from btc_collection import (
    dated_flow_rows, dated_flow_window, epoch_ms, iso_date,
    series_receipt, raw_series_receipt, settled_window, settlement_rows, time_window,
    settlement_window,
)
from btc_structure_engine import (
    CHECK_META,
    PHASE_META,
    STATUS_META,
    analyze_structure,
    bars_are_contiguous,
    bars_to_model_snapshots,
    calc_atr,
    bar_volume,
    calc_delta_4h_from_bars,
)
from btc_runtime import (
    BOT_TOKEN,
    CHAT_ID_RAW,
    MAX_EVENT_LOG_DAYS,
    MAX_SNAPSHOTS,
    OLLAMA_API,
    OLLAMA_MODEL,
    STATE_FILE,
    TG_SAFE_LEN,
    TOKYO_TZ,
    do_repair,
    get_btc_market,
    get_btc_oi,
    get_news_titles as get_english_news_titles,
    seconds_until_next_hour_tokyo,
    tg_send,
    tg_send_document,
    tokyo_day_str,
    tokyo_now,
)


APP_ID = "btc-hourly"
REPORT_SCHEMA = "btc_hourly_report"
REPORT_SCHEMA_REVISION = 2
NEWS_DEDUPE_TTL_SEC = int(os.getenv("NEWS_DEDUPE_TTL_SEC", str(48 * 3600)))
NEWS_TRANSLATION_CACHE_TTL_SEC = int(os.getenv("NEWS_TRANSLATION_CACHE_TTL_SEC", str(7 * 86400)))
NEWS_TRANSLATION_CACHE_VERSION = "full_sentence_v2"
NEWS_MAX_ITEMS = int(os.getenv("BTC_PUSH_NEWS_MAX_ITEMS", "3"))
DELTA_TARGET_SEC = int(os.getenv("DELTA_TARGET_SEC", str(4 * 3600)))
DELTA_TOLERANCE_SEC = int(os.getenv("DELTA_TOLERANCE_SEC", "2700"))
LOG_FILE = os.path.expanduser(os.getenv("BTC_PUSH_LOG_FILE", "/tmp/btcpush.log"))
STATUS_RULES_FILE = os.path.expanduser(
    os.getenv(
        "BTC_STATUS_RULES_FILE",
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "btc_status_rules.json"),
    )
)
OLLAMA_TIMEOUT_SEC = int(os.getenv("BTC_PUSH_OLLAMA_TIMEOUT_SEC", "20"))
OLLAMA_CACHE_TTL_SEC = int(os.getenv("BTC_PUSH_OLLAMA_CACHE_TTL_SEC", str(4 * 3600)))
TG_COMMAND_TIMEOUT_SEC = int(os.getenv("BTC_PUSH_TG_COMMAND_TIMEOUT_SEC", "2"))
NEWS_OLLAMA_TIMEOUT_SEC = int(os.getenv("BTC_PUSH_NEWS_OLLAMA_TIMEOUT_SEC", "8"))
NEWS_OLLAMA_COOLDOWN_SEC = int(os.getenv("BTC_PUSH_NEWS_OLLAMA_COOLDOWN_SEC", "600"))
NEWS_FETCH_TIMEOUT_SEC = int(os.getenv("BTC_PUSH_NEWS_FETCH_TIMEOUT_SEC", "8"))
FEAR_GREED_API_URL = os.getenv("BTC_PUSH_FEAR_GREED_API_URL", "https://api.alternative.me/fng/")
FEAR_GREED_FETCH_TIMEOUT_SEC = int(os.getenv("BTC_PUSH_FEAR_GREED_FETCH_TIMEOUT_SEC", "8"))
FEAR_GREED_HISTORY_DAYS = int(os.getenv("BTC_PUSH_FEAR_GREED_HISTORY_DAYS", "7"))
SEVEN_DAY_TARGET_SEC = int(os.getenv("BTC_PUSH_7D_TARGET_SEC", str(7 * 86400)))
SEVEN_DAY_TOLERANCE_SEC = int(os.getenv("BTC_PUSH_7D_TOLERANCE_SEC", str(6 * 3600)))
MARKET_24H_TARGET_SEC = int(os.getenv("BTC_PUSH_MARKET_24H_TARGET_SEC", str(24 * 3600)))
MARKET_24H_TOLERANCE_SEC = int(os.getenv("BTC_PUSH_MARKET_24H_TOLERANCE_SEC", str(2 * 3600)))
BTC_PUSH_MARKET_SOURCE = os.getenv("BTC_PUSH_MARKET_SOURCE", "binance").strip().lower()
BTC_PUSH_SIGNAL_SOURCE = os.getenv("BTC_PUSH_SIGNAL_SOURCE", "binance_futures").strip().lower()
BINANCE_FUTURES_BASE_URL = os.getenv("BTC_PUSH_BINANCE_FUTURES_BASE_URL", "https://fapi.binance.com").rstrip("/")
BINANCE_SPOT_BASE_URL = os.getenv("BTC_PUSH_BINANCE_SPOT_BASE_URL", "https://api.binance.com").rstrip("/")
BINANCE_SYMBOL = os.getenv("BTC_PUSH_BINANCE_SYMBOL", "BTCUSDT").strip().upper()
BINANCE_INTERVAL = os.getenv("BTC_PUSH_BINANCE_INTERVAL", "1h").strip()
BINANCE_BARS_LIMIT = int(os.getenv("BTC_PUSH_BINANCE_BARS_LIMIT", "240"))
BINANCE_FETCH_TIMEOUT_SEC = int(os.getenv("BTC_PUSH_BINANCE_FETCH_TIMEOUT_SEC", "8"))
BINANCE_OI_MAX_DRIFT_SEC = int(os.getenv("BTC_PUSH_BINANCE_OI_MAX_DRIFT_SEC", "300"))
SIGNAL_BAR_STALE_SEC = int(os.getenv("BTC_PUSH_SIGNAL_BAR_STALE_SEC", str(2 * 3600)))
HYPERLIQUID_INFO_URL = os.getenv(
    "BTC_PUSH_HYPERLIQUID_INFO_URL",
    "https://api.hyperliquid.xyz/info",
).strip()
HYPERLIQUID_WS_URL = os.getenv(
    "BTC_PUSH_HYPERLIQUID_WS_URL",
    "wss://api.hyperliquid.xyz/ws",
).strip()
HYPERLIQUID_COIN = os.getenv("BTC_PUSH_HYPERLIQUID_COIN", "BTC").strip().upper()
HYPERLIQUID_MODE = os.getenv("BTC_PUSH_HYPERLIQUID_MODE", "shadow").strip().lower()
if HYPERLIQUID_MODE not in ("shadow", "primary"):
    HYPERLIQUID_MODE = "shadow"
HYPERLIQUID_FETCH_TIMEOUT_SEC = int(os.getenv("BTC_PUSH_HYPERLIQUID_FETCH_TIMEOUT_SEC", "8"))
HYPERLIQUID_WS_TIMEOUT_SEC = int(os.getenv("BTC_PUSH_HYPERLIQUID_WS_TIMEOUT_SEC", "30"))
HYPERLIQUID_WS_CONNECT_TIMEOUT_SEC = int(
    os.getenv("BTC_PUSH_HYPERLIQUID_WS_CONNECT_TIMEOUT_SEC", "10")
)
HYPERLIQUID_CTX_STALE_SEC = int(os.getenv("BTC_PUSH_HYPERLIQUID_CTX_STALE_SEC", "180"))
HYPERLIQUID_HEARTBEAT_SAVE_SEC = int(
    os.getenv("BTC_PUSH_HYPERLIQUID_HEARTBEAT_SAVE_SEC", "60")
)
HYPERLIQUID_SAMPLE_RETENTION_H = max(
    75,
    int(os.getenv("BTC_PUSH_HYPERLIQUID_SAMPLE_RETENTION_H", "75")),
)
HYPERLIQUID_ALIGN_MAX_DRIFT_SEC = int(
    os.getenv("BTC_PUSH_HYPERLIQUID_ALIGN_MAX_DRIFT_SEC", "90")
)
HYPERLIQUID_PRIMARY_MIN_HOURS = max(
    72,
    int(os.getenv("BTC_PUSH_HYPERLIQUID_PRIMARY_MIN_HOURS", "72")),
)
HYPERLIQUID_STATE_SCHEMA_REVISION = 1
HYPERLIQUID_SOURCE = "Hyperliquid BTC Perp activeAssetCtx"
HYPERLIQUID_FUNDING_SOURCE = "Hyperliquid BTC Perp fundingHistory + predictedFundings"
STABLECOIN_HISTORY_MIN_COVERAGE_PCT = float(os.getenv(
    "BTC_PUSH_STABLECOIN_HISTORY_MIN_COVERAGE_PCT",
    "99.0",
))
STABLECOIN_FLOW_DEADZONE_USD = float(os.getenv(
    "BTC_PUSH_STABLECOIN_FLOW_DEADZONE_USD",
    "100000000",
))
PUBLIC_SNAPSHOT_MAX = int(os.getenv(
    "BTC_PUSH_PUBLIC_SNAPSHOT_MAX",
    os.getenv("BTC_PUSH_V4_PUBLIC_SNAPSHOT_MAX", "240"),
))
PUBLIC_SNAPSHOT_STALE_SEC = int(os.getenv(
    "BTC_PUSH_PUBLIC_SNAPSHOT_STALE_SEC",
    os.getenv("BTC_PUSH_V4_PUBLIC_SNAPSHOT_STALE_SEC", str(36 * 3600)),
))
PUBLIC_SNAPSHOT_7D_TOLERANCE_SEC = int(os.getenv(
    "BTC_PUSH_PUBLIC_SNAPSHOT_7D_TOLERANCE_SEC",
    os.getenv("BTC_PUSH_V4_7D_TOLERANCE_SEC", str(12 * 3600)),
))
MARKET_STRUCTURE_STATE_KEY = "market_structure_state"
PRICE_TREND_STATE_KEY = MARKET_STRUCTURE_STATE_KEY
MARKET_PATTERN_HISTORY_KEY = "market_pattern_history"
MARKET_PATTERN_HISTORY_MAX = int(os.getenv("BTC_PUSH_MARKET_PATTERN_HISTORY_MAX", "24"))
PRICE_TREND_STATE_SCHEMA_REVISION = 5
PRICE_TREND_CONFIRM_HOURS = int(os.getenv("BTC_PUSH_PRICE_TREND_CONFIRM_HOURS", "2"))
PRICE_TREND_BREAKDOWN_HOURS = int(os.getenv("BTC_PUSH_PRICE_TREND_BREAKDOWN_HOURS", "2"))
PRICE_TREND_LEVEL_RESET_REL = float(os.getenv("BTC_PUSH_PRICE_TREND_LEVEL_RESET_REL", "0.003"))
MACRO_CACHE_TTL_SEC = int(os.getenv(
    "BTC_PUSH_MACRO_CACHE_TTL_SEC",
    os.getenv("BTC_PUSH_V5_MACRO_CACHE_TTL_SEC", str(6 * 3600)),
))
MACRO_CACHE_SCHEMA_REVISION = 12
MACRO_SOURCE_STALE_SEC = int(os.getenv(
    "BTC_PUSH_MACRO_SOURCE_STALE_SEC",
    os.getenv("BTC_PUSH_V5_MACRO_SOURCE_STALE_SEC", str(96 * 3600)),
))
MACRO_WEEKLY_SOURCE_STALE_SEC = int(os.getenv(
    "BTC_PUSH_MACRO_WEEKLY_SOURCE_STALE_SEC",
    os.getenv("BTC_PUSH_V5_MACRO_WEEKLY_SOURCE_STALE_SEC", str(10 * 86400)),
))
ETF_SOURCE_STALE_SEC = int(os.getenv(
    "BTC_PUSH_ETF_SOURCE_STALE_SEC",
    os.getenv("BTC_PUSH_V5_ETF_SOURCE_STALE_SEC", str(36 * 3600)),
))
MACRO_FETCH_TIMEOUT_SEC = int(os.getenv(
    "BTC_PUSH_MACRO_FETCH_TIMEOUT_SEC",
    os.getenv("BTC_PUSH_V5_MACRO_FETCH_TIMEOUT_SEC", "8"),
))
FRED_FETCH_TIMEOUT_SEC = int(os.getenv("BTC_PUSH_FRED_FETCH_TIMEOUT_SEC", "20"))
TREASURY_HISTORY_MONTHS = int(os.getenv(
    "BTC_PUSH_TREASURY_HISTORY_MONTHS",
    os.getenv("BTC_PUSH_V5_TREASURY_MONTHS", "4"),
))
YAHOO_CHART_BASE_URL = os.getenv(
    "BTC_PUSH_YAHOO_CHART_BASE_URL",
    "https://query1.finance.yahoo.com/v8/finance/chart",
).rstrip("/")
YAHOO_US10Y_SYMBOL = os.getenv("BTC_PUSH_YAHOO_US10Y_SYMBOL", "^TNX").strip()
YAHOO_DXY_SYMBOL = os.getenv("BTC_PUSH_YAHOO_DXY_SYMBOL", "DX-Y.NYB").strip()
YAHOO_GOLD_SYMBOL = os.getenv("BTC_PUSH_YAHOO_GOLD_SYMBOL", "GC=F").strip()
FRED_BALANCE_SHEET_SERIES_ID = os.getenv("BTC_PUSH_FRED_BALANCE_SHEET_SERIES_ID", "WALCL").strip()
TREASURY_XML_BASE_URL = os.getenv(
    "BTC_PUSH_TREASURY_XML_BASE_URL",
    "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml",
).strip()
CME_FEDWATCH_URL = os.getenv("BTC_PUSH_CME_FEDWATCH_URL", "").strip()
FRED_CSV_URL = os.getenv(
    "BTC_PUSH_FRED_CSV_URL",
    "https://fred.stlouisfed.org/graph/fredgraph.csv",
).strip()
FRED_FED_SERIES_IDS = ("DFEDTARU", "DFEDTARL", "DFF")
FRED_M2_SERIES_ID = os.getenv("BTC_PUSH_FRED_M2_SERIES_ID", "M2SL").strip()
FRED_COST_OF_LIVING_SERIES = {
    "rent": ("CUSR0000SEHA", "房租"),
    "owners_equiv_rent": ("CUSR0000SEHC", "业主等价租金"),
    "medical": ("CPIMEDSL", "医疗"),
    "education": ("CUSR0000SEEB", "教育/学费"),
    "auto_insurance": ("CUSR0000SETD", "汽车保险"),
    "home_insurance_proxy": ("PCU5241265241262", "房屋保险代理"),
}
FRED_WAGE_SERIES_ID = os.getenv("BTC_PUSH_FRED_WAGE_SERIES_ID", "CES0500000003").strip()
FRED_FISCAL_SERIES = {
    "deficit_gdp": ("FYFSGDA188S", "财政赤字/GDP"),
    "interest_payments": ("A091RC1Q027SBEA", "联邦利息支出"),
    "federal_receipts": ("W006RC1Q027SBEA", "财政收入"),
    "debt_gdp": ("GFDEGDQ188S", "债务/GDP"),
}
FISCALDATA_API_BASE_URL = os.getenv(
    "BTC_PUSH_FISCALDATA_API_BASE_URL",
    "https://api.fiscaldata.treasury.gov/services/api/fiscal_service",
).rstrip("/")
FISCALDATA_FETCH_TIMEOUT_SEC = int(os.getenv("BTC_PUSH_FISCALDATA_FETCH_TIMEOUT_SEC", "12"))
TREASURY_BUYBACK_LOOKBACK_DAYS = int(os.getenv("BTC_PUSH_TREASURY_BUYBACK_LOOKBACK_DAYS", "14"))
TREASURY_BUYBACK_EFFECTIVE_USD = float(os.getenv("BTC_PUSH_TREASURY_BUYBACK_EFFECTIVE_USD", "2000000000"))
TREASURY_BUYBACK_STRONG_USD = float(os.getenv("BTC_PUSH_TREASURY_BUYBACK_STRONG_USD", "5000000000"))
FISCAL_INTEREST_REVENUE_STRESS_PCT = float(
    os.getenv("BTC_PUSH_FISCAL_INTEREST_REVENUE_STRESS_PCT", "15.0")
)
FISCAL_INTEREST_REVENUE_DOMINANCE_PCT = float(
    os.getenv("BTC_PUSH_FISCAL_INTEREST_REVENUE_DOMINANCE_PCT", "20.0")
)
FISCAL_DEFICIT_GDP_STRESS_PCT = float(os.getenv("BTC_PUSH_FISCAL_DEFICIT_GDP_STRESS_PCT", "-6.0"))
FISCAL_DEFICIT_GDP_DOMINANCE_PCT = float(os.getenv("BTC_PUSH_FISCAL_DEFICIT_GDP_DOMINANCE_PCT", "-7.5"))
FISCAL_DEBT_GDP_STRESS_PCT = float(os.getenv("BTC_PUSH_FISCAL_DEBT_GDP_STRESS_PCT", "120.0"))
FISCAL_LONG_AUCTION_BTC_STRESS = float(os.getenv("BTC_PUSH_FISCAL_LONG_AUCTION_BTC_STRESS", "2.40"))
COST_OF_LIVING_GAP_NEUTRAL_PP = float(os.getenv("BTC_PUSH_COST_OF_LIVING_GAP_NEUTRAL_PP", "0.50"))
ETF_SOURCE_READY_ET_HOUR = int(os.getenv("BTC_PUSH_ETF_SOURCE_READY_ET_HOUR", "20"))
ETF_SOURCE_STALE_GRACE_DAYS = int(os.getenv("BTC_PUSH_ETF_SOURCE_STALE_GRACE_DAYS", "1"))
ETF_FLOW_5D_INCONSISTENT_MIN_USD = float(
    os.getenv("BTC_PUSH_ETF_FLOW_5D_INCONSISTENT_MIN_USD", "100000000")
)
ETF_FLOW_5D_INCONSISTENT_REL = float(
    os.getenv("BTC_PUSH_ETF_FLOW_5D_INCONSISTENT_REL", "0.50")
)
US_EASTERN_TZ = ZoneInfo("America/New_York") if ZoneInfo else None
LIQUIDATION_WS_URL = os.getenv(
    "BTC_PUSH_LIQUIDATION_WS_URL",
    f"wss://fstream.binance.com/market/ws/{BINANCE_SYMBOL.lower()}@forceOrder",
).strip()
LIQUIDATION_WS_TIMEOUT_SEC = int(os.getenv("BTC_PUSH_LIQUIDATION_WS_TIMEOUT_SEC", "60"))
LIQUIDATION_WS_CONNECT_TIMEOUT_SEC = int(os.getenv("BTC_PUSH_LIQUIDATION_WS_CONNECT_TIMEOUT_SEC", "10"))
LIQUIDATION_WS_HEARTBEAT_SAVE_SEC = int(os.getenv("BTC_PUSH_LIQUIDATION_WS_HEARTBEAT_SAVE_SEC", "60"))
LIQUIDATION_COVERAGE_GAP_RESET_SEC = int(os.getenv("BTC_PUSH_LIQUIDATION_COVERAGE_GAP_RESET_SEC", "180"))
LIQUIDATION_RETENTION_H = int(os.getenv("BTC_PUSH_LIQUIDATION_RETENTION_H", "26"))
LIQUIDATION_MAX_EVENTS = int(os.getenv("BTC_PUSH_LIQUIDATION_MAX_EVENTS", "100000"))
DEFILLAMA_STABLECOINS_URL = os.getenv(
    "BTC_PUSH_DEFILLAMA_STABLECOINS_URL",
    "https://stablecoins.llama.fi/stablecoins?includePrices=true",
).strip()
STRUCTURE_LOOKBACK_H = int(os.getenv("BTC_PUSH_STRUCTURE_LOOKBACK_H", "72"))
MARKET_STRUCTURE_SHORT_LOOKBACK_H = int(os.getenv("BTC_PUSH_MARKET_STRUCTURE_SHORT_LOOKBACK_H", "24"))
MARKET_STRUCTURE_NEAR_SUPPORT_LOOKBACK_H = int(os.getenv("BTC_PUSH_MARKET_STRUCTURE_NEAR_SUPPORT_LOOKBACK_H", "12"))
MARKET_STRUCTURE_NEAR_SUPPORT_FALLBACK_H = int(os.getenv("BTC_PUSH_MARKET_STRUCTURE_NEAR_SUPPORT_FALLBACK_H", "6"))
MARKET_STRUCTURE_TREND_LOOKBACK_H = int(os.getenv("BTC_PUSH_MARKET_STRUCTURE_TREND_LOOKBACK_H", str(STRUCTURE_LOOKBACK_H)))
STRUCTURE_BREAKOUT_BUFFER_PCT = float(os.getenv("BTC_PUSH_STRUCTURE_BREAKOUT_BUFFER_PCT", "0.0015"))
STRUCTURE_BREAKOUT_BUFFER_ATR = float(os.getenv("BTC_PUSH_STRUCTURE_BREAKOUT_BUFFER_ATR", "0.2"))
STRUCTURE_OI_FAST_WINDOW_H = int(os.getenv("BTC_PUSH_STRUCTURE_OI_FAST_WINDOW_H", "4"))
STRUCTURE_OI_FAST_MIN_DELTA_PCT = float(os.getenv("BTC_PUSH_STRUCTURE_OI_FAST_MIN_DELTA_PCT", "1.0"))
STRUCTURE_OI_FAST_MIN_RISING_STEPS = int(os.getenv("BTC_PUSH_STRUCTURE_OI_FAST_MIN_RISING_STEPS", "3"))
STRUCTURE_OI_SLOW_DAY_H = int(os.getenv("BTC_PUSH_STRUCTURE_OI_SLOW_DAY_H", "24"))
STRUCTURE_OI_SLOW_TREND_H = int(os.getenv("BTC_PUSH_STRUCTURE_OI_SLOW_TREND_H", "72"))
STRUCTURE_OI_SLOW_DAY_MIN_DELTA_PCT = float(os.getenv("BTC_PUSH_STRUCTURE_OI_SLOW_DAY_MIN_DELTA_PCT", "1.5"))
STRUCTURE_OI_SLOW_TREND_MIN_DELTA_PCT = float(os.getenv("BTC_PUSH_STRUCTURE_OI_SLOW_TREND_MIN_DELTA_PCT", "0.0"))
STRUCTURE_VOLUME_FAST_WINDOW_H = int(os.getenv("BTC_PUSH_STRUCTURE_VOLUME_FAST_WINDOW_H", "4"))
STRUCTURE_VOLUME_FAST_BASELINE_H = int(os.getenv("BTC_PUSH_STRUCTURE_VOLUME_FAST_BASELINE_H", "24"))
STRUCTURE_VOLUME_FAST_MULTIPLIER = float(os.getenv("BTC_PUSH_STRUCTURE_VOLUME_FAST_MULTIPLIER", "1.3"))
STRUCTURE_VOLUME_SLOW_DAY_H = int(os.getenv("BTC_PUSH_STRUCTURE_VOLUME_SLOW_DAY_H", "24"))
STRUCTURE_VOLUME_SLOW_BASELINE_DAYS = int(os.getenv("BTC_PUSH_STRUCTURE_VOLUME_SLOW_BASELINE_DAYS", "7"))
STRUCTURE_VOLUME_SLOW_MULTIPLIER = float(os.getenv("BTC_PUSH_STRUCTURE_VOLUME_SLOW_MULTIPLIER", "1.1"))
STRUCTURE_PULLBACK_ATR = float(os.getenv("BTC_PUSH_STRUCTURE_PULLBACK_ATR", "0.5"))
STRUCTURE_FAIL_ATR = float(os.getenv("BTC_PUSH_STRUCTURE_FAIL_ATR", "0.75"))
MACRO_RATE_HIKE_US10Y_LEVEL = float(os.getenv("BTC_PUSH_MACRO_RATE_HIKE_US10Y_LEVEL", "4.80"))
MACRO_RATE_HIKE_DXY_LEVEL = float(os.getenv("BTC_PUSH_MACRO_RATE_HIKE_DXY_LEVEL", "102.0"))
MACRO_REAL_YIELD_HIGH_LEVEL = float(os.getenv("BTC_PUSH_MACRO_REAL_YIELD_HIGH_LEVEL", "2.00"))
MACRO_REAL_YIELD_UP_30D_PP = float(os.getenv("BTC_PUSH_MACRO_REAL_YIELD_UP_30D_PP", "0.10"))
MACRO_USD_UP_30D_PCT = float(os.getenv("BTC_PUSH_MACRO_USD_UP_30D_PCT", "0.50"))
MACRO_FED_HIKE_PROB_LEVEL = float(os.getenv("BTC_PUSH_MACRO_FED_HIKE_PROB_LEVEL", "35.0"))
MACRO_CYCLE_US10Y_HIGH_LEVEL = float(os.getenv("BTC_PUSH_MACRO_CYCLE_US10Y_HIGH_LEVEL", "4.00"))
MACRO_CYCLE_DXY_STRONG_LEVEL = float(os.getenv("BTC_PUSH_MACRO_CYCLE_DXY_STRONG_LEVEL", "100.0"))
MACRO_CYCLE_BALANCE_SHEET_EXPANSION_30D_PCT = float(
    os.getenv("BTC_PUSH_MACRO_CYCLE_BALANCE_SHEET_EXPANSION_30D_PCT", "0.50")
)
FUNDING_NEUTRAL_8H_ABS_PCT = float(os.getenv("BTC_PUSH_FUNDING_NEUTRAL_8H_ABS_PCT", "0.010"))
FUNDING_NEUTRAL_24H_ABS_PCT = float(os.getenv("BTC_PUSH_FUNDING_NEUTRAL_24H_ABS_PCT", "0.030"))
BASIS_NEUTRAL_ABS_PCT = float(os.getenv("BTC_PUSH_BASIS_NEUTRAL_ABS_PCT", "0.100"))
FUNDING_NEGATIVE_EXTREME_8H_PCT = float(os.getenv("BTC_PUSH_FUNDING_NEGATIVE_EXTREME_8H_PCT", "-0.050"))
FUNDING_NEGATIVE_EXTREME_24H_PCT = float(os.getenv("BTC_PUSH_FUNDING_NEGATIVE_EXTREME_24H_PCT", "-0.100"))

STATE_LOCK = threading.RLock()
_persisted_hyperliquid_minutes = {}
liquidation_collector_thread = None
hyperliquid_collector_thread = None

model = MarketModel("btc_model_rules.md")


def env_bool(name, default=False):
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


BTC_PUSH_OLLAMA_ENABLED = env_bool("BTC_PUSH_OLLAMA_ENABLED", False)
BTC_PUSH_CHECK_TG_COMMANDS = env_bool("BTC_PUSH_CHECK_TG_COMMANDS", True)
BTC_PUSH_EVIDENCE_ENABLED = env_bool("BTC_PUSH_EVIDENCE_ENABLED", False)
BTC_PUSH_TG_POLL_INTERVAL_SEC = int(os.getenv("BTC_PUSH_TG_POLL_INTERVAL_SEC", "5"))
BTC_PUSH_SCHEDULE_RETRY_DELAY_SEC = max(
    1,
    int(os.getenv("BTC_PUSH_SCHEDULE_RETRY_DELAY_SEC", "15")),
)
BTC_PUSH_SCHEDULE_MAX_RETRIES = max(
    0,
    int(os.getenv("BTC_PUSH_SCHEDULE_MAX_RETRIES", "24")),
)
BTC_PUSH_NEWS_BILINGUAL = env_bool("BTC_PUSH_NEWS_BILINGUAL", True)
BTC_PUSH_NEWS_TRANSLATE_WITH_OLLAMA = env_bool("BTC_PUSH_NEWS_TRANSLATE_WITH_OLLAMA", True)
BTC_PUSH_FEAR_GREED_ENABLED = env_bool("BTC_PUSH_FEAR_GREED_ENABLED", True)
BTC_PUSH_LIQUIDATION_WS_ENABLED = env_bool("BTC_PUSH_LIQUIDATION_WS_ENABLED", True)
BTC_PUSH_HYPERLIQUID_ENABLED = env_bool("BTC_PUSH_HYPERLIQUID_ENABLED", True)
NEWS_CN_FALLBACK_TO_EN = env_bool("BTC_PUSH_NEWS_CN_FALLBACK_TO_EN", False)

PUSH_RESULT_SENT = "sent"
PUSH_RESULT_RETRYABLE = "retryable"
PUSH_RESULT_FAILED = "failed"
PUSH_RESULT_QUEUED = "queued"
PUSH_RESULT_UNKNOWN = "unknown"

_delivery_stores = {}


class SignalDataNotReadyError(RuntimeError):
    pass

news_ollama_disabled_until_ts = 0

FEAR_GREED_CLASS_CN = {
    "Extreme Fear": "极度恐惧",
    "Fear": "恐惧",
    "Neutral": "中性",
    "Greed": "贪婪",
    "Extreme Greed": "极度贪婪",
}

FEAR_GREED_SHORT_CN = {
    "Extreme Fear": "极恐",
    "Fear": "恐惧",
    "Neutral": "中性",
    "Greed": "贪婪",
    "Extreme Greed": "极贪",
}

VALUE_SHORT_CN = {
    "UPTREND": "上升",
    "DOWNTREND": "下降",
    "DELEVERAGING": "去杠杆",
    "RANGE": "区间",
    "LOW_VOL": "低波",
    "MID_VOL": "中波",
    "HIGH_VOL": "高波",
    "EXPANDING": "扩张",
    "STABLE": "稳定",
    "CONTRACTING": "收缩",
    "WEAK": "弱",
    "NORMAL": "普通",
    "STRONG": "强",
    "EXTREME": "过热",
    "LOW": "低",
    "MID": "中",
    "HIGH": "高",
    "EXECUTE": "执行",
    "WAIT": "等待",
}


def log_line(message):
    ts = datetime.now(TOKYO_TZ).strftime("%Y-%m-%d %H:%M:%S JST")
    line = f"[{ts}] {message}\n"
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        sys.stderr.write(line)


def log_exception(context):
    log_line(f"{context}\n{traceback.format_exc().rstrip()}")


def migrate_state_schema(state):
    state = state if isinstance(state, dict) else {}

    legacy_state_keys = {
        "v4_public_snapshots": "public_snapshots",
        "v4_liquidation_ws": "liquidation_ws",
        "v5_macro_cache": "macro_cache",
        "v5_market_structure_state": "market_structure_state",
    }
    for legacy_key, canonical_key in legacy_state_keys.items():
        if canonical_key not in state and legacy_key in state:
            state[canonical_key] = state.get(legacy_key)
        state.pop(legacy_key, None)

    if not isinstance(state.get("report"), dict):
        legacy_report = state.get("report_v5") or state.get("report_v4") or state.get("report_v31") or state.get("report_v3")
        if isinstance(legacy_report, dict):
            state["report"] = legacy_report
    for key in ("report_v3", "report_v31", "report_v4", "report_v5"):
        state.pop(key, None)

    trend = state.get(PRICE_TREND_STATE_KEY)
    trend_schema = int((trend or {}).get("schema_version") or 1) if isinstance(trend, dict) else 1
    if isinstance(trend, dict) and trend_schema in (2, 3, 4):
        trend = dict(trend)
        anchor = safe_float(trend.get("support_anchor"))
        levels = trend.get("levels") or {}
        trend["schema_version"] = PRICE_TREND_STATE_SCHEMA_REVISION
        if trend_schema == 2 and "support_anchor_source" not in trend:
            trend["support_anchor_source"] = (
                "migrated_v2_anchor" if anchor is not None else None
            )
        trend["rolling_short_support"] = (
            safe_float(trend.get("rolling_short_support"))
            or safe_float(levels.get("rolling_short_support"))
            or safe_float(levels.get("short_support"))
        )
        trend["support_confirmation_sources"] = list(
            trend.get("support_confirmation_sources") or []
        )
        trend.setdefault("support_breakdown_confirmed_bucket", None)
        if trend.get("status") == "breakdown_confirmed":
            trend["status_text"] = "原支撑失守状态持续"
            trend["support_state_text"] = "原支撑失守持续"
        trend.setdefault("support_test_anchor", None)
        trend.setdefault("support_test_formed_bucket", None)
        trend.setdefault("support_test_first_retest_bucket", None)
        trend.setdefault("support_test_phase", "inactive")
        trend.setdefault("post_support_phase", "inactive")
        state[PRICE_TREND_STATE_KEY] = trend
    return state


def load_state():
    # 只读兼容旧布局；旧状态转换必须通过显式 --migrate-state。
    return migrate_state_schema(state_store().load())


def state_store():
    # STATE_FILE 可由环境或隔离测试替换，侧车路径不能在导入时固化。
    return StateStore(assets.state_path(STATE_FILE))


def save_state(state, reset_collectors=False):
    started = time.perf_counter()
    state_store().save(state, pruners={
        "liquidation_ws": prune_liquidation_ws_state,
        "hyperliquid_derivatives": prune_hyperliquid_state,
    }, reset_collectors=reset_collectors)
    log_line(f"state_write duration_ms={(time.perf_counter() - started) * 1000:.3f}")


def recycle_state(state):
    snaps = state.get("snapshots") or []
    if len(snaps) > MAX_SNAPSHOTS:
        state["snapshots"] = snaps[-MAX_SNAPSHOTS:]

    bars = state.get("bars_1h") or []
    bar_limit = 1000 if assets.current().equity else BINANCE_BARS_LIMIT
    if len(bars) > bar_limit:
        state["bars_1h"] = bars[-bar_limit:]

    public_snaps = state.get("public_snapshots") or []
    if len(public_snaps) > PUBLIC_SNAPSHOT_MAX:
        state["public_snapshots"] = public_snaps[-PUBLIC_SNAPSHOT_MAX:]

    liq_state = state.get("liquidation_ws")
    if isinstance(liq_state, dict):
        state["liquidation_ws"] = prune_liquidation_ws_state(liq_state)

    hyperliquid_state = state.get("hyperliquid_derivatives")
    if isinstance(hyperliquid_state, dict):
        state["hyperliquid_derivatives"] = prune_hyperliquid_state(hyperliquid_state)

    now_ts = int(time.time())
    logs = state.get("event_logs") or []
    if logs:
        cutoff = now_ts - MAX_EVENT_LOG_DAYS * 86400
        state["event_logs"] = [x for x in logs if int(x.get("ts", 0) or 0) >= cutoff]

    news_seen = state.get("news_seen") or {}
    state["news_seen"] = {
        key: ts for key, ts in news_seen.items()
        if now_ts - int(ts or 0) < NEWS_DEDUPE_TTL_SEC
    }

    cache = state.get("ollama_summary_cache") or {}
    if cache and now_ts - int(cache.get("ts", 0) or 0) > OLLAMA_CACHE_TTL_SEC:
        state.pop("ollama_summary_cache", None)

    title_cache = state.get("news_title_cn_cache") or {}
    state["news_title_cn_cache"] = {
        key: item for key, item in title_cache.items()
        if (
            (item or {}).get("ver") == NEWS_TRANSLATION_CACHE_VERSION
            and now_ts - int((item or {}).get("ts", 0) or 0) < NEWS_TRANSLATION_CACHE_TTL_SEC
        )
    }


def parse_tg_command_text(text):
    return assets.parse_command(text)


def _tg_command_chat_id():
    if not BTC_PUSH_CHECK_TG_COMMANDS:
        return None
    if not BOT_TOKEN or not CHAT_ID_RAW:
        return None
    try:
        return int(CHAT_ID_RAW)
    except (TypeError, ValueError):
        return None


def _fetch_tg_updates(offset):
    """只拉取更新；接收 offset 的持久化由调用方事务负责。"""
    if _tg_command_chat_id() is None:
        return []
    params = {"timeout": 0, "limit": 10}
    if offset > 0:
        params["offset"] = offset
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
    try:
        r = requests.get(url, params=params, timeout=(3, max(1, TG_COMMAND_TIMEOUT_SEC)))
        if r.status_code != 200:
            log_line(f"telegram getUpdates skipped status={r.status_code}")
            return []
        data = r.json() or {}
        if not isinstance(data, dict) or data.get("ok") is not True or not isinstance(data.get("result"), list):
            return []
        return data["result"]
    except Exception as exc:
        log_line(f"telegram command check skipped error={type(exc).__name__}")
        return []


def check_tg_command_and_update_offset(state):
    """旧调用的纯内存兼容入口；daemon 使用持久命令队列。"""
    chat_id = _tg_command_chat_id()
    if chat_id is None:
        return None
    offset = int(state.get("tg_update_offset", 0) or 0)
    cmd = None
    max_update_id = None
    for item in _fetch_tg_updates(offset):
        if not isinstance(item, dict):
            continue
        update_id = item.get("update_id")
        if type(update_id) is int and update_id >= offset:
            max_update_id = update_id if max_update_id is None else max(max_update_id, update_id)
        msg = item.get("message") or item.get("channel_post") or {}
        if not isinstance(msg, dict):
            continue
        chat = msg.get("chat") or {}
        if not isinstance(chat, dict) or chat.get("id") != chat_id or not isinstance(msg.get("text"), str):
            continue
        parsed_cmd = parse_tg_command_text(msg.get("text") or "")
        if parsed_cmd:
            cmd = parsed_cmd
    if max_update_id is not None:
        state["tg_update_offset"] = int(max_update_id) + 1
    return cmd


def get_delivery_store():
    key = delivery_store_key()
    path = key[1]
    with STATE_LOCK:
        if key not in _delivery_stores:
            store = DeliveryStore(path, clock=lambda: time.time(),
                                  max_retries=int(os.getenv("BTC_PUSH_DELIVERY_MAX_RETRIES", "2")),
                                  retry_delay=float(os.getenv("BTC_PUSH_DELIVERY_RETRY_DELAY_SEC", "30")))
            store.recover()
            _delivery_stores[key] = store
    return _delivery_stores[key]


def delivery_store_key():
    path = os.path.abspath(os.path.expanduser(os.getenv("BTC_PUSH_DELIVERY_DB", STATE_FILE + ".delivery.sqlite3")))
    return os.getpid(), path


def macro_cache_identity():
    return (MACRO_CACHE_SCHEMA_REVISION, YAHOO_CHART_BASE_URL, os.path.abspath(STATE_FILE))


def initialize_common_macro(store):
    if store.load_metadata("common:macro") is not None:
        return
    candidates = []
    for asset in assets.INSTRUMENTS:
        with assets.asset_scope(asset):
            try:
                with open(assets.state_path(STATE_FILE), encoding="utf-8") as stream:
                    record = (json.load(stream).get("macro_cache") or {})
            except FileNotFoundError:
                continue
            if (record_is_current(record) and record.get("schema_version") == MACRO_CACHE_SCHEMA_REVISION
                    and 0 < float(record.get("updated_at") or 0) <= time.time()):
                candidates.append(record)
    if candidates:
        record = max(candidates, key=lambda value: float(value.get("updated_at") or 0))
        store.save_metadata("common:macro", {"identity": list(macro_cache_identity()), "hour": None, "record": record})


def initialize_delivery():
    # main() 已取得 report writer 锁；发送恢复另取 worker flock。
    get_delivery_store().recover(recover_commands=True)
    initialize_common_macro(get_delivery_store())


def poll_tg_command():
    chat_id = _tg_command_chat_id()
    if chat_id is None:
        return None
    store = get_delivery_store()
    queued = store.claim_command()
    if queued:
        return queued
    offset = store.get_offset(lambda: load_state().get("tg_update_offset", 0))
    store.ingest_commands(_fetch_tg_updates(offset), chat_id, parse_tg_command_text)
    return store.claim_command()


def snapshot_hour_key(ts):
    return datetime.fromtimestamp(int(ts), TOKYO_TZ).strftime("%Y-%m-%d %H:00")


def append_snapshot(state, snap):
    snap = dict(snap)
    snap["hour"] = snapshot_hour_key(snap["ts"])
    arr = state.get("snapshots") or []
    out = []
    for item in arr:
        item_ts = int(item.get("ts", 0) or 0)
        if not item_ts:
            continue
        item_hour = item.get("hour") or snapshot_hour_key(item_ts)
        if item_hour == snap["hour"]:
            continue
        item = dict(item)
        item["hour"] = item_hour
        out.append(item)
    out.append(snap)
    out.sort(key=lambda x: int(x.get("ts", 0) or 0))
    state["snapshots"] = out[-MAX_SNAPSHOTS:]


def append_bars_1h(state, bars):
    merged = {}
    for item in state.get("bars_1h") or []:
        try:
            merged[int(item.get("open_time"))] = dict(item)
        except Exception:
            continue
    for item in bars or []:
        try:
            merged[int(item.get("open_time"))] = dict(item)
        except Exception:
            continue
    out = [merged[key] for key in sorted(merged)]
    state["bars_1h"] = out[-BINANCE_BARS_LIMIT:]


def binance_get(path, params=None):
    url = f"{BINANCE_FUTURES_BASE_URL}{path}"
    headers = {
        "User-Agent": f"btcpush/{APP_ID}",
        "Accept": "application/json",
    }
    r = public_io.get(
        url,
        params=params or {},
        headers=headers,
        timeout=(3, max(1, BINANCE_FETCH_TIMEOUT_SEC)),
    )
    r.raise_for_status()
    return r.json()


def binance_spot_get(path, params=None):
    url = f"{BINANCE_SPOT_BASE_URL}{path}"
    headers = {
        "User-Agent": f"btcpush/{APP_ID}",
        "Accept": "application/json",
    }
    r = public_io.get(
        url,
        params=params or {},
        headers=headers,
        timeout=(3, max(1, BINANCE_FETCH_TIMEOUT_SEC)),
    )
    r.raise_for_status()
    return r.json()


def public_get_json(url, params=None):
    headers = {
        "User-Agent": f"btcpush/{APP_ID}",
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    }
    r = public_io.get(
        url,
        params=params or {},
        headers=headers,
        timeout=(3, max(1, BINANCE_FETCH_TIMEOUT_SEC)),
    )
    r.raise_for_status()
    return r.json()


def public_get_text(url, params=None, timeout_sec=None, retry_read_timeout=False):
    headers = {
        "User-Agent": f"btcpush/{APP_ID}",
        "Accept": "text/csv,text/plain,*/*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    }
    r = public_io.get(
        url,
        params=params or {},
        headers=headers,
        timeout=(3, max(1, int(timeout_sec or BINANCE_FETCH_TIMEOUT_SEC))),
        retry_read_timeout=retry_read_timeout,
    )
    r.raise_for_status()
    return r.text


def public_post_json(url, payload=None):
    headers = {
        "User-Agent": f"btcpush/{APP_ID}",
        "Accept": "application/json,text/plain,*/*",
        "Content-Type": "application/json",
    }
    r = public_io.post(
        url,
        json=payload or {},
        headers=headers,
        timeout=(3, max(1, BINANCE_FETCH_TIMEOUT_SEC)),
    )
    r.raise_for_status()
    return r.json()


def hyperliquid_post_json(payload):
    headers = {
        "User-Agent": f"btcpush/{APP_ID}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    r = public_io.post(
        HYPERLIQUID_INFO_URL,
        json=payload or {},
        headers=headers,
        timeout=(3, max(1, HYPERLIQUID_FETCH_TIMEOUT_SEC)),
    )
    r.raise_for_status()
    return r.json()


def fetch_hyperliquid_asset_ctx_rest():
    payload = hyperliquid_post_json({"type": "metaAndAssetCtxs"})
    return hyperliquid_asset_ctx_from_meta_response(payload)


def get_binance_24h_market():
    data = binance_spot_get("/api/v3/ticker/24hr", {"symbol": BINANCE_SYMBOL})
    return {
        "price": safe_float(data.get("lastPrice")),
        "chg24": safe_float(data.get("priceChangePercent")),
        "vol24": safe_float(data.get("quoteVolume")),
        "quote_source_as_of_ms": epoch_ms(data.get("closeTime")),
        "quote_window_start_ms": epoch_ms(data.get("openTime")),
        "quote_retrieved_at": time.time(),
        "quote_endpoint_url": f"{BINANCE_SPOT_BASE_URL}/api/v3/ticker/24hr",
    }

def get_display_market():
    if assets.current().equity:
        return assets.stock_quote()
    quote = None
    if BTC_PUSH_MARKET_SOURCE == "binance":
        try:
            quote = get_binance_24h_market()
            if finite_metric(quote.get("price"), 0) is None:
                raise ValueError("missing_market_price")
        except Exception:
            log_exception("binance market skipped; fallback to coingecko")
            quote = None
    cached = public_io.cache_get("coingecko_market_cap", 3600) if quote else None
    cg_price = cg_mcap = cg_chg24 = cg_vol24 = None
    retrieved_at = None
    if cached is not None:
        cg_mcap, retrieved_at = cached["value"], cached["retrieved_at"]
    else:
        try:
            cg_price, cg_mcap, cg_chg24, cg_vol24 = get_btc_market()
            retrieved_at = time.time()
            if cg_mcap is not None:
                public_io.cache_put("coingecko_market_cap", cg_mcap, now=retrieved_at)
        except Exception:
            log_exception("coingecko market skipped")
    endpoint = "https://api.coingecko.com/api/v3/coins/bitcoin"
    if quote is None:
        if finite_metric(cg_price, 0) is None:
            raise RuntimeError("market_data_unavailable")
        quote = {"price": cg_price, "chg24": cg_chg24, "vol24": cg_vol24,
                 "quote_source_as_of_ms": None, "quote_retrieved_at": retrieved_at,
                 "quote_endpoint_url": endpoint}
        source = "CoinGecko"
    else:
        source = "Binance Spot 24hr"
    return {**quote, "source": source, "mcap": cg_mcap,
            "mcap_source": "CoinGecko" if cg_mcap is not None else "N/A",
            "mcap_source_as_of_ms": None, "mcap_retrieved_at": retrieved_at,
            "mcap_endpoint_url": endpoint, "mcap_cache_used": cached is not None,
            "mcap_cache_age_sec": cached["age_sec"] if cached else 0}


def fetch_binance_klines(limit=None, symbol=None):
    symbol = symbol or BINANCE_SYMBOL
    target = int(time.time() // 3600) * 3600000 - 1
    key = ("closed_klines", BINANCE_FUTURES_BASE_URL, symbol, BINANCE_INTERVAL, limit, target)
    cached = public_io.cache_get(key, 3600)
    if cached is not None:
        return cached["value"]
    raw = binance_get(
        "/fapi/v1/klines",
        {
            "symbol": symbol,
            "interval": BINANCE_INTERVAL,
            "limit": min(1500, int(limit or BINANCE_BARS_LIMIT) + 1),
        },
    )
    now_ms = int(time.time() * 1000)
    bars = []
    for item in raw or []:
        if len(item) < 11:
            continue
        close_time = int(item[6])
        if close_time > now_ms:
            continue
        bars.append({
            "symbol": symbol,
            "interval": BINANCE_INTERVAL,
            "open_time": int(item[0]),
            "open": float(item[1]),
            "high": float(item[2]),
            "low": float(item[3]),
            "close": float(item[4]),
            "volume": float(item[5]),
            "close_time": close_time,
            "quote_volume": float(item[7]),
            "trade_count": int(item[8]),
            "taker_buy_volume": float(item[9]),
            "taker_buy_quote_volume": float(item[10]),
            "source": "Binance Futures Kline",
            "retrieved_at_ms": now_ms,
            "endpoint_url": f"{BINANCE_FUTURES_BASE_URL}/fapi/v1/klines",
        })
    bars = sorted(bars, key=lambda row: row["close_time"])[-int(limit or BINANCE_BARS_LIMIT):]
    if (len(bars) >= 9 and bars[-1]["close_time"] == target and bars_are_contiguous(bars)
            and all(finite_metric(row.get("close"), 0) is not None for row in bars)):
        public_io.cache_put(key, bars)
    return bars


def fetch_binance_oi_hist(limit=None, symbol=None):
    symbol = symbol or BINANCE_SYMBOL
    boundary = int(time.time() // 3600) * 3600000
    key = ("closed_oi", BINANCE_FUTURES_BASE_URL, symbol, BINANCE_INTERVAL, limit, boundary)
    cached = public_io.cache_get(key, 3600)
    if cached is not None:
        return cached["value"]
    raw = binance_get(
        "/futures/data/openInterestHist",
        {
            "symbol": symbol,
            "period": BINANCE_INTERVAL,
            "limit": int(limit or BINANCE_BARS_LIMIT),
        },
    )
    retrieved_at_ms = int(time.time() * 1000)
    out = []
    for item in raw or []:
        try:
            out.append({
                "timestamp": int(item.get("timestamp")),
                "retrieved_at_ms": retrieved_at_ms,
                "endpoint_url": f"{BINANCE_FUTURES_BASE_URL}/futures/data/openInterestHist",
                "oi": float(item.get("sumOpenInterest")),
                "oi_value": float(item.get("sumOpenInterestValue")),
                "cmc_supply": float(item.get("CMCCirculatingSupply") or 0.0),
            })
        except Exception:
            continue
    out.sort(key=lambda x: int(x.get("timestamp") or 0))
    recent = out[-9:]
    if (len(recent) == 9 and abs(recent[-1]["timestamp"] - boundary) <= BINANCE_OI_MAX_DRIFT_SEC * 1000
            and all(b["timestamp"] - a["timestamp"] == 3600000 for a, b in zip(recent, recent[1:]))
            and all(finite_metric(row.get("oi"), 0) is not None for row in recent)):
        public_io.cache_put(key, out)
    return out


def merge_oi_into_bars(bars, oi_hist):
    if not bars:
        return []
    if not oi_hist:
        return [dict(item) for item in bars]

    oi_items = sorted(oi_hist, key=lambda x: int(x.get("timestamp") or 0))
    merged = []
    max_drift_ms = max(1, BINANCE_OI_MAX_DRIFT_SEC) * 1000
    for bar in bars:
        close_time = int(bar.get("close_time") or bar.get("open_time") or 0)
        best = min(oi_items, key=lambda x: abs(int(x.get("timestamp") or 0) - close_time))
        drift = abs(int(best.get("timestamp") or 0) - close_time)
        item = dict(bar)
        if drift <= max_drift_ms:
            item["oi"] = best.get("oi")
            item["oi_value"] = best.get("oi_value")
            item["cmc_supply"] = best.get("cmc_supply")
            item["oi_timestamp"] = best.get("timestamp")
            item["oi_source_as_of_ms"] = best.get("timestamp")
            item["oi_time_basis"] = "source_observation"
            item["oi_retrieved_at_ms"] = best.get("retrieved_at_ms")
            item["oi_endpoint_url"] = best.get("endpoint_url")
            item["oi_drift_ms"] = drift
            item["oi_aligned"] = True
        else:
            item["oi_drift_ms"] = drift
            item["oi_aligned"] = False
        merged.append(item)
    return merged


def get_signal_bars(state):
    if assets.current().equity:
        return get_equity_signal_bars(state)
    cached = state.get("bars_1h") or []
    if BTC_PUSH_SIGNAL_SOURCE != "binance_futures":
        return cached
    try:
        bars = fetch_binance_klines(BINANCE_BARS_LIMIT)
        if HYPERLIQUID_MODE == "primary":
            bars = merge_hyperliquid_oi_into_bars(bars, state)
        else:
            oi_hist = fetch_binance_oi_hist(BINANCE_BARS_LIMIT)
            bars = merge_oi_into_bars(bars, oi_hist)
            for item in bars:
                item["signal_source"] = "Binance Futures 1h Kline + OI Hist"
                item["oi_source"] = "Binance openInterestHist"
        append_bars_1h(state, bars)
        selected = state.get("bars_1h") or []
        if HYPERLIQUID_MODE == "primary":
            selected = [
                item for item in selected
                if item.get("oi_source") == HYPERLIQUID_SOURCE
            ]
        return selected
    except Exception:
        log_exception("signal bars skipped; fallback to same-source cached bars")
        if HYPERLIQUID_MODE == "primary":
            return [item for item in cached if item.get("oi_source") == HYPERLIQUID_SOURCE]
        return cached


def build_signal_data_quality(bars, current_ms=None):
    if assets.current().equity:
        return assets.stock_quality(bars, current_ms)
    ordered = sorted(
        [item for item in (bars or []) if int(item.get("open_time") or 0) > 0],
        key=lambda item: int(item.get("open_time") or 0),
    )
    current_ms = int(current_ms or time.time() * 1000)
    latest = ordered[-1] if ordered else {}
    latest_close_ms = int(latest.get("close_time") or 0)
    age_sec = max(0.0, (current_ms - latest_close_ms) / 1000.0) if latest_close_ms else None
    recent = ordered[-9:]
    contiguous = len(recent) == 9 and bars_are_contiguous(recent)
    oi_aligned = bool(recent) and all(
        item.get("oi_aligned") is not False
        and finite_metric(item.get("oi"), 0.0) is not None
        and int(item.get("oi_drift_ms") or 0)
        <= (
            HYPERLIQUID_ALIGN_MAX_DRIFT_SEC
            if item.get("oi_source") == HYPERLIQUID_SOURCE
            else BINANCE_OI_MAX_DRIFT_SEC
        ) * 1000
        for item in recent
    )
    hyperliquid_primary_ready = not ordered or all(
        item.get("oi_source") != HYPERLIQUID_SOURCE
        or item.get("oi_primary_ready") is True
        for item in recent
    )
    fresh = age_sec is not None and age_sec <= SIGNAL_BAR_STALE_SEC
    reasons = []
    if not ordered:
        reasons.append("no_closed_bars")
    if ordered and not fresh:
        reasons.append("latest_bar_stale")
    if ordered and not contiguous:
        reasons.append("recent_bars_not_contiguous")
    if ordered and not oi_aligned:
        reasons.append("oi_not_aligned")
    if ordered and not hyperliquid_primary_ready:
        reasons.append("hyperliquid_oi_warmup")
    source = latest.get("signal_source") or "Binance Futures 1h Kline + OI Hist"
    return {
        "usable": bool(
            ordered
            and fresh
            and contiguous
            and oi_aligned
            and hyperliquid_primary_ready
        ),
        "fresh": fresh,
        "contiguous_4h": contiguous,
        "oi_aligned_4h": oi_aligned,
        "hyperliquid_primary_ready": hyperliquid_primary_ready,
        "latest_close_ms": latest_close_ms or None,
        "age_sec": age_sec,
        "loaded_bars": len(ordered),
        "source": source,
        "reasons": reasons,
    }


def pct(now_value, prev_value):
    try:
        prev = float(prev_value)
        if not prev:
            return None
        return (float(now_value) - prev) / prev * 100.0
    except Exception:
        return None


def calc_delta_4h(state):
    arr = [
        s for s in (state.get("snapshots") or [])
        if int(s.get("ts", 0) or 0) > 0
    ]
    arr.sort(key=lambda x: int(x.get("ts", 0) or 0))
    if len(arr) < 2:
        return None

    now = arr[-1]
    now_ts = int(now.get("ts", 0) or 0)
    target_ts = now_ts - DELTA_TARGET_SEC
    candidates = arr[:-1]
    prev = min(candidates, key=lambda x: abs(int(x.get("ts", 0) or 0) - target_ts))
    prev_ts = int(prev.get("ts", 0) or 0)
    drift = abs(prev_ts - target_ts)
    if drift > DELTA_TOLERANCE_SEC:
        return None

    return {
        "price_4h_pct": pct(now.get("price"), prev.get("price")),
        "oi_4h_pct": pct(now.get("oi_usd"), prev.get("oi_usd")),
        "vol_4h_pct": pct(now.get("vol24"), prev.get("vol24")),
        "window_sec": now_ts - prev_ts,
        "prev_tokyo": prev.get("tokyo"),
    }


def find_snapshot_near(state, target_sec, tolerance_sec):
    arr = [
        s for s in (state.get("snapshots") or [])
        if int(s.get("ts", 0) or 0) > 0
    ]
    arr.sort(key=lambda x: int(x.get("ts", 0) or 0))
    if len(arr) < 2:
        return None

    now = arr[-1]
    now_ts = int(now.get("ts", 0) or 0)
    target_ts = now_ts - int(target_sec)
    candidates = arr[:-1]
    prev = min(candidates, key=lambda x: abs(int(x.get("ts", 0) or 0) - target_ts))
    prev_ts = int(prev.get("ts", 0) or 0)
    drift = abs(prev_ts - target_ts)
    if drift > int(tolerance_sec):
        return None

    prev_index = arr.index(prev)
    return {
        "now": now,
        "prev": prev,
        "prev_snapshots": arr[:prev_index + 1],
        "window_sec": now_ts - prev_ts,
        "drift_sec": drift,
    }


def build_7d_comparison(state, market, model_state, fear_greed):
    result = {
        "available": False,
        "market": {},
        "factors": {},
        "fear_greed": None,
    }

    snap_pair = find_snapshot_near(state, SEVEN_DAY_TARGET_SEC, SEVEN_DAY_TOLERANCE_SEC)
    if snap_pair:
        prev = snap_pair["prev"]
        now = snap_pair["now"]
        result["available"] = True
        result["window_days"] = round(float(snap_pair["window_sec"]) / 86400.0, 1)
        result["prev_tokyo"] = prev.get("tokyo")
        result["market"] = {
            "price": {"prev": prev.get("price"), "now": market.get("price") or now.get("price")},
            "oi_usd": {"prev": prev.get("oi_usd"), "now": market.get("oi_usd") or now.get("oi_usd")},
        }
        if prev.get("source") and prev.get("source") == market.get("source"):
            result["market"]["vol24"] = {"prev": prev.get("vol24"), "now": market.get("vol24") or now.get("vol24")}
        if prev.get("mcap") is not None:
            result["market"]["mcap"] = {"prev": prev.get("mcap"), "now": market.get("mcap")}
        if prev.get("chg24") is not None:
            result["market"]["chg24"] = {"prev": prev.get("chg24"), "now": market.get("chg24")}

        prev_delta = calc_delta_4h({"snapshots": snap_pair["prev_snapshots"]})
        comparison_model = MarketModel(parameter=assets.current().parameter) if assets.current().equity else model
        prev_model_state = comparison_model.build_state_from_delta(prev_delta, snapshots=snap_pair["prev_snapshots"])
        factor_keys = [
            "long_term_trend",
            "trend",
            "volatility",
            "liquidity",
            "tq",
            "risk",
            "execution",
        ]
        result["factors"] = {
            "prev": {key: prev_model_state.get(key) for key in factor_keys},
            "now": {key: model_state.get(key) for key in factor_keys},
        }

    history = [
        item for item in ((fear_greed or {}).get("history") or [])
        if int(item.get("value", -1)) >= 0
    ]
    if len(history) >= 2:
        result["available"] = True
        result["fear_greed"] = {
            "prev": history[0],
            "now": history[-1],
            "days": len(history) - 1,
        }
    return result


def build_snapshot_market_24h_delta(state, market):
    snap_pair = find_snapshot_near(state, MARKET_24H_TARGET_SEC, MARKET_24H_TOLERANCE_SEC)
    if not snap_pair:
        return {"available": False, "metrics": {}, "sources": {}}

    prev = snap_pair["prev"]
    now = snap_pair["now"]
    metrics = {}
    sources = {}
    for key in ("mcap", "vol24", "oi_usd"):
        now_value = market.get(key) if market.get(key) is not None else now.get(key)
        metrics[key] = pct(now_value, prev.get(key))
        if metrics[key] is not None:
            sources[key] = "snapshot"

    return {
        "available": True,
        "window_hours": round(float(snap_pair["window_sec"]) / 3600.0, 1),
        "prev_tokyo": prev.get("tokyo"),
        "metrics": metrics,
        "sources": sources,
    }


def build_exchange_market_24h_delta(signal_bars, market):
    bars = [
        item for item in (signal_bars or [])
        if int(item.get("open_time", 0) or 0) > 0
    ]
    bars.sort(key=lambda x: int(x.get("open_time") or 0))
    metrics = {}
    sources = {}
    if assets.current().equity and (not bars or bars[-1]["close_time"] != int(time.time() // 3600) * assets.HOUR - 1):
        return {"available": False, "metrics": {}, "sources": {}, "window_hours": None}

    if len(bars) >= 25 and (not assets.current().equity or bars[-1]["open_time"] - bars[-25]["open_time"] == 24 * assets.HOUR):
        latest = bars[-1]
        prev_24h = bars[-25]

        oi_now = latest.get("oi_value") if assets.current().equity else market.get("oi_usd") or latest.get("oi_value")
        oi_prev = prev_24h.get("oi_value")
        oi_delta = pct(oi_now, oi_prev)
        if oi_delta is not None:
            metrics["oi_usd"] = oi_delta
            sources["oi_usd"] = "binance_open_interest_hist"

        now_supply = latest.get("cmc_supply")
        prev_supply = prev_24h.get("cmc_supply")
        if now_supply and prev_supply:
            mcap_now = float(market.get("price") or latest.get("close") or 0.0) * float(now_supply)
            mcap_prev = float(prev_24h.get("close") or 0.0) * float(prev_supply)
            mcap_delta = pct(mcap_now, mcap_prev)
            if mcap_delta is not None:
                metrics["mcap"] = mcap_delta
                sources["mcap"] = "binance_price_x_cmc_supply"

    if len(bars) >= 48 and bars_are_contiguous(bars[-48:]):
        volumes = [finite_metric(bar_volume(item), 0.0) for item in bars[-48:]]
        vol_delta = (
            pct(sum(volumes[-24:]), sum(volumes[:24]))
            if all(value is not None for value in volumes) else None
        )
        if vol_delta is not None:
            metrics["vol24"] = vol_delta
            sources["vol24"] = "binance_stocks_share_volume" if assets.current().equity else "binance_klines_quote_volume"

    return {
        "available": bool(metrics),
        "metrics": metrics,
        "sources": sources,
        "window_hours": 24.0 if metrics else None,
    }


def build_market_24h_delta(state, market, signal_bars=None):
    exchange = build_exchange_market_24h_delta(signal_bars, market)
    if assets.current().equity:
        return exchange
    snapshot = build_snapshot_market_24h_delta(state, market)

    metrics = {}
    sources = {}
    for key in ("mcap", "vol24", "oi_usd"):
        value = (exchange.get("metrics") or {}).get(key)
        source = (exchange.get("sources") or {}).get(key)
        if value is None:
            value = (snapshot.get("metrics") or {}).get(key)
            source = (snapshot.get("sources") or {}).get(key)
        if value is not None:
            metrics[key] = value
            sources[key] = source or "unknown"

    return {
        "available": bool(metrics),
        "window_hours": exchange.get("window_hours") or snapshot.get("window_hours"),
        "prev_tokyo": snapshot.get("prev_tokyo"),
        "metrics": metrics,
        "sources": sources,
        "fallback_used": any(source == "snapshot" for source in sources.values()),
    }


def safe_float(value, default=None):
    try:
        if value is None:
            return default
        number = float(value)
        return number if math.isfinite(number) else default
    except Exception:
        return default


def now_ms():
    return int(time.time() * 1000)


def hyperliquid_asset_ctx_from_meta_response(payload, coin=None):
    coin = (coin or HYPERLIQUID_COIN).strip().upper()
    if not isinstance(payload, list) or len(payload) < 2:
        raise ValueError("hyperliquid metaAndAssetCtxs response shape invalid")
    meta = payload[0] or {}
    contexts = payload[1] or []
    universe = meta.get("universe") or []
    for index, item in enumerate(universe):
        if str((item or {}).get("name") or "").upper() != coin:
            continue
        if index >= len(contexts) or not isinstance(contexts[index], dict):
            raise ValueError(f"hyperliquid asset context missing coin={coin}")
        return dict(contexts[index])
    raise ValueError(f"hyperliquid coin missing coin={coin}")


def hyperliquid_asset_ctx_from_ws_message(message, coin=None):
    coin = (coin or HYPERLIQUID_COIN).strip().upper()
    if not isinstance(message, dict) or message.get("channel") != "activeAssetCtx":
        return None
    data = message.get("data") or {}
    if str(data.get("coin") or "").upper() != coin:
        return None
    ctx = data.get("ctx") or {}
    return dict(ctx) if isinstance(ctx, dict) else None


def normalize_hyperliquid_ctx(ctx, received_at_ms=None):
    received_at_ms = int(now_ms() if received_at_ms is None else received_at_ms)
    if received_at_ms <= 0:
        raise ValueError("hyperliquid receive time invalid")
    ctx = ctx or {}
    open_interest = safe_float(ctx.get("openInterest"))
    mark_price = safe_float(ctx.get("markPx"))
    oracle_price = safe_float(ctx.get("oraclePx"))
    if (
        open_interest is None
        or not math.isfinite(open_interest)
        or open_interest < 0
        or mark_price is None
        or not math.isfinite(mark_price)
        or mark_price <= 0
    ):
        raise ValueError("hyperliquid BTC context missing finite OI/mark price")
    funding_rate = safe_float(ctx.get("funding"))
    premium = safe_float(ctx.get("premium"))
    return {
        "minute_ms": received_at_ms - (received_at_ms % 60000),
        "received_at_ms": received_at_ms,
        "coin": HYPERLIQUID_COIN,
        "open_interest_btc": open_interest,
        "mark_px": mark_price,
        "oracle_px": oracle_price,
        "open_interest_usd": open_interest * mark_price,
        "funding_rate": funding_rate if funding_rate is None or math.isfinite(funding_rate) else None,
        "premium": premium if premium is None or math.isfinite(premium) else None,
        "source": HYPERLIQUID_SOURCE,
        "scope": "Hyperliquid BTC perpetual",
    }


def prune_hyperliquid_state(derivatives_state, current_ms=None):
    if not isinstance(derivatives_state, dict):
        return {}
    current_ms = int(current_ms or now_ms())
    previous_stream = derivatives_state.get("stream")
    previous_coin = str(derivatives_state.get("coin") or "").upper()
    if (
        (previous_stream and previous_stream != HYPERLIQUID_WS_URL)
        or (previous_coin and previous_coin != HYPERLIQUID_COIN)
    ):
        return {
            "schema_version": HYPERLIQUID_STATE_SCHEMA_REVISION,
            "mode": HYPERLIQUID_MODE,
            "stream": HYPERLIQUID_WS_URL,
            "coin": HYPERLIQUID_COIN,
            "minute_samples": [],
            "stream_reset_at_ms": current_ms,
        }
    cutoff_ms = current_ms - HYPERLIQUID_SAMPLE_RETENTION_H * 3600 * 1000
    by_minute = {}
    for raw in derivatives_state.get("minute_samples") or []:
        try:
            sample = normalize_hyperliquid_ctx(
                {
                    "openInterest": raw.get("open_interest_btc"),
                    "markPx": raw.get("mark_px"),
                    "oraclePx": raw.get("oracle_px"),
                    "funding": raw.get("funding_rate"),
                    "premium": raw.get("premium"),
                },
                received_at_ms=int(raw.get("received_at_ms") or 0),
            )
        except Exception:
            continue
        if sample["received_at_ms"] < cutoff_ms:
            continue
        minute_ms = int(sample["minute_ms"])
        previous = by_minute.get(minute_ms)
        if previous is None or sample["received_at_ms"] > previous["received_at_ms"]:
            by_minute[minute_ms] = sample
    derivatives_state["schema_version"] = HYPERLIQUID_STATE_SCHEMA_REVISION
    derivatives_state["mode"] = HYPERLIQUID_MODE
    derivatives_state["stream"] = HYPERLIQUID_WS_URL
    derivatives_state["coin"] = HYPERLIQUID_COIN
    derivatives_state["minute_samples"] = [by_minute[key] for key in sorted(by_minute)]
    return derivatives_state


def append_hyperliquid_minute_sample(state, ctx, received_at_ms=None):
    current_ms = int(received_at_ms or now_ms())
    derivatives_state = prune_hyperliquid_state(
        state.get("hyperliquid_derivatives") or {},
        current_ms,
    )
    sample = normalize_hyperliquid_ctx(ctx, current_ms)
    samples = list(derivatives_state.get("minute_samples") or [])
    samples = [item for item in samples if int(item.get("minute_ms") or 0) != sample["minute_ms"]]
    samples.append(sample)
    samples.sort(key=lambda item: int(item.get("received_at_ms") or 0))
    derivatives_state["minute_samples"] = samples
    derivatives_state["latest_ctx"] = sample
    derivatives_state["last_seen_at_ms"] = current_ms
    derivatives_state["last_sample_at_ms"] = current_ms
    derivatives_state["updated_at_ms"] = current_ms
    derivatives_state["status"] = "connected"
    derivatives_state["last_error"] = None
    state["hyperliquid_derivatives"] = prune_hyperliquid_state(derivatives_state, current_ms)
    return sample


def align_hyperliquid_sample_to_close(samples, close_ms, max_drift_sec=None):
    close_ms = int(close_ms or 0)
    max_drift_ms = max(1, int(max_drift_sec or HYPERLIQUID_ALIGN_MAX_DRIFT_SEC)) * 1000
    candidates = []
    for item in samples or []:
        received_at_ms = int(item.get("received_at_ms") or 0)
        if received_at_ms <= 0 or received_at_ms > close_ms:
            continue
        drift_ms = close_ms - received_at_ms
        if drift_ms <= max_drift_ms:
            candidates.append((drift_ms, item))
    if not candidates:
        return None
    drift_ms, sample = min(candidates, key=lambda item: item[0])
    result = dict(sample)
    result["aligned_close_ms"] = close_ms
    result["oi_drift_ms"] = drift_ms
    return result


def latest_closed_hour_ms(current_ms=None):
    current_ms = int(current_ms or now_ms())
    hour_ms = 3600 * 1000
    return (current_ms // hour_ms) * hour_ms - 1


def build_hyperliquid_oi_receipt(state, current_ms=None):
    current_ms = int(current_ms or now_ms())
    derivatives_state = prune_hyperliquid_state(
        (state or {}).get("hyperliquid_derivatives") or {},
        current_ms,
    )
    samples = derivatives_state.get("minute_samples") or []
    required_points = HYPERLIQUID_PRIMARY_MIN_HOURS + 1
    last_close_ms = latest_closed_hour_ms(current_ms)
    hour_ms = 3600 * 1000
    slots = []
    for offset in range(required_points - 1, -1, -1):
        close_ms = last_close_ms - offset * hour_ms
        slots.append(align_hyperliquid_sample_to_close(samples, close_ms))
    continuous_points = 0
    for item in reversed(slots):
        if item is None:
            break
        continuous_points += 1
    primary_ready = continuous_points >= required_points
    latest_ctx = derivatives_state.get("latest_ctx") or {}
    latest_age_sec = (
        max(0.0, (current_ms - int(latest_ctx.get("received_at_ms") or 0)) / 1000.0)
        if latest_ctx
        else None
    )
    latest_current = latest_age_sec is not None and latest_age_sec <= HYPERLIQUID_CTX_STALE_SEC
    reason = None
    if not BTC_PUSH_HYPERLIQUID_ENABLED:
        reason = "disabled"
    elif not latest_current:
        reason = "ctx_stale_or_missing"
    elif not primary_ready:
        reason = "warming_up"
    return {
        "mode": HYPERLIQUID_MODE,
        "source": HYPERLIQUID_SOURCE,
        "scope": "Hyperliquid BTC perpetual",
        "status": derivatives_state.get("status") or "not_started",
        "reason": reason,
        "minute_samples": len(samples),
        "continuous_points": continuous_points,
        "continuous_hours": max(0, continuous_points - 1),
        "required_points": required_points,
        "primary_ready": primary_ready,
        "ready_4h": continuous_points >= 5,
        "ready_24h": continuous_points >= 25,
        "ready_72h": primary_ready,
        "latest_ctx_current": latest_current,
        "latest_ctx_age_sec": latest_age_sec,
        "last_seen_at_ms": derivatives_state.get("last_seen_at_ms"),
        "last_error": derivatives_state.get("last_error"),
        "aligned_samples": [item for item in slots if item is not None],
    }


def merge_hyperliquid_oi_into_bars(bars, state, current_ms=None):
    receipt = build_hyperliquid_oi_receipt(state, current_ms=current_ms)
    samples = ((state or {}).get("hyperliquid_derivatives") or {}).get("minute_samples") or []
    merged = []
    for raw in bars or []:
        item = dict(raw)
        close_ms = int(item.get("close_time") or 0)
        aligned = align_hyperliquid_sample_to_close(samples, close_ms)
        item["signal_source"] = "Binance Futures 1h Kline + Hyperliquid BTC Perp OI"
        item["oi_source"] = HYPERLIQUID_SOURCE
        item["oi_time_basis"] = "received_at"
        item["oi_source_as_of_ms"] = None
        item["oi_endpoint_url"] = HYPERLIQUID_INFO_URL
        item["oi_primary_ready"] = receipt.get("primary_ready") is True
        if aligned:
            item["oi"] = aligned.get("open_interest_btc")
            item["oi_value"] = aligned.get("open_interest_usd")
            item["oi_mark_px"] = aligned.get("mark_px")
            item["oi_timestamp"] = aligned.get("received_at_ms")
            item["oi_retrieved_at_ms"] = aligned.get("received_at_ms")
            item["oi_drift_ms"] = aligned.get("oi_drift_ms")
            item["oi_aligned"] = True
            item["aligned_close_ms"] = aligned.get("aligned_close_ms")
        else:
            item.pop("oi", None)
            item.pop("oi_value", None)
            item["oi_aligned"] = False
            item["oi_drift_ms"] = None
        merged.append(item)
    return merged


def merge_latest_hyperliquid_state(state):
    if assets.current().equity:
        return
    if not BTC_PUSH_HYPERLIQUID_ENABLED or not os.path.exists(STATE_FILE):
        return state
    try:
        latest_derivatives = state_store().read_domain("hyperliquid_derivatives")
        if isinstance(latest_derivatives, dict):
            state["hyperliquid_derivatives"] = prune_hyperliquid_state(merge_domain(
                "hyperliquid_derivatives", latest_derivatives,
                state.get("hyperliquid_derivatives"),
            ))
    except Exception:
        log_exception("hyperliquid state merge skipped")
    return state


def prune_liquidation_ws_state(liq_state, current_ms=None):
    if not isinstance(liq_state, dict):
        return {}
    current_ms = int(current_ms or now_ms())
    previous_stream = liq_state.get("stream")
    if previous_stream and previous_stream != LIQUIDATION_WS_URL:
        return {
            "events": [],
            "previous_stream": previous_stream,
            "stream_reset_at_ms": current_ms,
        }
    retention_ms = max(25, LIQUIDATION_RETENTION_H) * 3600 * 1000
    cutoff_ms = current_ms - retention_ms
    events = []
    for item in liq_state.get("events") or []:
        try:
            item_ts = int(item.get("ts_ms") or 0)
        except Exception:
            continue
        if item_ts < cutoff_ms:
            continue
        event = dict(item)
        event["ts_ms"] = item_ts
        events.append(event)
    events.sort(key=lambda item: int(item.get("ts_ms") or 0))
    liq_state["events"] = events[-LIQUIDATION_MAX_EVENTS:]
    return liq_state


def merge_latest_liquidation_ws_state(state):
    if assets.current().equity:
        return
    if not BTC_PUSH_LIQUIDATION_WS_ENABLED or not os.path.exists(STATE_FILE):
        return state
    try:
        latest_liq = state_store().read_domain("liquidation_ws")
        if isinstance(latest_liq, dict):
            state["liquidation_ws"] = prune_liquidation_ws_state(merge_domain(
                "liquidation_ws", latest_liq, state.get("liquidation_ws"),
            ))
    except Exception:
        log_exception("liquidation ws state merge skipped")
    return state


def update_liquidation_ws_status(status, reason=None):
    if not BTC_PUSH_LIQUIDATION_WS_ENABLED:
        return
    current_ms = now_ms()
    def update(liq_state):
        liq_state = prune_liquidation_ws_state(liq_state, current_ms)
        reported_status = status
        previous_seen_ms = int(liq_state.get("last_seen_at_ms") or 0)
        coverage_started_ms = int(liq_state.get("coverage_started_at_ms") or 0)
        if status == "connected":
            gap_ms = current_ms - previous_seen_ms if previous_seen_ms else None
            if not coverage_started_ms or gap_ms is None or gap_ms > LIQUIDATION_COVERAGE_GAP_RESET_SEC * 1000:
                liq_state["coverage_started_at_ms"] = current_ms
            liq_state["last_connect_at_ms"] = current_ms
            liq_state["last_seen_at_ms"] = current_ms
            liq_state["last_server_response_at_ms"] = current_ms
            liq_state["last_error"] = None
        elif status == "heartbeat":
            liq_state["last_seen_at_ms"] = current_ms
            liq_state["last_server_response_at_ms"] = current_ms
        elif status == "client_ping":
            liq_state["last_client_ping_at_ms"] = current_ms
            reported_status = liq_state.get("status") or "connected"
        elif status in ("connecting", "disconnected", "error"):
            if status == "connecting":
                liq_state["last_connect_attempt_at_ms"] = current_ms
            else:
                liq_state["last_disconnect_at_ms"] = current_ms
            if reason:
                liq_state["last_error"] = str(reason)[:240]
        liq_state["enabled"] = True
        liq_state["stream"] = LIQUIDATION_WS_URL
        liq_state["status"] = reported_status
        liq_state["updated_at_ms"] = current_ms
        return liq_state
    state_store().update_domain("liquidation_ws", update)


def append_liquidation_ws_event(event):
    current_ms = now_ms()
    def update(liq_state):
        liq_state = prune_liquidation_ws_state(liq_state, current_ms)
        previous_seen_ms = int(liq_state.get("last_seen_at_ms") or 0)
        coverage_started_ms = int(liq_state.get("coverage_started_at_ms") or 0)
        gap_ms = current_ms - previous_seen_ms if previous_seen_ms else None
        if not coverage_started_ms or gap_ms is None or gap_ms > LIQUIDATION_COVERAGE_GAP_RESET_SEC * 1000:
            liq_state["coverage_started_at_ms"] = current_ms

        events = liq_state.get("events") or []
        event_id = event.get("id")
        if event_id and any(item.get("id") == event_id for item in events):
            return
        events.append(event)
        liq_state["events"] = events
        liq_state["last_event_at_ms"] = int(event.get("ts_ms") or current_ms)
        liq_state["last_seen_at_ms"] = current_ms
        liq_state["last_server_response_at_ms"] = current_ms
        liq_state["last_valid_sample_at_ms"] = current_ms
        liq_state["status"] = "connected"
        liq_state["enabled"] = True
        liq_state["stream"] = LIQUIDATION_WS_URL
        liq_state["last_error"] = None
        liq_state["event_count_total"] = int(liq_state.get("event_count_total") or 0) + 1
        liq_state["updated_at_ms"] = current_ms
        return prune_liquidation_ws_state(liq_state, current_ms)
    state_store().update_domain("liquidation_ws", update)


def update_hyperliquid_status(status, reason=None):
    if not BTC_PUSH_HYPERLIQUID_ENABLED:
        return
    current_ms = now_ms()
    def update(derivatives_state):
        derivatives_state = prune_hyperliquid_state(derivatives_state, current_ms)
        reported_status = status
        if status == "connecting":
            derivatives_state["last_connect_attempt_at_ms"] = current_ms
        elif status == "connected":
            derivatives_state["last_connect_at_ms"] = current_ms
            derivatives_state["last_seen_at_ms"] = current_ms
            derivatives_state["last_server_response_at_ms"] = current_ms
            derivatives_state["last_error"] = None
        elif status == "heartbeat":
            derivatives_state["last_seen_at_ms"] = current_ms
            derivatives_state["last_server_response_at_ms"] = current_ms
        elif status == "client_ping":
            derivatives_state["last_client_ping_at_ms"] = current_ms
            reported_status = derivatives_state.get("status") or "connected"
        elif status in ("error", "disconnected"):
            derivatives_state["last_disconnect_at_ms"] = current_ms
            if reason:
                derivatives_state["last_error"] = str(reason)[:240]
        derivatives_state["status"] = reported_status
        derivatives_state["enabled"] = True
        derivatives_state["updated_at_ms"] = current_ms
        return derivatives_state
    state_store().update_domain("hyperliquid_derivatives", update)


def persist_hyperliquid_ctx(ctx, received_at_ms=None, *, from_websocket=False):
    if not BTC_PUSH_HYPERLIQUID_ENABLED:
        return False
    current_ms = int(received_at_ms or now_ms())
    # Validate even when this minute is already persisted: only a valid WS ctx
    # may refresh the in-memory sample clock in the collector.
    normalize_hyperliquid_ctx(ctx, current_ms)
    current_minute_ms = current_ms - (current_ms % 60000)
    path = os.path.abspath(STATE_FILE)
    with STATE_LOCK:
        if _persisted_hyperliquid_minutes.get(path) == current_minute_ms:
            return False
        def update(derivatives_state):
            derivatives_state = prune_hyperliquid_state(derivatives_state, current_ms)
            latest_ctx = derivatives_state.get("latest_ctx") or {}
            if int(latest_ctx.get("minute_ms") or 0) == current_minute_ms:
                return None
            state = {"hyperliquid_derivatives": derivatives_state}
            append_hyperliquid_minute_sample(state, ctx, current_ms)
            if from_websocket:
                state["hyperliquid_derivatives"]["last_server_response_at_ms"] = current_ms
                state["hyperliquid_derivatives"]["last_valid_sample_at_ms"] = current_ms
            return state["hyperliquid_derivatives"]
        saved = state_store().update_domain("hyperliquid_derivatives", update)
        _persisted_hyperliquid_minutes[path] = current_minute_ms
    try:
        market_public.archive_hyperliquid(ctx, current_ms, market_history.history_path(STATE_FILE))
    except Exception as exc:
        log_line(f"Hyperliquid history pending: {type(exc).__name__}")
    return saved


def open_websocket(url, connect_timeout_sec=None, read_timeout_sec=None):
    return connect_websocket(
        url,
        connect_timeout=max(1, int(connect_timeout_sec or LIQUIDATION_WS_CONNECT_TIMEOUT_SEC)),
        read_timeout=min(5, max(1, int(read_timeout_sec or LIQUIDATION_WS_TIMEOUT_SEC))),
        user_agent=f"btcpush/{APP_ID}",
    )


def ws_send_frame(sock, opcode, payload=b""):
    return sock.send_frame(opcode, payload)


def ws_read_frame(sock):
    return sock.read_frame()


def ws_poll(sock, health, ping_opcode, ping_payload):
    current = time.monotonic()
    health.last_server_response = sock.last_server_response_mono
    health.check(current)
    sent_ping = False
    if health.ping_due(current):
        ws_send_frame(sock, ping_opcode, ping_payload)
        health.last_client_ping = current
        sent_ping = True
    try:
        frame = ws_read_frame(sock)
    except socket.timeout:
        frame = None
    health.last_server_response = sock.last_server_response_mono
    health.check(time.monotonic())
    return frame, sent_ping


def liquidation_event_from_ws_message(message):
    if not isinstance(message, dict):
        return None
    data = message.get("data") if isinstance(message.get("data"), dict) else message
    if not isinstance(data, dict) or data.get("e") != "forceOrder":
        return None
    order = data.get("o") or {}
    symbol = (order.get("s") or "").upper()
    if symbol != BINANCE_SYMBOL:
        return None
    side = (order.get("S") or "").upper()
    bucket = "long" if side == "SELL" else "short" if side == "BUY" else None
    if not bucket:
        return None
    qty = safe_float(order.get("z"))
    if qty is None or qty <= 0:
        qty = safe_float(order.get("q"), 0.0) or 0.0
    price = safe_float(order.get("ap"))
    if price is None or price <= 0:
        price = safe_float(order.get("p"), 0.0) or 0.0
    notional = abs(qty * price)
    if not all(math.isfinite(value) and value > 0 for value in (qty, price, notional)):
        return None
    event_ms = int(order.get("T") or data.get("E") or now_ms())
    event_id = f"{event_ms}:{symbol}:{side}:{qty:.8f}:{price:.2f}"
    return {
        "id": event_id,
        "ts_ms": event_ms,
        "symbol": symbol,
        "side": side,
        "bucket": bucket,
        "qty": qty,
        "price": price,
        "notional_usd": notional,
    }


def persist_collector_recovery_status(writer, status, reason=None):
    # 状态存储故障不能让恢复处理本身退出；连接仍由外层关闭并退避。
    try:
        writer(status, reason)
    except Exception as exc:
        log_line(f"collector status persistence pending status={status} error={type(exc).__name__}")


def run_liquidation_ws_collector():
    backoff_sec = 1
    while BTC_PUSH_LIQUIDATION_WS_ENABLED:
        sock = None
        try:
            update_liquidation_ws_status("connecting")
            sock = open_websocket(LIQUIDATION_WS_URL)
            connected_at = time.monotonic()
            health = WebSocketLiveness(
                connected_at, response_timeout=max(90, LIQUIDATION_WS_TIMEOUT_SEC * 3),
            )
            last_heartbeat_save = last_ping_save = connected_at
            update_liquidation_ws_status("connected")
            log_line(f"liquidation ws connected url={LIQUIDATION_WS_URL}")
            while BTC_PUSH_LIQUIDATION_WS_ENABLED:
                frame, sent_ping = ws_poll(sock, health, 0x9, b"btcpush")
                current = time.monotonic()
                if sent_ping and current - last_ping_save >= LIQUIDATION_WS_HEARTBEAT_SAVE_SEC:
                    update_liquidation_ws_status("client_ping")
                    last_ping_save = current
                if frame is None:
                    continue
                if current - connected_at >= 60:
                    backoff_sec = 1
                opcode, payload = frame
                if opcode == 0x8:
                    ws_send_frame(sock, 0x8, payload)
                    raise RuntimeError("websocket close frame")
                if opcode == 0x9:
                    ws_send_frame(sock, 0xA, payload)
                event = None
                if opcode == 0x1:
                    message = json.loads(payload.decode("utf-8"))
                    event = liquidation_event_from_ws_message(message)
                if event:
                    append_liquidation_ws_event(event)
                    health.last_valid_sample = current
                    last_heartbeat_save = current
                elif current - last_heartbeat_save >= LIQUIDATION_WS_HEARTBEAT_SAVE_SEC:
                    update_liquidation_ws_status("heartbeat")
                    last_heartbeat_save = current
        except Exception as exc:
            persist_collector_recovery_status(update_liquidation_ws_status, "error", str(exc))
            log_line(f"liquidation ws error: {exc}")
        finally:
            try:
                if sock:
                    sock.close()
            except Exception:
                pass
        persist_collector_recovery_status(update_liquidation_ws_status, "disconnected")
        if BTC_PUSH_LIQUIDATION_WS_ENABLED:
            time.sleep(min(60, random.uniform(0.8, 1.2) * backoff_sec))
            backoff_sec = min(backoff_sec * 2, 60)


def start_liquidation_ws_collector():
    global liquidation_collector_thread
    if not BTC_PUSH_LIQUIDATION_WS_ENABLED:
        log_line("liquidation ws disabled")
        return
    if liquidation_collector_thread and liquidation_collector_thread.is_alive():
        return
    liquidation_collector_thread = threading.Thread(
        target=run_liquidation_ws_collector,
        name="btc-liquidation-ws",
        daemon=True,
    )
    liquidation_collector_thread.start()


def run_hyperliquid_ws_collector():
    backoff_sec = 1
    while BTC_PUSH_HYPERLIQUID_ENABLED:
        sock = None
        try:
            update_hyperliquid_status("connecting")
            try:
                persist_hyperliquid_ctx(fetch_hyperliquid_asset_ctx_rest())
            except Exception:
                log_exception("hyperliquid REST bootstrap skipped")
            sock = open_websocket(
                HYPERLIQUID_WS_URL,
                connect_timeout_sec=HYPERLIQUID_WS_CONNECT_TIMEOUT_SEC,
                read_timeout_sec=HYPERLIQUID_WS_TIMEOUT_SEC,
            )
            connected_at = time.monotonic()
            health = WebSocketLiveness(
                connected_at, response_timeout=max(90, HYPERLIQUID_WS_TIMEOUT_SEC * 3),
                sample_timeout=max(5, HYPERLIQUID_CTX_STALE_SEC),
            )
            last_heartbeat_save = last_ping_save = connected_at
            subscription = {
                "method": "subscribe",
                "subscription": {"type": "activeAssetCtx", "coin": HYPERLIQUID_COIN},
            }
            ws_send_frame(sock, 0x1, json.dumps(subscription, separators=(",", ":")))
            update_hyperliquid_status("connected")
            log_line(
                "hyperliquid ws connected "
                f"url={HYPERLIQUID_WS_URL} coin={HYPERLIQUID_COIN} mode={HYPERLIQUID_MODE}"
            )
            while BTC_PUSH_HYPERLIQUID_ENABLED:
                frame, sent_ping = ws_poll(sock, health, 0x1, '{"method":"ping"}')
                current = time.monotonic()
                if sent_ping and current - last_ping_save >= HYPERLIQUID_HEARTBEAT_SAVE_SEC:
                    update_hyperliquid_status("client_ping")
                    last_ping_save = current
                if frame is None:
                    continue
                if current - connected_at >= 60:
                    backoff_sec = 1
                opcode, payload = frame
                if opcode == 0x8:
                    ws_send_frame(sock, 0x8, payload)
                    raise RuntimeError("websocket close frame")
                if opcode == 0x9:
                    ws_send_frame(sock, 0xA, payload)
                if opcode == 0x1:
                    message = json.loads(payload.decode("utf-8"))
                    ctx = hyperliquid_asset_ctx_from_ws_message(message)
                    if ctx is not None:
                        saved = persist_hyperliquid_ctx(ctx, from_websocket=True)
                        health.last_valid_sample = current
                        if saved:
                            last_heartbeat_save = current
                if current - last_heartbeat_save >= HYPERLIQUID_HEARTBEAT_SAVE_SEC:
                    update_hyperliquid_status("heartbeat")
                    last_heartbeat_save = current
        except Exception as exc:
            persist_collector_recovery_status(update_hyperliquid_status, "error", str(exc))
            log_line(f"hyperliquid ws error: {exc}")
        finally:
            try:
                if sock:
                    sock.close()
            except Exception:
                pass
        persist_collector_recovery_status(update_hyperliquid_status, "disconnected")
        if BTC_PUSH_HYPERLIQUID_ENABLED:
            time.sleep(min(60, random.uniform(0.8, 1.2) * backoff_sec))
            backoff_sec = min(backoff_sec * 2, 60)


def start_hyperliquid_ws_collector():
    global hyperliquid_collector_thread
    if not BTC_PUSH_HYPERLIQUID_ENABLED:
        log_line("hyperliquid ws disabled")
        return
    if hyperliquid_collector_thread and hyperliquid_collector_thread.is_alive():
        return
    hyperliquid_collector_thread = threading.Thread(
        target=run_hyperliquid_ws_collector,
        name="btc-hyperliquid-ws",
        daemon=True,
    )
    hyperliquid_collector_thread.start()


def make_data_record(value, source, status="ok", reason=None, updated_at=None):
    return {
        "value": value,
        "source": source,
        "status": status,
        "reason": reason,
        "updated_at": int(updated_at or time.time()),
    }


def make_unavailable_record(source, reason):
    return make_data_record({}, source, status="unavailable", reason=reason)


def value_has_data(value):
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return True
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, dict):
        return any(value_has_data(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(value_has_data(item) for item in value)
    return False


def record_has_data(record):
    if not record or record.get("status") == "unavailable":
        return False
    return value_has_data(record.get("value"))


def append_public_snapshot(state, snapshot):
    snaps = state.get("public_snapshots") or []
    snaps.append(snapshot)
    if len(snaps) > PUBLIC_SNAPSHOT_MAX:
        snaps = snaps[-PUBLIC_SNAPSHOT_MAX:]
    state["public_snapshots"] = compact_public_snapshots(snaps)


def find_public_record_near(state, key, target_sec, tolerance_sec):
    snaps = [
        item for item in (state.get("public_snapshots") or [])
        if int(item.get("ts", 0) or 0) > 0
    ]
    if not snaps:
        return None
    now_ts = int(time.time())
    target_ts = now_ts - int(target_sec)
    candidates = []
    for snap in snaps:
        record = ((snap.get("public") or {}).get(key) or {})
        if record_has_data(record):
            candidates.append((abs(int(snap.get("ts") or 0) - target_ts), snap, record))
    if not candidates:
        return None
    drift, snap, record = min(candidates, key=lambda item: item[0])
    if drift > int(tolerance_sec):
        return None
    return {
        "record": record,
        "snapshot": snap,
        "drift_sec": drift,
    }


def find_previous_public_record(state, key, before_ts):
    for snap in reversed(state.get("public_snapshots") or []):
        try:
            snap_ts = int(snap.get("ts") or 0)
        except Exception:
            continue
        if before_ts and snap_ts >= int(before_ts):
            continue
        record = ((snap.get("public") or {}).get(key) or {})
        if record_has_data(record):
            return {
                "record": record,
                "snapshot": snap,
                "drift_sec": max(0, int(before_ts or snap_ts) - snap_ts),
            }
    return None


def source_age_reason(key, record, now_ts):
    # Validate the source record before primary/shadow selection changes its key.
    if key == "hyperliquid_funding":
        key = "funding"
    if key not in ("funding", "spot_cvd", "basis"):
        return None
    value = record.get("value") or {}
    stamp = epoch_ms(record.get("source_as_of_ms"))
    if stamp is None:
        fallback = "latest_funding_time_ms" if key == "funding" else "as_of_ms" if key == "spot_cvd" else None
        stamp = epoch_ms(value.get(fallback)) if fallback else None
    if stamp is None:
        return "source_time_missing"
    age = now_ts - stamp / 1000
    if age < 0:
        return "source_time_in_future"
    if key == "funding":
        hours = safe_float(value.get("actual_interval_hours", value.get("current_interval_hours")))
        if hours is None or not math.isfinite(hours) or hours <= 0:
            hours = None
            for window in ("8h", "24h"):
                interval = safe_float(((value.get("settlement_windows") or {}).get(window) or {}).get("interval_ms"))
                if interval is not None and math.isfinite(interval) and interval > 0:
                    hours = interval / 3600000
                    break
        if hours is None:
            return "funding_interval_unknown"
        maximum = hours * 3600 + HYPERLIQUID_CTX_STALE_SEC
    else:
        maximum = SIGNAL_BAR_STALE_SEC if key == "spot_cvd" else HYPERLIQUID_CTX_STALE_SEC
    return "source_age_exceeded" if age > maximum else None


def fear_greed_is_current(value, now_ts=None):
    if not value or value.get("available") is not True:
        return False
    if any(value.get(key) for key in ("stale", "source_stale", "snapshot_used")):
        return False
    stamp = epoch_ms(value.get("source_as_of_ms"))
    if stamp is None:
        seconds = safe_float(value.get("timestamp"))
        stamp = epoch_ms(int(seconds * 1000)) if seconds is not None else None
    number = safe_float(value.get("value"))
    if stamp is None or number is None or not 0 <= number <= 100:
        return False
    current = time.time() if now_ts is None else now_ts
    return 0 <= current - stamp / 1000 <= PUBLIC_SNAPSHOT_STALE_SEC


def record_background_label(record):
    if record_is_current(record):
        return ""
    record = record or {}
    dates = (record.get("value") or {}).get("source_date") or record.get("source_date")
    if isinstance(dates, dict):
        dates = " / ".join(str(value) for value in sorted(set(str(v) for v in dates.values() if v))[:3])
    if not dates:
        stamp = epoch_ms(record.get("source_as_of_ms"))
        dates = datetime.fromtimestamp(stamp / 1000, TOKYO_TZ).isoformat() if stamp else "源日期缺失"
    return f"（历史背景，截至 {dates}）"


def select_public_records(state, current_snapshot):
    selected = {}
    now_ts = time.time()
    keys = (
        "funding",
        "liquidation",
        "basis",
        "etf",
        "spot_cvd",
        "stablecoin",
        "macro",
        "hyperliquid_funding",
        "hyperliquid_basis",
    )
    for key in keys:
        current = ((current_snapshot.get("public") or {}).get(key) or {})
        if key == "liquidation" or current.get("status") == "not_applicable":
            record = dict(current) if current else make_unavailable_record(key, "no_snapshot")
            record["snapshot_used"] = False
        elif record_has_data(current):
            record = dict(current)
            record["snapshot_used"] = False
        else:
            record = None
            for snap in reversed(state.get("public_snapshots") or []):
                candidate = ((snap.get("public") or {}).get(key) or {})
                if key == "etf" and btc_etf.is_holdings_source(current) and not btc_etf.is_holdings_source(candidate):
                    continue
                if record_has_data(candidate):
                    record = dict(candidate)
                    record["snapshot_used"] = True
                    if key == "etf":
                        record["collection_reason"] = current.get("reason")
                    break
            if record is None:
                record = dict(current) if current else make_unavailable_record(key, "no_snapshot")
                record["snapshot_used"] = False
        age_sec = max(0, now_ts - int(record.get("updated_at") or now_ts))
        stale_sec = MACRO_SOURCE_STALE_SEC if key == "macro" else PUBLIC_SNAPSHOT_STALE_SEC
        source_reason = source_age_reason(key, record, now_ts)
        if source_reason:
            record["source_stale"] = True
            record["source_stale_reason"] = record.get("source_stale_reason") or source_reason
        record["age_sec"] = age_sec
        record["stale"] = (
            bool(record.get("snapshot_used"))
            or bool(record.get("source_stale"))
            or age_sec > stale_sec
        )
        selected[key] = record
    selected = apply_derivatives_source_mode(state, selected)
    return selected


def apply_derivatives_source_mode(state, selected):
    selected = dict(selected or {})
    if assets.current().equity or HYPERLIQUID_MODE != "primary":
        return selected
    receipt = build_hyperliquid_oi_receipt(state)
    selected["binance_funding_shadow"] = selected.get("funding") or {}
    selected["binance_basis_shadow"] = selected.get("basis") or {}
    if receipt.get("primary_ready") is not True:
        reason = receipt.get("reason") or "primary_not_ready"
        selected["funding"] = make_unavailable_record(HYPERLIQUID_FUNDING_SOURCE, reason)
        selected["basis"] = make_unavailable_record(HYPERLIQUID_SOURCE, reason)
        return selected
    hyper_funding = selected.get("hyperliquid_funding") or {}
    hyper_basis = selected.get("hyperliquid_basis") or {}
    selected["funding"] = (
        dict(hyper_funding)
        if record_is_current(hyper_funding)
        else make_unavailable_record(
            HYPERLIQUID_FUNDING_SOURCE,
            hyper_funding.get("reason") or "hyperliquid_funding_unavailable",
        )
    )
    selected["basis"] = (
        dict(hyper_basis)
        if record_is_current(hyper_basis)
        else make_unavailable_record(
            HYPERLIQUID_SOURCE,
            hyper_basis.get("reason") or "hyperliquid_basis_unavailable",
        )
    )
    return selected


def clamp(value, low=0.0, high=100.0):
    try:
        value = float(value)
    except Exception:
        value = 0.0
    return max(float(low), min(float(high), value))


def parse_ymd(value):
    if not value:
        return None
    text = str(value).strip()[:10]
    try:
        return datetime.strptime(text, "%Y-%m-%d")
    except Exception:
        return None


def source_date_age_sec(source_date):
    dt = parse_ymd(source_date)
    if not dt:
        return None
    now_dt = datetime.now(TOKYO_TZ).replace(tzinfo=None)
    return max(0, int((now_dt - dt).total_seconds()))


def is_source_date_stale(source_date, stale_sec):
    age = source_date_age_sec(source_date)
    if age is None:
        return True
    return age > int(stale_sec)


def macro_source_stale_sec(source_key):
    if source_key == "fed_balance_sheet":
        return MACRO_WEEKLY_SOURCE_STALE_SEC
    return MACRO_SOURCE_STALE_SEC


def previous_us_weekday(day):
    current = day
    while current.weekday() >= 5:
        current -= timedelta(days=1)
    return current


def expected_etf_source_date(now_dt=None):
    if not US_EASTERN_TZ:
        return None
    now_dt = now_dt or datetime.now(TOKYO_TZ)
    now_et = now_dt.astimezone(US_EASTERN_TZ)
    expected_day = now_et.date()
    if expected_day.weekday() >= 5:
        expected_day = previous_us_weekday(expected_day)
    elif now_et.hour < ETF_SOURCE_READY_ET_HOUR:
        expected_day = previous_us_weekday(expected_day - timedelta(days=1))
    return expected_day


def etf_source_stale_reason(source_date, now_dt=None):
    source_dt = parse_ymd(source_date)
    if not source_dt:
        return "missing_date"
    expected_day = expected_etf_source_date(now_dt)
    if not expected_day:
        return "age" if is_source_date_stale(source_date, ETF_SOURCE_STALE_SEC) else None
    lag_days = (expected_day - source_dt.date()).days
    if lag_days > max(0, ETF_SOURCE_STALE_GRACE_DAYS):
        return f"{source_date}<expected:{expected_day.strftime('%Y-%m-%d')}"
    return None


def previous_month_key(year, month, offset):
    month -= int(offset)
    while month <= 0:
        month += 12
        year -= 1
    return f"{year}{month:02d}"


def latest_series_point(series):
    points = [item for item in (series or []) if safe_float(item.get("value")) is not None and item.get("date")]
    if not points:
        return None
    points.sort(key=lambda item: item["date"])
    return points[-1]


def series_prev_point_by_days(series, days):
    latest = latest_series_point(series)
    latest_dt = parse_ymd((latest or {}).get("date"))
    if not latest_dt:
        return None
    target_dt = latest_dt - timedelta(days=int(days))
    candidates = []
    for item in series or []:
        dt = parse_ymd(item.get("date"))
        if dt and dt <= target_dt and safe_float(item.get("value")) is not None:
            candidates.append((dt, item))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0])
    return candidates[-1][1]


def series_delta_pp(series, days):
    latest = latest_series_point(series)
    prev = series_prev_point_by_days(series, days)
    if not latest or not prev:
        return None
    latest_value = safe_float(latest.get("value"))
    prev_value = safe_float(prev.get("value"))
    if latest_value is None or prev_value is None:
        return None
    return latest_value - prev_value


def series_delta_pct(series, days):
    latest = latest_series_point(series)
    prev = series_prev_point_by_days(series, days)
    if not latest or not prev:
        return None
    return pct(latest.get("value"), prev.get("value"))


def series_delta_pct_for_point(series, point, days):
    point_dt = parse_ymd((point or {}).get("date"))
    if not point_dt:
        return None
    target_dt = point_dt - timedelta(days=int(days))
    candidates = []
    for item in series or []:
        dt = parse_ymd(item.get("date"))
        if dt and dt <= target_dt and safe_float(item.get("value")) is not None:
            candidates.append((dt, item))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0])
    return pct((point or {}).get("value"), candidates[-1][1].get("value"))


def yahoo_symbol_url(symbol):
    return str(symbol).replace("^", "%5E").replace("/", "%2F")


def fetch_yahoo_chart_series(symbol, range_value="3mo", interval="1d"):
    url = f"{YAHOO_CHART_BASE_URL}/{yahoo_symbol_url(symbol)}"
    headers = {
        "User-Agent": f"btcpush/{APP_ID} Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*",
    }
    r = public_io.get(
        url,
        params={"range": range_value, "interval": interval},
        headers=headers,
        timeout=(3, max(1, MACRO_FETCH_TIMEOUT_SEC)),
    )
    r.raise_for_status()
    data = r.json()
    result = (((data.get("chart") or {}).get("result") or [None])[0]) or {}
    timestamps = result.get("timestamp") or []
    closes = ((((result.get("indicators") or {}).get("quote") or [{}])[0]).get("close") or [])
    series = []
    for ts, close in zip(timestamps, closes):
        value = safe_float(close)
        if value is None:
            continue
        date_text = datetime.fromtimestamp(int(ts), TOKYO_TZ).strftime("%Y-%m-%d")
        series.append({"date": date_text, "value": value})
    series.sort(key=lambda item: item["date"])
    return series


def fetch_treasury_xml_series(data_key, value_tag, months=None):
    now = datetime.now(TOKYO_TZ)
    months = int(months or TREASURY_HISTORY_MONTHS)
    points = {}
    for offset in range(max(1, months)):
        month_key = previous_month_key(now.year, now.month, offset)
        headers = {
            "User-Agent": f"btcpush/{APP_ID} Mozilla/5.0",
            "Accept": "application/xml,text/xml,*/*",
        }
        r = public_io.get(
            TREASURY_XML_BASE_URL,
            params={"data": data_key, "field_tdr_date_value_month": month_key},
            headers=headers,
            timeout=(3, max(1, MACRO_FETCH_TIMEOUT_SEC)),
        )
        r.raise_for_status()
        text = r.text or ""
        for entry in re.findall(r"<entry>(.*?)</entry>", text, flags=re.S):
            date_match = re.search(r"<d:NEW_DATE[^>]*>([^<]+)</d:NEW_DATE>", entry)
            value_match = re.search(rf"<d:{re.escape(value_tag)}[^>]*>([^<]+)</d:{re.escape(value_tag)}>", entry)
            if not date_match or not value_match:
                continue
            value = safe_float(value_match.group(1))
            if value is None:
                continue
            date_text = date_match.group(1)[:10]
            points[date_text] = {"date": date_text, "value": value}
    return [points[key] for key in sorted(points)]


def classify_fed_policy(cut_probability=None, hike_probability=None, hold_probability=None):
    cut = safe_float(cut_probability, 0.0) or 0.0
    hike = safe_float(hike_probability, 0.0) or 0.0
    hold = safe_float(hold_probability, 0.0) or 0.0
    if hike >= 35.0 or (hike >= 20.0 and hike > cut + 10.0):
        return "Hawkish"
    if cut >= 35.0 or (cut >= 20.0 and cut > hike + 10.0):
        return "Dovish"
    if hold >= 60.0:
        return "Neutral"
    return "Neutral"


def fred_clean_float(value):
    text = str(value or "").strip()
    if not text or text == ".":
        return None
    return safe_float(text)


def fetch_fred_csv_text(series_ids=None):
    ids = tuple(series_ids or FRED_FED_SERIES_IDS)
    params = {"id": ",".join(ids)}
    curl_path = shutil.which("curl")
    if curl_path:
        try:
            result = subprocess.run(
                [
                    curl_path,
                    "-L",
                    "-sS",
                    "-G",
                    FRED_CSV_URL,
                    "--data-urlencode",
                    f"id={params['id']}",
                ],
                check=False,
                capture_output=True,
                text=True,
                timeout=max(5, FRED_FETCH_TIMEOUT_SEC + 5),
            )
            if result.returncode == 0 and "observation_date" in result.stdout:
                return result.stdout
        except Exception:
            pass
    return public_get_text(
        FRED_CSV_URL,
        params=params,
        timeout_sec=FRED_FETCH_TIMEOUT_SEC,
    )


def fetch_fred_series(series_id):
    text = fetch_fred_csv_text((series_id,))
    if not text.lstrip().startswith("observation_date"):
        raise RuntimeError(f"unexpected_fred_response:{series_id}")
    reader = csv.DictReader(StringIO(text))
    series = []
    for row in reader:
        value = fred_clean_float(row.get(series_id))
        date_text = row.get("observation_date")
        if value is None or not date_text:
            continue
        series.append({"date": date_text, "value": value})
    series.sort(key=lambda item: item["date"])
    return series


def fetch_fed_status_value():
    if not FRED_CSV_URL:
        return {"available": False, "reason": "fred_url_not_configured"}
    text = fetch_fred_csv_text()
    reader = csv.DictReader(StringIO(text))
    latest_target = None
    latest_effr = None
    target_mid_series = []
    observed = {key: [] for key in FRED_FED_SERIES_IDS}
    for row in reader:
        lower = fred_clean_float(row.get("DFEDTARL"))
        upper = fred_clean_float(row.get("DFEDTARU"))
        effr = fred_clean_float(row.get("DFF"))
        date_text = row.get("observation_date")
        for key in observed:
            number = fred_clean_float(row.get(key))
            if number is not None and date_text:
                observed[key].append({"date": date_text, "value": number})
        if lower is not None and upper is not None:
            latest_target = {
                "date": date_text,
                "lower": lower,
                "upper": upper,
            }
            target_mid_series.append({"date": date_text, "value": (lower + upper) / 2.0})
        if effr is not None:
            latest_effr = {
                "date": date_text,
                "value": effr,
            }
    if not latest_target and not latest_effr:
        return {"available": False, "reason": "empty_fred_fed_status"}
    fed_target_mid = None
    if latest_target:
        fed_target_mid = (latest_target["lower"] + latest_target["upper"]) / 2.0
    return {
        "available": True,
        "fed_target_lower": (latest_target or {}).get("lower"),
        "fed_target_upper": (latest_target or {}).get("upper"),
        "fed_target_mid": fed_target_mid,
        "fed_target_mid_30d_pp": series_delta_pp(target_mid_series, 30),
        "fed_target_date": (latest_target or {}).get("date"),
        "effective_fed_funds": (latest_effr or {}).get("value"),
        "effective_fed_funds_date": (latest_effr or {}).get("date"),
        "source": "FRED CSV DFEDTARU/DFEDTARL/DFF",
        "history": [series_receipt(rows, sid, FRED_CSV_URL, time.time(),
                    {"DFEDTARU": ["fed_target_upper", "fed_target_mid"],
                     "DFEDTARL": ["fed_target_lower", "fed_target_mid"],
                     "DFF": ["effective_fed_funds"]}[sid]) for sid, rows in observed.items()],
    }


def fetch_fedwatch_value():
    override = os.getenv("BTC_PUSH_FED_POLICY_OVERRIDE", "").strip()
    cut = safe_float(os.getenv("BTC_PUSH_FED_CUT_PROB"))
    hike = safe_float(os.getenv("BTC_PUSH_FED_HIKE_PROB"))
    hold = safe_float(os.getenv("BTC_PUSH_FED_HOLD_PROB"))
    expected_rate = safe_float(os.getenv("BTC_PUSH_FED_EXPECTED_RATE"))
    next_meeting = os.getenv("BTC_PUSH_FED_NEXT_MEETING_DATE", "").strip() or None
    if override:
        return {
            "fed_policy": override,
            "cut_probability": cut,
            "hike_probability": hike,
            "hold_probability": hold,
            "expected_rate": expected_rate,
            "next_meeting_date": next_meeting,
            "source_date": None,
            "published_at": None,
            "publication_time_missing": True,
            "source": "env override",
            "retrieved_at": None, "endpoint_url": None, "access_scope": "local_configuration",
        }
    if cut is not None or hike is not None or hold is not None:
        return {
            "fed_policy": classify_fed_policy(cut, hike, hold),
            "cut_probability": cut,
            "hike_probability": hike,
            "hold_probability": hold,
            "expected_rate": expected_rate,
            "next_meeting_date": next_meeting,
            "source_date": None,
            "published_at": None,
            "publication_time_missing": True,
            "source": "env probabilities",
            "retrieved_at": None, "endpoint_url": None, "access_scope": "local_configuration",
        }
    if not CME_FEDWATCH_URL:
        return {
            "fed_policy": None,
            "source": "CME FedWatch",
            "retrieved_at": time.time(), "endpoint_url": CME_FEDWATCH_URL, "access_scope": "public",
            "reason": "source_not_configured",
        }
    try:
        data = public_get_json(CME_FEDWATCH_URL)
        # The public CME shape changes frequently; support explicit numeric keys
        # when a stable endpoint is configured through the plist/environment.
        cut = safe_float(data.get("cut_probability") or data.get("cutProbability"))
        hike = safe_float(data.get("hike_probability") or data.get("hikeProbability"))
        hold = safe_float(data.get("hold_probability") or data.get("holdProbability"))
        return {
            "fed_policy": classify_fed_policy(cut, hike, hold),
            "cut_probability": cut,
            "hike_probability": hike,
            "hold_probability": hold,
            "expected_rate": safe_float(data.get("expected_rate") or data.get("expectedRate")),
            "next_meeting_date": data.get("next_meeting_date") or data.get("nextMeetingDate"),
            "source_date": None,
            "published_at": None,
            "publication_time_missing": True,
            "source": "CME FedWatch",
            "retrieved_at": time.time(), "endpoint_url": CME_FEDWATCH_URL, "access_scope": "public",
        }
    except Exception as exc:
        return {
            "fed_policy": None,
            "source": "CME FedWatch",
            "retrieved_at": time.time(), "endpoint_url": CME_FEDWATCH_URL, "access_scope": "public",
            "reason": str(exc)[:120],
        }


def fiscaldata_get_json(path, params=None):
    url = f"{FISCALDATA_API_BASE_URL}/{str(path).lstrip('/')}"
    headers = {
        "User-Agent": f"btcpush/{APP_ID} Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*",
    }
    r = public_io.get(
        url,
        params=params or {},
        headers=headers,
        timeout=(3, max(1, FISCALDATA_FETCH_TIMEOUT_SEC)),
    )
    r.raise_for_status()
    return r.json()


def fiscaldata_latest_record_date(path, sort_field):
    data = fiscaldata_get_json(path, {"page[size]": "1", "sort": f"-{sort_field}"})
    rows = data.get("data") or []
    return (rows[0] or {}).get(sort_field) if rows else None


def fetch_treasury_mts_ytd_value(record_date=None):
    path = "v1/accounting/mts/mts_table_1"
    record_date = record_date or fiscaldata_latest_record_date(path, "record_date")
    if not record_date:
        return {"available": False, "reason": "missing_mts_date"}
    data = fiscaldata_get_json(
        path,
        {
            "page[size]": "500",
            "filter": f"record_date:eq:{record_date}",
            "sort": "print_order_nbr",
        },
    )
    rows = data.get("data") or []
    period = parse_ymd(record_date)
    if not period:
        return {"available": False, "reason": "invalid_mts_date"}
    fiscal_year = period.year + (period.month >= 10)
    parents = {str(row.get("classification_id")) for row in rows
               if (row.get("classification_desc") or "").strip() == f"FY {fiscal_year}"}
    ytd_rows = [
        row for row in rows
        if (row.get("classification_desc") or "").strip().lower() == "year-to-date"
        and str(row.get("parent_id")) in parents
    ]
    if len(ytd_rows) != 1:
        return {"available": False, "reason": "missing_ytd_row", "record_date": record_date}
    row = ytd_rows[0]
    return {
        "available": True,
        "record_date": record_date,
        "fiscal_year": fiscal_year,
        "receipts_ytd_usd": safe_float(row.get("current_month_gross_rcpt_amt")),
        "outlays_ytd_usd": safe_float(row.get("current_month_gross_outly_amt")),
        "deficit_ytd_usd": safe_float(row.get("current_month_dfct_sur_amt")),
        "source": "Treasury Fiscal Data MTS Table 1",
        "history": [raw_series_receipt(rows, 'record_date', 'treasury_mts',
                    f"{FISCALDATA_API_BASE_URL}/{path}", time.time(), ['fiscal_proxy'])],
    }


def fetch_treasury_interest_expense_value(record_date=None):
    path = "v2/accounting/od/interest_expense"
    record_date = record_date or fiscaldata_latest_record_date(path, "record_date")
    if not record_date:
        return {"available": False, "reason": "missing_interest_date"}
    data = fiscaldata_get_json(
        path,
        {
            "page[size]": "500",
            "filter": f"record_date:eq:{record_date}",
        },
    )
    rows = data.get("data") or []
    month_total = 0.0
    fytd_total = 0.0
    count = 0
    for row in rows:
        month_value = safe_float(row.get("month_expense_amt"))
        fytd_value = safe_float(row.get("fytd_expense_amt"))
        if month_value is None and fytd_value is None:
            continue
        month_total += month_value or 0.0
        fytd_total += fytd_value or 0.0
        count += 1
    if count == 0:
        return {"available": False, "reason": "empty_interest_rows", "record_date": record_date}
    return {
        "available": True,
        "record_date": record_date,
        "interest_month_usd": month_total,
        "interest_fytd_usd": fytd_total,
        "source": "Treasury Fiscal Data Interest Expense",
        "history": [raw_series_receipt(rows, 'record_date', 'treasury_interest',
                    f"{FISCALDATA_API_BASE_URL}/{path}", time.time(), ['fiscal_proxy'])],
    }


def fetch_treasury_long_auction_value():
    path = "v1/accounting/od/auctions_query"
    data = fiscaldata_get_json(
        path,
        {
            "page[size]": "20",
            "sort": "-auction_date",
            "filter": "security_type:in:(Note,Bond)",
        },
    )
    rows = data.get("data") or []
    retrieved_at = time.time()
    today_utc = datetime.utcfromtimestamp(retrieved_at).date().isoformat()
    observations = []
    bid_to_cover = []
    offering_total = 0.0
    latest = None
    for row in rows:
        if not isinstance(row, dict):
            continue
        auction_date = iso_date(row.get("auction_date"))
        btc = safe_float(row.get("bid_to_cover_ratio"))
        offering = safe_float(row.get("offering_amt"))
        if (auction_date is None or auction_date > today_utc
                or isinstance(row.get("bid_to_cover_ratio"), bool)
                or btc is None or not math.isfinite(btc) or btc < 0):
            continue
        # Announcement/issue/maturity dates describe plans or contracts, not
        # completed auction observations. Only result fields enter the history.
        observations.append({"auction_date": auction_date, "bid_to_cover_ratio": btc,
                             "offering_amt": offering})
        if offering is not None:
            offering_total += offering
        item = {
            "auction_date": row.get("auction_date"),
            "security_type": row.get("security_type"),
            "security_term": row.get("security_term"),
            "bid_to_cover": btc,
        }
        bid_to_cover.append(item)
        if latest is None or item["auction_date"] > latest["auction_date"]:
            latest = item
    if not bid_to_cover:
        return {"available": False, "reason": "empty_long_auction_results"}
    return {
        "available": True,
        "source": "Treasury Fiscal Data Auctions Query",
        "history": [raw_series_receipt(observations, 'auction_date', 'treasury_auction',
                    f"{FISCALDATA_API_BASE_URL}/{path}", retrieved_at, ['fiscal_proxy'])],
        "latest_auction_date": latest.get("auction_date") if latest else None,
        "latest_bid_to_cover": latest.get("bid_to_cover") if latest else None,
        "avg_bid_to_cover": sum(item["bid_to_cover"] for item in bid_to_cover) / len(bid_to_cover),
        "auction_count": len(bid_to_cover),
        "offering_recent_usd": offering_total,
    }


def fetch_m2_liquidity_value():
    if not FRED_M2_SERIES_ID:
        return {"available": False, "reason": "m2_series_not_configured"}
    series = fetch_fred_series(FRED_M2_SERIES_ID)
    latest = latest_series_point(series)
    if not latest:
        return {"available": False, "reason": "empty_m2_series"}

    points = [item for item in series or [] if safe_float(item.get("value")) is not None and item.get("date")]
    points.sort(key=lambda item: item["date"])
    yoy_items = []
    for point in points[-4:]:
        yoy = series_delta_pct_for_point(points, point, 365)
        if yoy is not None:
            yoy_items.append({"date": point.get("date"), "yoy_pct": yoy})

    yoy_pct = series_delta_pct_for_point(points, latest, 365)
    yoy_two_month_rising = None
    if len(yoy_items) >= 3:
        last_three = yoy_items[-3:]
        yoy_two_month_rising = (
            last_three[2]["yoy_pct"] > last_three[1]["yoy_pct"]
            and last_three[1]["yoy_pct"] > last_three[0]["yoy_pct"]
        )

    if yoy_pct is None:
        status = "unknown"
    elif yoy_pct < 0:
        status = "contracting"
    elif yoy_pct > 3.0:
        status = "confirmed_expansion"
    elif yoy_two_month_rising is True:
        status = "improving"
    else:
        status = "mild_expansion"

    return {
        "available": True,
        "latest": safe_float(latest.get("value")),
        "date": latest.get("date"),
        "yoy_pct": yoy_pct,
        "yoy_two_month_rising_rule": yoy_two_month_rising,
        "status": status,
        "recent_yoy": yoy_items,
        "history": [series_receipt(points, FRED_M2_SERIES_ID, FRED_CSV_URL, time.time(),
                    ("m2_level", "m2_yoy_pct", "m2_yoy_two_month_rising_rule"))],
        "source": f"FRED CSV {FRED_M2_SERIES_ID}",
    }


def buyback_scale_status(amount):
    amount = safe_float(amount)
    if amount is None:
        return "unknown"
    if amount >= TREASURY_BUYBACK_STRONG_USD:
        return "strong"
    if amount >= TREASURY_BUYBACK_EFFECTIVE_USD:
        return "effective"
    return "limited"


def fetch_treasury_buyback_value():
    path = "v1/accounting/od/buybacks_operations"
    data = fiscaldata_get_json(
        path,
        {
            "page[size]": "50",
            "sort": "-operation_date",
        },
    )
    rows = data.get("data") or []
    rows = [row for row in rows if row.get("operation_date")]
    if not rows:
        return {"available": False, "reason": "empty_buyback_results"}

    today = datetime.now(US_EASTERN_TZ or TOKYO_TZ).date()
    # Announcements share this endpoint with results. A scheduled date or a
    # maximum amount does not establish a completed buyback.
    completed = []
    for row in rows:
        operation_date = parse_ymd(row.get("operation_date"))
        accepted = safe_float(row.get("total_par_amt_accepted"))
        if (operation_date and operation_date.date() <= today
                and accepted is not None and math.isfinite(accepted) and accepted >= 0):
            completed.append(row)
    if not completed:
        return {"available": False, "reason": "no_completed_buyback_results"}
    completed.sort(key=lambda row: row["operation_date"], reverse=True)
    latest = completed[0]
    latest_date = latest["operation_date"]
    latest_dt = parse_ymd(latest_date)
    cutoff = today - timedelta(days=max(1, TREASURY_BUYBACK_LOOKBACK_DAYS))
    recent_rows = []
    for row in completed:
        dt = parse_ymd(row.get("operation_date"))
        if dt and cutoff <= dt.date() <= today:
            recent_rows.append(row)

    latest_accepted = safe_float(latest.get("total_par_amt_accepted"))
    recent_accepted = sum(
        safe_float(row.get("total_par_amt_accepted")) or 0.0
        for row in recent_rows
    )
    days_since_latest = (today - latest_dt.date()).days if latest_dt else None

    return {
        "available": True,
        "operation_date": latest_date,
        "operation_type": latest.get("operation_type"),
        "security_type": latest.get("security_type"),
        "maturity_bucket": latest.get("maturity_bucket"),
        "total_par_amt_accepted": latest_accepted,
        "max_par_amt_redeemed": safe_float(latest.get("max_par_amt_redeemed")),
        "scale_status": buyback_scale_status(latest_accepted),
        "recent_count": len(recent_rows),
        "recent_total_accepted": recent_accepted,
        "recent_window_days": TREASURY_BUYBACK_LOOKBACK_DAYS,
        "recent_exists_rule": True if recent_rows else False,
        "repeated_rule": True if len(recent_rows) >= 2 else False,
        "days_since_latest": days_since_latest,
        "source": "Treasury Fiscal Data Buybacks Operations",
        "history": [raw_series_receipt(completed, 'operation_date', 'treasury_buyback',
                    f"{FISCALDATA_API_BASE_URL}/{path}", time.time(), ['treasury_buyback', 'treasury_buyback_recent_count', 'treasury_buyback_total_par_accepted', 'treasury_buyback_scale_status'])],
    }


def series_latest_and_yoy(series):
    latest = latest_series_point(series)
    return {
        "series": list(series or [])[-450:],
        "latest": safe_float((latest or {}).get("value")),
        "date": (latest or {}).get("date"),
        "yoy_pct": series_delta_pct(series, 365),
    }


def classify_cost_of_living_gap(cost_yoy, wage_yoy):
    cost = safe_float(cost_yoy)
    wage = safe_float(wage_yoy)
    if cost is None or wage is None:
        return "unknown", None
    gap = cost - wage
    if gap > COST_OF_LIVING_GAP_NEUTRAL_PP:
        return "cost_outpacing", gap
    if gap < -COST_OF_LIVING_GAP_NEUTRAL_PP:
        return "wage_outpacing", gap
    return "mixed", gap


def fetch_cost_of_living_proxy_value():
    components = {}
    missing = {}
    yoys = []
    source_dates = {}
    history = []
    for key, (series_id, label) in FRED_COST_OF_LIVING_SERIES.items():
        try:
            item = series_latest_and_yoy(fetch_fred_series(series_id))
            history.append(series_receipt(item["series"], series_id, FRED_CSV_URL,
                                          time.time(), ("cost_of_living_proxy",)))
            components[key] = {
                "label": label,
                "series_id": series_id,
                "latest": item.get("latest"),
                "date": item.get("date"),
                "yoy_pct": item.get("yoy_pct"),
            }
            if item.get("date"):
                source_dates[key] = item.get("date")
            if item.get("yoy_pct") is not None:
                yoys.append(item.get("yoy_pct"))
        except Exception as exc:
            missing[label] = str(exc)[:80]

    wage = {}
    try:
        wage_item = series_latest_and_yoy(fetch_fred_series(FRED_WAGE_SERIES_ID))
        history.append(series_receipt(wage_item["series"], FRED_WAGE_SERIES_ID,
                                      FRED_CSV_URL, time.time(), ("cost_of_living_proxy",)))
        wage = {
            "label": "工资增长",
            "series_id": FRED_WAGE_SERIES_ID,
            "latest": wage_item.get("latest"),
            "date": wage_item.get("date"),
            "yoy_pct": wage_item.get("yoy_pct"),
        }
        if wage_item.get("date"):
            source_dates["wage"] = wage_item.get("date")
    except Exception as exc:
        missing["工资增长"] = str(exc)[:80]

    cost_yoy = sum(yoys) / len(yoys) if yoys else None
    status, gap = classify_cost_of_living_gap(cost_yoy, wage.get("yoy_pct"))
    return {
        "status": status,
        "cost_proxy_yoy_pct": cost_yoy,
        "wage_yoy_pct": wage.get("yoy_pct"),
        "cost_wage_gap_pp": gap,
        "components": components,
        "history": history,
        "wage": wage,
        "missing": missing,
        "source": "FRED CSV official/proxy series",
        "source_date": source_dates,
    }


def fetch_fred_fiscal_series_value():
    values = {}
    missing = {}
    source_dates = {}
    history = []
    for key, (series_id, label) in FRED_FISCAL_SERIES.items():
        try:
            item = series_latest_and_yoy(fetch_fred_series(series_id))
            history.append(series_receipt(item["series"], series_id, FRED_CSV_URL,
                                          time.time(), ("fiscal_proxy",)))
            values[key] = item.get("latest")
            values[f"{key}_date"] = item.get("date")
            if item.get("date"):
                source_dates[key] = item.get("date")
        except Exception as exc:
            missing[label] = str(exc)[:80]
    interest = safe_float(values.get("interest_payments"))
    receipts = safe_float(values.get("federal_receipts"))
    if (interest is not None and receipts and values.get("interest_payments_date")
            and values.get("interest_payments_date") == values.get("federal_receipts_date")):
        values["interest_revenue_ratio_pct"] = interest / receipts * 100.0
    elif interest is not None and receipts:
        missing["利息/收入"] = "fred_period_mismatch"
    return {
        "values": values,
        "history": history,
        "missing": missing,
        "source": "FRED CSV fiscal series",
        "source_date": source_dates,
    }


def classify_fiscal_status(fiscal, us10y):
    interest_ratio = safe_float(fiscal.get("interest_revenue_ratio_pct"))
    deficit_gdp = safe_float(fiscal.get("deficit_gdp_pct"))
    debt_gdp = safe_float(fiscal.get("debt_gdp_pct"))
    bid_to_cover = safe_float(fiscal.get("long_auction_avg_bid_to_cover"))
    us10y_value = safe_float(us10y)

    checks = {
        "interest_revenue_stressed": rule_ge(interest_ratio, FISCAL_INTEREST_REVENUE_STRESS_PCT),
        "interest_revenue_dominance": rule_ge(interest_ratio, FISCAL_INTEREST_REVENUE_DOMINANCE_PCT),
        "deficit_gdp_stressed": rule_le(deficit_gdp, FISCAL_DEFICIT_GDP_STRESS_PCT),
        "deficit_gdp_dominance": rule_le(deficit_gdp, FISCAL_DEFICIT_GDP_DOMINANCE_PCT),
        "debt_gdp_stressed": rule_ge(debt_gdp, FISCAL_DEBT_GDP_STRESS_PCT),
        "long_auction_weak": rule_le(bid_to_cover, FISCAL_LONG_AUCTION_BTC_STRESS),
        "us10y_high": rule_ge(us10y_value, MACRO_RATE_HIKE_US10Y_LEVEL),
    }
    known = rule_known_count(checks.values())
    if known == 0:
        return "unknown", checks

    stress_hits = rule_hit_count([
        checks["interest_revenue_stressed"],
        checks["deficit_gdp_stressed"],
        checks["debt_gdp_stressed"],
        checks["long_auction_weak"],
        checks["us10y_high"],
    ])
    dominance = rule_all(
        checks["interest_revenue_dominance"],
        rule_any(checks["deficit_gdp_stressed"], checks["debt_gdp_stressed"]),
        rule_any(checks["long_auction_weak"], checks["us10y_high"]),
    )
    if dominance is True or (checks["interest_revenue_dominance"] is True and stress_hits >= 3):
        return "fiscal_dominance", checks
    if stress_hits >= 2:
        return "stressed", checks
    return "normal", checks


def fetch_fiscal_proxy_value(us10y=None):
    fred = fetch_fred_fiscal_series_value()
    fiscal = {
        "history": fred.get("history") or [],
        "deficit_gdp_pct": safe_float((fred.get("values") or {}).get("deficit_gdp")),
        "interest_payments_annual_usd": (
            safe_float((fred.get("values") or {}).get("interest_payments")) * 1_000_000_000.0
            if safe_float((fred.get("values") or {}).get("interest_payments")) is not None else None
        ),
        "federal_receipts_annual_usd": (
            safe_float((fred.get("values") or {}).get("federal_receipts")) * 1_000_000_000.0
            if safe_float((fred.get("values") or {}).get("federal_receipts")) is not None else None
        ),
        "debt_gdp_pct": safe_float((fred.get("values") or {}).get("debt_gdp")),
        "interest_revenue_ratio_pct": safe_float((fred.get("values") or {}).get("interest_revenue_ratio_pct")),
        "interest_revenue_ratio_source": "FRED annualized federal receipts",
        "interest_revenue_ratio_date": (fred.get("values") or {}).get("interest_payments_date"),
        "missing": dict(fred.get("missing") or {}),
        "source_date": dict(fred.get("source_date") or {}),
        "sources": {"fred": fred.get("source")},
    }

    mts = {}
    interest = {}
    try:
        mts = fetch_treasury_mts_ytd_value()
        if mts.get("available"):
            fiscal["history"].extend(mts.get("history") or [])
            fiscal.update({
                "treasury_receipts_ytd_usd": mts.get("receipts_ytd_usd"),
                "treasury_outlays_ytd_usd": mts.get("outlays_ytd_usd"),
                "treasury_deficit_ytd_usd": mts.get("deficit_ytd_usd"),
            })
            fiscal["source_date"]["treasury_mts"] = mts.get("record_date")
            fiscal["sources"]["treasury_mts"] = mts.get("source")
        else:
            fiscal["missing"]["Treasury MTS"] = mts.get("reason")
    except Exception as exc:
        fiscal["missing"]["Treasury MTS"] = str(exc)[:80]

    try:
        interest = fetch_treasury_interest_expense_value()
        if interest.get("available"):
            fiscal["history"].extend(interest.get("history") or [])
            fiscal.update({
                "treasury_interest_month_usd": interest.get("interest_month_usd"),
                "treasury_interest_fytd_usd": interest.get("interest_fytd_usd"),
            })
            fiscal["source_date"]["treasury_interest"] = interest.get("record_date")
            fiscal["sources"]["treasury_interest"] = interest.get("source")
        else:
            fiscal["missing"]["Treasury Interest Expense"] = interest.get("reason")
    except Exception as exc:
        fiscal["missing"]["Treasury Interest Expense"] = str(exc)[:80]

    if mts.get("available") and interest.get("available"):
        try:
            ratio_date = min(mts["record_date"], interest["record_date"])
            aligned_mts = mts if mts["record_date"] == ratio_date else fetch_treasury_mts_ytd_value(ratio_date)
            aligned_interest = (interest if interest["record_date"] == ratio_date
                                else fetch_treasury_interest_expense_value(ratio_date))
            receipts_ytd = safe_float(aligned_mts.get("receipts_ytd_usd"))
            interest_ytd = safe_float(aligned_interest.get("interest_fytd_usd"))
            if (not aligned_mts.get("available") or not aligned_interest.get("available")
                    or aligned_mts.get("record_date") != ratio_date
                    or aligned_interest.get("record_date") != ratio_date
                    or not receipts_ytd or interest_ytd is None):
                raise ValueError("matched_fiscal_period_unavailable")
            fiscal.update({
                "interest_revenue_ratio_pct": interest_ytd / receipts_ytd * 100.0,
                "interest_revenue_ratio_source": "Treasury Fiscal Data YTD",
                "interest_revenue_ratio_date": ratio_date,
                "interest_revenue_ratio_fiscal_year": aligned_mts["fiscal_year"],
                "interest_revenue_ratio_interest_ytd_usd": interest_ytd,
                "interest_revenue_ratio_receipts_ytd_usd": receipts_ytd,
            })
            for aligned, original in ((aligned_mts, mts), (aligned_interest, interest)):
                if aligned is not original:
                    for receipt in aligned.get("history") or []:
                        # Keep the existing evidence source identity. Both the
                        # latest supplement and matched ratio need their raw rows.
                        for existing in fiscal["history"]:
                            if existing["series_id"] == receipt["series_id"]:
                                existing.update(raw_series_receipt(
                                    existing["rows"] + receipt["rows"], "record_date",
                                    receipt["series_id"], receipt["endpoint_url"],
                                    max(existing["retrieved_at"], receipt["retrieved_at"]),
                                    receipt["fields"]))
                                break
                        else:
                            fiscal["history"].append(copy.deepcopy(receipt))
        except Exception as exc:
            fiscal["missing"]["Treasury YTD Ratio"] = type(exc).__name__

    try:
        auction = fetch_treasury_long_auction_value()
        if auction.get("available"):
            fiscal["history"].extend(auction.get("history") or [])
            fiscal.update({
                "long_auction_latest_date": auction.get("latest_auction_date"),
                "long_auction_latest_bid_to_cover": auction.get("latest_bid_to_cover"),
                "long_auction_avg_bid_to_cover": auction.get("avg_bid_to_cover"),
                "long_auction_count": auction.get("auction_count"),
                "long_auction_offering_recent_usd": auction.get("offering_recent_usd"),
            })
            fiscal["source_date"]["treasury_auction"] = auction.get("latest_auction_date")
            fiscal["sources"]["treasury_auction"] = auction.get("source")
        else:
            fiscal["missing"]["Treasury Auction"] = auction.get("reason")
    except Exception as exc:
        fiscal["missing"]["Treasury Auction"] = str(exc)[:80]

    status, checks = classify_fiscal_status(fiscal, us10y)
    fiscal["status"] = status
    fiscal["checks"] = checks
    return fiscal


def fetch_long_term_macro_value(value):
    missing = {}
    cost = {}
    fiscal = {}
    try:
        cost = fetch_cost_of_living_proxy_value()
    except Exception as exc:
        missing["Cost of Living"] = str(exc)[:120]
    try:
        fiscal = fetch_fiscal_proxy_value(value.get("us10y"))
    except Exception as exc:
        missing["Fiscal"] = str(exc)[:120]
    return {
        "cost_of_living_proxy": cost,
        "fiscal_proxy": fiscal,
        "long_term_missing_reason": missing,
        "long_term_sources": {
            "cost_of_living": (cost or {}).get("source"),
            "fiscal": (fiscal or {}).get("sources"),
        },
        "long_term_source_date": {
            "cost_of_living": (cost or {}).get("source_date"),
            "fiscal": (fiscal or {}).get("source_date"),
        },
    }


def score_yield_position(value):
    value = safe_float(value)
    if value is None:
        return None
    if value < 3.5:
        return 100.0
    if value < 4.0:
        return 80.0
    if value < 4.5:
        return 60.0
    if value < 5.0:
        return 30.0
    return 10.0


def score_real_yield_position(value):
    value = safe_float(value)
    if value is None:
        return None
    if value < 1.0:
        return 100.0
    if value < 1.5:
        return 80.0
    if value < 2.0:
        return 60.0
    if value < 2.5:
        return 35.0
    return 15.0


def yield_trend_adjust(delta_pp):
    delta = safe_float(delta_pp)
    if delta is None:
        return 0.0
    if delta <= -0.30:
        return 15.0
    if delta <= -0.10:
        return 5.0
    if delta >= 0.30:
        return -15.0
    if delta >= 0.10:
        return -5.0
    return 0.0


def score_usd_trend(delta_30d_pct):
    delta = safe_float(delta_30d_pct)
    if delta is None:
        return None
    if delta <= -2.0:
        return 85.0
    if delta <= -0.5:
        return 70.0
    if delta < 0.5:
        return 55.0
    if delta < 2.0:
        return 35.0
    return 20.0


def score_fed_policy(policy):
    if policy == "Dovish":
        return 75.0
    if policy == "Hawkish":
        return 30.0
    if policy == "Neutral":
        return 55.0
    return None


def build_macro_model(value):
    us10y_score = None
    if value.get("us10y") is not None:
        us10y_score = clamp(score_yield_position(value.get("us10y")) + yield_trend_adjust(value.get("us10y_30d_pp")))
    real_score = None
    if value.get("real_yield_10y") is not None:
        real_score = clamp(score_real_yield_position(value.get("real_yield_10y")) + yield_trend_adjust(value.get("real_yield_10y_30d_pp")))
    usd_score = score_usd_trend(value.get("usd_index_30d_pct"))
    fed_score = score_fed_policy(value.get("fed_policy"))

    scores = {
        "real_yield": real_score,
        "us10y": us10y_score,
        "usd_index": usd_score,
        "fed": fed_score,
    }
    weights = {
        "real_yield": 40.0,
        "us10y": 25.0,
        "usd_index": 25.0,
        "fed": 10.0,
    }
    if real_score is None and us10y_score is not None:
        weights["us10y"] += weights["real_yield"]
        weights["real_yield"] = 0.0

    available_weight = sum(weight for key, weight in weights.items() if scores.get(key) is not None)
    macro_score = None
    if available_weight > 0:
        macro_score = sum(scores[key] * weights[key] for key in weights if scores.get(key) is not None) / available_weight

    expansion_votes = 0
    contraction_votes = 0
    yield_down = (
        safe_float(value.get("real_yield_10y_30d_pp")) is not None
        and safe_float(value.get("real_yield_10y_30d_pp")) <= -0.10
    ) or (
        safe_float(value.get("us10y_30d_pp")) is not None
        and safe_float(value.get("us10y_30d_pp")) <= -0.10
    )
    yield_up = (
        safe_float(value.get("real_yield_10y_30d_pp")) is not None
        and safe_float(value.get("real_yield_10y_30d_pp")) >= 0.10
    ) or (
        safe_float(value.get("us10y_30d_pp")) is not None
        and safe_float(value.get("us10y_30d_pp")) >= 0.10
    )
    usd_down = safe_float(value.get("usd_index_30d_pct")) is not None and safe_float(value.get("usd_index_30d_pct")) <= -0.5
    usd_up = safe_float(value.get("usd_index_30d_pct")) is not None and safe_float(value.get("usd_index_30d_pct")) >= 0.5
    if yield_down:
        expansion_votes += 1
    if yield_up:
        contraction_votes += 1
    if usd_down:
        expansion_votes += 1
    if usd_up:
        contraction_votes += 1
    if value.get("fed_policy") == "Dovish":
        expansion_votes += 1
    if value.get("fed_policy") == "Hawkish":
        contraction_votes += 1

    if contraction_votes >= 2:
        macro_regime = "Liquidity Contraction"
        risk_budget = "Reduced"
    elif expansion_votes >= 2:
        macro_regime = "Liquidity Expansion"
        risk_budget = "Expanded"
    else:
        macro_regime = "Neutral"
        risk_budget = "Normal"

    missing = []
    if value.get("us10y") is None:
        missing.append("US10Y")
    if value.get("real_yield_10y") is None:
        missing.append("Real Yield")
    if value.get("usd_index") is None:
        missing.append("USD Index")

    return {
        "macro_score": macro_score,
        "macro_regime": macro_regime,
        "risk_budget": risk_budget,
        "partial_macro": bool(missing),
        "score_components": scores,
        "missing": missing,
    }


def fetch_funding_record(symbol=None):
    symbol = symbol or BINANCE_SYMBOL
    raw = binance_get("/fapi/v1/fundingRate", {"symbol": symbol, "limit": 1000 if assets.current().equity else 241})
    retrieved_at = time.time()
    if not isinstance(raw, list) or any(not isinstance(item, dict) or item.get("symbol", symbol) != symbol for item in raw):
        raise ValueError("funding_symbol_or_response_mismatch")
    current_interval = None
    metadata_reason = None
    try:
        metadata = public_io.cached_call(("funding_metadata", BINANCE_FUTURES_BASE_URL), 60,
                                         lambda: binance_get("/fapi/v1/fundingInfo"))
        for item in metadata or []:
            if isinstance(item, dict) and item.get("symbol") == symbol:
                current_interval = safe_float(item.get("fundingIntervalHours"))
                break
        if current_interval is None or current_interval <= 0:
            current_interval = None
            metadata_reason = "current_interval_not_published"
    except Exception:
        metadata_reason = "current_interval_unavailable"
    rows = settlement_rows(raw, current_ms=int(retrieved_at * 1000))
    special = settlement_rows(raw, current_ms=int(retrieved_at * 1000), rate_type="Special")
    if not rows and not special:
        return make_unavailable_record("Binance Futures fundingRate", "empty_response")
    funding_8h, quality_8h, _ = settled_window(rows, 8, current_interval, as_of_ms=retrieved_at * 1000)
    funding_24h, quality_24h, _ = settled_window(rows, 24, current_interval, as_of_ms=retrieved_at * 1000)
    _, history_quality, history = settled_window(rows, 240, current_interval, as_of_ms=retrieved_at * 1000)
    value = {
        "funding_8h_pct": funding_8h, "funding_24h_pct": funding_24h,
        "latest_settled_rate_pct": rows[-1]["rate"] * 100 if rows and rows[-1]["rate"] is not None else None,
        "latest_funding_time_ms": rows[-1]["time"] if rows else None,
        "window_start_time_ms": quality_24h["start_ms"],
        "current_interval_hours": current_interval,
        "current_interval_metadata_scope": "current_only",
        "current_interval_reason": metadata_reason,
        "settlement_windows": {"8h": quality_8h, "24h": quality_24h},
        "history": history, "window_quality": history_quality,
        "rate_type": "Regular",
        "special_settlements": special,
        "unknown_rate_types": sorted({str(item.get("rateType")) for item in raw if item.get("rateType", "Regular") not in ("Regular", "Special")}),
    }
    record = make_data_record(value, "Binance Futures fundingRate",
                              status="ok" if funding_8h is not None and funding_24h is not None else "partial",
                              reason=None if funding_8h is not None and funding_24h is not None
                              else "incomplete_settlement_window",
                              updated_at=retrieved_at)
    record.update(endpoint_url=f"{BINANCE_FUTURES_BASE_URL}/fapi/v1/fundingRate",
                  venue="Binance", source_as_of_ms=max(row["time"] for row in rows + special), retrieved_at=retrieved_at,
                  interval_metadata_endpoint=f"{BINANCE_FUTURES_BASE_URL}/fapi/v1/fundingInfo")
    if assets.current().equity and not assets.regular_funding_is_fresh(value, retrieved_at * 1000):
        record.update(source_stale=True, source_stale_reason="regular_settlement_stale_or_missing")
    return record

def funding_rows_contiguous(rows, required, current_ms=None):
    recent = list(rows or [])[-int(required):]
    return settlement_window(recent, int(required), 3600000, as_of_ms=current_ms)[0]["complete"]


def normalize_predicted_fundings(payload, coin=None):
    coin = (coin or HYPERLIQUID_COIN).strip().upper()
    for item in payload or []:
        if not isinstance(item, list) or len(item) < 2 or str(item[0]).upper() != coin:
            continue
        normalized = {}
        for venue_item in item[1] or []:
            if not isinstance(venue_item, list) or len(venue_item) < 2:
                continue
            venue = str(venue_item[0] or "").strip()
            data = venue_item[1] or {}
            rate = safe_float(data.get("fundingRate"))
            interval_hours = safe_float(data.get("fundingIntervalHours"))
            if not venue or rate is None or interval_hours is None or interval_hours <= 0:
                continue
            normalized[venue] = {
                "funding_rate_pct": rate * 100.0,
                "funding_interval_hours": interval_hours,
                "funding_8h_equivalent_pct": rate * (8.0 / interval_hours) * 100.0,
                "next_funding_time_ms": int(data.get("nextFundingTime") or 0) or None,
            }
        return normalized
    return {}


def build_hyperliquid_funding_value(history_payload, predicted_payload, current_ms=None):
    rows = []
    for item in history_payload or []:
        rate = safe_float((item or {}).get("fundingRate"))
        funding_time = int((item or {}).get("time") or 0)
        if rate is None or not math.isfinite(rate) or funding_time <= 0:
            continue
        rows.append({
            "rate": rate,
            "time": funding_time,
            "premium": safe_float((item or {}).get("premium")),
        })
    rows.sort(key=lambda item: item["time"])
    if not rows:
        return {}
    predicted = normalize_predicted_fundings(predicted_payload)
    value = {
        "funding_1h_pct": rows[-1]["rate"] * 100.0,
        "funding_8h_pct": (
            sum(item["rate"] for item in rows[-8:]) * 100.0
            if funding_rows_contiguous(rows, 8, current_ms)
            else None
        ),
        "funding_24h_pct": (
            sum(item["rate"] for item in rows[-24:]) * 100.0
            if funding_rows_contiguous(rows, 24, current_ms)
            else None
        ),
        "latest_funding_time_ms": rows[-1]["time"],
        "window_start_time_ms": rows[-24]["time"] if len(rows) >= 24 else rows[0]["time"],
        "actual_interval_hours": 1,
        "history": rows[-24:],
        "window_quality": settlement_window(rows[-24:], 24, 3600000, as_of_ms=current_ms)[0],
        "predicted_by_venue": predicted,
        "predicted_8h_equivalent_pct": {
            venue: data["funding_8h_equivalent_pct"]
            for venue, data in predicted.items()
        },
    }
    return value


def fetch_hyperliquid_funding_record():
    if not BTC_PUSH_HYPERLIQUID_ENABLED:
        return make_unavailable_record(HYPERLIQUID_FUNDING_SOURCE, "disabled")
    current_ms = now_ms()
    history = hyperliquid_post_json({
        "type": "fundingHistory",
        "coin": HYPERLIQUID_COIN,
        "startTime": current_ms - 30 * 3600 * 1000,
        "endTime": current_ms,
    })
    predicted = hyperliquid_post_json({"type": "predictedFundings"})
    value = build_hyperliquid_funding_value(history, predicted, current_ms)
    if not value:
        return make_unavailable_record(HYPERLIQUID_FUNDING_SOURCE, "empty_response")
    complete = (
        value.get("funding_8h_pct") is not None
        and value.get("funding_24h_pct") is not None
        and bool(value.get("predicted_by_venue"))
    )
    record = make_data_record(
        value,
        HYPERLIQUID_FUNDING_SOURCE,
        status="ok" if complete else "partial",
        reason=None if complete else "incomplete_funding_window_or_prediction",
    )
    record["scope"] = "Hyperliquid BTC perpetual funding and cross-venue predictions"
    record.update(endpoint_url=HYPERLIQUID_INFO_URL, venue="Hyperliquid", retrieved_at=time.time())
    record["source_as_of_ms"] = value.get("latest_funding_time_ms")
    stale_reason = source_age_reason("hyperliquid_funding", record, record["retrieved_at"])
    record["source_stale"] = stale_reason is not None
    record["source_stale_reason"] = stale_reason
    return record


def ensure_hyperliquid_latest_ctx(state):
    derivatives_state = prune_hyperliquid_state((state or {}).get("hyperliquid_derivatives") or {})
    latest_ctx = derivatives_state.get("latest_ctx") or {}
    age_sec = (
        max(0.0, (now_ms() - int(latest_ctx.get("received_at_ms") or 0)) / 1000.0)
        if latest_ctx
        else None
    )
    if age_sec is not None and age_sec <= HYPERLIQUID_CTX_STALE_SEC:
        state["hyperliquid_derivatives"] = derivatives_state
        return latest_ctx
    ctx = fetch_hyperliquid_asset_ctx_rest()
    append_hyperliquid_minute_sample(state, ctx)
    return ((state.get("hyperliquid_derivatives") or {}).get("latest_ctx") or {})


def fetch_hyperliquid_basis_record(state):
    if not BTC_PUSH_HYPERLIQUID_ENABLED:
        return make_unavailable_record(HYPERLIQUID_SOURCE, "disabled")
    latest_ctx = ensure_hyperliquid_latest_ctx(state)
    received_at_ms = int(latest_ctx.get("received_at_ms") or 0)
    if not received_at_ms or now_ms() - received_at_ms > HYPERLIQUID_CTX_STALE_SEC * 1000:
        return make_unavailable_record(HYPERLIQUID_SOURCE, "ctx_stale_or_missing")
    mark_price = safe_float(latest_ctx.get("mark_px"))
    oracle_price = safe_float(latest_ctx.get("oracle_px"))
    premium = safe_float(latest_ctx.get("premium"))
    basis_pct = (
        (mark_price - oracle_price) / oracle_price * 100.0
        if mark_price is not None and oracle_price
        else None
    )
    value = {
        "basis_pct": basis_pct,
        "premium_pct": premium * 100.0 if premium is not None else None,
        "mark_price": mark_price,
        "index_price": oracle_price,
        "open_interest_btc": safe_float(latest_ctx.get("open_interest_btc")),
        "open_interest_usd": safe_float(latest_ctx.get("open_interest_usd")),
        "received_at_ms": received_at_ms,
    }
    if not value_has_data(value):
        return make_unavailable_record(HYPERLIQUID_SOURCE, "ctx_fields_missing")
    record = make_data_record(value, HYPERLIQUID_SOURCE, updated_at=int(received_at_ms / 1000))
    record["scope"] = "Hyperliquid BTC perpetual"
    record.update(source_as_of_ms=None, time_basis="received_at", retrieved_at=received_at_ms / 1000,
                  source_time_missing=True, endpoint_url=HYPERLIQUID_INFO_URL, venue="Hyperliquid")
    return record


def fetch_basis_record(symbol=None):
    data = binance_get("/fapi/v1/premiumIndex", {"symbol": symbol or BINANCE_SYMBOL})
    retrieved_at = time.time()
    mark_price = safe_float(data.get("markPrice"))
    index_price = safe_float(data.get("indexPrice"))
    last_funding_rate = safe_float(data.get("lastFundingRate"))
    basis_pct = None
    if mark_price is not None and index_price:
        basis_pct = (mark_price - index_price) / index_price * 100.0
    value = {
        "basis_pct": basis_pct,
        "mark_price": mark_price,
        "index_price": index_price,
        "last_funding_rate_pct": last_funding_rate * 100.0 if last_funding_rate is not None else None,
        "next_funding_time": int(data.get("nextFundingTime") or 0) or None,
    }
    if not value_has_data(value):
        return make_unavailable_record("Binance Futures premiumIndex", "empty_response")
    record = make_data_record(value, "Binance Futures premiumIndex")
    record.update(endpoint_url=f"{BINANCE_FUTURES_BASE_URL}/fapi/v1/premiumIndex",
                  venue="Binance", source_as_of_ms=epoch_ms(data.get("time")), retrieved_at=retrieved_at)
    return record


def fetch_macro_record(state):
    # A print/dry-run never opens or mutates the live delivery database.
    store = _delivery_stores.get(delivery_store_key())
    record = common.macro_snapshot(store, macro_cache_identity(), state.get("macro_cache"),
        lambda cached: _fetch_macro_record({"macro_cache": cached}), time.time())
    state["macro_cache"] = copy.deepcopy(record)
    return record


def _fetch_macro_record(state):
    cached = state.get("macro_cache") or {}
    now_ts = int(time.time())
    if (
        record_has_data(cached)
        and cached.get("schema_version") == MACRO_CACHE_SCHEMA_REVISION
        and now_ts - int(cached.get("updated_at") or 0) < MACRO_CACHE_TTL_SEC
    ):
        return dict(cached)

    value = {
        "source_date": {},
        "sources": {},
        "missing_reason": {},
        "history": [],
        "field_sources": {},
    }

    def capture_series(rows, series_id, endpoint, fields):
        value["history"].append(series_receipt(rows, series_id, endpoint, time.time(), fields))

    try:
        us10y_series = fetch_yahoo_chart_series(YAHOO_US10Y_SYMBOL)
        capture_series(us10y_series, "yahoo_us10y",
                       f"{YAHOO_CHART_BASE_URL}/{yahoo_symbol_url(YAHOO_US10Y_SYMBOL)}",
                       ("us10y", "us10y_1d_pp", "us10y_7d_pp", "us10y_30d_pp", "us10y_60d_pp"))
        us10y_latest = latest_series_point(us10y_series)
        if us10y_latest:
            value["us10y"] = safe_float(us10y_latest.get("value"))
            value["us10y_1d_pp"] = series_delta_pp(us10y_series, 1)
            value["us10y_7d_pp"] = series_delta_pp(us10y_series, 7)
            value["us10y_30d_pp"] = series_delta_pp(us10y_series, 30)
            value["us10y_60d_pp"] = series_delta_pp(us10y_series, 60)
            value["source_date"]["us10y"] = us10y_latest.get("date")
            value["sources"]["us10y"] = f"Yahoo Finance {YAHOO_US10Y_SYMBOL}"
            for key in ("us10y", "us10y_1d_pp", "us10y_7d_pp", "us10y_30d_pp", "us10y_60d_pp"):
                if value.get(key) is not None:
                    value["field_sources"][key] = "yahoo_us10y"
    except Exception as exc:
        value["missing_reason"]["US10Y"] = f"yahoo:{str(exc)[:80]}"

    us10y_source_stale = is_source_date_stale(
        (value.get("source_date") or {}).get("us10y"),
        MACRO_SOURCE_STALE_SEC,
    )
    us10y_delta_keys = ("us10y_1d_pp", "us10y_7d_pp", "us10y_30d_pp", "us10y_60d_pp")
    us10y_delta_missing = any(value.get(key) is None for key in us10y_delta_keys)
    if value.get("us10y") is None or us10y_source_stale or us10y_delta_missing:
        try:
            us10y_series = fetch_treasury_xml_series("daily_treasury_yield_curve", "BC_10YEAR")
            capture_series(us10y_series, "treasury_us10y", TREASURY_XML_BASE_URL, ())
            us10y_latest = latest_series_point(us10y_series)
            if us10y_latest:
                treasury_us10y_fields = {
                    "us10y": safe_float(us10y_latest.get("value")),
                    "us10y_1d_pp": series_delta_pp(us10y_series, 1),
                    "us10y_7d_pp": series_delta_pp(us10y_series, 7),
                    "us10y_30d_pp": series_delta_pp(us10y_series, 30),
                    "us10y_60d_pp": series_delta_pp(us10y_series, 60),
                }
                if value.get("us10y") is None or us10y_source_stale:
                    value.update(treasury_us10y_fields)
                    for key, number in treasury_us10y_fields.items():
                        if number is not None:
                            value["field_sources"][key] = "treasury_us10y"
                    value["source_date"]["us10y"] = us10y_latest.get("date")
                    value["sources"]["us10y"] = "US Treasury yield curve"
                else:
                    for key in us10y_delta_keys:
                        if value.get(key) is None:
                            value[key] = treasury_us10y_fields.get(key)
                            if value[key] is not None:
                                value["field_sources"][key] = "treasury_us10y"
                    if any(value.get(key) is not None for key in us10y_delta_keys):
                        value["source_date"]["us10y_delta"] = us10y_latest.get("date")
                        value["sources"]["us10y_delta"] = "US Treasury yield curve"
                value["missing_reason"].pop("US10Y", None)
        except Exception as exc:
            value["missing_reason"]["US10Y"] = f"treasury:{str(exc)[:80]}"

    try:
        real_series = fetch_treasury_xml_series("daily_treasury_real_yield_curve", "TC_10YEAR")
        capture_series(real_series, "treasury_real_10y", TREASURY_XML_BASE_URL,
                       ("real_yield_10y", "real_yield_10y_1d_pp", "real_yield_10y_7d_pp",
                        "real_yield_10y_30d_pp", "real_yield_10y_60d_pp"))
        real_latest = latest_series_point(real_series)
        if real_latest:
            value["real_yield_10y"] = safe_float(real_latest.get("value"))
            value["real_yield_10y_1d_pp"] = series_delta_pp(real_series, 1)
            value["real_yield_10y_7d_pp"] = series_delta_pp(real_series, 7)
            value["real_yield_10y_30d_pp"] = series_delta_pp(real_series, 30)
            value["real_yield_10y_60d_pp"] = series_delta_pp(real_series, 60)
            value["source_date"]["real_yield_10y"] = real_latest.get("date")
            value["sources"]["real_yield_10y"] = "US Treasury real yield curve"
    except Exception as exc:
        value["missing_reason"]["Real Yield"] = f"treasury:{str(exc)[:80]}"

    try:
        usd_series = fetch_yahoo_chart_series(YAHOO_DXY_SYMBOL)
        capture_series(usd_series, "yahoo_dxy",
                       f"{YAHOO_CHART_BASE_URL}/{yahoo_symbol_url(YAHOO_DXY_SYMBOL)}",
                       ("usd_index", "usd_index_1d_pct", "usd_index_7d_pct", "usd_index_30d_pct", "gold_vs_dxy_30d_pct"))
        usd_latest = latest_series_point(usd_series)
        if usd_latest:
            value["usd_index"] = safe_float(usd_latest.get("value"))
            value["usd_index_1d_pct"] = series_delta_pct(usd_series, 1)
            value["usd_index_7d_pct"] = series_delta_pct(usd_series, 7)
            value["usd_index_30d_pct"] = series_delta_pct(usd_series, 30)
            value["source_date"]["usd_index"] = usd_latest.get("date")
            value["sources"]["usd_index"] = f"Yahoo Finance {YAHOO_DXY_SYMBOL}"
    except Exception as exc:
        value["missing_reason"]["USD Index"] = f"yahoo:{str(exc)[:80]}"

    try:
        fed_status = fetch_fed_status_value()
        if fed_status.get("available"):
            value["history"].extend(fed_status.get("history") or [])
            value["fed_target_lower"] = fed_status.get("fed_target_lower")
            value["fed_target_upper"] = fed_status.get("fed_target_upper")
            value["fed_target_mid"] = fed_status.get("fed_target_mid")
            value["fed_target_mid_30d_pp"] = fed_status.get("fed_target_mid_30d_pp")
            value["fed_target_date"] = fed_status.get("fed_target_date")
            value["effective_fed_funds"] = fed_status.get("effective_fed_funds")
            value["effective_fed_funds_date"] = fed_status.get("effective_fed_funds_date")
            value["sources"]["fed_status"] = fed_status.get("source")
        else:
            value["missing_reason"]["Fed Status"] = fed_status.get("reason") or "unavailable"
    except Exception as exc:
        value["missing_reason"]["Fed Status"] = f"fred:{str(exc)[:80]}"

    try:
        balance_sheet_series = fetch_fred_series(FRED_BALANCE_SHEET_SERIES_ID)
        capture_series(balance_sheet_series, FRED_BALANCE_SHEET_SERIES_ID, FRED_CSV_URL,
                       ("fed_balance_sheet_assets", "fed_balance_sheet_30d_pct"))
        balance_sheet_latest = latest_series_point(balance_sheet_series)
        if balance_sheet_latest:
            value["fed_balance_sheet_assets"] = safe_float(balance_sheet_latest.get("value"))
            value["fed_balance_sheet_30d_pct"] = series_delta_pct(balance_sheet_series, 30)
            value["source_date"]["fed_balance_sheet"] = balance_sheet_latest.get("date")
            value["sources"]["fed_balance_sheet"] = f"FRED {FRED_BALANCE_SHEET_SERIES_ID}"
    except Exception as exc:
        value["missing_reason"]["Fed Balance Sheet"] = f"fred:{str(exc)[:80]}"

    try:
        m2 = fetch_m2_liquidity_value()
        if m2.get("available"):
            value["history"].extend(m2.get("history") or [])
            value["m2_liquidity"] = {k: v for k, v in m2.items() if k != "history"}
            value["m2_level"] = m2.get("latest")
            value["m2_yoy_pct"] = m2.get("yoy_pct")
            value["m2_yoy_two_month_rising_rule"] = m2.get("yoy_two_month_rising_rule")
            value["m2_status"] = m2.get("status")
            value["source_date"]["m2"] = m2.get("date")
            value["sources"]["m2"] = m2.get("source")
        else:
            value["missing_reason"]["M2"] = m2.get("reason") or "unavailable"
    except Exception as exc:
        value["missing_reason"]["M2"] = f"fred:{str(exc)[:80]}"

    try:
        gold_series = fetch_yahoo_chart_series(YAHOO_GOLD_SYMBOL)
        capture_series(gold_series, "yahoo_gold",
                       f"{YAHOO_CHART_BASE_URL}/{yahoo_symbol_url(YAHOO_GOLD_SYMBOL)}",
                       ("gold_price", "gold_30d_pct", "gold_vs_dxy_30d_pct"))
        gold_latest = latest_series_point(gold_series)
        if gold_latest:
            value["gold_price"] = safe_float(gold_latest.get("value"))
            value["gold_30d_pct"] = series_delta_pct(gold_series, 30)
            if value.get("gold_30d_pct") is not None and value.get("usd_index_30d_pct") is not None:
                value["gold_vs_dxy_30d_pct"] = value["gold_30d_pct"] - value["usd_index_30d_pct"]
            value["source_date"]["gold"] = gold_latest.get("date")
            value["sources"]["gold"] = f"Yahoo Finance {YAHOO_GOLD_SYMBOL}"
    except Exception as exc:
        value["missing_reason"]["Gold"] = f"yahoo:{str(exc)[:80]}"

    fed = fetch_fedwatch_value()
    if fed.get("fed_policy"):
        value["history"].append({
            "series_id": "fedwatch", "endpoint_url": fed.get("endpoint_url"),
            "access_scope": fed.get("access_scope"), "retrieved_at": fed.get("retrieved_at"),
            "published_at": fed.get("published_at"), "publication_time_missing": True,
            "end_date": fed.get("source_date"),
            "rows": [{key: fed.get(key) for key in ("cut_probability", "hike_probability", "hold_probability", "expected_rate")}],
            "fields": ["fed_policy", "fed_cut_probability", "fed_hike_probability", "fed_hold_probability", "fed_expected_rate"],
        })
        value["fed_policy"] = fed.get("fed_policy")
        value["fed_cut_probability"] = fed.get("cut_probability")
        value["fed_hike_probability"] = fed.get("hike_probability")
        value["fed_hold_probability"] = fed.get("hold_probability")
        value["fed_expected_rate"] = fed.get("expected_rate")
        value["fed_next_meeting_date"] = fed.get("next_meeting_date")
        value["source_date"]["fed_policy"] = fed.get("source_date")
        value["sources"]["fed_policy"] = fed.get("source") or "FedWatch"
    else:
        value["missing_reason"]["FedWatch"] = fed.get("reason") or "unavailable"

    try:
        value.update(fetch_long_term_macro_value(value))
        for proxy in ("cost_of_living_proxy", "fiscal_proxy"):
            if isinstance(value.get(proxy), dict):
                value["history"].extend(value[proxy].pop("history", []))
    except Exception as exc:
        value["long_term_missing_reason"] = {"Long-Term Macro": str(exc)[:120]}

    try:
        buyback = fetch_treasury_buyback_value()
        if buyback.get("available"):
            value["history"].extend(buyback.get("history") or [])
            value["treasury_buyback"] = {k: v for k, v in buyback.items() if k != "history"}
            value["treasury_buyback_recent_count"] = buyback.get("recent_count")
            value["treasury_buyback_total_par_accepted"] = buyback.get("total_par_amt_accepted")
            value["treasury_buyback_scale_status"] = buyback.get("scale_status")
            value["source_date"]["treasury_buyback"] = buyback.get("operation_date")
            value["sources"]["treasury_buyback"] = buyback.get("source")
        else:
            value["missing_reason"]["Treasury Buyback"] = buyback.get("reason") or "unavailable"
    except Exception as exc:
        value["missing_reason"]["Treasury Buyback"] = f"fiscaldata:{str(exc)[:80]}"

    value.update(build_macro_model(value))
    source_stale = False
    stale_reasons = []
    for key, date_text in (value.get("source_date") or {}).items():
        if key in ("m2", "treasury_buyback"):
            continue
        if is_source_date_stale(date_text, macro_source_stale_sec(key)):
            source_stale = True
            stale_reasons.append(f"{key}:{date_text}")
    value["source_stale_reasons"] = stale_reasons

    if not value_has_data({k: value.get(k) for k in ("us10y", "real_yield_10y", "usd_index")}):
        record = make_unavailable_record("Macro liquidity composite", "all_macro_sources_unavailable")
        record["missing_reason"] = value.get("missing_reason")
        return record

    for receipt in value["history"]:
        for field in receipt.get("fields") or []:
            if field in value and value[field] is not None:
                previous = value["field_sources"].get(field)
                if previous is None:
                    value["field_sources"][field] = receipt["series_id"]
                elif not field.startswith("us10y"):
                    refs = previous if isinstance(previous, list) else [previous]
                    if receipt["series_id"] not in refs:
                        value["field_sources"][field] = refs + [receipt["series_id"]]
    for receipt in value["history"]:
        receipt["fields"] = [field for field, sid in value["field_sources"].items()
                             if receipt["series_id"] in (sid if isinstance(sid, list) else [sid])]
    record = make_data_record(value, "Macro liquidity composite")
    record.update(source_as_of_ms=None, published_at=None, publication_time_missing=True)
    record["source_date"] = value.get("source_date")
    record["source_stale"] = source_stale
    record["source_stale_reason"] = " / ".join(stale_reasons) if stale_reasons else None
    record["schema_version"] = MACRO_CACHE_SCHEMA_REVISION
    state["macro_cache"] = dict(record)
    return record


def liquidation_record_ready(record, window):
    return bool(((record or {}).get("coverage") or {}).get(f"ready_{window}"))


def liquidation_record_collecting(record):
    return (record or {}).get("status") == "collecting"


def fetch_liquidation_record(state):
    liq_state = prune_liquidation_ws_state(state.get("liquidation_ws") or {})
    state["liquidation_ws"] = liq_state
    source = "Binance Futures forceOrder WebSocket"
    if not BTC_PUSH_LIQUIDATION_WS_ENABLED:
        return make_unavailable_record(source, "disabled")

    current_ms = now_ms()
    coverage_started_ms = int(liq_state.get("coverage_started_at_ms") or 0)
    last_seen_ms = int(liq_state.get("last_seen_at_ms") or 0)
    health_window_ms = max(
        LIQUIDATION_COVERAGE_GAP_RESET_SEC,
        LIQUIDATION_WS_TIMEOUT_SEC * 5,
        300,
    ) * 1000
    healthy = bool(last_seen_ms and current_ms - last_seen_ms <= health_window_ms)
    coverage_end_ms = min(current_ms, last_seen_ms) if last_seen_ms else current_ms
    coverage_sec = max(0.0, (coverage_end_ms - coverage_started_ms) / 1000.0) if coverage_started_ms else 0.0
    ready_4h = healthy and coverage_sec >= 4 * 3600
    ready_24h = healthy and coverage_sec >= 24 * 3600

    coverage = {
        "healthy": healthy,
        "coverage_hours": coverage_sec / 3600.0,
        "ready_4h": ready_4h,
        "ready_24h": ready_24h,
        "last_seen_at_ms": last_seen_ms,
        "coverage_started_at_ms": coverage_started_ms,
        "status": liq_state.get("status"),
        "event_count_24h": 0,
    }

    if not coverage_started_ms or not healthy:
        record = make_unavailable_record(
            source,
            "waiting_for_websocket_coverage" if coverage_started_ms else "websocket_not_started",
        )
        record["status"] = "collecting"
        record["coverage"] = coverage
        record["updated_at"] = int((last_seen_ms or current_ms) / 1000)
        return record

    windows = {
        "4h": current_ms - 4 * 3600 * 1000,
        "24h": current_ms - 24 * 3600 * 1000,
    }
    totals = {
        "long_4h_usd": 0.0,
        "short_4h_usd": 0.0,
        "long_24h_usd": 0.0,
        "short_24h_usd": 0.0,
    }
    for item in liq_state.get("events") or []:
        order_time = int(item.get("ts_ms") or 0)
        if order_time < windows["24h"]:
            continue
        coverage["event_count_24h"] += 1
        notional = safe_float(item.get("notional_usd"), 0.0) or 0.0
        bucket = item.get("bucket")
        if not bucket:
            continue
        totals[f"{bucket}_24h_usd"] += notional
        if order_time >= windows["4h"]:
            totals[f"{bucket}_4h_usd"] += notional

    value = {
        "long_4h_usd": totals["long_4h_usd"] if ready_4h else None,
        "short_4h_usd": totals["short_4h_usd"] if ready_4h else None,
        "long_24h_usd": totals["long_24h_usd"] if ready_24h else None,
        "short_24h_usd": totals["short_24h_usd"] if ready_24h else None,
    }
    status = "ok" if ready_24h else "collecting"
    reason = None if ready_24h else "insufficient_24h_coverage"
    record = make_data_record(value, source, status=status, reason=reason, updated_at=int(last_seen_ms / 1000))
    record["coverage"] = coverage
    return record


def fetch_spot_cvd_record():
    raw = binance_spot_get(
        "/api/v3/klines",
        {"symbol": BINANCE_SYMBOL, "interval": "1h", "limit": 241},
    )
    now_ms = int(time.time() * 1000)
    bars = []
    for item in raw or []:
        if len(item) < 11:
            continue
        close_time = int(item[6])
        if close_time > now_ms:
            continue
        quote_volume = finite_metric(item[7], 0.0)
        taker_buy_quote = finite_metric(item[10], 0.0)
        # Binance K线只给出主动买入额；总成交额扣除主动买入额后才是主动卖出额。
        net_quote = (
            (2.0 * taker_buy_quote) - quote_volume
            if quote_volume is not None and taker_buy_quote is not None
            and taker_buy_quote <= quote_volume else None
        )
        bars.append({
            "open_time": int(item[0]),
            "open": safe_float(item[1]), "high": safe_float(item[2]),
            "low": safe_float(item[3]), "volume": finite_metric(item[5], 0.0),
            "taker_buy_quote": taker_buy_quote,
            "close_time": close_time,
            "close": safe_float(item[4]),
            "quote_volume": quote_volume,
            "net_quote": net_quote,
        })
    bars.sort(key=lambda item: int(item.get("close_time") or 0))
    bars = bars[-240:]

    def contiguous(window):
        recent = bars[-int(window):]
        if len(recent) < int(window):
            return False
        for previous, current in zip(recent, recent[1:]):
            delta = int(current.get("close_time") or 0) - int(previous.get("close_time") or 0)
            if abs(delta - 3600 * 1000) > 1000:
                return False
        return True

    contiguous_4h = contiguous(8)
    contiguous_24h = contiguous(48)
    if len(bars) < 8 or not contiguous_4h:
        return make_unavailable_record("Binance Spot klines", "insufficient_bars")

    def cvd_pct(window):
        recent = bars[-window:]
        if any(item["quote_volume"] is None or item["net_quote"] is None for item in recent):
            return None
        total = sum(item["quote_volume"] for item in recent)
        if total <= 0:
            return None
        return sum(item["net_quote"] for item in recent) / total * 100.0

    def volume_delta_pct(window):
        if len(bars) < window * 2:
            return None
        if any(item["quote_volume"] is None for item in bars[-(window * 2):]):
            return None
        current = sum(item["quote_volume"] for item in bars[-window:])
        previous = sum(item["quote_volume"] for item in bars[-(window * 2):-window])
        return pct(current, previous)

    value = {
        "cvd_4h_pct": cvd_pct(4),
        "cvd_24h_pct": cvd_pct(24) if len(bars) >= 24 and contiguous(24) else None,
        "volume_4h_pct": volume_delta_pct(4),
        "volume_24h_pct": volume_delta_pct(24) if contiguous_24h else None,
        "as_of_ms": int(bars[-1].get("close_time") or 0),
        "bars_loaded": len(bars),
        "contiguous_4h": contiguous_4h,
        "contiguous_24h": contiguous_24h,
        "scope": "Binance Spot BTCUSDT taker-flow proxy",
        # 保留有界逐小时收据，供触碰关键位后的事件判断使用；不能据此推断盘口挂单身份。
        "hourly_flow": [dict(item) for item in bars[-240:]],
    }
    missing_metrics = [
        key for key in ("cvd_4h_pct", "cvd_24h_pct", "volume_4h_pct", "volume_24h_pct")
        if value[key] is None
    ]
    record = make_data_record(
        value,
        "Binance Spot klines",
        status="partial" if missing_metrics else "ok",
        reason="missing_or_invalid_metrics:" + ",".join(missing_metrics) if missing_metrics else None,
    )
    record["source_as_of_ms"] = value["as_of_ms"]
    record.update(endpoint_url=f"{BINANCE_SPOT_BASE_URL}/api/v3/klines", venue="Binance",
                  updated_at=now_ms / 1000, retrieved_at=now_ms / 1000)
    record["history_window"] = time_window(bars, "close_time", 240, 3600000)
    return record


def fetch_stablecoin_record():
    data = public_get_json(DEFILLAMA_STABLECOINS_URL)
    retrieved_at = time.time()
    total = 0.0
    asset_samples = []
    day_current = day_previous = 0.0
    week_current = week_previous = 0.0
    count = 0
    day_count = 0
    week_count = 0
    for asset in data.get("peggedAssets") or []:
        if asset.get("pegType") != "peggedUSD":
            continue
        current = safe_float((asset.get("circulating") or {}).get("peggedUSD"))
        day = safe_float((asset.get("circulatingPrevDay") or {}).get("peggedUSD"))
        week = safe_float((asset.get("circulatingPrevWeek") or {}).get("peggedUSD"))
        asset_samples.append({
            "id": str(asset.get("id") or ""), "symbol": asset.get("symbol"),
            "current_usd": current, "previous_day_usd": day, "previous_week_usd": week,
            "source_as_of_ms": None,
        })
        if current is None:
            continue
        total += current
        count += 1
        if day is not None:
            day_current += current
            day_previous += day
            day_count += 1
        if week is not None:
            week_current += current
            week_previous += week
            week_count += 1
    if count == 0:
        return make_unavailable_record("DefiLlama stablecoins", "empty_response")

    day_coverage_pct = day_current / total * 100.0 if total > 0 else 0.0
    week_coverage_pct = week_current / total * 100.0 if total > 0 else 0.0
    day_ready = day_coverage_pct >= STABLECOIN_HISTORY_MIN_COVERAGE_PCT
    week_ready = week_coverage_pct >= STABLECOIN_HISTORY_MIN_COVERAGE_PCT
    value = {
        "total_usd": total,
        "history": asset_samples,
        "flow_24h_usd": day_current - day_previous if day_ready else None,
        "flow_7d_usd": week_current - week_previous if week_ready else None,
        "day_coverage_pct": day_coverage_pct,
        "week_coverage_pct": week_coverage_pct,
        "asset_count": count,
        "day_asset_count": day_count,
        "week_asset_count": week_count,
    }
    missing_windows = []
    if not day_ready:
        missing_windows.append("24h")
    if not week_ready:
        missing_windows.append("7d")
    status = "partial" if missing_windows else "ok"
    reason = f"insufficient_history_coverage:{','.join(missing_windows)}" if missing_windows else None
    record = make_data_record(value, "DefiLlama stablecoins", status=status, reason=reason)
    record["coverage"] = {
        "minimum_pct": STABLECOIN_HISTORY_MIN_COVERAGE_PCT,
        "day_pct": day_coverage_pct,
        "week_pct": week_coverage_pct,
        "ready_24h": day_ready,
        "ready_7d": week_ready,
    }
    record.update(endpoint_url=DEFILLAMA_STABLECOINS_URL, venue="DefiLlama",
                  source_as_of_ms=None, source_time_missing=True, retrieved_at=retrieved_at)
    return record


def _fetch_etf_holdings(state):
    source = "Bitbo US spot BTC ETF holdings"
    background = btc_etf.flow_background(state)
    try:
        parsed = btc_etf.parse_page(public_get_text(btc_etf.ENDPOINT, retry_read_timeout=True))
        retrieved_at = time.time()
        if parsed["source_date"] > datetime.utcfromtimestamp(retrieved_at).date().isoformat():
            raise ValueError("future_holdings_date")
    except (requests.RequestException, ValueError) as exc:
        status = getattr(getattr(exc, "response", None), "status_code", None)
        reason = (f"http_{status}" if status is not None else
                  str(exc) if isinstance(exc, ValueError) else type(exc).__name__)
        record = make_unavailable_record(source, reason)
        record.update(endpoint_url=btc_etf.ENDPOINT, venue=btc_etf.VENUE,
                      flow_background=background)
        return record
    holdings_btc = parsed["holdings_btc"]
    source_date = {"holdings": parsed["source_date"]}
    holding_7d_pct = None
    prev = btc_etf.seven_day_basis(state, parsed["source_date"], retrieved_at,
                                  PUBLIC_SNAPSHOT_7D_TOLERANCE_SEC)
    if prev:
        holding_7d_pct = pct(holdings_btc, prev["value"]["holdings_btc"])
    holding_7d_status = "ok" if holding_7d_pct is not None else "collecting"
    value = {
        "history": [],
        **{key: None for key in btc_etf.FLOW_FIELDS},
        "holdings_btc": holdings_btc,
        "holdings_market_pct": None,
        "holding_7d_pct": holding_7d_pct,
        "holding_7d_status": holding_7d_status,
        "holding_7d_reason": None if holding_7d_status == "ok" else "need_7d_snapshot",
        "source_date": source_date,
    }
    record = make_data_record(value, source, status="ok")
    stale_reason = etf_source_stale_reason(parsed["source_date"], datetime.fromtimestamp(retrieved_at, TOKYO_TZ))
    record.update(endpoint_url=btc_etf.ENDPOINT, venue=btc_etf.VENUE,
                  source_as_of_ms=None, retrieved_at=retrieved_at, time_basis="observation_date",
                  source_date=source_date, source_stale=bool(stale_reason),
                  source_stale_reason=stale_reason,
                  flow_background=background, holdings_scope=parsed["holdings_scope"],
                  holdings_rows=parsed["holdings_rows"])
    if holding_7d_pct is not None:
        record["holding_7d_basis"] = {key: copy.deepcopy(prev.get(key)) for key in (
            "venue", "endpoint_url", "holdings_scope", "holdings_rows", "retrieved_at")}
        record["holding_7d_basis"].update(holdings_btc=prev["value"]["holdings_btc"],
                                       source_date=prev["value"]["source_date"]["holdings"])
    return record


def _fetch_etf_flow(venue, endpoint, parser, params=None):
    flow = {"venue": venue, "endpoint_url": endpoint,
            "retrieved_at": time.time(), "status": "unavailable", "value": {}}
    try:
        parsed = parser(public_get_text(endpoint, params=params))
        retrieved_at = time.time()
        stale, expected = btc_etf_flow.freshness(parsed["source_date"], retrieved_at,
            ETF_SOURCE_READY_ET_HOUR, ETF_SOURCE_STALE_GRACE_DAYS)
        if stale in ("future_flow_date", "flow_session_not_closed", "flow_calendar_year_unverified", "flow_non_trading_date"):
            raise ValueError(stale)
        flow.update(retrieved_at=retrieved_at, source_date=parsed["source_date"],
                    expected_source_date=expected, source_stale=bool(stale),
                    source_stale_reason=stale, flow_scope=parsed["flow_scope"],
                    window_quality=parsed["window_quality"], history=parsed["history"],
                    value=copy.deepcopy(parsed["values"]))
        complete = all(v is not None for v in parsed["values"].values())
        flow.update(status="unavailable" if stale else "ok" if complete else "partial",
                    reason=stale or (None if complete else "incomplete_flow_windows"))
    except (requests.RequestException, ValueError) as exc:
        code = getattr(getattr(exc, "response", None), "status_code", None)
        flow["reason"] = f"http_{code}" if code is not None else str(exc) if isinstance(exc, ValueError) else type(exc).__name__
    return flow


def fetch_etf_record(state):
    record = _fetch_etf_holdings(state)
    holding_valid = record_is_current(record)
    record["holdings_record"] = {key: copy.deepcopy(record.get(key)) for key in (
        "status", "reason", "value", "source_stale", "source_stale_reason", "retrieved_at")}
    value = copy.deepcopy(record.get("value") or {key: None for key in btc_etf.FLOW_FIELDS})
    value.setdefault("source_date", {})
    if not holding_valid:
        for key in ("holdings_btc", "holding_7d_pct", "holding_7d_status"):
            value[key] = None
    candidates = [
        _fetch_etf_flow(btc_etf_flow.COINBOSS_VENUE, btc_etf_flow.COINBOSS_ENDPOINT,
                        btc_etf_flow.parse_coinboss_json, {"asset": "BTC", "days": 30}),
        _fetch_etf_flow(btc_etf_flow.VENUE, btc_etf_flow.ENDPOINT, btc_etf_flow.parse_json),
    ]
    # Select one source history, never splice sources or skip a newer
    # partial day. On equal dates prefer the anonymous per-fund CoinBoss API.
    dated = [item for item in candidates if item.get("source_date")]
    flow = max(dated, key=lambda item: item["source_date"]) if dated else candidates[-1]
    record["flow_sources"] = [{k: copy.deepcopy(item.get(k)) for k in (
        "venue", "endpoint_url", "status", "reason", "source_date", "retrieved_at")}
        for item in candidates]
    if flow.get("source_date"):
        if not flow.get("source_stale"):
            value.update(flow["value"])
        value["history"] = flow.pop("history")
        value["source_date"].update({f"flow_{days}d": flow["source_date"] for days in btc_etf_flow.WINDOWS})
    flow_valid = any(value.get(f"flow_{days}d_usd") is not None for days in btc_etf_flow.WINDOWS)
    record.update(value=value, flow_record=flow, flow_status=flow["status"], flow_reason=flow.get("reason"),
                  source_date=value["source_date"], updated_at=time.time(),
                  status="ok" if holding_valid and flow["status"] == "ok" else "partial" if holding_valid or flow_valid else "unavailable",
                  source_stale=False if holding_valid or flow_valid else record.get("source_stale", False),
                  source_stale_reason=None if holding_valid or flow_valid else record.get("source_stale_reason"))
    record["reason"] = None if record["status"] == "ok" else (
        flow.get("reason") if holding_valid else record["holdings_record"].get("reason") or record["holdings_record"].get("source_stale_reason"))
    return record


def get_equity_signal_bars(state):
    spec = assets.current()
    try:
        bars = assets.fetch_stock_bars(state, spec)
        state.pop("stock_history_error", None)
    except Exception as exc:
        state["stock_history_error"] = type(exc).__name__
        log_line(f"{spec.asset_id} stock history unavailable: {type(exc).__name__}")
        # Old internal-REST prices must never return through an error fallback.
        bars = [row for row in state.get("bars_1h") or [] if row.get("symbol") == spec.price_symbol
                and row.get("endpoint_url") == market_public.STOCK_WS]
    try:
        history = fetch_binance_oi_hist(240, symbol=spec.derivatives_symbol)
        derivative_bars = fetch_binance_klines(240, symbol=spec.derivatives_symbol)
        state["derivative_bars"] = merge_oi_into_bars(derivative_bars, history)
        if spec.derivatives_relation == "direct":
            bars = merge_oi_into_bars(bars, history)
            for row in bars:
                row.pop("cmc_supply", None)
                row["oi_instrument"] = spec.derivatives_symbol
                row["oi_relation"] = "direct"
                row["oi_source"] = "Binance openInterestHist " + spec.derivatives_symbol
    except Exception as exc:
        state["derivative_bars"] = []
        log_line(f"{spec.asset_id} derivatives unavailable: {type(exc).__name__}")
    state["bars_1h"] = bars
    return bars


def get_equity_derivatives_market(state):
    spec = assets.current()
    result = {"instrument": spec.derivatives_symbol, "relation": spec.derivatives_relation,
              "source": "Binance Futures " + spec.derivatives_symbol, "quote_currency": "USDT",
              "oi_btc": None}
    try:
        oi = binance_get("/fapi/v1/openInterest", {"symbol": spec.derivatives_symbol})
        price = binance_get("/fapi/v1/premiumIndex", {"symbol": spec.derivatives_symbol})
        if oi.get("symbol") != spec.derivatives_symbol or price.get("symbol") != spec.derivatives_symbol:
            raise ValueError("derivative_symbol_mismatch")
        quantity, mark = finite_metric(oi.get("openInterest"), 0), finite_metric(price.get("markPrice"), 0)
        return {**result, "oi_quantity": quantity, "oi_usd": quantity * mark if quantity is not None and mark else None,
                "mark_price": mark, "source_as_of_ms": epoch_ms(oi.get("time")),
                "mark_source_as_of_ms": epoch_ms(price.get("time")), "retrieved_at": time.time()}
    except Exception as exc:
        return {**result, "oi_quantity": None, "oi_usd": None, "mark_price": None,
                "reason": type(exc).__name__}


def collect_equity_public_snapshot(state):
    spec = assets.current()
    macro = {"macro_cache": copy.deepcopy(state.get("macro_cache") or {})}
    fetchers = {
        "funding": lambda: assets.derivative_identity(fetch_funding_record(spec.derivatives_symbol), spec),
        "basis": lambda: assets.derivative_identity(fetch_basis_record(spec.derivatives_symbol), spec),
        "macro": lambda: fetch_macro_record(macro),
    }
    public = public_io.collect_sources(fetchers, make_unavailable_record)
    for key in ("funding", "basis"):
        public[key] = assets.derivative_identity(public[key], spec)
    for key in ("etf", "stablecoin", "hyperliquid_funding", "hyperliquid_basis", "liquidation"):
        public[key] = assets.not_applicable("crypto_only_source")
    public["spot_cvd"] = assets.not_applicable("stock_source_has_no_aggressor_direction")
    if macro.get("macro_cache"):
        state["macro_cache"] = macro["macro_cache"]
    return {"ts": int(time.time()), "tokyo": tokyo_now().strftime("%Y-%m-%d %H:%M"),
            "asset_id": spec.asset_id, "public": public}


def collect_public_snapshot(state):
    if assets.current().equity:
        return collect_equity_public_snapshot(state)
    # Each state-mutating collector owns a private copy; all network work is unlocked.
    liq = {"liquidation_ws": copy.deepcopy(state.get("liquidation_ws") or {})}
    hl = {"hyperliquid_derivatives": copy.deepcopy(state.get("hyperliquid_derivatives") or {})}
    macro = {"macro_cache": copy.deepcopy(state.get("macro_cache") or {})}
    fetchers = {
        "funding": fetch_funding_record,
        "liquidation": lambda: fetch_liquidation_record(liq),
        "basis": fetch_basis_record,
        "hyperliquid_funding": fetch_hyperliquid_funding_record,
        "hyperliquid_basis": lambda: fetch_hyperliquid_basis_record(hl),
        "etf": lambda: fetch_etf_record(state),
        "spot_cvd": fetch_spot_cvd_record,
        "stablecoin": fetch_stablecoin_record,
        "macro": lambda: fetch_macro_record(macro),
    }
    public = public_io.collect_sources(fetchers, make_unavailable_record)
    state["hyperliquid_derivatives"] = merge_domain(
        "hyperliquid_derivatives", state.get("hyperliquid_derivatives"), hl.get("hyperliquid_derivatives"))
    if macro.get("macro_cache"):
        state["macro_cache"] = macro["macro_cache"]
    return {"ts": int(time.time()), "tokyo": tokyo_now().strftime("%Y-%m-%d %H:%M"), "public": public}


def normalize_title(title):
    text = (title or "").strip().lower()
    text = re.sub(r"\s+[-|–]\s+[^-|–]{2,30}$", "", text).strip()
    text = re.sub(r"[^\w\s]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def dedupe_news_titles(state, titles):
    now_ts = int(time.time())
    recycle_state(state)
    seen = state.get("news_seen") or {}
    fresh = []
    for title in titles:
        key = normalize_title(title)
        if not key or key in seen:
            continue
        seen[key] = now_ts
        fresh.append(title)
    state["news_seen"] = seen
    return fresh


def get_fear_greed_index():
    source = "Alternative.me"
    if not BTC_PUSH_FEAR_GREED_ENABLED:
        return {"available": False, "reason": "disabled", "source": source}

    try:
        limit = max(1, FEAR_GREED_HISTORY_DAYS + 1)
        r = public_io.get(
            FEAR_GREED_API_URL,
            params={"limit": limit, "format": "json"},
            timeout=(3, max(1, FEAR_GREED_FETCH_TIMEOUT_SEC)),
        )
        r.raise_for_status()
        data = (r.json().get("data") or [])
        if not data:
            return {"available": False, "reason": "empty_response", "source": source}

        item = data[0] or {}
        value = int(float(item.get("value")))
        classification = (item.get("value_classification") or "").strip()
        timestamp = int(item.get("timestamp", 0) or 0)
        history = []
        for raw in data[:limit]:
            try:
                history.append({
                    "value": int(float(raw.get("value"))),
                    "classification": (raw.get("value_classification") or "").strip(),
                    "timestamp": int(raw.get("timestamp", 0) or 0),
                })
            except Exception:
                continue
        history.sort(key=lambda x: int(x.get("timestamp") or 0))
        return {
            "available": True,
            "value": value,
            "classification": classification,
            "classification_cn": FEAR_GREED_CLASS_CN.get(classification, classification or "未知"),
            "timestamp": timestamp,
            "time_until_update": int(item.get("time_until_update", 0) or 0),
            "history": history,
            "retrieved_at": time.time(),
            "endpoint_url": FEAR_GREED_API_URL,
            "source_as_of_ms": timestamp * 1000 if timestamp > 0 else None,
            "source": source,
        }
    except Exception:
        log_exception("fear greed index skipped")
        return {"available": False, "reason": "fetch_failed", "source": source}


def trend_label(delta):
    if delta >= 8:
        return "明显回暖"
    if delta >= 3:
        return "小幅回暖"
    if delta <= -8:
        return "明显转冷"
    if delta <= -3:
        return "小幅转冷"
    return "基本持平"


def value_short(value):
    return VALUE_SHORT_CN.get(value, VALUE_META.get(value, {}).get("cn", value))


def fear_greed_short(classification):
    return FEAR_GREED_SHORT_CN.get(classification, classification or "未知")


def format_money(value, decimals=0, compact=False):
    value = safe_float(value)
    try:
        value = float(value)
    except Exception:
        return "NA"
    if compact:
        abs_value = abs(value)
        if abs_value >= 1_000_000_000_000:
            return f"${value / 1_000_000_000_000:.2f}T"
        if abs_value >= 1_000_000_000:
            return f"${value / 1_000_000_000:.2f}B"
        if abs_value >= 1_000_000:
            return f"${value / 1_000_000:.2f}M"
    return f"${value:,.{int(decimals)}f}"


def format_signed_number(value, decimals=2, suffix=""):
    value = safe_float(value)
    if value is None:
        return "NA"
    try:
        value = float(value)
    except Exception:
        return "NA"
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.{int(decimals)}f}{suffix}"


def format_metric_compare(label, prev_value, now_value, money=False, compact_money=False, decimals=0):
    try:
        prev = float(prev_value)
        now = float(now_value)
    except Exception:
        return None
    delta = pct(now, prev)
    delta_text = format_signed_number(delta, 2, "%")
    if money:
        prev_text = format_money(prev, decimals=decimals, compact=compact_money)
        now_text = format_money(now, decimals=decimals, compact=compact_money)
    else:
        prev_text = f"{prev:.{int(decimals)}f}"
        now_text = f"{now:.{int(decimals)}f}"
    return f"{label}: {prev_text} -> {now_text} ({delta_text})"


def format_percent_point_compare(label, prev_value, now_value):
    try:
        prev = float(prev_value)
        now = float(now_value)
    except Exception:
        return None
    delta = now - prev
    return f"{label}: {prev:.2f}% -> {now:.2f}% ({format_signed_number(delta, 2, 'pct')})"


def format_factor_pair(label, prev_state, now_state, key):
    prev_value = (prev_state or {}).get(key)
    now_value = (now_state or {}).get(key)
    if prev_value is None or now_value is None:
        return None
    return f"{label} {value_short(prev_value)}->{value_short(now_value)}"


def market_delta_pct(market, key):
    item = (market or {}).get(key) or {}
    try:
        return pct(item.get("now"), item.get("prev"))
    except Exception:
        return None


def describe_flow_structure(price_delta, vol_delta, oi_delta):
    if price_delta is None or vol_delta is None or oi_delta is None:
        return None

    price_abs = abs(price_delta)
    vol_down = vol_delta <= -15.0
    oi_down = oi_delta <= -3.0
    vol_up = vol_delta >= 15.0
    oi_up = oi_delta >= 3.0

    if price_abs < 1.0 and vol_down and oi_down:
        return "结构摘要: 价格7D基本横盘，但成交量与OI同步收缩，说明当前更像缩量修复/去杠杆后的稳定，不是增量趋势确认。"
    if price_delta > 1.0 and vol_down and oi_down:
        return "结构摘要: 价格7D上行，但成交量与OI同步下降，偏情绪修复或空头回补，不足以确认新增趋势资金进场。"
    if price_delta < -1.0 and vol_down and oi_down:
        return "结构摘要: 价格7D回落，同时成交量与OI下降，偏降温后的缩量整理，暂未形成新的杠杆方向。"
    if price_delta > 1.0 and vol_up and oi_up:
        return "结构摘要: 价格、成交量与OI同步扩张，7D结构具备趋势确认特征。"
    if price_delta < -1.0 and oi_up:
        return "结构摘要: 价格7D下行且OI增加，说明下跌伴随杠杆压力，风险应上调观察。"
    if price_abs < 1.0:
        return "结构摘要: 价格7D变化有限，量能/OI未给出一致确认，整体仍按区间结构处理。"
    if price_delta > 0:
        return "结构摘要: 价格7D偏上，但量能/OI确认不足，趋势质量仍需继续观察。"
    return "结构摘要: 价格7D偏弱，量能/OI未形成反向修复信号，仍以防守观察为主。"


def describe_factor_structure(prev_state, now_state):
    if not prev_state or not now_state:
        return None

    parts = []
    if prev_state.get("long_term_trend") != now_state.get("long_term_trend"):
        parts.append(
            f"长周期{value_short(prev_state.get('long_term_trend'))}->{value_short(now_state.get('long_term_trend'))}"
        )
    if now_state.get("trend") == "RANGE":
        parts.append("短周期仍是区间")
    elif prev_state.get("trend") != now_state.get("trend"):
        parts.append(f"短周期{value_short(prev_state.get('trend'))}->{value_short(now_state.get('trend'))}")

    if prev_state.get("liquidity") != now_state.get("liquidity"):
        parts.append(f"流动性{value_short(prev_state.get('liquidity'))}->{value_short(now_state.get('liquidity'))}")
    if now_state.get("tq") in ("WEAK", "NORMAL"):
        parts.append(f"趋势质量仍{value_short(now_state.get('tq'))}")
    if now_state.get("risk") in ("MID", "HIGH"):
        parts.append(f"风险仍{value_short(now_state.get('risk'))}")
    if now_state.get("execution") != "EXECUTE":
        parts.append("执行结论仍等待")

    if not parts:
        return "因子摘要: 7D因子结构未出现关键变化。"
    return "因子摘要: " + "，".join(parts) + "。"


def format_fear_greed_line(fear_greed):
    source = (fear_greed or {}).get("source") or "Alternative.me"
    if not fear_greed or not fear_greed.get("available"):
        return f"📌 恐惧/贪婪指数: 暂不可用（{source}）"

    updated = ""
    ts = int(fear_greed.get("timestamp") or 0)
    if ts > 0:
        updated = f"，更新 {datetime.fromtimestamp(ts, TOKYO_TZ).strftime('%m-%d %H:%M JST')}"

    return (
        f"📌 恐惧/贪婪指数: {int(fear_greed['value'])}/100"
        f"（{fear_greed.get('classification_cn')}/{fear_greed.get('classification')}"
        f"，{source}{updated}）"
    )


def format_fear_greed_compare_line(fear_greed_compare):
    if not fear_greed_compare:
        return None
    prev = fear_greed_compare.get("prev") or {}
    now = fear_greed_compare.get("now") or {}
    try:
        prev_value = int(prev.get("value"))
        now_value = int(now.get("value"))
    except Exception:
        return None
    delta = now_value - prev_value
    sign = "+" if delta > 0 else ""
    days = int(fear_greed_compare.get("days") or FEAR_GREED_HISTORY_DAYS)
    return (
        f"恐惧/贪婪: {prev_value}({fear_greed_short(prev.get('classification'))}) "
        f"-> {now_value}({fear_greed_short(now.get('classification'))}) "
        f"({sign}{delta}, {trend_label(delta)}, {days}D)"
    )


def format_7d_comparison_block(seven_day_comparison):
    if not seven_day_comparison or not seven_day_comparison.get("available"):
        return []

    days = int(round(float(seven_day_comparison.get("window_days") or 7)))
    lines = [f"7D对比（{days}日前 -> 现在）"]
    market = seven_day_comparison.get("market") or {}

    fear_line = format_fear_greed_compare_line(seven_day_comparison.get("fear_greed"))
    if fear_line:
        lines.append(fear_line)

    price = market.get("price") or {}
    price_line = format_metric_compare(
        "价格",
        price.get("prev"),
        price.get("now"),
        money=True,
        compact_money=False,
        decimals=0,
    )
    if price_line:
        lines.append(price_line)

    flow_line = describe_flow_structure(
        market_delta_pct(market, "price"),
        market_delta_pct(market, "vol24"),
        market_delta_pct(market, "oi_usd"),
    )
    if flow_line:
        lines.append(flow_line)

    factors = seven_day_comparison.get("factors") or {}
    prev_state = factors.get("prev") or {}
    now_state = factors.get("now") or {}
    factor_line = describe_factor_structure(prev_state, now_state)
    if factor_line:
        lines.append(factor_line)

    return lines


def format_7d_compact(seven_day_comparison):
    if not seven_day_comparison or not seven_day_comparison.get("available"):
        return ""
    lines = format_7d_comparison_block(seven_day_comparison)
    return " | ".join(lines[:4])


def format_fear_greed_compact(fear_greed):
    if not fear_greed or not fear_greed.get("available"):
        return "恐惧指数 NA（Alternative.me）"
    history = fear_greed.get("history") or []
    suffix = ""
    if len(history) >= 2:
        delta = int(history[-1].get("value") or 0) - int(history[0].get("value") or 0)
        sign = "+" if delta > 0 else ""
        suffix = f" 7D{sign}{delta}"
    return f"恐惧指数 {int(fear_greed['value'])}/100 {fear_greed.get('classification_cn')}{suffix}（Alternative.me）"


def format_fear_greed_prompt(fear_greed):
    if not fear_greed or not fear_greed.get("available"):
        return "N/A"
    history = fear_greed.get("history") or []
    trend = ""
    if len(history) >= 2:
        values = [int(item.get("value") or 0) for item in history]
        delta = values[-1] - values[0]
        trend = f", {len(values) - 1}d_delta={delta}, {len(values) - 1}d_values={values}"
    return (
        f"{int(fear_greed['value'])}/100, "
        f"{fear_greed.get('classification_cn')}/{fear_greed.get('classification')}, "
        f"source={fear_greed.get('source') or 'Alternative.me'}"
        f"{trend}"
    )


def shorten_title(title, max_len=92):
    title = re.sub(r"\s+[-|–]\s+[^-|–]{2,30}$", "", (title or "").strip())
    title = re.sub(r"https?://\S+|www\.\S+", "", title).strip()
    if len(title) <= max_len:
        return title
    return title[: max_len - 1].rstrip() + "…"


CN_NEWS_SOURCES = [
    ("BlockBeats", "https://www.theblockbeats.info/newsflash"),
]


def clean_news_title(title):
    text = html.unescape(title or "")
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"\s+[-|–]\s+[^-|–]{2,30}$", "", text).strip()
    return text


def is_cn_news_title(title):
    return bool(re.search(r"[\u4e00-\u9fff]", title or ""))


def is_relevant_cn_news_title(title):
    return news.is_crypto_news(title) and not news.is_promotion(title)


def fetch_blockbeats_titles(n=30):
    url = "https://www.theblockbeats.info/newsflash"
    headers = {
        "User-Agent": "btcpush/3.1 (+https://www.theblockbeats.info/newsflash)",
        "Accept-Language": "zh-CN,zh;q=0.9",
    }
    r = public_io.get(url, headers=headers, timeout=(3, max(1, NEWS_FETCH_TIMEOUT_SEC)))
    r.raise_for_status()
    body = r.text or ""

    titles = []
    patterns = [
        r'<a\b(?=[^>]*\bclass="[^"]*news-flash-title[^"]*")(?=[^>]*\btitle="([^"]+)")[^>]*>',
        r'<div\b[^>]*\bclass="[^"]*news-flash-title-text[^"]*"[^>]*>(.*?)</div>',
    ]
    for pattern in patterns:
        for raw in re.findall(pattern, body, flags=re.IGNORECASE | re.DOTALL):
            title = clean_news_title(raw)
            if title and is_cn_news_title(title):
                titles.append(title)
            if len(titles) >= n:
                break
        if len(titles) >= n:
            break
    return titles


def get_chinese_news_titles(n=30):
    titles = []
    for source_name, _ in CN_NEWS_SOURCES:
        try:
            if source_name == "BlockBeats":
                titles.extend(fetch_blockbeats_titles(n))
        except Exception:
            log_exception(f"Chinese news source skipped: {source_name}")
        if len(titles) >= n:
            break

    seen = set()
    relevant = []
    for title in titles:
        key = normalize_title(title)
        if not key or key in seen:
            continue
        seen.add(key)
        if is_relevant_cn_news_title(title):
            relevant.append(title)
        if len(relevant) >= n:
            break
    return relevant[:n]


def get_news_titles(n=30):
    titles = get_chinese_news_titles(n)
    if titles or not NEWS_CN_FALLBACK_TO_EN:
        return titles
    return [title for title in get_english_news_titles(n) if is_relevant_cn_news_title(title)]


NEWS_PHRASE_CN = [
    ("prediction markets", "预测市场"),
    ("user funds", "用户资金"),
    ("gambling law hurdles", "博彩法律障碍"),
    ("gambling law", "博彩法律"),
    ("exploit losses", "漏洞攻击损失"),
    ("Japan entry", "进入日本市场"),
    ("SEC approves Nasdaq to list Bitcoin index options on the exchange", "SEC批准纳斯达克在交易所上市比特币指数期权"),
    ("70% of all crypto wrench attacks happen in France", "70%的加密货币扳手攻击发生在法国"),
    ("all crypto wrench attacks", "所有加密货币扳手攻击"),
    ("crypto wrench attacks", "加密货币扳手攻击"),
    ("wrench attacks", "扳手攻击"),
    ("on the exchange", "在交易所"),
    ("to list", "上市"),
    ("Bitcoin index options", "比特币指数期权"),
    ("spot Bitcoin ETF", "现货比特币ETF"),
    ("spot Ether ETF", "现货以太坊ETF"),
    ("exchange-traded fund", "交易所交易基金"),
    ("open interest", "未平仓合约"),
    ("market cap", "市值"),
    ("stablecoin bill", "稳定币法案"),
    ("crypto market", "加密市场"),
    ("crypto exchange", "加密货币交易所"),
    ("Federal Reserve", "美联储"),
    ("South Korea", "韩国"),
    ("Hong Kong", "香港"),
    ("United States", "美国"),
    ("Nasdaq", "纳斯达克"),
    ("BlackRock", "贝莱德"),
    ("Coinbase", "Coinbase"),
    ("Binance", "Binance"),
    ("Tether", "Tether"),
    ("Circle", "Circle"),
    ("Strategy", "Strategy"),
    ("MicroStrategy", "MicroStrategy"),
    ("Ethereum", "以太坊"),
    ("Bitcoin", "比特币"),
    ("Solana", "Solana"),
    ("XRP", "XRP"),
    ("Dogecoin", "Dogecoin"),
    ("France", "法国"),
    ("Japan", "日本"),
    ("China", "中国"),
    ("Europe", "欧洲"),
]

NEWS_TITLE_EXACT_CN = {
    "cftc officials who questioned prediction markets were suspended nyt": "据 NYT 报道，质疑预测市场的 CFTC 官员已被停职",
    "polymarket seeks japan entry despite gambling law hurdles report": "报告：尽管面临博彩法律障碍，Polymarket 仍寻求进入日本市场",
    "polymarket team says user funds safe as exploit losses climb above 600k": "Polymarket 团队称，用户资金安全，漏洞攻击损失已超过60万美元",
}

NEWS_TERM_CN = {
    "prediction markets": "预测市场",
    "user funds": "用户资金",
    "gambling law hurdles": "博彩法律障碍",
    "gambling law": "博彩法律",
    "exploit losses": "漏洞攻击损失",
    "exploit": "漏洞攻击",
    "losses": "损失",
    "japan entry": "进入日本市场",
    "japan": "日本",
    "market": "市场",
    "markets": "市场",
    "officials": "官员",
    "team": "团队",
    "funds": "资金",
}

NEWS_ALLOWED_EN_TOKENS = {
    "ARK",
    "Binance",
    "Bitcoin",
    "BlackRock",
    "BTC",
    "CFTC",
    "Circle",
    "Coinbase",
    "Dogecoin",
    "ETF",
    "Ethereum",
    "Fed",
    "MicroStrategy",
    "Nasdaq",
    "NYT",
    "Polymarket",
    "SEC",
    "Solana",
    "Strategy",
    "Tether",
    "US",
    "XRP",
}

NEWS_ORDINARY_EN_WORDS = {
    "above",
    "as",
    "climb",
    "climbs",
    "despite",
    "entry",
    "hurdle",
    "hurdles",
    "loss",
    "losses",
    "official",
    "officials",
    "questioned",
    "safe",
    "seek",
    "seeks",
    "suspended",
    "team",
    "were",
    "who",
}

NEWS_WORD_CN = {
    "sec": "SEC",
    "cftc": "CFTC",
    "fed": "美联储",
    "bitcoin": "比特币",
    "btc": "BTC",
    "ether": "以太坊",
    "ethereum": "以太坊",
    "crypto": "加密货币",
    "cryptocurrency": "加密货币",
    "token": "代币",
    "tokens": "代币",
    "stablecoin": "稳定币",
    "stablecoins": "稳定币",
    "etf": "ETF",
    "etfs": "ETF",
    "index": "指数",
    "option": "期权",
    "options": "期权",
    "exchange": "交易所",
    "exchanges": "交易所",
    "market": "市场",
    "markets": "市场",
    "price": "价格",
    "prices": "价格",
    "fund": "基金",
    "funds": "基金",
    "investor": "投资者",
    "investors": "投资者",
    "trader": "交易员",
    "traders": "交易员",
    "whale": "巨鲸",
    "whales": "巨鲸",
    "miner": "矿工",
    "miners": "矿工",
    "bank": "银行",
    "banks": "银行",
    "treasury": "金库",
    "reserve": "储备",
    "reserves": "储备",
    "attack": "攻击",
    "attacks": "攻击",
    "hack": "黑客攻击",
    "hacks": "黑客攻击",
    "exploit": "漏洞攻击",
    "lawsuit": "诉讼",
    "court": "法院",
    "judge": "法官",
    "bill": "法案",
    "senate": "参议院",
    "house": "众议院",
    "government": "政府",
    "regulator": "监管机构",
    "regulators": "监管机构",
    "report": "报告",
    "says": "称",
    "said": "称",
    "approves": "批准",
    "approved": "批准",
    "approve": "批准",
    "rejects": "拒绝",
    "files": "提交申请",
    "filed": "已提交申请",
    "launches": "推出",
    "launched": "推出",
    "lists": "上市",
    "list": "上市",
    "listing": "上市",
    "buy": "买入",
    "buys": "买入",
    "bought": "买入",
    "sell": "卖出",
    "sells": "卖出",
    "sold": "卖出",
    "raises": "筹集",
    "raised": "筹集",
    "falls": "下跌",
    "fall": "下跌",
    "drops": "下跌",
    "drop": "下跌",
    "rises": "上涨",
    "rise": "上涨",
    "surges": "大涨",
    "plunges": "大跌",
    "happen": "发生",
    "happens": "发生",
    "in": "在",
    "on": "在",
    "to": "",
    "for": "为",
    "of": "的",
    "the": "",
    "a": "",
    "an": "",
    "all": "所有",
    "with": "与",
    "after": "在之后",
    "before": "在之前",
}


def split_news_title_source(title):
    text = shorten_title(title, max_len=140)
    match = re.search(r"\s*:\s*(Report|NYT|Reuters|Bloomberg|CNBC|WSJ)\s*$", text, re.IGNORECASE)
    if not match:
        return text, ""
    return text[:match.start()].strip(), match.group(1).upper()


def money_to_cn(text):
    match = re.fullmatch(r"\$?\s*([\d,.]+)\s*([KMB])?", (text or "").strip(), re.IGNORECASE)
    if not match:
        return text
    value = float(match.group(1).replace(",", ""))
    unit = (match.group(2) or "").upper()
    if unit == "K":
        value *= 1_000
    elif unit == "M":
        value *= 1_000_000
    elif unit == "B":
        value *= 1_000_000_000

    def clean_num(num):
        return f"{num:.2f}".rstrip("0").rstrip(".")

    if value >= 100_000_000:
        return f"{clean_num(value / 100_000_000)}亿美元"
    if value >= 10_000:
        return f"{clean_num(value / 10_000)}万美元"
    return f"{clean_num(value)}美元"


def translate_news_term_cn(text):
    clean = re.sub(r"\s+", " ", (text or "").strip(" -:：,，.。"))
    if not clean:
        return ""
    lower = clean.lower()
    if lower in NEWS_TERM_CN:
        return NEWS_TERM_CN[lower]
    for en, cn in sorted(NEWS_PHRASE_CN, key=lambda x: len(x[0]), reverse=True):
        if lower == en.lower():
            return cn
    if clean in NEWS_ALLOWED_EN_TOKENS or re.fullmatch(r"[A-Z0-9]{2,12}", clean):
        return clean
    if re.fullmatch(r"[A-Z][A-Za-z0-9&.'-]{1,40}", clean):
        return clean
    return ""


def translate_news_clause_cn(text):
    clean = re.sub(r"\s+", " ", (text or "").strip())
    lower = clean.lower()
    if lower in NEWS_TERM_CN:
        return NEWS_TERM_CN[lower]

    match = re.fullmatch(r"(.+?) safe", clean, re.IGNORECASE)
    if match:
        subject = translate_news_term_cn(match.group(1))
        if subject:
            return f"{subject}安全"

    match = re.fullmatch(r"(.+?) losses climb above (\$?[\d,.]+\s*[KMB]?)", clean, re.IGNORECASE)
    if match:
        loss_subject = translate_news_term_cn(f"{match.group(1)} losses") or translate_news_term_cn(match.group(1))
        if loss_subject:
            if not loss_subject.endswith("损失"):
                loss_subject = f"{loss_subject}损失"
            return f"{loss_subject}已超过{money_to_cn(match.group(2))}"

    return ""


def translate_news_title_pattern_cn(text):
    clean = re.sub(r"\s+", " ", (text or "").strip())

    match = re.fullmatch(r"(.+?) officials who questioned (.+?) were suspended", clean, re.IGNORECASE)
    if match:
        org = translate_news_term_cn(match.group(1))
        topic = translate_news_term_cn(match.group(2))
        if org and topic:
            return f"质疑{topic}的{org}官员已被停职"

    match = re.fullmatch(r"(.+?) seeks (.+?) entry despite (.+?) hurdles", clean, re.IGNORECASE)
    if match:
        entity = translate_news_term_cn(match.group(1))
        market = translate_news_term_cn(match.group(2))
        hurdles = translate_news_term_cn(f"{match.group(3)} hurdles") or translate_news_term_cn(match.group(3))
        if entity and market and hurdles:
            if not market.endswith("市场"):
                market = f"{market}市场"
            if not hurdles.endswith("障碍"):
                hurdles = f"{hurdles}障碍"
            return f"尽管面临{hurdles}，{entity}仍寻求进入{market}"

    match = re.fullmatch(
        r"(.+?) team says (.+?) safe as (.+?) losses climb above (\$?[\d,.]+\s*[KMB]?)",
        clean,
        re.IGNORECASE,
    )
    if match:
        entity = translate_news_term_cn(match.group(1))
        subject = translate_news_term_cn(match.group(2))
        loss_subject = translate_news_term_cn(f"{match.group(3)} losses") or translate_news_term_cn(match.group(3))
        if entity and subject and loss_subject:
            if not loss_subject.endswith("损失"):
                loss_subject = f"{loss_subject}损失"
            return f"{entity}团队称，{subject}安全，{loss_subject}已超过{money_to_cn(match.group(4))}"

    match = re.fullmatch(r"(.+?) says (.+)", clean, re.IGNORECASE)
    if match:
        speaker = translate_news_term_cn(match.group(1))
        clause = translate_news_clause_cn(match.group(2))
        if speaker and clause:
            return f"{speaker}称，{clause}"

    return ""


def has_untranslated_english_fragment(text):
    for token in re.findall(r"[A-Za-z][A-Za-z0-9'-]*", text or ""):
        if token in NEWS_ALLOWED_EN_TOKENS:
            continue
        if token.lower() in NEWS_ORDINARY_EN_WORDS or token.lower() in NEWS_WORD_CN:
            return True
        if token.upper() == token:
            continue
        if token[:1].isupper():
            continue
        return True
    return False


def normalize_cn_title(text):
    text = (text or "").splitlines()[0].strip(" -•\t'\"“”")
    text = re.sub(r"^(中文|翻译|中)\s*[:：]\s*", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"\s*([：:，,。；;])\s*", r"\1", text)
    text = re.sub(r"(?<=[\u4e00-\u9fff])\s+(?=[\u4e00-\u9fff])", "", text)
    return text


def is_usable_cn_title(text):
    if not re.search(r"[\u4e00-\u9fff]", text or ""):
        return False
    return not has_untranslated_english_fragment(text)


def add_news_source_cn(sentence, source):
    if not source:
        return sentence
    if source == "REPORT":
        return f"报告：{sentence}"
    if sentence.startswith("据 "):
        return sentence
    return f"据 {source} 报道，{sentence}"


def manual_translate_title_cn(title):
    exact = NEWS_TITLE_EXACT_CN.get(normalize_title(title))
    if exact:
        return exact

    text, source = split_news_title_source(title)
    translated = translate_news_title_pattern_cn(text)

    if not translated:
        phrase_rewrite = text
        for en, cn in sorted(NEWS_PHRASE_CN, key=lambda x: len(x[0]), reverse=True):
            phrase_rewrite = re.sub(re.escape(en), cn, phrase_rewrite, flags=re.IGNORECASE)
        phrase_rewrite = normalize_cn_title(phrase_rewrite)
        if is_usable_cn_title(phrase_rewrite):
            translated = phrase_rewrite

    if not translated:
        translated = "新闻标题暂未翻译（见英文原文）"
    return add_news_source_cn(translated, source)


def get_cached_title_cn(state, title):
    cache = state.get("news_title_cn_cache") or {}
    item = cache.get(normalize_title(title)) or {}
    if item.get("ver") == NEWS_TRANSLATION_CACHE_VERSION and item.get("cn"):
        return item["cn"]
    return None


def cache_title_cn(state, title, cn):
    cache = state.get("news_title_cn_cache") or {}
    cache[normalize_title(title)] = {
        "ver": NEWS_TRANSLATION_CACHE_VERSION,
        "ts": int(time.time()),
        "cn": cn,
    }
    state["news_title_cn_cache"] = cache


def ollama_translate_title_cn(title):
    prompt = f"""
你是专业金融新闻标题翻译器。把下面英文加密货币新闻标题整句翻译成自然中文短句。
	要求：
	- 只输出一行中文标题，不要解释。
	- 必须整体翻译，不要逐词替换，不要留下普通英文单词。
	- BTC/ETF/SEC/CFTC/NYT 等缩写和 Polymarket、Coinbase 等专有名词可以保留英文。
	- Report/NYT/Reuters 等来源信息要翻译成“报告：”或“据 XXX 报道，...”。
	- 不判断利多/利空，不添加市场方向、原因分析、风险结论或交易建议。

标题：
{title}
""".strip()
    text = ollama_generate_fast(prompt, num_predict=80, timeout_sec=NEWS_OLLAMA_TIMEOUT_SEC)
    cn = normalize_cn_title(text)
    if not is_usable_cn_title(cn):
        return ""
    return cn


def translate_title_cn(state, title):
    global news_ollama_disabled_until_ts

    cached = get_cached_title_cn(state, title)
    if cached:
        return cached

    cn = ""
    if BTC_PUSH_NEWS_TRANSLATE_WITH_OLLAMA and int(time.time()) >= news_ollama_disabled_until_ts:
        try:
            cn = ollama_translate_title_cn(title)
        except Exception:
            news_ollama_disabled_until_ts = int(time.time()) + NEWS_OLLAMA_COOLDOWN_SEC
            log_exception("news title ollama translation skipped")

    if not cn:
        cn = manual_translate_title_cn(title)

    cache_title_cn(state, title, cn)
    return cn


def build_event_digest(state, titles):
    if not titles:
        return {
            "count": 0,
            "lines": ["无新增中文新闻标题"],
        }
    if all(is_cn_news_title(title) for title in titles):
        lines = [f"新增中文新闻标题 {len(titles)} 条（48h去重）"]
        for title in titles[:NEWS_MAX_ITEMS]:
            lines.append(f"• {shorten_title(title)}")
        return {
            "count": len(titles),
            "lines": lines,
        }
    if BTC_PUSH_NEWS_BILINGUAL:
        lines = [f"新增新闻标题 {len(titles)} 条（48h去重，中/英双语）"]
        for title in titles[:NEWS_MAX_ITEMS]:
            lines.append(f"• 中：{translate_title_cn(state, title)}")
            lines.append(f"  EN: {shorten_title(title)}")
    else:
        lines = [f"新增新闻标题 {len(titles)} 条（48h去重）"]
        for title in titles[:NEWS_MAX_ITEMS]:
            lines.append(f"• {shorten_title(title)}")
    return {
        "count": len(titles),
        "lines": lines,
    }


def fallback_summary(model_state):
    long_term = VALUE_META[model_state["long_term_trend"]]["cn"]
    trend = VALUE_META[model_state["trend"]]["cn"]
    volatility = VALUE_META[model_state["volatility"]]["cn"]
    risk = VALUE_META[model_state["risk"]]["cn"]
    execution = model_state["execution"]
    structure = model_state.get("structure") or {}
    if structure:
        phase = structure.get("phase") or "DATA_INSUFFICIENT"
        phase_cn = structure.get("phase_cn") or PHASE_META.get(phase, phase)
        missing = [
            CHECK_META.get(key, key)
            for key in (structure.get("missing") or [])
        ]
        missing_text = "无" if not missing else "、".join(missing[:4])
        if execution == "EXECUTE":
            return (
                f"summary: Breakout（突破）/OI（未平仓）/Volume（成交量）/Retest（回踩）链路已确认。\n"
                "5) 【一句话总结】突破链路完成，可按计划执行。"
            )
        return (
            f"summary: Breakout（突破）链路处于{phase_cn}（{phase}），缺口：{missing_text}。\n"
            "5) 【一句话总结】结构链路未完成，继续等待。"
        )
    if execution == "EXECUTE":
        return (
            f"summary: 72h 长期趋势（Long-Term Trend）处于{long_term}（{model_state['long_term_trend']}），"
            f"短周期处于{trend}（{model_state['trend']}），{volatility}（{model_state['volatility']}），"
            f"{risk}（{model_state['risk']}），当前具备 Execution Stage（执行许可）。\n"
            "5) 【一句话总结】五因子共振成立，可按计划执行趋势单。"
        )
    return (
        f"summary: 72h 长期趋势（Long-Term Trend）处于{long_term}（{model_state['long_term_trend']}），"
        f"短周期处于{trend}（{model_state['trend']}），{volatility}（{model_state['volatility']}），"
        f"{risk}（{model_state['risk']}），当前不具备 Execution Stage（执行许可）。\n"
        "5) 【一句话总结】五因子未共振，继续等待。"
    )


def validate_llm_line(line, model_state):
    structure = model_state.get("structure") or {}
    if structure:
        phase = structure.get("phase") or ""
        phase_cn = structure.get("phase_cn") or PHASE_META.get(phase, phase)
        if "突破" in line or "Breakout" in line or (phase and phase in line) or (phase_cn and phase_cn in line):
            return True
    required_pairs = [
        (VALUE_META[model_state["long_term_trend"]]["cn"], model_state["long_term_trend"]),
        (VALUE_META[model_state["trend"]]["cn"], model_state["trend"]),
        (VALUE_META[model_state["volatility"]]["cn"], model_state["volatility"]),
        (VALUE_META[model_state["risk"]]["cn"], model_state["risk"]),
    ]
    return any(cn in line and en in line for cn, en in required_pairs)


def summary_cache_key(market, model_state, event_count, fear_greed):
    delta4h = model_state.get("delta4h") or {}
    structure = model_state.get("structure") or {}
    fear_greed_part = "NA"
    if fear_greed and fear_greed.get("available"):
        fear_greed_part = f"{int(fear_greed.get('value'))}:{fear_greed.get('classification')}"
    parts = [
        str(round(float(market["price"]), -1)),
        f"{float(market['chg24']):.1f}",
        str(round(float(market["oi_usd"]), -7)),
        model_state["long_term_trend"],
        model_state["trend"],
        model_state["volatility"],
        model_state["liquidity"],
        model_state["tq"],
        model_state["risk"],
        model_state["execution"],
        f"{float(delta4h.get('price_4h_pct') or 0.0):.1f}",
        f"{float(delta4h.get('oi_4h_pct') or 0.0):.1f}",
        f"{float(delta4h.get('vol_4h_pct') or 0.0):.1f}",
        structure.get("phase", "NA"),
        ",".join(structure.get("missing") or []),
        str(event_count),
        fear_greed_part,
    ]
    return "|".join(parts)


def ollama_generate_fast(prompt, num_predict=160, timeout_sec=None):
    if timeout_sec is None:
        timeout_sec = OLLAMA_TIMEOUT_SEC
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
        "keep_alive": "30m",
        "options": {
            "temperature": 0.1,
            "num_predict": int(num_predict),
        },
    }
    r = requests.post(
        OLLAMA_API,
        json=payload,
        timeout=(3, max(1, timeout_sec)),
    )
    r.raise_for_status()
    return (r.json().get("response") or "").strip()


def generate_summary(state, market, model_state, event_digest, fear_greed):
    return (
        "summary: v6.1状态层模式不生成市场分析\n"
        "5) 【一句话总结】仅输出原始数据与状态标签。",
        "status_layer_no_analysis",
    )


def shorten_summary_line(summary_line):
    if len(summary_line) <= 72:
        return summary_line
    text = summary_line[len("summary:"):].strip() if summary_line.startswith("summary:") else summary_line
    parts = re.split(r"[，。]", text)
    compact = []
    total = 0
    for part in parts:
        part = part.strip()
        if not part:
            continue
        extra = len(part) + (1 if compact else 0)
        if total + extra > 60:
            break
        compact.append(part)
        total += extra
    if not compact:
        compact = [text[:60].rstrip("，。 ")]
    return f"summary: {'，'.join(compact)}。"


def build_factor_block(model_state):
    return [
        (
            "LTT={ltt} | Trend={trend} | Vol={vol} | Liq={liq} | TQ={tq} | Risk={risk}"
        ).format(
            ltt=f"{value_short(model_state['long_term_trend'])}({model_state['long_term_trend']})",
            trend=f"{value_short(model_state['trend'])}({model_state['trend']})",
            vol=f"{value_short(model_state['volatility'])}({model_state['volatility']})",
            liq=f"{value_short(model_state['liquidity'])}({model_state['liquidity']})",
            tq=f"{value_short(model_state['tq'])}({model_state['tq']})",
            risk=f"{value_short(model_state['risk'])}({model_state['risk']})",
        )
    ]


def format_market_delta_text(market_24h_delta, key, fallback=None):
    value = ((market_24h_delta or {}).get("metrics") or {}).get(key)
    if value is None:
        value = fallback
    if value is None:
        return "24h NA"
    return f"24h {format_signed_number(value, 2, '%')}"


def format_market_snapshot_block(market, fear_greed, delta4h, market_24h_delta=None):
    price = market["price"]
    mcap = market["mcap"]
    chg24 = market["chg24"]
    vol24 = market["vol24"]
    oi_usd = market["oi_usd"]
    mcap_delta_text = format_market_delta_text(market_24h_delta, "mcap", fallback=chg24)
    vol_delta_text = format_market_delta_text(market_24h_delta, "vol24")
    oi_delta_text = format_market_delta_text(market_24h_delta, "oi_usd")
    line1 = (
        f"价格 ${price:,.0f} | 24h {chg24:+.2f}% | "
        f"F&G {format_fear_greed_compact(fear_greed).replace('恐惧指数 ', '')}"
    )
    line2 = (
        f"24h变化: 市值 {mcap_delta_text.replace('24h ', '')} | "
        f"24h总量 {vol_delta_text.replace('24h ', '')} | "
        f"OI {oi_delta_text.replace('24h ', '')}"
    )
    if delta4h:
        line3 = (
            f"4H: Price {format_signed_number(delta4h.get('price_4h_pct'), 2, '%')} | "
            f"OI {format_signed_number(delta4h.get('oi_4h_pct'), 2, '%')} | "
            f"Vol {format_signed_number(delta4h.get('vol_4h_pct'), 2, '%')}"
        )
    else:
        line3 = "4H: 数据不足"
    return [line1, line2, line3]


def format_structure_price(value):
    value = safe_float(value)
    try:
        return f"${float(value):,.2f}" if assets.current().equity else f"${float(value):,.0f}"
    except Exception:
        return "NA"


def format_structure_check_line(checks, key):
    item = (checks or {}).get(key) or {}
    status = item.get("status") or "NA"
    label = CHECK_META.get(key, key)
    status_text = STATUS_META.get(status, status)
    detail = item.get("detail") or ""
    return f"{label}: {status}（{status_text}，{detail}）"


def format_structure_check_compact(checks, key):
    item = (checks or {}).get(key) or {}
    status = item.get("status") or "NA"
    label = CHECK_META.get(key, key)
    compact = item.get("compact")
    if compact:
        return f"{label}={status}({compact})"
    return f"{label}={status}"


def format_structure_aux_line(checks):
    oi_fast = (checks or {}).get("oi_fast_expansion") or {}
    oi_slow = (checks or {}).get("oi_slow_confirmation") or {}
    vol_fast = (checks or {}).get("volume_fast_expansion") or {}
    vol_slow = (checks or {}).get("volume_slow_confirmation") or {}
    parts = [
        f"OI4H={oi_fast.get('status', 'NA')}({oi_fast.get('compact', 'NA')})",
        f"OI24/72={oi_slow.get('status', 'NA')}({oi_slow.get('compact', 'NA')})",
        f"量4H={vol_fast.get('status', 'NA')}({vol_fast.get('compact', 'NA')})",
        f"量24H={vol_slow.get('status', 'NA')}({vol_slow.get('compact', 'NA')})",
    ]
    return "辅助: " + " | ".join(parts)


def format_structure_status_text(status, key=None):
    status = status or "NA"
    if status == "YES":
        return "达成"
    if status == "NO":
        return "未达成"
    if status == "PARTIAL":
        return "接近"
    if status == "WAIT":
        return "等待"
    if status == "NA" and key == "pullback_confirmed":
        return "未触发"
    if status == "NA":
        return "数据不足"
    return status


def format_structure_chain_line(checks):
    checks = checks or {}
    breakout = format_structure_status_text(
        (checks.get("breakout") or {}).get("status"), "breakout"
    )
    oi_fast = format_structure_status_text(
        (checks.get("oi_fast_expansion") or {}).get("status"), "oi_fast_expansion"
    )
    oi_slow = format_structure_status_text(
        (checks.get("oi_slow_confirmation") or {}).get("status"), "oi_slow_confirmation"
    )
    vol_fast = format_structure_status_text(
        (checks.get("volume_fast_expansion") or {}).get("status"), "volume_fast_expansion"
    )
    vol_slow = format_structure_status_text(
        (checks.get("volume_slow_confirmation") or {}).get("status"), "volume_slow_confirmation"
    )
    pullback = format_structure_status_text(
        (checks.get("pullback_confirmed") or {}).get("status"), "pullback_confirmed"
    )
    return (
        "达成状态: "
        f"突破={breakout} | "
        f"OI4H={oi_fast} | "
        f"OI24/72={oi_slow} | "
        f"量4H={vol_fast} | "
        f"量24H={vol_slow} | "
        f"回踩={pullback}"
    )


def format_structure_detail_line(checks):
    checks = checks or {}
    oi_fast = checks.get("oi_fast_expansion") or {}
    oi_slow = checks.get("oi_slow_confirmation") or {}
    vol_fast = checks.get("volume_fast_expansion") or {}
    vol_slow = checks.get("volume_slow_confirmation") or {}
    return (
        "细节: "
        f"OI4H {oi_fast.get('compact', 'NA')} | "
        f"OI24/72 {oi_slow.get('compact', 'NA')} | "
        f"量4H {vol_fast.get('compact', 'NA')} | "
        f"量24H {vol_slow.get('compact', 'NA')}"
    )


def format_structure_block(structure_state):
    if not structure_state:
        return ["结构数据: NA（未启用）"]

    phase = structure_state.get("phase") or "DATA_INSUFFICIENT"
    phase_cn = structure_state.get("phase_cn") or PHASE_META.get(phase, phase)
    level = format_structure_price(structure_state.get("breakout_level"))
    threshold = format_structure_price(structure_state.get("breakout_threshold"))
    latest = format_structure_price(structure_state.get("latest_close"))
    checks = structure_state.get("checks") or {}
    lines = [
        f"{phase_cn} ({phase}) | 收盘 {latest} | 突破 {level} | 确认 {threshold}",
        format_structure_chain_line(checks),
        format_structure_detail_line(checks),
    ]
    return lines


def format_structure_compact(structure_state):
    if not structure_state:
        return "结构 NA"
    phase = structure_state.get("phase") or "DATA_INSUFFICIENT"
    phase_cn = structure_state.get("phase_cn") or PHASE_META.get(phase, phase)
    missing = [
        CHECK_META.get(key, key)
        for key in (structure_state.get("missing") or [])
    ]
    missing_text = "无" if not missing else "/".join(missing[:3])
    return f"结构 {phase_cn} ({phase}) | 缺口 {missing_text}"


def format_missing_text(structure_state):
    missing = [
        CHECK_META.get(key, key)
        for key in ((structure_state or {}).get("missing") or [])
    ]
    return "无" if not missing else "、".join(missing)


def format_context_block(seven_day_comparison, model_state):
    lines = []
    if seven_day_comparison and seven_day_comparison.get("available"):
        market = seven_day_comparison.get("market") or {}
        price_delta = market_delta_pct(market, "price")
        fear = seven_day_comparison.get("fear_greed") or {}
        fear_text = "F&G NA"
        if fear:
            prev = fear.get("prev") or {}
            now = fear.get("now") or {}
            try:
                prev_value = int(prev.get("value"))
                now_value = int(now.get("value"))
                fear_text = f"F&G {prev_value}->{now_value}({now_value - prev_value:+d})"
            except Exception:
                pass
        lines.append(f"7D: 价格 {format_signed_number(price_delta, 2, '%')} | {fear_text}")
    lines.append(
        "六因子: "
        f"LTT {value_short(model_state['long_term_trend'])} | "
        f"Trend {value_short(model_state['trend'])} | "
        f"Liq {value_short(model_state['liquidity'])} | "
        f"TQ {value_short(model_state['tq'])} | "
        f"Risk {value_short(model_state['risk'])}"
    )
    return lines


def format_conclusion_block(model_state, structure_state, one_line):
    execution = model_state.get("execution")
    if structure_state:
        phase = structure_state.get("phase") or "DATA_INSUFFICIENT"
        phase_cn = structure_state.get("phase_cn") or PHASE_META.get(phase, phase)
        missing_text = format_missing_text(structure_state)
        if execution == "EXECUTE":
            return ["执行结论：允许执行 (EXECUTE)，突破链路已完成。"]
        return [f"WAIT：{phase_cn} ({phase})，缺口：{missing_text}。"]
    return [one_line]


def apply_structure_gate(model_state, structure_state):
    out = dict(model_state)
    out["structure"] = structure_state
    if structure_state:
        execution = "EXECUTE" if structure_state.get("execution") == "EXECUTE" else "WAIT"
        out["execution"] = execution
        out["execution_stage"] = model.execution_stage(execution)
        out["display"] = model.build_display_map(out)
    return out


def status_icon(ok):
    return "YES" if bool(ok) else "NO"


def record_mark(record):
    return "＊" if record_has_data(record) and not record_is_current(record) else ""


def record_value(record, key, default=None):
    return (((record or {}).get("value") or {}).get(key, default))


def current_record_values(record):
    """当前判断只消费有效记录；原值继续由 record_value 供历史展示读取。"""
    return ((record or {}).get("value") or {}) if record_is_current(record) else {}


def current_record_value(record, key, default=None):
    return current_record_values(record).get(key, default)


def format_pct_value(value, decimals=2, signed=True):
    value = safe_float(value)
    if value is None:
        return "NA"
    try:
        value = float(value)
    except Exception:
        return "NA"
    sign = "+" if signed and value > 0 else ""
    return f"{sign}{value:.{int(decimals)}f}%"


def format_pp_value(value, decimals=2, signed=True):
    value = safe_float(value)
    if value is None:
        return "NA"
    try:
        value = float(value)
    except Exception:
        return "NA"
    sign = "+" if signed and value > 0 else ""
    return f"{sign}{value:.{int(decimals)}f}pp"


def format_rate_value(value, decimals=2):
    value = safe_float(value)
    if value is None:
        return "NA"
    try:
        return f"{float(value):.{int(decimals)}f}%"
    except Exception:
        return "NA"


def format_plain_number(value, decimals=2):
    value = safe_float(value)
    if value is None:
        return "NA"
    try:
        return f"{float(value):.{int(decimals)}f}"
    except Exception:
        return "NA"


def format_score_value(value):
    value = safe_float(value)
    if value is None:
        return "NA"
    try:
        return f"{float(value):.0f}/100"
    except Exception:
        return "NA"


def format_fed_status_line(value):
    parts = []
    lower = value.get("fed_target_lower")
    upper = value.get("fed_target_upper")
    effr = value.get("effective_fed_funds")
    if lower is not None and upper is not None:
        parts.append(f"Target {format_rate_value(lower)}-{format_rate_value(upper)}")
    if effr is not None:
        parts.append(f"EFFR {format_rate_value(effr)}")
    if value.get("fed_policy"):
        parts.append(f"FedWatch {fed_policy_cn(value.get('fed_policy'))}")
    else:
        parts.append("FedWatch未启用")
    return "Fed: " + " | ".join(parts)


def fiscal_status_cn(status):
    if status == "normal":
        return "正常"
    if status == "stressed":
        return "承压"
    if status == "fiscal_dominance":
        return "财政主导风险"
    return "数据不足"


def cost_gap_cn(status):
    if status == "cost_outpacing":
        return "生活成本快于工资"
    if status == "wage_outpacing":
        return "工资快于生活成本"
    if status == "mixed":
        return "基本混合"
    return "数据不足"


def fiscal_pressure_text(fiscal):
    status = (fiscal or {}).get("status")
    if status == "fiscal_dominance":
        return "高"
    if status == "stressed":
        return "偏高"
    if status == "normal":
        return "未确认偏高"
    return "观察中"


def debt_cost_text(fiscal):
    ratio = safe_float((fiscal or {}).get("interest_revenue_ratio_pct"))
    if ratio is None:
        return "观察中"
    if ratio >= FISCAL_INTEREST_REVENUE_DOMINANCE_PCT:
        return "高"
    if ratio >= FISCAL_INTEREST_REVENUE_STRESS_PCT:
        return "偏高"
    return "未确认偏高"


def m2_status_text(value):
    status = value.get("m2_status")
    if status == "confirmed_expansion":
        return "扩张确认"
    if status == "improving":
        return "同比连续改善"
    if status == "mild_expansion":
        return "温和扩张"
    if status == "contracting":
        return "仍在收缩"
    return "未确认"


def fed_balance_sheet_status_text(value):
    delta = safe_float(value.get("fed_balance_sheet_30d_pct"))
    if delta is None:
        return "未确认"
    if delta > 0.0:
        return "扩表迹象"
    if abs(delta) <= 0.10:
        return "缩表放缓/基本持平"
    return "仍在缩表"


def buyback_scale_text(status):
    if status == "strong":
        return "明显维护"
    if status == "effective":
        return "有效维护"
    if status == "limited":
        return "规模有限"
    return "规模未知"


def buyback_status_text(value):
    buyback = value.get("treasury_buyback") or {}
    if not buyback:
        return "观察中"
    count = safe_float(buyback.get("recent_count"))
    if count is None or count <= 0:
        return "未确认"
    scale = buyback_scale_text(buyback.get("scale_status"))
    return f"存在，{scale}"


def format_macro_pressure_lines(value, macro_state):
    environment = (macro_state or {}).get("environment") or {}
    bands = environment.get("bands") or {}
    mark = record_mark({"value": value})
    return [
        "现实压制：",
        (
            f"US10Y {format_us10y_directional_band_value(value.get('us10y'), value.get('us10y_30d_pp'))}{mark}"
            f"（{bands.get('us10y') or classify_us10y_band(value.get('us10y'))}，30D {format_pp_value(value.get('us10y_30d_pp'), 2)}）"
        ),
        (
            f"Real {format_real_yield_directional_band_value(value.get('real_yield_10y'), value.get('real_yield_10y_30d_pp'))}{mark}"
            f"（{bands.get('real_yield') or classify_real_yield_band(value.get('real_yield_10y'))}，30D {format_pp_value(value.get('real_yield_10y_30d_pp'), 2)}）"
        ),
        (
            f"DXY {format_dxy_directional_band_value(value.get('usd_index'), value.get('usd_index_30d_pct'))}{mark}"
            f"（{bands.get('dxy') or classify_dxy_band(value.get('usd_index'))}，30D {format_pct_value(value.get('usd_index_30d_pct'), 2)}）"
        ),
        f"状态：{environment.get('status_text') or '宏观压制数据不足。'}",
    ]


def format_long_money_lines(value, macro_state):
    fiscal = value.get("fiscal_proxy") or {}
    long_term = (macro_state or {}).get("long_term_environment") or {}
    return [
        "长期货币层：",
        (
            f"财政压力：{fiscal_pressure_text(fiscal)} | "
            f"债务成本：{debt_cost_text(fiscal)} | "
            f"生活成本代理：{cost_gap_cn(((value.get('cost_of_living_proxy') or {}).get('status')))}"
        ),
        (
            f"M2：{m2_status_text(value)}"
            f"（YoY {format_pct_value(value.get('m2_yoy_pct'), 2)}） | "
            f"Fed资产负债表：{fed_balance_sheet_status_text(value)}"
            f"（30D {format_pct_value(value.get('fed_balance_sheet_30d_pct'), 2)}）"
        ),
        (
            f"Treasury Buyback：{buyback_status_text(value)}"
            f"（最新 {format_money_compact_plain(((value.get('treasury_buyback') or {}).get('total_par_amt_accepted')), 2)}）"
        ),
        f"长期状态：{long_term.get('status_text') or '长期货币层数据不足，维持观察。'}",
    ]


def format_compact_long_money_lines(value, macro_state):
    fiscal = value.get("fiscal_proxy") or {}
    long_term = (macro_state or {}).get("long_term_environment") or {}
    return [
        "长期货币层：",
        "长期压力来源：",
        (
            f"财政压力：{fiscal_pressure_text(fiscal)} | "
            f"债务成本：{debt_cost_text(fiscal)} | "
            f"生活成本代理：{cost_gap_cn(((value.get('cost_of_living_proxy') or {}).get('status')))}"
        ),
        "",
        "长期流动性变化：",
        (
            f"M2：{m2_status_text(value)}"
            f"（YoY {format_pct_value(value.get('m2_yoy_pct'), 2)}） | "
            f"Fed资产负债表：{fed_balance_sheet_status_text(value)}"
            f"（30D {format_pct_value(value.get('fed_balance_sheet_30d_pct'), 2)}）"
        ),
        "",
        "债市流动性维护：",
        (
            f"Treasury Buyback：{buyback_status_text(value)}"
            f"（最新 {format_money_compact_plain(((value.get('treasury_buyback') or {}).get('total_par_amt_accepted')), 2)}）"
        ),
        "",
        "长期状态：",
        f"{long_term.get('status_text') or '长期货币层数据不足，维持观察。'}",
    ]


def format_component_yoy(components, key):
    item = (components or {}).get(key) or {}
    label = item.get("label") or key
    return f"{label} {format_pct_value(item.get('yoy_pct'), 2)}"


def format_cost_of_living_lines(value):
    cost = value.get("cost_of_living_proxy") or {}
    if not cost:
        return ["生活成本代理: 数据不足（免费官方代理源不可用）"]
    components = cost.get("components") or {}
    return [
        (
            f"生活成本代理: 成本YoY {format_pct_value(cost.get('cost_proxy_yoy_pct'), 2)} | "
            f"工资YoY {format_pct_value(cost.get('wage_yoy_pct'), 2)} | "
            f"差 {format_pp_value(cost.get('cost_wage_gap_pp'), 2)} | "
            f"{cost_gap_cn(cost.get('status'))}"
        ),
        (
            "分项YoY: "
            + " | ".join([
                format_component_yoy(components, "rent"),
                format_component_yoy(components, "home_insurance_proxy"),
                format_component_yoy(components, "medical"),
                format_component_yoy(components, "education"),
                format_component_yoy(components, "auto_insurance"),
            ])
        ),
        "生活成本说明: 以上为官方免费代理指标，只提示官方通胀与居民成本可能偏离，不单独下恶化结论。",
    ]


def format_fiscal_lines(value, macro_state):
    fiscal = value.get("fiscal_proxy") or {}
    if not fiscal:
        return ["财政状态: 数据不足（免费官方财政源不可用）"]
    status = macro_state.get("fiscal_status") or fiscal.get("status")
    ratio_date = fiscal.get("interest_revenue_ratio_date")
    ratio_note = ""
    if ratio_date:
        ratio_note = (f"（YTD截至{ratio_date}）" if fiscal.get("interest_revenue_ratio_source") == "Treasury Fiscal Data YTD"
                      else f"（{ratio_date}季调年率）")
    interest_date = (fiscal.get("source_date") or {}).get("treasury_interest")
    interest_note = f"（截至{interest_date}）" if interest_date else ""
    return [
        (
            f"财政状态: {fiscal_status_cn(status)} | "
            f"赤字/GDP {format_pct_value(fiscal.get('deficit_gdp_pct'), 2)} | "
            f"利息/收入 {format_pct_value(fiscal.get('interest_revenue_ratio_pct'), 2, signed=False)}{ratio_note} | "
            f"债务/GDP {format_pct_value(fiscal.get('debt_gdp_pct'), 2, signed=False)} | "
            f"长债拍卖投标倍数 {format_ratio_value(fiscal.get('long_auction_avg_bid_to_cover'))}"
        ),
        (
            f"财政补充: 利息YTD {format_money_compact_plain(fiscal.get('treasury_interest_fytd_usd'), 2)}{interest_note} | "
            f"长债发行样本 {format_money_compact_plain(fiscal.get('long_auction_offering_recent_usd'), 2)} | "
            "财政状态仅用于长期配置，不作为短线交易信号。"
        ),
    ]


def format_macro_boundary_lines():
    return [
        "解释边界: 真实利率基于官方通胀口径，不能等同居民真实生活成本；DXY只代表美元相对主要货币，不代表美国国内购买力。",
        "长期逻辑: 高利率会累积财政和债务成本压力；该压力是否转化为流动性，必须等待M2、Fed资产负债表和市场资金数据确认。",
    ]


def format_ratio_value(value):
    if value is None:
        return "NA"
    try:
        return f"{float(value):.2f}x"
    except Exception:
        return "NA"


def format_signed_money_compact(value, decimals=1):
    if value is None:
        return "NA"
    try:
        value = float(value)
    except Exception:
        return "NA"
    sign = "+" if value > 0 else "-" if value < 0 else ""
    text = format_money(abs(value), decimals=decimals, compact=True)
    return f"{sign}{text}"


def format_money_compact_plain(value, decimals=2):
    if value is None:
        return "NA"
    return format_money(value, decimals=decimals, compact=True)


def format_btc_holding(value):
    if value is None:
        return "NA"
    try:
        return f"{float(value) / 10000.0:.1f}万BTC"
    except Exception:
        return "NA"


def format_liquidation_window_value(record, key, window):
    if liquidation_record_ready(record, window):
        return format_money_compact_plain(record_value(record, key), 2) + record_mark(record)
    if liquidation_record_collecting(record):
        return "采集中"
    return "NA" + record_mark(record)


def format_etf_holding_7d(etf):
    value = record_value(etf, "holding_7d_pct")
    if value is not None:
        return format_pct_value(value, 2) + record_mark(etf)
    if record_value(etf, "holding_7d_status") == "collecting":
        return "采集中(需7D快照)"
    return "NA" + record_mark(etf)


def calc_bars_delta_pct(signal_bars, hours):
    bars = [
        item for item in (signal_bars or [])
        if safe_float(item.get("close")) is not None
    ]
    bars.sort(key=lambda item: int(item.get("open_time") or 0))
    if assets.current().equity:
        target = int(bars[-1]["close_time"]) - int(hours) * assets.HOUR if bars else 0
        boundary = next((row for row in bars if row.get("close_time") == target), None)
        return pct(bars[-1].get("close"), boundary.get("close")) if boundary else None
    if len(bars) < int(hours) + 1:
        return None
    return pct(bars[-1].get("close"), bars[-int(hours) - 1].get("close"))


def calc_window_extreme(signal_bars, lookback, field, mode):
    bars = [
        item for item in (signal_bars or [])
        if safe_float(item.get(field)) is not None
    ]
    bars.sort(key=lambda item: int(item.get("open_time") or 0))
    if len(bars) < 2:
        return None
    window = bars[-int(lookback):]
    if assets.current().equity and bars:
        cutoff = int(bars[-1]["close_time"]) - int(lookback) * assets.HOUR
        window = [row for row in bars if row["close_time"] > cutoff]
    values = [safe_float(item.get(field)) for item in window]
    values = [value for value in values if value is not None]
    if not values:
        return None
    return max(values) if mode == "max" else min(values)


def calc_support_level(signal_bars, lookback):
    return calc_window_extreme(signal_bars, lookback, "low", "min")


def calc_near_term_support_level(signal_bars, lookback, fallback_lookback):
    bars = []
    for item in signal_bars or []:
        if safe_float(item.get("low")) is None or safe_float(item.get("close")) is None:
            continue
        try:
            int(item.get("open_time") or 0)
        except Exception:
            continue
        bars.append(item)
    bars.sort(key=lambda item: int(item.get("open_time") or 0))
    if len(bars) < 2:
        return None

    lookback = max(3, int(lookback))
    window = bars[-lookback:]
    if assets.current().equity:
        window = [row for row in bars if row["close_time"] > bars[-1]["close_time"] - lookback * assets.HOUR]
    latest_close = safe_float(window[-1].get("close"))
    if latest_close is None:
        return None

    swing_lows = []
    for idx in range(1, len(window) - 1):
        if assets.current().equity and window[idx + 1]["open_time"] - window[idx - 1]["open_time"] != 2 * assets.HOUR:
            continue
        low = safe_float(window[idx].get("low"))
        prev_low = safe_float(window[idx - 1].get("low"))
        next_low = safe_float(window[idx + 1].get("low"))
        if low is None or prev_low is None or next_low is None:
            continue
        if low > latest_close:
            continue
        if low <= prev_low and low <= next_low and (low < prev_low or low < next_low):
            swing_lows.append(low)
    if swing_lows:
        return max(swing_lows)

    fallback_lookback = max(1, int(fallback_lookback))
    fallback_window = window[-fallback_lookback:]
    if assets.current().equity:
        fallback_window = [row for row in window if row["close_time"] > window[-1]["close_time"] - fallback_lookback * assets.HOUR]
    lows = [safe_float(item.get("low")) for item in fallback_window]
    lows = [value for value in lows if value is not None]
    if not lows:
        return None
    return min(lows)


def calc_resistance_level(signal_bars, lookback):
    return calc_window_extreme(signal_bars, lookback, "high", "max")


def latest_close_from_bars(signal_bars):
    bars = [
        item for item in (signal_bars or [])
        if safe_float(item.get("close")) is not None
    ]
    bars.sort(key=lambda item: int(item.get("open_time") or 0))
    return safe_float((bars[-1] if bars else {}).get("close"))


def classify_market_position(price, support, resistance):
    price = safe_float(price)
    support = safe_float(support)
    resistance = safe_float(resistance)
    if price is None or support is None or resistance is None:
        return "data_insufficient", "数据不足"
    if resistance <= support:
        return "range_invalid", "区间异常"
    if price >= resistance:
        return "above_upper", "站上上沿"
    if price < support:
        return "below_lower", "跌破下沿"
    span = resistance - support
    if price >= resistance - span * 0.2:
        return "near_upper", "接近上沿"
    if price <= support + span * 0.2:
        return "near_lower", "接近下沿"
    return "mid_range", "区间中部"


def build_support_test_state(signal_bars, rolling_support):
    support = safe_float(rolling_support)
    bars = [
        item for item in (signal_bars or [])
        if safe_float(item.get("low")) is not None
        and safe_float(item.get("close")) is not None
        and int(item.get("close_time") or 0) > 0
    ]
    bars.sort(key=lambda item: int(item.get("open_time") or 0))
    lookback = max(2, assets.current().parameter("MARKET_STRUCTURE_NEAR_SUPPORT_LOOKBACK_H", MARKET_STRUCTURE_NEAR_SUPPORT_LOOKBACK_H))
    bars = ([row for row in bars if row["close_time"] > bars[-1]["close_time"] - lookback * assets.HOUR]
            if assets.current().equity and bars else bars[-lookback:])
    base = {
        "anchor": support,
        "formed_bucket": None,
        "first_retest_bucket": None,
        "phase": "inactive",
        "completed_rule": False,
        "buffer": None,
    }
    if support is None or len(bars) < 2:
        return base

    tolerance = max(0.1, abs(support) * 1e-9)
    formed_index = None
    for index, bar in enumerate(bars):
        low = safe_float(bar.get("low"))
        if low is not None and math.isclose(low, support, rel_tol=1e-9, abs_tol=tolerance):
            formed_index = index
            break
    if formed_index is None:
        return base

    formed_bucket = int(bars[formed_index].get("close_time") or 0) // (3600 * 1000)
    result = {
        **base,
        "formed_bucket": formed_bucket,
        "phase": "formed",
    }
    first_retest_bucket = None
    first_retest_buffer = None
    for index in range(formed_index + 1, len(bars)):
        bar = bars[index]
        close = safe_float(bar.get("close"))
        low = safe_float(bar.get("low"))
        if close is None or low is None:
            continue
        if close < support:
            return {
                **result,
                "phase": "broken",
            }
        atr = calc_atr(bars[:index])
        buffer_value = max(
            abs(support) * assets.current().parameter("STRUCTURE_BREAKOUT_BUFFER_PCT", STRUCTURE_BREAKOUT_BUFFER_PCT),
            (atr or 0.0) * assets.current().parameter("STRUCTURE_BREAKOUT_BUFFER_ATR", STRUCTURE_BREAKOUT_BUFFER_ATR),
        )
        if first_retest_bucket is None and low <= support + buffer_value:
            first_retest_bucket = int(bar.get("close_time") or 0) // (3600 * 1000)
            first_retest_buffer = buffer_value
    if first_retest_bucket is not None:
        return {
            **result,
            "first_retest_bucket": first_retest_bucket,
            "phase": "tested",
            "completed_rule": True,
            "buffer": first_retest_buffer,
        }
    return result


def build_market_levels(signal_bars, structure_state):
    complete_bars = [
        item for item in (signal_bars or [])
        if safe_float(item.get("low")) is not None
        and safe_float(item.get("close")) is not None
    ]
    complete_bars.sort(key=lambda item: int(item.get("open_time") or 0))
    prior_bars = complete_bars[:-1]
    prior_short_support = calc_near_term_support_level(
        prior_bars,
        assets.current().parameter("MARKET_STRUCTURE_NEAR_SUPPORT_LOOKBACK_H", MARKET_STRUCTURE_NEAR_SUPPORT_LOOKBACK_H),
        assets.current().parameter("MARKET_STRUCTURE_NEAR_SUPPORT_FALLBACK_H", MARKET_STRUCTURE_NEAR_SUPPORT_FALLBACK_H),
    )
    rolling_short_support = calc_near_term_support_level(
        complete_bars,
        assets.current().parameter("MARKET_STRUCTURE_NEAR_SUPPORT_LOOKBACK_H", MARKET_STRUCTURE_NEAR_SUPPORT_LOOKBACK_H),
        assets.current().parameter("MARKET_STRUCTURE_NEAR_SUPPORT_FALLBACK_H", MARKET_STRUCTURE_NEAR_SUPPORT_FALLBACK_H),
    )
    support_test = build_support_test_state(complete_bars, rolling_short_support)
    short_support = prior_short_support or rolling_short_support
    short_resistance = calc_resistance_level(complete_bars, assets.current().parameter("MARKET_STRUCTURE_SHORT_LOOKBACK_H", MARKET_STRUCTURE_SHORT_LOOKBACK_H))
    trend_support = calc_support_level(complete_bars, assets.current().parameter("MARKET_STRUCTURE_TREND_LOOKBACK_H", MARKET_STRUCTURE_TREND_LOOKBACK_H))
    trend_resistance = calc_resistance_level(complete_bars, assets.current().parameter("MARKET_STRUCTURE_TREND_LOOKBACK_H", MARKET_STRUCTURE_TREND_LOOKBACK_H))
    latest_close = latest_close_from_bars(complete_bars)
    breakout_level = safe_float((structure_state or {}).get("breakout_level")) or trend_resistance
    confirmation_price = safe_float((structure_state or {}).get("breakout_threshold"))
    breakout_ok = (
        latest_close is not None
        and breakout_level is not None
        and latest_close >= breakout_level
    )
    confirmation_ok = (
        latest_close is not None
        and confirmation_price is not None
        and latest_close >= confirmation_price
    )
    short_breakout_ok = (
        latest_close is not None
        and short_resistance is not None
        and latest_close >= short_resistance
    )
    short_breakdown_ok = (
        latest_close is not None
        and short_support is not None
        and latest_close < short_support
    )
    position_state, position_text = classify_market_position(
        latest_close,
        short_support,
        short_resistance,
    )
    return {
        "support": trend_support,
        "breakout_level": breakout_level,
        "confirmation_price": confirmation_price,
        "breakout_ok": breakout_ok,
        "confirmation_ok": confirmation_ok,
        "short_support": short_support,
        "rolling_short_support": rolling_short_support,
        "support_test": support_test,
        "short_resistance": short_resistance,
        "trend_support": trend_support,
        "trend_resistance": trend_resistance,
        "short_breakout_ok": short_breakout_ok,
        "short_breakdown_ok": short_breakdown_ok,
        "position_state": position_state,
        "position_text": position_text,
    }


def rule_status_text(hit):
    if hit is None:
        return "数据不足"
    return "命中" if bool(hit) else "未命中"


def rule_ge(value, threshold):
    number = safe_float(value)
    limit = safe_float(threshold)
    if number is None or limit is None:
        return None
    return number >= limit


def rule_gt(value, threshold):
    number = safe_float(value)
    limit = safe_float(threshold)
    if number is None or limit is None:
        return None
    return number > limit


def rule_le(value, threshold):
    number = safe_float(value)
    limit = safe_float(threshold)
    if number is None or limit is None:
        return None
    return number <= limit


def rule_lt(value, threshold):
    number = safe_float(value)
    limit = safe_float(threshold)
    if number is None or limit is None:
        return None
    return number < limit


def rule_abs_le(value, threshold):
    number = safe_float(value)
    limit = safe_float(threshold)
    if number is None or limit is None:
        return None
    return abs(number) <= limit


def rule_all(*items):
    has_unknown = False
    for item in items:
        if item is False:
            return False
        if item is None:
            has_unknown = True
    return None if has_unknown else True


def rule_any(*items):
    has_unknown = False
    for item in items:
        if item is True:
            return True
        if item is None:
            has_unknown = True
    return None if has_unknown else False


def rule_not(item):
    if item is None:
        return None
    return not bool(item)


def rule_hit_count(items):
    return sum(1 for item in items if item is True)


def rule_known_count(items):
    return sum(1 for item in items if item is not None)


def load_status_rules_config():
    with open(os.getenv(assets.current().asset_id + "_STATUS_RULES_FILE", STATUS_RULES_FILE), "r", encoding="utf-8") as f:
        config = json.load(f)
    layers = config.get("status_layers")
    if not isinstance(layers, list) or not layers:
        raise RuntimeError("status rules config missing status_layers")
    for layer in layers:
        if not isinstance(layer, dict):
            raise RuntimeError("status layer config item must be object")
        if not layer.get("key") or not layer.get("label"):
            raise RuntimeError("status layer config missing key/label")
        if not isinstance(layer.get("inputs"), list) or not layer.get("inputs"):
            raise RuntimeError(f"status layer {layer.get('key')} missing inputs")
        if not isinstance(layer.get("statuses"), list) or not layer.get("statuses"):
            raise RuntimeError(f"status layer {layer.get('key')} missing statuses")
    return config


def resolve_status_metric(context, path):
    current = context
    for part in str(path or "").split("."):
        if not part:
            return None
        if not isinstance(current, dict) or part not in current:
            return None
        current = current.get(part)
    return current


def compare_status_metric(value, op, expected=None):
    if value is None:
        return None
    op = str(op or "exists").lower()
    if op == "exists":
        return True
    if op == "eq":
        if isinstance(expected, bool):
            return bool(value) is expected
        value_number = safe_float(value)
        expected_number = safe_float(expected)
        if value_number is not None and expected_number is not None:
            return value_number == expected_number
        return value == expected
    if op == "ne":
        result = compare_status_metric(value, "eq", expected)
        return None if result is None else not result
    if op == "in":
        return value in (expected or [])

    value_number = safe_float(value)
    expected_number = safe_float(expected)
    if value_number is None or expected_number is None:
        return None
    if op in ("gt", ">"):
        return value_number > expected_number
    if op in ("gte", ">="):
        return value_number >= expected_number
    if op in ("lt", "<"):
        return value_number < expected_number
    if op in ("lte", "<="):
        return value_number <= expected_number
    if op == "abs_lte":
        return abs(value_number) <= expected_number
    if op == "abs_gte":
        return abs(value_number) >= expected_number
    raise RuntimeError(f"unsupported status rule op: {op}")


def evaluate_status_condition(condition, context):
    if not isinstance(condition, dict):
        return None
    if "all" in condition:
        return rule_all(*(evaluate_status_condition(item, context) for item in (condition.get("all") or [])))
    if "any" in condition:
        return rule_any(*(evaluate_status_condition(item, context) for item in (condition.get("any") or [])))
    if "not" in condition:
        return rule_not(evaluate_status_condition(condition.get("not"), context))
    metric = condition.get("metric")
    if not metric:
        return None
    value = resolve_status_metric(context, metric)
    return compare_status_metric(value, condition.get("op"), condition.get("value"))


def status_from_score(score, statuses, default_label):
    for item in statuses or []:
        label = item.get("label")
        if not label:
            continue
        min_value = item.get("min")
        max_value = item.get("max")
        if min_value is not None and score < float(min_value):
            continue
        if max_value is not None and score > float(max_value):
            continue
        return label
    return default_label


def build_status_context(payload):
    public_records = payload.get("public_records") or {}
    macro = public_records.get("macro") or {}
    macro_value = current_record_values(macro)
    etf = public_records.get("etf") or {}
    etf_rule_value = (etf.get("value") or {}) if record_is_current(etf) else {}
    funding = public_records.get("funding") or {}
    basis = public_records.get("basis") or {}
    spot_cvd = public_records.get("spot_cvd") or {}
    stablecoin = public_records.get("stablecoin") or {}
    delta4h = payload.get("delta4h") or {}
    checks = ((payload.get("structure_state") or {}).get("checks") or {})
    oi_slow = checks.get("oi_slow_confirmation") or {}
    leverage_state = (((payload.get("rule_state") or {}).get("leverage")) or {})
    buyback = macro_value.get("treasury_buyback") or {}
    return {
        "raw": {
            "market": payload.get("market") or {},
            "delta4h": delta4h,
            "macro": macro.get("value") or {},
            "etf": (etf.get("value") or {}),
            "funding": (funding.get("value") or {}),
            "basis": (basis.get("value") or {}),
            "spot_cvd": (spot_cvd.get("value") or {}),
            "stablecoin": (stablecoin.get("value") or {}),
            "structure_checks": checks,
        },
        "macro": {
            "us10y": safe_float(macro_value.get("us10y")),
            "real_yield_10y": safe_float(macro_value.get("real_yield_10y")),
            "m2_yoy_pct": safe_float(macro_value.get("m2_yoy_pct")),
            "m2_yoy_two_month_rising_rule": macro_value.get("m2_yoy_two_month_rising_rule"),
            "fed_balance_sheet_30d_pct": safe_float(macro_value.get("fed_balance_sheet_30d_pct")),
            "treasury_buyback_recent_count": safe_float(buyback.get("recent_count")),
            "treasury_buyback_total_par_accepted": safe_float(buyback.get("total_par_amt_accepted")),
        },
        "etf": {
            "flow_1d_usd": safe_float(etf_rule_value.get("flow_1d_usd")),
            "flow_5d_usd": safe_float(etf_rule_value.get("flow_5d_usd")),
            "flow_30d_usd": safe_float(etf_rule_value.get("flow_30d_usd")),
        },
        "leverage": {
            "oi_4h_pct": safe_float(delta4h.get("oi_4h_pct")),
            "oi_24h_pct": safe_float(oi_slow.get("delta_24h_pct")),
            "oi_72h_pct": safe_float(oi_slow.get("delta_72h_pct")),
            "status": leverage_state.get("status"),
            "oi_short_term_status": leverage_state.get("oi_short_term_status"),
            "oi_72h_context": leverage_state.get("oi_72h_context"),
            "funding_8h_pct": safe_float(current_record_value(funding, "funding_8h_pct")),
            "funding_24h_pct": safe_float(current_record_value(funding, "funding_24h_pct")),
            "basis_pct": safe_float(current_record_value(basis, "basis_pct")),
        },
        "spot": {
            "cvd_4h_pct": safe_float(current_record_value(spot_cvd, "cvd_4h_pct")),
            "cvd_24h_pct": safe_float(current_record_value(spot_cvd, "cvd_24h_pct")),
            "stablecoin_24h_usd": safe_float(current_record_value(stablecoin, "flow_24h_usd")),
            "stablecoin_7d_usd": safe_float(current_record_value(stablecoin, "flow_7d_usd")),
        },
    }


def evaluate_status_layer(layer, context):
    inputs = layer.get("inputs") or []
    input_values = {metric: resolve_status_metric(context, metric) for metric in inputs}
    known_count = sum(1 for value in input_values.values() if value is not None)
    min_required = int(layer.get("min_required") or 1)
    insufficient_label = layer.get("insufficient_label") or "数据不足"
    result = {
        "key": layer.get("key"),
        "label": layer.get("label"),
        "status": insufficient_label,
        "score": None,
        "known_count": known_count,
        "input_count": len(inputs),
        "inputs": input_values,
    }
    if known_count < min_required:
        return result

    for match_rule in layer.get("match_rules") or []:
        hit = evaluate_status_condition(match_rule.get("when") or match_rule, context)
        if hit is True:
            result["status"] = match_rule.get("label") or insufficient_label
            result["score"] = None
            return result

    score = float(layer.get("base_score") or 0)
    for score_rule in layer.get("score_rules") or []:
        hit = evaluate_status_condition(score_rule, context)
        if hit is True:
            score += float(score_rule.get("score") or 0)
    result["score"] = score
    result["status"] = status_from_score(score, layer.get("statuses") or [], insufficient_label)
    return result


def build_status_layer(payload):
    config = load_status_rules_config()
    context = build_status_context(payload)
    status_layer = {
        "schema": config.get("schema") or "btc_status_rules",
        "revision": config.get("revision"),
        "rules_file": STATUS_RULES_FILE,
        "items": [],
    }
    for layer in config.get("status_layers") or []:
        item = evaluate_status_layer(layer, context)
        status_layer[item["key"]] = item
        status_layer["items"].append(item)
    return status_layer


MARKET_PATTERN_STATE_TEXT = {
    "YES": "已出现",
    "NO": "未出现",
    "WAIT": "正在确认",
    "NA": "数据不足",
}


def finite_metric(value, low=None, high=None):
    number = safe_float(value)
    if number is None or not math.isfinite(number):
        return None
    if low is not None and number < float(low):
        return None
    if high is not None and number > float(high):
        return None
    return number


def record_is_current(record):
    return bool(
        record_has_data(record)
        and not (record or {}).get("stale")
        and not (record or {}).get("source_stale")
        and not (record or {}).get("snapshot_used")
        and (record or {}).get("status") not in ("unavailable", "error", "not_applicable")
        and (record or {}).get("analysis_eligible", True)
    )


def build_market_pattern_context(payload):
    quality = payload.get("signal_data_quality") or {}
    signal_usable = quality.get("usable") is True
    delta4h = payload.get("delta4h") or {}
    window_sec = int(delta4h.get("window_sec") or 0)
    window_aligned = 3 * 3600 + 45 * 60 <= window_sec <= 4 * 3600 + 15 * 60
    public_records = payload.get("public_records") or {}
    spot_record = public_records.get("spot_cvd") or {}
    funding_record = public_records.get("funding") or {}
    basis_record = public_records.get("basis") or {}
    liquidation_record = public_records.get("liquidation") or {}
    signal_as_of_ms = int(quality.get("latest_close_ms") or 0)

    spot_value = (spot_record.get("value") or {}) if record_is_current(spot_record) else {}
    spot_as_of_ms = int(spot_value.get("as_of_ms") or spot_record.get("source_as_of_ms") or 0)
    spot_aligned = bool(
        spot_value.get("contiguous_4h")
        and signal_as_of_ms
        and spot_as_of_ms
        and abs(signal_as_of_ms - spot_as_of_ms) <= 60 * 1000
    )
    if not spot_aligned:
        spot_value = {}

    funding_value = (funding_record.get("value") or {}) if record_is_current(funding_record) else {}
    basis_value = (basis_record.get("value") or {}) if record_is_current(basis_record) else {}
    liquidation_value = {}
    liquidation_coverage = (liquidation_record.get("coverage") or {})
    if (
        record_is_current(liquidation_record)
        and liquidation_coverage.get("healthy")
        and liquidation_coverage.get("ready_4h")
    ):
        liquidation_value = liquidation_record.get("value") or {}

    levels = payload.get("levels") or {}
    position_state = levels.get("position_state")
    in_range = (
        position_state in ("near_lower", "mid_range", "near_upper")
        if position_state is not None
        else None
    )
    metrics_available = signal_usable and window_aligned
    return {
        "meta": {
            "as_of_ms": signal_as_of_ms or None,
            "bucket": signal_as_of_ms // (3600 * 1000) if signal_as_of_ms else None,
            "signal_usable": signal_usable,
            "window_aligned": window_aligned,
            "spot_aligned": spot_aligned,
            "signal_source": quality.get("source") or "Binance USD-M BTCUSDT perpetual closed 1h bars",
            "spot_source": "Stock volume; no aggressor direction" if assets.current().equity else "Binance Spot BTCUSDT taker-flow proxy",
        },
        "price": {
            "change_4h_pct": finite_metric(delta4h.get("price_4h_pct"), -100.0, 100.0) if metrics_available else None,
        },
        "derivatives": {
            "oi_change_4h_pct": finite_metric(delta4h.get("oi_4h_pct"), -100.0, 100.0) if metrics_available else None,
            "oi_notional_change_4h_pct": finite_metric(delta4h.get("oi_notional_4h_pct"), -100.0, 100.0) if metrics_available else None,
            "volume_change_4h_pct": finite_metric(delta4h.get("vol_4h_pct"), -100.0) if metrics_available else None,
            "funding_8h_pct": finite_metric(funding_value.get("funding_8h_pct"), -10.0, 10.0),
            "basis_pct": finite_metric(basis_value.get("basis_pct"), -10.0, 10.0),
            "long_liquidation_4h_usd": finite_metric(liquidation_value.get("long_4h_usd"), 0.0),
            "short_liquidation_4h_usd": finite_metric(liquidation_value.get("short_4h_usd"), 0.0),
        },
        "spot": {
            "cvd_4h_pct": finite_metric(spot_value.get("cvd_4h_pct"), -100.0, 100.0),
            "volume_change_4h_pct": finite_metric(delta4h.get("vol_4h_pct") if assets.current().equity and metrics_available else spot_value.get("volume_4h_pct"), -100.0),
        },
        "structure": {
            "in_range": in_range if signal_usable else None,
        },
    }


def market_pattern_result(definition, state, evidence=None, reason=None, raw_hit=None):
    confirmed_bias = definition.get("surface_bias_on_confirmation")
    return {
        "key": definition.get("key"),
        "label": definition.get("label"),
        "category": definition.get("category"),
        "window": definition.get("window"),
        "source_scope": definition.get("source_scope"),
        "interpretation_limit": definition.get("interpretation_limit"),
        "surface_bias_on_confirmation": confirmed_bias,
        "surface_bias_horizon": definition.get("surface_bias_horizon"),
        "surface_bias": confirmed_bias if state == "YES" else None,
        "is_forecast": False,
        "required_inputs": definition.get("required_all") or [],
        "criteria": definition.get("when") or {},
        "state": state,
        "status": MARKET_PATTERN_STATE_TEXT.get(state, "数据不足"),
        "raw_hit": raw_hit,
        "evidence": evidence or {},
        "reason": reason,
    }


def evaluate_snapshot_market_pattern(definition, context):
    required = definition.get("required_all") or []
    values = {path: resolve_status_metric(context, path) for path in required}
    missing = [path for path, value in values.items() if value is None]
    if missing:
        return market_pattern_result(
            definition,
            "NA",
            evidence=values,
            reason="missing_or_unaligned:" + ",".join(missing),
            raw_hit=None,
        )
    hit = evaluate_status_condition(definition.get("when") or {}, context)
    if hit is None:
        return market_pattern_result(definition, "NA", evidence=values, reason="condition_unknown", raw_hit=None)
    return market_pattern_result(
        definition,
        "YES" if hit else "NO",
        evidence=values,
        raw_hit=bool(hit),
    )


def market_pattern_volume_ratio(bars, window_h=4, baseline_h=24):
    required = int(window_h) + int(baseline_h)
    recent = list(bars or [])[-required:]
    if len(recent) < required or not bars_are_contiguous(recent):
        return None
    current = [finite_metric(bar_volume(item), 0.0) for item in recent[-int(window_h):]]
    baseline = [finite_metric(bar_volume(item), 0.0) for item in recent[:-int(window_h)]]
    if any(value is None for value in current + baseline):
        return None
    base = median(baseline)
    return (sum(current) / len(current)) / base if base else None


def spot_flow_metrics_at_bucket(hourly_flow, end_bucket, window_h=4):
    by_bucket = {}
    for item in hourly_flow or []:
        close_time = int((item or {}).get("close_time") or 0)
        quote_volume = finite_metric((item or {}).get("quote_volume"), 0.0)
        net_quote = finite_metric((item or {}).get("net_quote"))
        if close_time <= 0 or quote_volume is None or quote_volume <= 0 or net_quote is None:
            continue
        by_bucket[close_time // (3600 * 1000)] = {
            "quote_volume": quote_volume,
            "net_quote": net_quote,
        }

    end_bucket = int(end_bucket or 0)
    window_h = max(1, int(window_h))
    required_buckets = list(range(end_bucket - window_h * 2 + 1, end_bucket + 1))
    if any(bucket not in by_bucket for bucket in required_buckets):
        return None
    previous = [by_bucket[bucket] for bucket in required_buckets[:window_h]]
    current = [by_bucket[bucket] for bucket in required_buckets[window_h:]]
    current_volume = sum(item["quote_volume"] for item in current)
    previous_volume = sum(item["quote_volume"] for item in previous)
    if current_volume <= 0 or previous_volume <= 0:
        return None
    return {
        "cvd_pct": sum(item["net_quote"] for item in current) / current_volume * 100.0,
        "volume_change_pct": pct(current_volume, previous_volume),
    }


def spot_flow_segment_metrics(hourly_flow, start_bucket, end_bucket):
    by_bucket = {}
    for item in hourly_flow or []:
        close_time = int((item or {}).get("close_time") or 0)
        quote_volume = finite_metric((item or {}).get("quote_volume"), 0.0)
        net_quote = finite_metric((item or {}).get("net_quote"))
        if close_time <= 0 or quote_volume is None or quote_volume <= 0 or net_quote is None:
            continue
        by_bucket[close_time // (3600 * 1000)] = {
            "quote_volume": quote_volume,
            "net_quote": net_quote,
        }

    buckets = list(range(int(start_bucket), int(end_bucket) + 1))
    if not buckets or any(bucket not in by_bucket for bucket in buckets):
        return None
    rows = [by_bucket[bucket] for bucket in buckets]
    total = sum(item["quote_volume"] for item in rows)
    if total <= 0:
        return None
    return {
        "cvd_pct": sum(item["net_quote"] for item in rows) / total * 100.0,
        "hourly_pct": [item["net_quote"] / item["quote_volume"] * 100.0 for item in rows],
    }


def evaluate_resistance_stall_market_pattern(signal_bars, spot_record, signal_quality, definition, config):
    if not definition:
        return None
    if not signal_quality or signal_quality.get("usable") is not True:
        return market_pattern_result(
            definition,
            "NA",
            evidence={"quality_reasons": list((signal_quality or {}).get("reasons") or [])},
            reason="signal_data_unusable",
        )

    spot_value = (spot_record.get("value") or {}) if record_is_current(spot_record) else {}
    hourly_flow = spot_value.get("hourly_flow") or []
    if len(hourly_flow) < 8:
        return market_pattern_result(definition, "NA", reason="spot_hourly_flow_unavailable")

    bars = sorted(
        [item for item in (signal_bars or []) if int(item.get("open_time") or 0) > 0],
        key=lambda item: int(item.get("open_time") or 0),
    )
    confirm_hours = max(1, int(config.get("resistance_stall_confirm_hours") or 2))
    scan_hours = max(confirm_hours + 1, int(config.get("resistance_stall_scan_hours") or 6))
    prefix_required = assets.current().parameter("MARKET_STRUCTURE_SHORT_LOOKBACK_H", MARKET_STRUCTURE_SHORT_LOOKBACK_H)
    required_history = prefix_required + scan_hours + confirm_hours
    bars = bars[-required_history:]
    if len(bars) < prefix_required + confirm_hours + 1 or not bars_are_contiguous(bars):
        return market_pattern_result(definition, "NA", reason="structure_history_unavailable")

    max_progress_pct = float(config.get("resistance_stall_max_progress_pct") or 0.15)
    min_spot_cvd_pct = float(config.get("resistance_stall_min_spot_cvd_pct") or 1.0)
    min_spot_volume_change_pct = float(
        config.get("resistance_stall_min_spot_volume_change_pct") or 30.0
    )
    latest_index = len(bars) - 1
    latest_bucket = int(bars[-1].get("close_time") or 0) // (3600 * 1000)
    events = []
    candidates = []
    missing_hourly_flow = False
    start_index = max(prefix_required, len(bars) - scan_hours - confirm_hours)

    for index in range(start_index, len(bars)):
        prefix = bars[:index]
        bar = bars[index]
        resistance = calc_resistance_level(prefix, assets.current().parameter("MARKET_STRUCTURE_SHORT_LOOKBACK_H", MARKET_STRUCTURE_SHORT_LOOKBACK_H))
        previous_close = finite_metric(prefix[-1].get("close")) if prefix else None
        high = finite_metric(bar.get("high"))
        close = finite_metric(bar.get("close"))
        if None in (resistance, previous_close, high, close):
            continue
        atr = calc_atr(prefix)
        buffer_value = max(
            abs(previous_close) * assets.current().parameter("STRUCTURE_BREAKOUT_BUFFER_PCT", STRUCTURE_BREAKOUT_BUFFER_PCT),
            (atr or 0.0) * assets.current().parameter("STRUCTURE_BREAKOUT_BUFFER_ATR", STRUCTURE_BREAKOUT_BUFFER_ATR),
        )
        threshold = resistance + buffer_value
        if previous_close > resistance or high < resistance or close > threshold:
            continue

        touch_bucket = int(bar.get("close_time") or 0) // (3600 * 1000)
        confirm_index = index + confirm_hours
        evaluation_index = min(confirm_index, latest_index)
        evaluation_bar = bars[evaluation_index]
        evaluation_bucket = int(evaluation_bar.get("close_time") or 0) // (3600 * 1000)
        segment = bars[index:evaluation_index + 1]
        if any(finite_metric(item.get("close")) is None for item in segment):
            continue
        if any(float(item.get("close")) > threshold for item in segment):
            continue

        volume_ratio = market_pattern_volume_ratio(bars[:evaluation_index + 1])
        spot_segment = spot_flow_segment_metrics(hourly_flow, touch_bucket, evaluation_bucket)
        if volume_ratio is None:
            continue
        if spot_segment is None:
            missing_hourly_flow = True
            continue
        price_progress_pct = max(0.0, pct(evaluation_bar.get("close"), close) or 0.0)
        evidence = {
            "anchor": resistance,
            "threshold": threshold,
            "trigger_high": high,
            "touch_close": close,
            "start_bucket": touch_bucket,
            "confirmation_hours": confirm_hours,
            "price_progress_pct": price_progress_pct,
            "futures_volume_ratio": volume_ratio,
            "post_touch_spot_cvd_pct": spot_segment.get("cvd_pct"),
            "spot_hourly_flow_pct": spot_segment.get("hourly_pct") or [],
        }
        base_hit = bool(
            volume_ratio >= assets.current().parameter("STRUCTURE_VOLUME_FAST_MULTIPLIER", STRUCTURE_VOLUME_FAST_MULTIPLIER)
            and spot_segment.get("cvd_pct") is not None
            and spot_segment.get("cvd_pct") >= min_spot_cvd_pct
            and price_progress_pct <= max_progress_pct
        )
        if not base_hit:
            continue

        if confirm_index > latest_index:
            candidates.append({
                **evidence,
                "current_bucket": latest_bucket,
                "remaining_confirm_bars": confirm_index - latest_index,
            })
            continue

        spot_window = spot_flow_metrics_at_bucket(hourly_flow, evaluation_bucket)
        if spot_window is None:
            missing_hourly_flow = True
            continue
        if (
            spot_window.get("cvd_pct") is None
            or spot_window.get("cvd_pct") < min_spot_cvd_pct
            or spot_window.get("volume_change_pct") is None
            or spot_window.get("volume_change_pct") < min_spot_volume_change_pct
        ):
            continue

        above_threshold = 0
        invalidated = False
        for later_bar in bars[confirm_index + 1:]:
            later_close = finite_metric(later_bar.get("close"))
            above_threshold = above_threshold + 1 if later_close is not None and later_close > threshold else 0
            if above_threshold >= 2:
                invalidated = True
                break
        if invalidated:
            continue
        events.append({
            **evidence,
            "confirmed_bucket": evaluation_bucket,
            "confirmed_close": finite_metric(evaluation_bar.get("close")),
            "spot_cvd_4h_pct": spot_window.get("cvd_pct"),
            "spot_volume_change_4h_pct": spot_window.get("volume_change_pct"),
        })

    events.sort(key=lambda item: (int(item.get("confirmed_bucket") or 0), int(item.get("start_bucket") or 0)))
    deduplicated_events = []
    for event in events:
        # 同一次触碰后的滚动窗口可能生成重叠候选，只保留最早确认的事件收据。
        if (
            deduplicated_events
            and int(event.get("start_bucket") or 0)
            <= int(deduplicated_events[-1].get("confirmed_bucket") or 0)
        ):
            continue
        deduplicated_events.append(event)
    active_hours = int(config.get("event_active_hours") or 4)
    active_events = [
        event for event in deduplicated_events
        if 0 <= latest_bucket - int(event.get("confirmed_bucket") or 0) < active_hours
    ]
    if active_events:
        return market_pattern_result(definition, "YES", evidence=active_events[-1], raw_hit=True)
    if candidates:
        candidates.sort(key=lambda item: int(item.get("start_bucket") or 0))
        return market_pattern_result(definition, "WAIT", evidence=candidates[0], raw_hit=False)
    if missing_hourly_flow:
        return market_pattern_result(definition, "NA", reason="spot_hourly_flow_unavailable")
    return market_pattern_result(definition, "NO", raw_hit=False)


def evaluate_structural_market_patterns(signal_bars, signal_quality, definitions, config):
    by_kind = {item.get("kind"): item for item in definitions or []}
    kinds = ("support_sweep", "false_breakout", "volume_breakout")
    quality_evidence = {
        "quality_reasons": list((signal_quality or {}).get("reasons") or [])
    }
    unavailable = {
        kind: market_pattern_result(
            by_kind[kind],
            "NA",
            evidence=quality_evidence,
            reason="signal_data_unusable",
        )
        for kind in kinds
        if kind in by_kind
    }
    if not signal_quality or signal_quality.get("usable") is not True:
        return unavailable

    bars = sorted(
        [item for item in (signal_bars or []) if int(item.get("open_time") or 0) > 0],
        key=lambda item: int(item.get("open_time") or 0),
    )
    scan_hours = int(config.get("structure_scan_hours") or 12)
    prefix_required = max(assets.current().parameter("MARKET_STRUCTURE_SHORT_LOOKBACK_H", MARKET_STRUCTURE_SHORT_LOOKBACK_H), assets.current().parameter("MARKET_STRUCTURE_NEAR_SUPPORT_LOOKBACK_H", MARKET_STRUCTURE_NEAR_SUPPORT_LOOKBACK_H), 15)
    required = prefix_required + scan_hours + 1
    recent = bars[-required:]
    if len(recent) < required or not bars_are_contiguous(recent):
        return {
            kind: market_pattern_result(by_kind[kind], "NA", reason="structure_history_unavailable")
            for kind in kinds
            if kind in by_kind
        }

    event_active_hours = int(config.get("event_active_hours") or 4)
    support_timeout = int(config.get("support_candidate_timeout_hours") or 3)
    pressure_timeout = int(config.get("pressure_candidate_timeout_hours") or 2)
    support_candidates = []
    pressure_candidates = []
    breakout_candidates = []
    support_events = []
    pressure_events = []
    breakout_events = []

    for index in range(prefix_required, len(recent)):
        prefix = recent[:index]
        bar = recent[index]
        bucket = int(bar.get("close_time") or 0) // (3600 * 1000)
        high = finite_metric(bar.get("high"))
        low = finite_metric(bar.get("low"))
        close = finite_metric(bar.get("close"))
        previous_close = finite_metric(prefix[-1].get("close")) if prefix else None
        if None in (high, low, close, previous_close):
            continue

        still_waiting = []
        for candidate in support_candidates:
            if bucket > candidate["expires_bucket"]:
                continue
            if close >= candidate["anchor"]:
                support_events.append({**candidate, "confirmed_bucket": bucket, "confirmed_close": close})
            elif bucket < candidate["expires_bucket"]:
                still_waiting.append(candidate)
        support_candidates = still_waiting

        still_waiting = []
        for candidate in pressure_candidates:
            if bucket > candidate["expires_bucket"]:
                continue
            if close <= candidate["anchor"]:
                pressure_events.append({**candidate, "confirmed_bucket": bucket, "confirmed_close": close})
            elif bucket < candidate["expires_bucket"]:
                still_waiting.append(candidate)
        pressure_candidates = still_waiting

        still_waiting = []
        for candidate in breakout_candidates:
            if bucket > candidate["expires_bucket"]:
                continue
            if close <= candidate["anchor"]:
                continue
            if bucket > candidate["start_bucket"] and close > candidate["threshold"]:
                breakout_events.append({**candidate, "confirmed_bucket": bucket, "confirmed_close": close})
            elif bucket < candidate["expires_bucket"]:
                still_waiting.append(candidate)
        breakout_candidates = still_waiting

        support = calc_near_term_support_level(
            prefix,
            assets.current().parameter("MARKET_STRUCTURE_NEAR_SUPPORT_LOOKBACK_H", MARKET_STRUCTURE_NEAR_SUPPORT_LOOKBACK_H),
            assets.current().parameter("MARKET_STRUCTURE_NEAR_SUPPORT_FALLBACK_H", MARKET_STRUCTURE_NEAR_SUPPORT_FALLBACK_H),
        )
        resistance = calc_resistance_level(prefix, assets.current().parameter("MARKET_STRUCTURE_SHORT_LOOKBACK_H", MARKET_STRUCTURE_SHORT_LOOKBACK_H))
        atr = calc_atr(prefix)
        reference = previous_close or close
        buffer_value = max(reference * assets.current().parameter("STRUCTURE_BREAKOUT_BUFFER_PCT", STRUCTURE_BREAKOUT_BUFFER_PCT), (atr or 0.0) * assets.current().parameter("STRUCTURE_BREAKOUT_BUFFER_ATR", STRUCTURE_BREAKOUT_BUFFER_ATR))

        if support is not None and low < support - buffer_value:
            candidate = {
                "anchor": support,
                "threshold": support - buffer_value,
                "start_bucket": bucket,
                "expires_bucket": bucket + support_timeout,
                "trigger_low": low,
            }
            if close >= support:
                support_events.append({**candidate, "confirmed_bucket": bucket, "confirmed_close": close})
            else:
                support_candidates.append(candidate)

        if resistance is not None and high > resistance + buffer_value:
            candidate = {
                "anchor": resistance,
                "threshold": resistance + buffer_value,
                "start_bucket": bucket,
                "expires_bucket": bucket + pressure_timeout,
                "trigger_high": high,
            }
            if close <= resistance:
                pressure_events.append({**candidate, "confirmed_bucket": bucket, "confirmed_close": close})
            else:
                pressure_candidates.append(candidate)

        if resistance is not None and previous_close <= resistance + buffer_value and close > resistance + buffer_value:
            volume_ratio = market_pattern_volume_ratio(recent[:index + 1])
            if volume_ratio is not None and volume_ratio >= assets.current().parameter("STRUCTURE_VOLUME_FAST_MULTIPLIER", STRUCTURE_VOLUME_FAST_MULTIPLIER):
                breakout_candidates.append({
                    "anchor": resistance,
                    "threshold": resistance + buffer_value,
                    "start_bucket": bucket,
                    "expires_bucket": bucket + pressure_timeout,
                    "trigger_close": close,
                    "volume_ratio": volume_ratio,
                })

    latest_bucket = int(recent[-1].get("close_time") or 0) // (3600 * 1000)

    def structural_result(kind, events, candidates):
        definition = by_kind.get(kind)
        if not definition:
            return None
        active_events = [
            event for event in events
            if 0 <= latest_bucket - int(event.get("confirmed_bucket") or 0) < event_active_hours
        ]
        if active_events:
            event = active_events[-1]
            return market_pattern_result(definition, "YES", evidence=event, raw_hit=True)
        active_candidates = [
            candidate for candidate in candidates
            if latest_bucket < int(candidate.get("expires_bucket") or 0)
        ]
        if active_candidates:
            candidate = active_candidates[-1]
            evidence = {
                **candidate,
                "current_bucket": latest_bucket,
                "remaining_confirm_bars": max(
                    0,
                    int(candidate.get("expires_bucket") or 0) - latest_bucket,
                ),
            }
            return market_pattern_result(definition, "WAIT", evidence=evidence, raw_hit=False)
        return market_pattern_result(definition, "NO", raw_hit=False)

    return {
        "support_sweep": structural_result("support_sweep", support_events, support_candidates),
        "false_breakout": structural_result("false_breakout", pressure_events, pressure_candidates),
        "volume_breakout": structural_result("volume_breakout", breakout_events, breakout_candidates),
    }


def apply_rolling_market_pattern(result, definition, history, bucket, config):
    required = int(config.get("range_confirm_hours") or 3)
    result = dict(result)
    result["required_consecutive"] = required
    if result.get("state") == "NA" or result.get("raw_hit") is not True:
        return result
    consecutive = 1
    expected_bucket = int(bucket or 0) - 1
    for entry in reversed(history or []):
        entry_bucket = int(entry.get("bucket") or 0)
        if entry_bucket >= int(bucket or 0):
            continue
        if entry_bucket != expected_bucket:
            break
        if ((entry.get("raw_hits") or {}).get(definition.get("key"))) is not True:
            break
        consecutive += 1
        expected_bucket -= 1
    result["consecutive_hits"] = consecutive
    if consecutive < required:
        result["state"] = "WAIT"
        result["status"] = MARKET_PATTERN_STATE_TEXT["WAIT"]
        result["surface_bias"] = None
    return result


def market_pattern_metric_snapshot(context):
    return {
        "price_change_4h_pct": safe_float((context.get("price") or {}).get("change_4h_pct")),
        "spot_cvd_4h_pct": safe_float((context.get("spot") or {}).get("cvd_4h_pct")),
        "derivatives_volume_change_4h_pct": safe_float(
            (context.get("derivatives") or {}).get("volume_change_4h_pct")
        ),
        "spot_volume_change_4h_pct": safe_float(
            (context.get("spot") or {}).get("volume_change_4h_pct")
        ),
        "spot_aligned": (context.get("meta") or {}).get("spot_aligned") is True,
    }


def build_spot_sell_pressure_change(history, bucket, context):
    current_bucket = int(bucket or 0)
    current_metrics = market_pattern_metric_snapshot(context)
    current_cvd = safe_float(current_metrics.get("spot_cvd_4h_pct"))
    result = {
        "available": False,
        "easing_rule": None,
        "current_cvd_4h_pct": current_cvd,
        "previous_cvd_4h_pct": None,
        "previous_bucket": None,
        "bucket_gap": None,
    }
    if (
        current_bucket <= 0
        or current_cvd is None
        or current_metrics.get("spot_aligned") is not True
    ):
        return result
    for entry in reversed(history or []):
        previous_bucket = int(entry.get("bucket") or 0)
        if previous_bucket >= current_bucket:
            continue
        bucket_gap = current_bucket - previous_bucket
        if bucket_gap > 2:
            break
        previous_metrics = entry.get("metrics") or {}
        previous_cvd = safe_float(previous_metrics.get("spot_cvd_4h_pct"))
        if previous_cvd is None or previous_metrics.get("spot_aligned") is not True:
            continue
        comparable = current_cvd < 0.0 and previous_cvd < 0.0
        return {
            "available": comparable,
            "easing_rule": (
                abs(current_cvd) < abs(previous_cvd)
                if comparable
                else None
            ),
            "current_cvd_4h_pct": current_cvd,
            "previous_cvd_4h_pct": previous_cvd,
            "previous_bucket": previous_bucket,
            "bucket_gap": bucket_gap,
        }
    return result


def build_market_patterns(payload, signal_bars, state):
    config = load_status_rules_config().get("market_patterns") or {}
    definitions = config.get("patterns") or []
    context = build_market_pattern_context(payload)
    bucket = ((context.get("meta") or {}).get("bucket"))
    history = list(state.get(MARKET_PATTERN_HISTORY_KEY) or [])
    sell_pressure_change = build_spot_sell_pressure_change(history, bucket, context)
    structural = evaluate_structural_market_patterns(
        signal_bars,
        payload.get("signal_data_quality") or {},
        definitions,
        config,
    )
    stall_definition = next(
        (item for item in definitions if item.get("kind") == "resistance_stall"),
        None,
    )
    stall_result = evaluate_resistance_stall_market_pattern(
        signal_bars,
        ((payload.get("public_records") or {}).get("spot_cvd") or {}),
        payload.get("signal_data_quality") or {},
        stall_definition,
        config,
    )
    if stall_result is not None:
        structural["resistance_stall"] = stall_result
    items = []
    for definition in definitions:
        kind = definition.get("kind")
        if kind in structural:
            item = structural.get(kind)
        else:
            item = evaluate_snapshot_market_pattern(definition, context)
            if kind == "rolling":
                item = apply_rolling_market_pattern(item, definition, history, bucket, config)
        items.append(item)

    raw_hits = {item.get("key"): item.get("raw_hit") for item in items}
    states = {item.get("key"): item.get("state") for item in items}
    if bucket is not None and (not assets.current().equity or (payload.get("signal_data_quality") or {}).get("natural_window_current", True)):
        entry = {
            "bucket": int(bucket),
            "as_of_ms": (context.get("meta") or {}).get("as_of_ms"),
            "raw_hits": raw_hits,
            "states": states,
            "metrics": market_pattern_metric_snapshot(context),
        }
        history = [item for item in history if int(item.get("bucket") or 0) != int(bucket)]
        history.append(entry)
        history.sort(key=lambda item: int(item.get("bucket") or 0))
        history = history[-max(MARKET_PATTERN_HISTORY_MAX, int(config.get("history_hours") or 12)):]
        state[MARKET_PATTERN_HISTORY_KEY] = history

    result = {
        "schema": config.get("schema") or "btc_market_patterns",
        "revision": config.get("revision"),
        "groups": config.get("groups") or [],
        "interpretation_note": "表面方向只描述已确认形态发生时的表现，不证明原因，也不预测下一步。",
        "as_of_ms": (context.get("meta") or {}).get("as_of_ms"),
        "items": items,
        "data_quality": context.get("meta") or {},
        "spot_sell_pressure_change": sell_pressure_change,
        "recent_history": history[-int(config.get("history_hours") or 12):],
    }
    for item in items:
        result[item.get("key")] = item
    return result


def fed_policy_cn(policy):
    if policy == "Hawkish":
        return "偏鹰"
    if policy == "Dovish":
        return "偏鸽"
    if policy == "Neutral":
        return "中性"
    return "未启用"


def classify_us10y_band(value):
    value = safe_float(value)
    if value is None:
        return "数据不足"
    if value < 3.0:
        return "极低"
    if value < 3.7:
        return "偏低"
    if value < 4.2:
        return "中性"
    if value <= 4.7:
        return "偏高"
    return "极高"


def directional_band_edge(value, direction_value, thresholds, decimals=2):
    text = compact_number(value, decimals)
    number = safe_float(value)
    direction = safe_float(direction_value)
    if number is None or direction is None or direction == 0:
        return text

    lower = None
    upper = None
    previous = None
    for threshold in thresholds:
        if number < threshold:
            lower = previous
            upper = threshold
            break
        previous = threshold
    else:
        lower = previous

    edge = None
    if direction > 0:
        edge = compact_number(upper, decimals) if upper is not None else f">{compact_number(lower, decimals)}"
    elif direction < 0:
        edge = compact_number(lower, decimals) if lower is not None else f"<{compact_number(upper, decimals)}"
    return f"{text}/{edge}" if edge else text


def format_us10y_directional_band_value(value, direction_value):
    return directional_band_edge(value, direction_value, [3.0, 3.7, 4.2, 4.7], 2)


def classify_real_yield_band(value):
    value = safe_float(value)
    if value is None:
        return "数据不足"
    if value < 1.0:
        return "极低"
    if value < 1.5:
        return "偏低"
    if value < 2.0:
        return "中性"
    if value <= 2.5:
        return "偏高"
    return "极高"


def format_real_yield_directional_band_value(value, direction_value):
    return directional_band_edge(value, direction_value, [1.0, 1.5, 2.0, 2.5], 2)


def classify_dxy_band(value):
    value = safe_float(value)
    if value is None:
        return "数据不足"
    if value < 96.0:
        return "偏弱"
    if value < 100.0:
        return "中性偏弱"
    if value < 103.0:
        return "中性偏强"
    if value <= 106.0:
        return "强势"
    return "极强"


def format_dxy_directional_band_value(value, direction_value):
    return directional_band_edge(value, direction_value, [96.0, 100.0, 103.0, 106.0], 2)


def build_macro_environment_state(value):
    real_pressure = rule_ge(value.get("real_yield_10y"), 2.0)
    us10y_pressure = rule_ge(value.get("us10y"), 4.2)
    high_real_rate_pressure = rule_all(real_pressure, us10y_pressure)
    dxy_30d_strong = rule_gt(value.get("usd_index_30d_pct"), 1.5)
    high_real_and_usd_pressure = rule_all(real_pressure, dxy_30d_strong)
    nominal_down_real_high = rule_all(rule_lt(value.get("us10y_30d_pp"), 0.0), real_pressure)
    dxy_strong = rule_gt(value.get("usd_index"), 103.0)
    dxy_stage_strength = rule_all(
        rule_ge(value.get("usd_index"), 100.0),
        rule_le(value.get("usd_index"), 103.0),
        rule_gt(value.get("usd_index_30d_pct"), 2.0),
    )
    pressure_easing = rule_all(rule_lt(value.get("real_yield_10y"), 1.5), rule_lt(value.get("usd_index"), 100.0))
    pressure_cleared = rule_lt(value.get("real_yield_10y"), 1.0)

    if pressure_cleared is True:
        status = "pressure_cleared"
        status_text = "高利率压制明显解除。"
    elif high_real_and_usd_pressure is True:
        status = "high_real_and_usd_pressure"
        status_text = "高真实利率压制仍在，美元阶段性走强。"
    elif high_real_rate_pressure is True:
        status = "high_real_rate_pressure"
        status_text = "高真实利率压制仍在。"
    elif nominal_down_real_high is True:
        status = "easing_but_real_pressure"
        status_text = "名义利率压力有所缓和，但真实利率压制并未解除。"
    elif dxy_strong is True or dxy_stage_strength is True:
        status = "usd_pressure"
        status_text = "美元走强压制仍在。"
    elif pressure_easing is True:
        status = "pressure_easing"
        status_text = "压制环境有所缓和。"
    elif rule_known_count([real_pressure, us10y_pressure, dxy_30d_strong, dxy_strong]) == 0:
        status = "unknown"
        status_text = "宏观压制数据不足。"
    else:
        status = "neutral"
        status_text = "现实压制环境未出现单一强主导。"

    return {
        "status": status,
        "status_text": status_text,
        "real_pressure_rule": real_pressure,
        "us10y_pressure_rule": us10y_pressure,
        "high_real_rate_pressure_rule": high_real_rate_pressure,
        "high_real_and_usd_pressure_rule": high_real_and_usd_pressure,
        "nominal_down_real_high_rule": nominal_down_real_high,
        "dxy_strong_rule": dxy_strong,
        "dxy_stage_strength_rule": dxy_stage_strength,
        "pressure_easing_rule": pressure_easing,
        "pressure_cleared_rule": pressure_cleared,
        "bands": {
            "us10y": classify_us10y_band(value.get("us10y")),
            "real_yield": classify_real_yield_band(value.get("real_yield_10y")),
            "dxy": classify_dxy_band(value.get("usd_index")),
        },
    }


def build_long_term_money_state(value):
    fiscal = value.get("fiscal_proxy") or {}
    buyback = value.get("treasury_buyback") or {}
    fiscal_status = fiscal.get("status")
    interest_ratio = safe_float(fiscal.get("interest_revenue_ratio_pct"))
    m2_yoy = safe_float(value.get("m2_yoy_pct"))
    fed_bs_30d = safe_float(value.get("fed_balance_sheet_30d_pct"))
    real_yield = safe_float(value.get("real_yield_10y"))
    real_30d = safe_float(value.get("real_yield_10y_30d_pp"))

    fiscal_pressure = (
        True if fiscal_status in ("stressed", "fiscal_dominance")
        else False if fiscal_status == "normal"
        else None
    )
    debt_cost_high = rule_ge(interest_ratio, FISCAL_INTEREST_REVENUE_DOMINANCE_PCT)
    debt_cost_stressed = rule_ge(interest_ratio, FISCAL_INTEREST_REVENUE_STRESS_PCT)
    m2_expansion = rule_gt(m2_yoy, 3.0)
    m2_improving = value.get("m2_yoy_two_month_rising_rule")
    fed_balance_sheet_expansion = rule_gt(fed_bs_30d, 0.0)
    buyback_exists = buyback.get("recent_exists_rule") if buyback else None
    buyback_effective = rule_ge(buyback.get("total_par_amt_accepted") if buyback else None, TREASURY_BUYBACK_EFFECTIVE_USD)
    debasement_expectation = rule_all(fiscal_pressure, rule_any(debt_cost_high, debt_cost_stressed))
    liquidity_conversion = rule_any(m2_expansion, fed_balance_sheet_expansion)
    real_pressure_active = rule_ge(real_yield, 2.0)
    real_pressure_easing = rule_any(rule_lt(real_yield, 1.5), rule_le(real_30d, -MACRO_REAL_YIELD_UP_30D_PP))

    if rule_all(liquidity_conversion, real_pressure_easing) is True:
        status = "liquidity_conversion"
        status_text = "货币稀释开始转化为流动性。"
    elif rule_all(debasement_expectation, real_pressure_active) is True:
        status = "exists_not_dominant"
        status_text = "货币稀释逻辑存在，但高真实利率仍主导。"
    elif debasement_expectation is True:
        status = "expectation_exists"
        status_text = "货币稀释预期存在，但尚未确认成为市场主导。"
    elif rule_known_count([fiscal_pressure, debt_cost_stressed, m2_expansion, fed_balance_sheet_expansion]) == 0:
        status = "unknown"
        status_text = "长期货币层数据不足，维持观察。"
    else:
        status = "watching"
        status_text = "长期货币稀释逻辑观察中，尚未确认主导。"

    return {
        "status": status,
        "status_text": status_text,
        "fiscal_pressure_rule": fiscal_pressure,
        "debt_cost_high_rule": debt_cost_high,
        "debt_cost_stressed_rule": debt_cost_stressed,
        "m2_expansion_rule": m2_expansion,
        "m2_improving_rule": m2_improving,
        "fed_balance_sheet_expansion_rule": fed_balance_sheet_expansion,
        "buyback_exists_rule": buyback_exists,
        "buyback_effective_rule": buyback_effective,
        "debasement_expectation_rule": debasement_expectation,
        "liquidity_conversion_rule": liquidity_conversion,
        "real_pressure_active_rule": real_pressure_active,
        "real_pressure_easing_rule": real_pressure_easing,
    }


def build_etf_rule_state(payload, state):
    public_records = payload.get("public_records") or {}
    selected_etf = public_records.get("etf") or {}
    current_snapshot = payload.get("public_snapshot") or {}
    current_ts = int(current_snapshot.get("ts") or 0)
    current_etf = ((current_snapshot.get("public") or {}).get("etf") or {})
    etf = selected_etf if record_has_data(selected_etf) else current_etf

    holdings_current = record_is_current(etf)
    raw_value = etf.get("value") or {}
    eligible_for_rules = holdings_current and any(
        safe_float(raw_value.get(key)) is not None for key in ("flow_1d_usd", "flow_5d_usd"))
    source_dates = raw_value.get("source_date") or etf.get("source_date") or {}
    if isinstance(source_dates, dict):
        source_date = (
            source_dates.get("flow_1d")
            or source_dates.get("flow_30d")
            or source_dates.get("holdings")
        )
    else:
        source_date = source_dates
    ineligible_reason = None
    if not eligible_for_rules:
        if holdings_current or not record_has_data(etf):
            ineligible_reason = "missing"
        else:
            ineligible_reason = (
                etf.get("source_stale_reason")
                or etf.get("stale_reason")
                or ("source_stale" if etf.get("source_stale") else "stale_snapshot")
            )

    consistency = {
        "flow_5d_inconsistent": False,
        "current_flow_5d_usd": None,
        "previous_flow_5d_usd": None,
        "diff_usd": None,
        "relative_change": None,
    }
    current_flow_5d = (
        safe_float(record_value(current_etf, "flow_5d_usd"))
        if record_is_current(current_etf)
        else None
    )
    previous = find_previous_public_record(state, "etf", current_ts)
    previous_record = (previous or {}).get("record") or {}
    previous_flow_5d = safe_float(record_value(previous_record, "flow_5d_usd"))
    current_source = etf.get("flow_record") or {}
    previous_source = previous_record.get("flow_record") or {}
    if btc_etf_flow.is_flow_source(current_source) and (
            not btc_etf_flow.is_flow_source(previous_source)
            or current_source.get("venue") != previous_source.get("venue")
            or current_source.get("source_date") != previous_source.get("source_date")):
        previous_flow_5d = None  # Different publishers/daily rolling windows are not comparable revisions.
    if current_flow_5d is not None and previous_flow_5d is not None:
        diff = abs(current_flow_5d - previous_flow_5d)
        denominator = max(abs(previous_flow_5d), 1.0)
        relative_change = diff / denominator
        consistency.update({
            "current_flow_5d_usd": current_flow_5d,
            "previous_flow_5d_usd": previous_flow_5d,
            "diff_usd": diff,
            "relative_change": relative_change,
            "flow_5d_inconsistent": (
                diff >= ETF_FLOW_5D_INCONSISTENT_MIN_USD
                and relative_change >= ETF_FLOW_5D_INCONSISTENT_REL
            ),
        })

    raw_flow_1d = safe_float(raw_value.get("flow_1d_usd"))
    raw_flow_5d = safe_float(raw_value.get("flow_5d_usd"))
    raw_flow_30d = safe_float(raw_value.get("flow_30d_usd"))
    raw_holding_7d = safe_float(raw_value.get("holding_7d_pct"))
    flow_1d = raw_flow_1d if eligible_for_rules else None
    flow_5d = raw_flow_5d if eligible_for_rules else None
    flow_30d = raw_flow_30d if eligible_for_rules else None
    holding_7d = raw_holding_7d if holdings_current else None
    holding_stable = None if holding_7d is None else abs(holding_7d) <= 1.0
    flow_5d_allowed = flow_5d is not None and not consistency["flow_5d_inconsistent"]
    if flow_1d is not None:
        flow_outflow = flow_1d < 0
        flow_source = "1D"
    elif flow_5d_allowed:
        flow_outflow = flow_5d < 0
        flow_source = "5D"
    else:
        flow_outflow = None
        flow_source = None
    holding_not_deteriorating = False if holding_stable is False else True
    mild_outflow = rule_all(flow_outflow, holding_not_deteriorating)
    if not record_has_data(etf) or holdings_current and not eligible_for_rules:
        status = "data_insufficient"
    elif not eligible_for_rules:
        status = "data_stale"
    elif consistency["flow_5d_inconsistent"]:
        status = "data_inconsistent"
    elif mild_outflow is True:
        status = "mild_outflow"
    else:
        status = "neutral"
    return {
        "status": status,
        "eligible_for_rules": eligible_for_rules,
        "etf_rule_eligible": eligible_for_rules,
        "source_date": source_date,
        "ineligible_reason": ineligible_reason,
        "flow_source": flow_source,
        "flow_outflow_rule": flow_outflow,
        "mild_outflow_rule": mild_outflow,
        "holding_stable_rule": holding_stable,
        "flow_1d_usd": flow_1d,
        "flow_5d_usd": flow_5d,
        "flow_30d_usd": flow_30d,
        "holding_7d_pct": holding_7d,
        "raw_flow_1d_usd": raw_flow_1d,
        "raw_flow_5d_usd": raw_flow_5d,
        "raw_flow_30d_usd": raw_flow_30d,
        "raw_holding_7d_pct": raw_holding_7d,
        "consistency": consistency,
    }


def classify_oi_position(oi_4h, oi_24h, oi_72h):
    oi_4h = safe_float(oi_4h)
    oi_24h = safe_float(oi_24h)
    oi_72h = safe_float(oi_72h)

    contraction_hit = rule_any(
        rule_le(oi_4h, -assets.current().parameter("STRUCTURE_OI_FAST_MIN_DELTA_PCT", STRUCTURE_OI_FAST_MIN_DELTA_PCT)),
        rule_le(oi_24h, -assets.current().parameter("STRUCTURE_OI_SLOW_DAY_MIN_DELTA_PCT", STRUCTURE_OI_SLOW_DAY_MIN_DELTA_PCT)),
    )
    expansion_hit = rule_any(
        rule_ge(oi_4h, assets.current().parameter("STRUCTURE_OI_FAST_MIN_DELTA_PCT", STRUCTURE_OI_FAST_MIN_DELTA_PCT)),
        rule_ge(oi_24h, assets.current().parameter("STRUCTURE_OI_SLOW_DAY_MIN_DELTA_PCT", STRUCTURE_OI_SLOW_DAY_MIN_DELTA_PCT)),
    )
    short_data_sufficient = oi_4h is not None or oi_24h is not None
    if not short_data_sufficient:
        short_term_status = "data_insufficient"
    elif contraction_hit is True and expansion_hit is True:
        short_term_status = "mixed"
    elif contraction_hit is True:
        short_term_status = "contraction"
    elif expansion_hit is True:
        short_term_status = "expansion"
    else:
        short_term_status = "stable"

    if oi_72h is None:
        oi_72h_context = "data_insufficient"
    elif oi_72h > assets.current().parameter("STRUCTURE_OI_SLOW_TREND_MIN_DELTA_PCT", STRUCTURE_OI_SLOW_TREND_MIN_DELTA_PCT):
        oi_72h_context = "accumulating"
    elif oi_72h < -assets.current().parameter("STRUCTURE_OI_SLOW_TREND_MIN_DELTA_PCT", STRUCTURE_OI_SLOW_TREND_MIN_DELTA_PCT):
        oi_72h_context = "declining"
    else:
        oi_72h_context = "flat"

    flags_known = short_term_status != "data_insufficient"
    return {
        "oi_short_term_status": short_term_status,
        "oi_72h_context": oi_72h_context,
        "oi_contraction_rule": None if not flags_known else short_term_status == "contraction",
        "oi_expansion_rule": None if not flags_known else short_term_status == "expansion",
        "oi_mixed_rule": None if not flags_known else short_term_status == "mixed",
        "oi_72h_positive_rule": None if oi_72h is None else oi_72h_context == "accumulating",
        "oi_72h_negative_rule": None if oi_72h is None else oi_72h_context == "declining",
    }


def build_leverage_rule_state(payload):
    delta4h = payload.get("delta4h") or {}
    public_records = payload.get("public_records") or {}
    funding = public_records.get("funding") or {}
    basis = public_records.get("basis") or {}
    checks = ((payload.get("structure_state") or {}).get("checks") or {})
    oi_slow = checks.get("oi_slow_confirmation") or {}
    oi_4h = safe_float(delta4h.get("oi_4h_pct"))
    oi_24h = safe_float(oi_slow.get("delta_24h_pct"))
    oi_72h = safe_float(oi_slow.get("delta_72h_pct"))
    funding_8h = safe_float(current_record_value(funding, "funding_8h_pct"))
    funding_24h = safe_float(current_record_value(funding, "funding_24h_pct"))
    basis_pct = safe_float(current_record_value(basis, "basis_pct"))

    oi_position = classify_oi_position(oi_4h, oi_24h, oi_72h)
    oi_contraction = oi_position.get("oi_contraction_rule")
    oi_expansion = oi_position.get("oi_expansion_rule")
    funding_neutral = rule_all(
        rule_abs_le(funding_8h, assets.current().parameter("FUNDING_NEUTRAL_8H_ABS_PCT", FUNDING_NEUTRAL_8H_ABS_PCT)),
        rule_abs_le(funding_24h, assets.current().parameter("FUNDING_NEUTRAL_24H_ABS_PCT", FUNDING_NEUTRAL_24H_ABS_PCT)),
    )
    basis_neutral = rule_abs_le(basis_pct, assets.current().parameter("BASIS_NEUTRAL_ABS_PCT", BASIS_NEUTRAL_ABS_PCT))
    funding_basis_neutral = rule_all(funding_neutral, basis_neutral)
    funding_negative_extreme = rule_any(
        rule_le(funding_8h, assets.current().parameter("FUNDING_NEGATIVE_EXTREME_8H_PCT", FUNDING_NEGATIVE_EXTREME_8H_PCT)),
        rule_le(funding_24h, assets.current().parameter("FUNDING_NEGATIVE_EXTREME_24H_PCT", FUNDING_NEGATIVE_EXTREME_24H_PCT)),
    )
    deleveraging = rule_all(oi_contraction, funding_basis_neutral)
    short_attack = rule_all(oi_expansion, funding_negative_extreme)
    if short_attack is True:
        status = "short_attack"
    elif deleveraging is True:
        status = "deleveraging"
    elif oi_position.get("oi_mixed_rule") is True:
        status = "mixed"
    elif oi_position.get("oi_short_term_status") == "data_insufficient":
        status = "data_insufficient"
    else:
        status = "neutral"
    return {
        "status": status,
        **oi_position,
        "funding_neutral_rule": funding_neutral,
        "basis_neutral_rule": basis_neutral,
        "funding_basis_neutral_rule": funding_basis_neutral,
        "funding_negative_extreme_rule": funding_negative_extreme,
        "deleveraging_rule": deleveraging,
        "short_attack_rule": short_attack,
        "oi_4h_pct": oi_4h,
        "oi_24h_pct": oi_24h,
        "oi_72h_pct": oi_72h,
        "funding_8h_pct": funding_8h,
        "funding_24h_pct": funding_24h,
        "basis_pct": basis_pct,
    }


def build_spot_rule_state(payload):
    public_records = payload.get("public_records") or {}
    spot_cvd = public_records.get("spot_cvd") or {}
    stablecoin = public_records.get("stablecoin") or {}
    cvd_4h = safe_float(current_record_value(spot_cvd, "cvd_4h_pct"))
    cvd_24h = safe_float(current_record_value(spot_cvd, "cvd_24h_pct"))
    stable_24h = safe_float(current_record_value(stablecoin, "flow_24h_usd"))
    stable_7d = safe_float(current_record_value(stablecoin, "flow_7d_usd"))
    stable_deadzone = STABLECOIN_FLOW_DEADZONE_USD
    cvd_weak = rule_any(rule_lt(cvd_4h, 0.0), rule_lt(cvd_24h, 0.0))
    cvd_positive = rule_gt(cvd_4h, 0.0)
    stable_24h_positive = rule_gt(stable_24h, stable_deadzone)
    stable_7d_positive = rule_gt(stable_7d, stable_deadzone)
    stable_sustained = rule_all(stable_24h_positive, stable_7d_positive)
    if stable_24h_positive is True and stable_7d_positive is False:
        stable_short_term = True
    elif stable_24h_positive is None or stable_7d_positive is None:
        stable_short_term = None
    else:
        stable_short_term = False
    return {
        "status": (
            "data_insufficient" if cvd_4h is None and cvd_24h is None
            else "weakening" if cvd_weak is True
            else "positive" if cvd_positive is True
            else "neutral"
        ),
        "cvd_weak_rule": cvd_weak,
        "cvd_positive_rule": cvd_positive,
        "stablecoin_24h_positive_rule": stable_24h_positive,
        "stablecoin_7d_positive_rule": stable_7d_positive,
        "stablecoin_short_term_rule": stable_short_term,
        "stablecoin_sustained_rule": stable_sustained,
        "stablecoin_liquidity_rule": stable_sustained,
        "cvd_4h_pct": cvd_4h,
        "cvd_24h_pct": cvd_24h,
        "stablecoin_24h_usd": stable_24h,
        "stablecoin_7d_usd": stable_7d,
        "stablecoin_flow_deadzone_usd": stable_deadzone,
    }


def build_macro_rule_state(payload, etf_state):
    public_records = payload.get("public_records") or {}
    macro = public_records.get("macro") or {}
    value = current_record_values(macro)
    fiscal_proxy = value.get("fiscal_proxy") or {}
    cost_proxy = value.get("cost_of_living_proxy") or {}
    environment = build_macro_environment_state(value)
    long_term_environment = build_long_term_money_state(value)
    levels = payload.get("levels") or {}
    price = safe_float((payload.get("market") or {}).get("price"))
    support = safe_float(levels.get("support"))
    us10y_ge_hike = rule_ge(value.get("us10y"), MACRO_RATE_HIKE_US10Y_LEVEL)
    dxy_ge_hike = rule_ge(value.get("usd_index"), MACRO_RATE_HIKE_DXY_LEVEL)
    real_yield_up = rule_ge(value.get("real_yield_10y_30d_pp"), MACRO_REAL_YIELD_UP_30D_PP)
    real_yield_high = rule_ge(value.get("real_yield_10y"), MACRO_REAL_YIELD_HIGH_LEVEL)
    real_yield_high_or_up = rule_any(real_yield_high, real_yield_up)
    usd_30d_up = rule_ge(value.get("usd_index_30d_pct"), MACRO_USD_UP_30D_PCT)
    fed_policy = value.get("fed_policy")
    fed_hike_prob = safe_float(value.get("fed_hike_probability"))
    fed_hike_rule = (
        True if fed_policy == "Hawkish" or rule_ge(fed_hike_prob, MACRO_FED_HIKE_PROB_LEVEL) is True
        else False if fed_policy in ("Dovish", "Neutral") or fed_hike_prob is not None
        else None
    )
    btc_broke_structure = rule_all(
        rule_lt(price, support),
        True if support is not None else None,
    )
    rate_hike_checks = [
        us10y_ge_hike,
        dxy_ge_hike,
        real_yield_up,
        fed_hike_rule,
    ]
    rate_hike_rule = (
        rule_hit_count(rate_hike_checks) >= 2
        if rule_known_count(rate_hike_checks) else None
    )
    higher_for_longer_rule = rule_all(
        real_yield_high_or_up,
        usd_30d_up,
        rule_not(us10y_ge_hike),
        rule_not(dxy_ge_hike),
        rule_not(rate_hike_rule),
    )
    yield_down = rule_any(
        rule_le(value.get("real_yield_10y_30d_pp"), -MACRO_REAL_YIELD_UP_30D_PP),
        rule_le(value.get("us10y_30d_pp"), -MACRO_REAL_YIELD_UP_30D_PP),
    )
    usd_down = rule_le(value.get("usd_index_30d_pct"), -MACRO_USD_UP_30D_PCT)
    fed_cut_rule = True if fed_policy == "Dovish" else False if fed_policy in ("Hawkish", "Neutral") else None
    rate_cut_rule = rule_all(rule_any(yield_down, fed_cut_rule), usd_down)
    if rate_hike_rule is True:
        status = "rate_hike_pricing"
    elif higher_for_longer_rule is True:
        status = "higher_for_longer"
    elif rate_cut_rule is True:
        status = "rate_cut_pricing"
    elif rule_known_count(rate_hike_checks + [higher_for_longer_rule, rate_cut_rule]) == 0:
        status = "data_insufficient"
    else:
        status = "neutral"
    return {
        "status": status,
        "rate_hike_rule": rate_hike_rule,
        "higher_for_longer_rule": higher_for_longer_rule,
        "rate_cut_rule": rate_cut_rule,
        "rate_hike_hit_count": rule_hit_count(rate_hike_checks),
        "rate_hike_known_count": rule_known_count(rate_hike_checks),
        "fiscal_status": fiscal_proxy.get("status") or "unknown",
        "cost_of_living_gap": cost_proxy.get("status") or "unknown",
        "environment": environment,
        "long_term_environment": long_term_environment,
        "checks": {
            "us10y_ge_hike": us10y_ge_hike,
            "dxy_ge_hike": dxy_ge_hike,
            "real_yield_up": real_yield_up,
            "fed_hike_rule": fed_hike_rule,
            "btc_broke_with_etf_outflow": None,
            "btc_broke_structure": btc_broke_structure,
            "real_yield_high_or_up": real_yield_high_or_up,
            "usd_30d_up": usd_30d_up,
            "fiscal": fiscal_proxy.get("checks") or {},
            "environment": environment,
            "long_term_environment": long_term_environment,
        },
    }


def build_level_rule_state(payload, leverage_state, spot_state):
    market = payload.get("market") or {}
    levels = payload.get("levels") or {}
    checks = ((payload.get("structure_state") or {}).get("checks") or {})
    vol_fast = checks.get("volume_fast_expansion") or {}
    price = safe_float(market.get("price"))
    support = safe_float(levels.get("short_support"))
    breakout = safe_float(levels.get("short_resistance"))
    confirmation = safe_float(levels.get("confirmation_price"))
    range_rule = rule_all(rule_ge(price, support), rule_lt(price, breakout))
    volume_4h_rule = True if vol_fast.get("ok") else False
    breakout_confirm_rule = rule_all(
        rule_ge(price, breakout),
        spot_state.get("cvd_positive_rule"),
    )
    breakdown_risk_rule = rule_all(
        rule_lt(price, support),
        spot_state.get("cvd_weak_rule"),
    )
    long_signal_weight = (
        "disabled" if breakdown_risk_rule is True
        else "normal" if breakout_confirm_rule is True
        else "reduced"
    )
    return {
        "status": "breakdown_risk" if breakdown_risk_rule is True else "breakout_confirmed" if breakout_confirm_rule is True else "range_deleveraging" if range_rule is True else "neutral",
        "range_rule": range_rule,
        "raw_breakout_confirm_rule": breakout_confirm_rule,
        "breakout_confirm_rule": breakout_confirm_rule,
        "breakdown_risk_rule": breakdown_risk_rule,
        "volume_4h_rule": volume_4h_rule,
        "long_signal_weight": long_signal_weight,
    }


def trend_level_changed(previous, current, keys=None):
    previous_levels = (previous or {}).get("levels") or {}
    for key in keys or ("short_support", "short_resistance", "trend_support", "trend_resistance"):
        old = safe_float(previous_levels.get(key))
        new = safe_float((current or {}).get(key))
        if old is None or new is None:
            if old != new:
                return True
            continue
        denominator = max(abs(old), 1.0)
        if abs(new - old) / denominator >= assets.current().parameter("PRICE_TREND_LEVEL_RESET_REL", PRICE_TREND_LEVEL_RESET_REL):
            return True
    return False


def price_trend_bucket(payload):
    if (payload.get("instrument") or {}).get("calendar") == "US_EQUITY":
        stamp = (payload.get("signal_data_quality") or {}).get("latest_close_ms")
        if stamp:
            return int(stamp) // 3600000
    snapshot_ts = int(((payload.get("public_snapshot") or {}).get("ts")) or 0)
    if snapshot_ts <= 0:
        snapshot_ts = int(time.time())
    return snapshot_ts // 3600


def price_trend_status_text(status):
    return {
        "range_observe": "区间观察",
        "support_tested": "支撑首次测试完成",
        "post_support_consolidation_watch": "支撑测试后缩量整理观察",
        "post_support_consolidation_confirmed": "支撑测试后缩量整理确认",
        "post_support_pressure_returned": "支撑测试后卖压重新出现",
        "breakout_attempt": "突破尝试",
        "breakout_hold": "站稳观察",
        "trend_confirmed": "趋势延续确认",
        "pullback_watch": "回踩观察",
        "support_reclaim_watch": "支撑收回待资金确认",
        "breakdown_watch": "跌破观察",
        "breakdown_confirmed": "跌破确认",
        "breakdown_risk": "跌破确认",
        "data_insufficient": "数据不足",
    }.get(status, "区间观察")


def pressure_state_text(status):
    return {
        "not_broken": "未突破",
        "breakout_attempt": "突破尝试",
        "breakout_hold": "已站稳",
        "breakout_confirmed": "突破确认",
        "pullback_watch": "回踩观察",
        "data_insufficient": "数据不足",
    }.get(status, "未突破")


def support_state_text(status):
    return {
        "not_tested": "未测试",
        "near_support": "接近支撑",
        "support_tested": "首次测试完成",
        "support_holding": "支撑有效",
        "support_reclaim_watch": "已收回，待资金确认",
        "breakdown_watch": "跌破观察",
        "breakdown_confirmed": "跌破确认",
        "bounce_watch": "反抽观察",
        "data_insufficient": "数据不足",
    }.get(status, "未测试")


def support_breakdown_phase(status, current_bucket, confirmed_bucket):
    if status != "breakdown_confirmed":
        return None
    try:
        current_bucket = int(current_bucket)
        confirmed_bucket = int(confirmed_bucket)
    except (TypeError, ValueError, OverflowError):
        return "active_below_former_support"
    if current_bucket == confirmed_bucket:
        return "confirmation_event"
    return "active_below_former_support"


def trend_support_breakdown_phase(trend):
    trend = trend or {}
    return support_breakdown_phase(
        trend.get("status"),
        trend.get("bucket"),
        trend.get("support_breakdown_confirmed_bucket"),
    )


def confirmed_breakdown_level_roles(trend, levels=None):
    trend = trend or {}
    levels = levels or {}
    former_support = safe_float(trend.get("support_anchor"))
    rolling_support = safe_float(trend.get("rolling_short_support"))
    if rolling_support is None:
        rolling_support = safe_float(levels.get("short_support"))
    current_support = None
    if (
        former_support is not None
        and rolling_support is not None
        and rolling_support < former_support
    ):
        current_support = rolling_support
    return {
        "former_support": former_support,
        "current_support": current_support,
        "rolling_support": rolling_support,
    }


def structure_check_summary(checks):
    items = list(checks or [])
    return {
        "hit_count": rule_hit_count(items),
        "known_count": rule_known_count(items),
        "total": len(items),
        "confirmed": rule_hit_count(items) >= 2 and rule_known_count(items) >= 2,
    }


def build_structure_funding_confirmation(leverage_state, spot_state, market_patterns=None):
    up_checks = {
        "oi_24h_positive": rule_gt((leverage_state or {}).get("oi_24h_pct"), 0.0),
        "spot_cvd_4h_positive": rule_gt((spot_state or {}).get("cvd_4h_pct"), 0.0),
        "funding_non_negative": rule_ge((leverage_state or {}).get("funding_8h_pct"), 0.0),
    }
    down_checks = {
        "oi_24h_non_positive": rule_le((leverage_state or {}).get("oi_24h_pct"), 0.0),
        "spot_cvd_4h_negative": rule_lt((spot_state or {}).get("cvd_4h_pct"), 0.0),
        "funding_non_positive": rule_le((leverage_state or {}).get("funding_8h_pct"), 0.0),
    }
    up_summary = structure_check_summary(up_checks.values())
    down_summary = structure_check_summary(down_checks.values())
    down_pattern_keys = []
    for key in ("spot_active_sell", "new_short_pressure", "deleveraging_down"):
        if ((market_patterns or {}).get(key) or {}).get("state") == "YES":
            down_pattern_keys.append(key)
    down_confirmation_sources = []
    if down_summary["confirmed"]:
        down_confirmation_sources.append("funding_down_confirmation")
    down_confirmation_sources.extend(down_pattern_keys)
    return {
        "up_checks": up_checks,
        "down_checks": down_checks,
        "up_hit_count": up_summary["hit_count"],
        "up_known_count": up_summary["known_count"],
        "up_confirmed_rule": up_summary["confirmed"],
        "down_hit_count": down_summary["hit_count"],
        "down_known_count": down_summary["known_count"],
        "down_funds_confirmed_rule": down_summary["confirmed"],
        "down_pattern_confirmed_rule": bool(down_pattern_keys),
        "down_pattern_keys": down_pattern_keys,
        "down_confirmation_sources": down_confirmation_sources,
        "down_confirmed_rule": down_summary["confirmed"] or bool(down_pattern_keys),
    }


def update_price_trend_state(payload, state, leverage_state, spot_state, level_state):
    market = payload.get("market") or {}
    levels = payload.get("levels") or {}
    structure_close = safe_float((payload.get("structure_state") or {}).get("latest_close"))
    price = structure_close if structure_close is not None else safe_float(market.get("price"))
    short_support = safe_float(levels.get("short_support"))
    rolling_short_support = (
        safe_float(levels.get("rolling_short_support"))
        or short_support
    )
    support_test = levels.get("support_test") or {}
    short_resistance = safe_float(levels.get("short_resistance"))
    trend_support = safe_float(levels.get("trend_support"))
    trend_resistance = safe_float(levels.get("trend_resistance"))
    previous = state.get(PRICE_TREND_STATE_KEY) or {}
    previous_schema = int(previous.get("schema_version") or 1)
    now_ts = int(time.time())
    bucket = price_trend_bucket(payload)
    if assets.current().equity and previous and previous.get("bucket") == bucket:
        return copy.deepcopy(previous)
    if (assets.current().equity and previous.get("bucket") is not None and bucket is not None
            and bucket - previous["bucket"] > 1):
        # Keep the frozen anchor across closure, but do not turn separated
        # observations into consecutive natural hours of confirmation.
        previous = {**previous, **{key: 0 for key in (
            "above_pressure_hours", "below_support_hours", "below_pressure_hours",
            "above_breakout_hours", "below_breakout_hours")}}

    data_sufficient = price is not None and short_support is not None and short_resistance is not None
    pressure_level_reset = trend_level_changed(previous, levels, keys=("short_resistance",))
    support_candidate_changed = trend_level_changed(previous, levels, keys=("short_support",))
    same_bucket = previous.get("bucket") == bucket
    previous_status = previous.get("status") or "range_observe"
    previous_pressure = previous.get("pressure_state") or "not_broken"
    previous_support = previous.get("support_state") or "not_tested"
    support_anchor = safe_float(previous.get("support_anchor")) if previous_schema >= 2 else None
    support_anchor_phase = previous.get("support_anchor_phase") if previous_schema >= 2 else "inactive"
    if support_anchor is None or support_anchor_phase not in ("watching", "confirmed"):
        support_anchor = None
        support_anchor_phase = "inactive"
    support_anchor_started_at = previous.get("support_anchor_started_at") if support_anchor is not None else None
    support_anchor_bucket = previous.get("support_anchor_bucket") if support_anchor is not None else None
    support_anchor_source = (
        previous.get("support_anchor_source")
        if support_anchor is not None
        else None
    )
    if support_anchor is not None and not support_anchor_source:
        support_anchor_source = "migrated_v2_anchor"
    support_confirmation_sources = (
        list(previous.get("support_confirmation_sources") or [])
        if support_anchor is not None
        else []
    )
    support_breakdown_confirmed_bucket = (
        previous.get("support_breakdown_confirmed_bucket")
        if support_anchor is not None
        else None
    )
    previous_support_test_anchor = safe_float(previous.get("support_test_anchor"))
    previous_support_test_formed_bucket = previous.get("support_test_formed_bucket")
    previous_support_test_first_retest_bucket = previous.get("support_test_first_retest_bucket")
    previous_support_test_phase = previous.get("support_test_phase") or "inactive"
    previous_post_support_phase = previous.get("post_support_phase") or "inactive"
    previous_sell_pressure_change = previous.get("spot_sell_pressure_change") or {}
    valid_statuses = {
        "range_observe",
        "support_tested",
        "post_support_consolidation_watch",
        "post_support_consolidation_confirmed",
        "post_support_pressure_returned",
        "breakout_attempt",
        "breakout_hold",
        "trend_confirmed",
        "pullback_watch",
        "support_reclaim_watch",
        "breakdown_watch",
        "breakdown_confirmed",
        "breakdown_risk",
        "data_insufficient",
    }
    if previous_status not in valid_statuses:
        previous_status = "range_observe"

    if not data_sufficient:
        previous_breakdown_phase = support_breakdown_phase(
            previous_status,
            bucket,
            support_breakdown_confirmed_bucket,
        )
        previous_status_text = price_trend_status_text(previous_status)
        previous_support_text = support_state_text(previous_support)
        if previous_breakdown_phase == "confirmation_event":
            previous_status_text = "本轮支撑跌破确认"
            previous_support_text = "本轮跌破确认"
        elif previous_breakdown_phase == "active_below_former_support":
            previous_status_text = "原支撑失守状态持续"
            previous_support_text = "原支撑失守持续"
        trend_state = {
            "schema_version": PRICE_TREND_STATE_SCHEMA_REVISION,
            "status": previous_status if previous_status != "data_insufficient" else "data_insufficient",
            "status_text": previous_status_text,
            "position_state": levels.get("position_state") or "data_insufficient",
            "position_text": levels.get("position_text") or "数据不足",
            "pressure_state": previous_pressure,
            "pressure_state_text": pressure_state_text(previous_pressure),
            "support_state": previous_support,
            "support_state_text": previous_support_text,
            "above_pressure_hours": int(previous.get("above_pressure_hours") or previous.get("above_breakout_hours") or 0),
            "below_support_hours": int(previous.get("below_support_hours") or 0),
            "below_pressure_hours": int(previous.get("below_pressure_hours") or previous.get("below_breakout_hours") or 0),
            "entered_at": previous.get("entered_at") or now_ts,
            "updated_at": now_ts,
            "bucket": bucket,
            "levels": {
                "short_support": short_support,
                "rolling_short_support": rolling_short_support,
                "support_test": support_test,
                "short_resistance": short_resistance,
                "trend_support": trend_support,
                "trend_resistance": trend_resistance,
            },
            "support_anchor": support_anchor,
            "support_anchor_phase": support_anchor_phase,
            "support_anchor_started_at": support_anchor_started_at,
            "support_anchor_bucket": support_anchor_bucket,
            "support_anchor_source": support_anchor_source,
            "rolling_short_support": rolling_short_support,
            "support_confirmation_sources": support_confirmation_sources,
            "support_breakdown_confirmed_bucket": support_breakdown_confirmed_bucket,
            "support_test_anchor": previous_support_test_anchor,
            "support_test_formed_bucket": previous_support_test_formed_bucket,
            "support_test_first_retest_bucket": previous_support_test_first_retest_bucket,
            "support_test_phase": previous_support_test_phase,
            "post_support_phase": previous_post_support_phase,
            "spot_sell_pressure_change": previous_sell_pressure_change,
            "support_recovery_rule": None,
            "support_price_reclaimed_rule": None,
            "support_candidate_changed": support_candidate_changed,
            "pressure_level_reset": pressure_level_reset,
            "data_sufficient": False,
            "level_reset": pressure_level_reset,
        }
        state[PRICE_TREND_STATE_KEY] = trend_state
        return trend_state

    position_state, position_text = classify_market_position(price, short_support, short_resistance)
    above_pressure = price >= short_resistance
    below_pressure = price < short_resistance
    if same_bucket:
        above_pressure_hours = int(previous.get("above_pressure_hours") or previous.get("above_breakout_hours") or 0)
        below_pressure_hours = int(previous.get("below_pressure_hours") or previous.get("below_breakout_hours") or 0)
    else:
        previous_above_pressure = 0 if pressure_level_reset else int(previous.get("above_pressure_hours") or previous.get("above_breakout_hours") or 0)
        previous_below_pressure = 0 if pressure_level_reset else int(previous.get("below_pressure_hours") or previous.get("below_breakout_hours") or 0)
        above_pressure_hours = previous_above_pressure + 1 if above_pressure else 0
        below_pressure_hours = previous_below_pressure + 1 if below_pressure else 0

    funding_confirmation = build_structure_funding_confirmation(
        leverage_state,
        spot_state,
        payload.get("market_patterns") or {},
    )
    up_confirmed = funding_confirmation.get("up_confirmed_rule") is True
    down_confirmed = funding_confirmation.get("down_confirmed_rule") is True
    current_support_test_anchor = safe_float(support_test.get("anchor"))
    current_support_test_formed_bucket = support_test.get("formed_bucket")
    current_support_test_first_retest_bucket = support_test.get("first_retest_bucket")
    current_support_test_phase = support_test.get("phase") or "inactive"
    current_support_test_completed = (
        support_test.get("completed_rule") is True
        and current_support_test_phase == "tested"
        and current_support_test_anchor is not None
        and price >= current_support_test_anchor
    )
    current_support_test_broken = (
        current_support_test_phase == "broken"
        and current_support_test_anchor is not None
        and price < current_support_test_anchor
    )
    previous_support_test_completed = (
        previous_support_test_phase == "tested"
        and previous_support_test_anchor is not None
        and previous_support_test_first_retest_bucket is not None
    )
    if current_support_test_completed:
        support_test_anchor = current_support_test_anchor
        support_test_formed_bucket = current_support_test_formed_bucket
        support_test_first_retest_bucket = current_support_test_first_retest_bucket
        support_test_phase = current_support_test_phase
        support_test_completed = current_support_test_completed
        support_test_broken = current_support_test_broken
    elif previous_support_test_completed:
        support_test_anchor = previous_support_test_anchor
        support_test_formed_bucket = previous_support_test_formed_bucket
        support_test_first_retest_bucket = previous_support_test_first_retest_bucket
        support_test_broken = price < previous_support_test_anchor
        support_test_completed = not support_test_broken
        support_test_phase = "broken" if support_test_broken else "tested"
    elif current_support_test_broken:
        support_test_anchor = current_support_test_anchor
        support_test_formed_bucket = current_support_test_formed_bucket
        support_test_first_retest_bucket = current_support_test_first_retest_bucket
        support_test_phase = current_support_test_phase
        support_test_completed = False
        support_test_broken = True
    else:
        support_test_anchor = current_support_test_anchor
        support_test_formed_bucket = current_support_test_formed_bucket
        support_test_first_retest_bucket = current_support_test_first_retest_bucket
        support_test_phase = current_support_test_phase
        support_test_completed = current_support_test_completed
        support_test_broken = current_support_test_broken
    market_patterns = payload.get("market_patterns") or {}
    down_pattern_states = {
        key: ((market_patterns.get(key) or {}).get("state"))
        for key in ("spot_active_sell", "new_short_pressure", "deleveraging_down")
    }
    down_pattern_active = any(value == "YES" for value in down_pattern_states.values())
    down_patterns_clear = all(value == "NO" for value in down_pattern_states.values())
    low_volume_range = market_patterns.get("low_volume_range") or {}
    sell_pressure_change = market_patterns.get("spot_sell_pressure_change") or {}

    support_recovery_rule = None
    support_price_reclaimed_rule = None
    support_reclaimed = False
    if support_anchor is not None:
        below_anchor = price < support_anchor
        support_price_reclaimed_rule = rule_ge(price, support_anchor)
        support_recovery_rule = rule_all(
            support_price_reclaimed_rule,
            rule_gt((spot_state or {}).get("cvd_4h_pct"), 0.0),
        )
        if support_recovery_rule is True:
            support_anchor = None
            support_anchor_phase = "inactive"
            support_anchor_started_at = None
            support_anchor_bucket = None
            support_anchor_source = None
            support_confirmation_sources = []
            support_breakdown_confirmed_bucket = None
            below_support_hours = 0
            support_state = "support_holding"
            support_reclaimed = True
        elif below_anchor:
            below_support_hours = int(previous.get("below_support_hours") or 0) if same_bucket else (
                int(previous.get("below_support_hours") or 0) + 1
            )
            anchor_was_confirmed = support_anchor_phase == "confirmed"
            if anchor_was_confirmed or (
                below_support_hours >= assets.current().parameter("PRICE_TREND_BREAKDOWN_HOURS", PRICE_TREND_BREAKDOWN_HOURS) and down_confirmed
            ):
                support_anchor_phase = "confirmed"
                support_state = "breakdown_confirmed"
                if not anchor_was_confirmed:
                    support_breakdown_confirmed_bucket = bucket
                    if not support_confirmation_sources:
                        support_confirmation_sources = list(
                            funding_confirmation.get("down_confirmation_sources") or []
                        )
            else:
                support_state = "breakdown_watch"
        else:
            below_support_hours = 0
            support_state = "support_reclaim_watch"
    else:
        below_support_hours = 0
        short_support_broken = not support_test_completed and price < short_support
        if support_test_broken or short_support_broken:
            support_anchor = support_test_anchor if support_test_broken else short_support
            support_anchor_phase = "watching"
            support_anchor_started_at = now_ts
            support_anchor_bucket = bucket
            support_anchor_source = (
                "completed_support_test"
                if support_test_broken
                else "previous_closed_support"
            )
            support_confirmation_sources = []
            support_breakdown_confirmed_bucket = None
            below_support_hours = 1
            if assets.current().parameter("PRICE_TREND_BREAKDOWN_HOURS", PRICE_TREND_BREAKDOWN_HOURS) <= 1 and down_confirmed:
                support_anchor_phase = "confirmed"
                support_state = "breakdown_confirmed"
                support_breakdown_confirmed_bucket = bucket
                support_confirmation_sources = list(
                    funding_confirmation.get("down_confirmation_sources") or []
                )
            else:
                support_state = "breakdown_watch"
        elif support_test_completed:
            support_state = "support_tested"
        elif position_state == "near_lower":
            support_state = "near_support"
        else:
            support_state = "not_tested"

    support_reference = support_anchor if support_anchor is not None else short_support
    below_support = price < support_reference

    if above_pressure:
        if previous_pressure in ("breakout_confirmed", "pullback_watch"):
            pressure_state = "breakout_confirmed"
        elif above_pressure_hours >= assets.current().parameter("PRICE_TREND_CONFIRM_HOURS", PRICE_TREND_CONFIRM_HOURS) and up_confirmed:
            pressure_state = "breakout_confirmed"
        elif above_pressure_hours >= assets.current().parameter("PRICE_TREND_CONFIRM_HOURS", PRICE_TREND_CONFIRM_HOURS):
            pressure_state = "breakout_hold"
        else:
            pressure_state = "breakout_attempt"
    elif previous_pressure in ("breakout_confirmed", "pullback_watch", "breakout_hold") and not below_support:
        pressure_state = "pullback_watch" if below_pressure_hours < assets.current().parameter("PRICE_TREND_BREAKDOWN_HOURS", PRICE_TREND_BREAKDOWN_HOURS) else "not_broken"
    else:
        pressure_state = "not_broken"

    post_support_phase = "inactive"
    if (
        support_anchor is None
        and not support_reclaimed
        and support_test_completed
    ):
        support_state = "support_tested"
        if down_pattern_active:
            post_support_phase = "pressure_returned"
        elif down_patterns_clear and low_volume_range.get("state") == "YES":
            post_support_phase = "consolidation_confirmed"
        elif down_patterns_clear and low_volume_range.get("raw_hit") is True:
            post_support_phase = "consolidation_watch"
        else:
            post_support_phase = "tested"

    if support_state == "breakdown_confirmed":
        status = "breakdown_confirmed"
    elif support_state == "breakdown_watch":
        status = "breakdown_watch"
    elif support_state == "support_reclaim_watch":
        status = "support_reclaim_watch"
    elif pressure_state == "breakout_confirmed":
        status = "trend_confirmed"
    elif pressure_state == "pullback_watch":
        status = "pullback_watch"
    elif pressure_state == "breakout_hold":
        status = "breakout_hold"
    elif pressure_state == "breakout_attempt":
        status = "breakout_attempt"
    elif post_support_phase == "pressure_returned":
        status = "post_support_pressure_returned"
    elif post_support_phase == "consolidation_confirmed":
        status = "post_support_consolidation_confirmed"
    elif post_support_phase == "consolidation_watch":
        status = "post_support_consolidation_watch"
    elif post_support_phase == "tested":
        status = "support_tested"
    else:
        status = "range_observe"

    entered_at = previous.get("entered_at") if previous_status == status else now_ts
    breakdown_phase = support_breakdown_phase(
        status,
        bucket,
        support_breakdown_confirmed_bucket,
    )
    status_text_value = price_trend_status_text(status)
    support_state_text_value = support_state_text(support_state)
    if breakdown_phase == "confirmation_event":
        status_text_value = "本轮支撑跌破确认"
        support_state_text_value = "本轮跌破确认"
    elif breakdown_phase == "active_below_former_support":
        status_text_value = "原支撑失守状态持续"
        support_state_text_value = "原支撑失守持续"
    trend_state = {
        "schema_version": PRICE_TREND_STATE_SCHEMA_REVISION,
        "status": status,
        "status_text": status_text_value,
        "position_state": position_state,
        "position_text": position_text,
        "pressure_state": pressure_state,
        "pressure_state_text": pressure_state_text(pressure_state),
        "support_state": support_state,
        "support_state_text": support_state_text_value,
        "above_pressure_hours": above_pressure_hours,
        "below_support_hours": below_support_hours,
        "below_pressure_hours": below_pressure_hours,
        "above_breakout_hours": above_pressure_hours,
        "below_breakout_hours": below_pressure_hours,
        "entered_at": entered_at,
        "updated_at": now_ts,
        "bucket": bucket,
        "levels": {
            "short_support": short_support,
            "rolling_short_support": rolling_short_support,
            "support_test": support_test,
            "short_resistance": short_resistance,
            "trend_support": trend_support,
            "trend_resistance": trend_resistance,
        },
        "support_anchor": support_anchor,
        "support_anchor_phase": support_anchor_phase,
        "support_anchor_started_at": support_anchor_started_at,
        "support_anchor_bucket": support_anchor_bucket,
        "support_anchor_source": support_anchor_source,
        "rolling_short_support": rolling_short_support,
        "support_confirmation_sources": support_confirmation_sources,
        "support_breakdown_confirmed_bucket": support_breakdown_confirmed_bucket,
        "support_test_anchor": support_test_anchor,
        "support_test_formed_bucket": support_test_formed_bucket,
        "support_test_first_retest_bucket": support_test_first_retest_bucket,
        "support_test_phase": support_test_phase,
        "post_support_phase": post_support_phase,
        "spot_sell_pressure_change": sell_pressure_change,
        "down_pattern_states": down_pattern_states,
        "support_recovery_rule": support_recovery_rule,
        "support_price_reclaimed_rule": support_price_reclaimed_rule,
        "support_reclaimed_rule": support_reclaimed,
        "support_candidate_changed": support_candidate_changed,
        "pressure_level_reset": pressure_level_reset,
        "price": price,
        "above_pressure_rule": above_pressure,
        "below_support_rule": below_support,
        "above_breakout_rule": above_pressure,
        "raw_confirm_rule": pressure_state == "breakout_confirmed",
        "weak_breakdown_rule": support_state == "breakdown_confirmed",
        "funding_confirmation": funding_confirmation,
        "data_sufficient": True,
        "level_reset": pressure_level_reset,
    }
    state[PRICE_TREND_STATE_KEY] = trend_state
    return trend_state


def apply_price_trend_to_level_state(level_state, trend_state):
    out = dict(level_state or {})
    status = (trend_state or {}).get("status") or "range_observe"
    out["price_trend"] = trend_state or {}
    out["position_state"] = (trend_state or {}).get("position_state") or out.get("position_state")
    out["position_text"] = (trend_state or {}).get("position_text") or out.get("position_text")
    out["pressure_state"] = (trend_state or {}).get("pressure_state") or "not_broken"
    out["pressure_state_text"] = (trend_state or {}).get("pressure_state_text") or pressure_state_text(out["pressure_state"])
    out["support_state"] = (trend_state or {}).get("support_state") or "not_tested"
    out["support_state_text"] = (trend_state or {}).get("support_state_text") or support_state_text(out["support_state"])
    out["status"] = status
    out["breakout_attempt_rule"] = status == "breakout_attempt"
    out["breakout_hold_rule"] = status == "breakout_hold"
    out["pullback_watch_rule"] = status == "pullback_watch"
    out["trend_confirmed_rule"] = status == "trend_confirmed"
    out["breakout_confirm_rule"] = status == "trend_confirmed"
    out["support_reclaim_watch_rule"] = status == "support_reclaim_watch"
    out["support_breakdown_watch_rule"] = status == "breakdown_watch"
    out["support_breakdown_confirm_rule"] = status == "breakdown_confirmed"
    out["support_tested_rule"] = status == "support_tested"
    out["post_support_consolidation_watch_rule"] = status == "post_support_consolidation_watch"
    out["post_support_consolidation_confirm_rule"] = status == "post_support_consolidation_confirmed"
    out["post_support_pressure_returned_rule"] = status == "post_support_pressure_returned"
    out["breakdown_risk_rule"] = status == "breakdown_confirmed"
    if status == "breakdown_confirmed":
        out["long_signal_weight"] = "disabled"
    elif status == "trend_confirmed":
        out["long_signal_weight"] = "normal"
    else:
        out["long_signal_weight"] = "reduced"
    return out


def rule_majority(items, minimum_hits, minimum_known=None):
    known = rule_known_count(items)
    if minimum_known is not None and known < int(minimum_known):
        return None
    return rule_hit_count(items) >= int(minimum_hits)


def build_fed_proxy_state(value):
    policy = value.get("fed_policy")
    target_delta_30d = safe_float(value.get("fed_target_mid_30d_pp"))
    target_lower = safe_float(value.get("fed_target_lower"))
    target_upper = safe_float(value.get("fed_target_upper"))
    effr = safe_float(value.get("effective_fed_funds"))
    effr_in_target = None
    if target_lower is not None and target_upper is not None and effr is not None:
        effr_in_target = target_lower <= effr <= target_upper

    target_cut = rule_lt(target_delta_30d, 0.0)
    target_not_cut = rule_ge(target_delta_30d, 0.0)
    if policy == "Dovish" or target_cut is True:
        easing_proxy = True
    elif policy in ("Hawkish", "Neutral") or rule_all(target_not_cut, effr_in_target) is True:
        easing_proxy = False
    else:
        easing_proxy = None

    if policy == "Dovish":
        source = "FedWatch"
    elif target_delta_30d is not None or effr_in_target is not None:
        source = "FRED target/EFFR proxy"
    else:
        source = None

    return {
        "easing_proxy_rule": easing_proxy,
        "target_cut_rule": target_cut,
        "target_not_cut_rule": target_not_cut,
        "effr_in_target_rule": effr_in_target,
        "source": source,
        "target_mid_30d_pp": target_delta_30d,
    }


def macro_cycle_stage_text(stage):
    if stage == "phase_1":
        return "① 高利率压制资产"
    if stage == "phase_2":
        return "② 财政压力累积"
    if stage == "phase_3":
        return "③ 市场交易未来宽松"
    if stage == "phase_4":
        return "④ 流动性重新释放"
    return "数据不足"


def build_macro_cycle_state(payload, macro_state, etf_state, spot_state, level_state):
    public_records = payload.get("public_records") or {}
    macro = public_records.get("macro") or {}
    value = current_record_values(macro)
    fiscal = value.get("fiscal_proxy") or {}
    price_7d = safe_float(payload.get("price_7d_pct"))

    fed_proxy = build_fed_proxy_state(value)
    us10y_high = rule_ge(value.get("us10y"), MACRO_CYCLE_US10Y_HIGH_LEVEL)
    real_high = rule_ge(value.get("real_yield_10y"), MACRO_REAL_YIELD_HIGH_LEVEL)
    dxy_strong = rule_any(
        rule_ge(value.get("usd_index"), MACRO_CYCLE_DXY_STRONG_LEVEL),
        rule_ge(value.get("usd_index_30d_pct"), MACRO_USD_UP_30D_PCT),
    )
    dxy_down = rule_le(value.get("usd_index_30d_pct"), -MACRO_USD_UP_30D_PCT)
    real_down = rule_le(value.get("real_yield_10y_30d_pp"), -MACRO_REAL_YIELD_UP_30D_PP)
    us10y_down = rule_le(value.get("us10y_30d_pp"), -MACRO_REAL_YIELD_UP_30D_PP)
    real_not_down = rule_not(real_down)
    dxy_not_weak = rule_not(dxy_down)
    fiscal_pressure = (
        True if fiscal.get("status") in ("stressed", "fiscal_dominance")
        else False if fiscal.get("status") == "normal"
        else None
    )
    liquidity_not_improved = rule_any(
        rule_lt(spot_state.get("stablecoin_7d_usd"), 0.0),
        rule_lt(etf_state.get("flow_5d_usd"), 0.0),
        spot_state.get("cvd_weak_rule"),
    )
    stable_sustained = spot_state.get("stablecoin_sustained_rule")
    btc_breakout_confirmed = level_state.get("breakout_confirm_rule")
    btc_liquidity_follow = rule_all(
        rule_gt(price_7d, 0.0),
        rule_any(spot_state.get("cvd_positive_rule"), stable_sustained, rule_gt(etf_state.get("flow_5d_usd"), 0.0)),
    )
    balance_sheet_expansion = rule_ge(
        value.get("fed_balance_sheet_30d_pct"),
        MACRO_CYCLE_BALANCE_SHEET_EXPANSION_30D_PCT,
    )
    gold_stronger_than_dxy = rule_gt(value.get("gold_vs_dxy_30d_pct"), 0.0)

    high_rate_maintained = rule_any(
        macro_state.get("higher_for_longer_rule"),
        rule_all(us10y_high, real_high),
    )
    phase_4_checks = [
        fed_proxy.get("easing_proxy_rule"),
        balance_sheet_expansion,
        real_down,
        dxy_down,
        stable_sustained,
        btc_breakout_confirmed,
    ]
    phase_3_checks = [
        dxy_down,
        real_down,
        us10y_down,
        fed_proxy.get("easing_proxy_rule"),
        gold_stronger_than_dxy,
        btc_liquidity_follow,
    ]
    phase_2_checks = [
        high_rate_maintained,
        fiscal_pressure,
        dxy_not_weak,
        real_not_down,
    ]
    phase_1_checks = [
        us10y_high,
        real_high,
        dxy_strong,
        rule_not(fed_proxy.get("easing_proxy_rule")),
        liquidity_not_improved,
    ]

    phase_4_rule = (
        rule_hit_count(phase_4_checks) >= 4
        and stable_sustained is True
        and btc_breakout_confirmed is True
        and rule_known_count(phase_4_checks) >= 5
    )
    phase_3_rule = rule_majority(phase_3_checks, 3, minimum_known=4)
    phase_2_rule = rule_majority(phase_2_checks, 3, minimum_known=3)
    phase_1_rule = rule_majority(phase_1_checks, 3, minimum_known=3)

    if phase_4_rule is True:
        stage = "phase_4"
    elif phase_3_rule is True:
        stage = "phase_3"
    elif phase_2_rule is True:
        stage = "phase_2"
    elif phase_1_rule is True:
        stage = "phase_1"
    else:
        stage = "unknown"

    easing_trade_rule = True if stage in ("phase_3", "phase_4") else False if stage in ("phase_1", "phase_2") else None
    return {
        "stage": stage,
        "stage_text": macro_cycle_stage_text(stage),
        "easing_trade_rule": easing_trade_rule,
        "long_logic_status": ((macro_state.get("long_term_environment") or {}).get("status_text")) or "观察中",
        "fed_proxy": fed_proxy,
        "checks": {
            "phase_1": phase_1_checks,
            "phase_2": phase_2_checks,
            "phase_3": phase_3_checks,
            "phase_4": phase_4_checks,
            "us10y_high": us10y_high,
            "real_high": real_high,
            "dxy_strong": dxy_strong,
            "dxy_down": dxy_down,
            "real_down": real_down,
            "us10y_down": us10y_down,
            "fiscal_pressure": fiscal_pressure,
            "stable_sustained": stable_sustained,
            "balance_sheet_expansion": balance_sheet_expansion,
            "gold_stronger_than_dxy": gold_stronger_than_dxy,
            "btc_breakout_confirmed": btc_breakout_confirmed,
        },
    }


def build_rule_state(payload, state):
    etf_state = build_etf_rule_state(payload, state)
    leverage_state = build_leverage_rule_state(payload)
    spot_state = build_spot_rule_state(payload)
    macro_state = build_macro_rule_state(payload, etf_state)
    level_state = build_level_rule_state(payload, leverage_state, spot_state)
    trend_state = update_price_trend_state(payload, state, leverage_state, spot_state, level_state)
    level_state = apply_price_trend_to_level_state(level_state, trend_state)
    macro_cycle_state = build_macro_cycle_state(payload, macro_state, etf_state, spot_state, level_state)
    return {
        "macro": macro_state,
        "macro_cycle": macro_cycle_state,
        "leverage": leverage_state,
        "etf": etf_state,
        "spot": spot_state,
        "levels": level_state,
    }


def build_signals(delta4h, structure_state, public_records, fear_greed):
    checks = (structure_state or {}).get("checks") or {}
    oi_fast = checks.get("oi_fast_expansion") or {}
    oi_slow = checks.get("oi_slow_confirmation") or {}
    vol_fast = checks.get("volume_fast_expansion") or {}
    vol_slow = checks.get("volume_slow_confirmation") or {}
    oi_4h = (delta4h or {}).get("oi_4h_pct")
    oi_24h = oi_slow.get("delta_24h_pct")
    etf = public_records.get("etf") or {}
    etf_rule_value = (etf.get("value") or {}) if record_is_current(etf) else {}
    funding = public_records.get("funding") or {}
    fng_value = safe_float((fear_greed or {}).get("value")) if fear_greed_is_current(fear_greed) else None
    funding_8h = safe_float(current_record_value(funding, "funding_8h_pct"))
    funding_24h = safe_float(current_record_value(funding, "funding_24h_pct"))
    etf_1d = safe_float(etf_rule_value.get("flow_1d_usd"))
    etf_5d = safe_float(etf_rule_value.get("flow_5d_usd"))
    return {
        "oi_expansion": bool(oi_fast.get("ok")) or (oi_4h is not None and oi_4h >= assets.current().parameter("SIGNAL_OI_EXPANSION_PCT", 1.0)),
        "oi_contraction": (
            (oi_4h is not None and oi_4h <= assets.current().parameter("SIGNAL_OI_CONTRACTION_4H_PCT", -1.0))
            or (oi_24h is not None and oi_24h <= assets.current().parameter("SIGNAL_OI_CONTRACTION_24H_PCT", -1.5))
        ),
        "volume_4h": bool(vol_fast.get("ok")),
        "volume_24h": bool(vol_slow.get("ok")),
        "etf_outflow": (
            (etf_1d is not None and etf_1d < 0)
            or (etf_5d is not None and etf_5d < 0)
        ),
        "funding_extreme": (
            (funding_8h is not None and abs(funding_8h) >= assets.current().parameter("SIGNAL_FUNDING_EXTREME_8H_PCT", 0.05))
            or (funding_24h is not None and abs(funding_24h) >= assets.current().parameter("SIGNAL_FUNDING_EXTREME_24H_PCT", 0.10))
        ),
        "fear_extreme": fng_value is not None and (fng_value <= 10 or fng_value >= 90),
    }


def build_equity_completeness(payload):
    records = payload.get("public_records") or {}
    quality = payload.get("signal_data_quality") or {}
    market = payload.get("market") or {}
    derivatives = payload.get("derivatives_market") or {}
    checks = {"股票闭合小时线": quality.get("usable"), "股票报价": market.get("price") is not None,
              "合约OI": derivatives.get("oi_usd") is not None,
              "普通Funding": not (records.get("funding") or {}).get("source_stale") and all(record_value(records.get("funding"), key) is not None for key in ("funding_8h_pct", "funding_24h_pct")),
              "合约基差": record_value(records.get("basis"), "basis_pct") is not None}
    missing = [key for key, value in checks.items() if not value]
    labels = {"stock_adjustment_unverified":"股票复权口径未确认，价格确认暂停",
              "target_hour_missing":"最新应有的闭合小时线尚未收到",
              "trading_hour_missing":"已积累的交易时段小时线尚不连续",
              "insufficient_price_history":"股票价格历史仍在积累"}
    notes = [labels.get(reason,"股票历史条件待确认") for reason in quality.get("reasons") or []]
    if market.get("quote_source_time_missing"):
        notes.append("股票报价缺少源时间；现价与结构的实时关系待确认，状态仅由闭合K线推进")
    if not quality.get("contiguous_4h"):
        notes.append("自然小时窗口不足；休市不算采集缺口，也不跨休市补齐4H样本")
    macro = current_record_values(records.get("macro"))
    macro_missing = [key for key in ("us10y", "real_yield_10y", "usd_index") if macro.get(key) is None]
    return {"available": len(checks) - len(missing), "total": len(checks), "missing": missing,
            "pending": [], "snapshot_notes": notes, "optional_notes": [],
            "not_applicable": ["加密恐慌指数", "稳定币供应量", "BTC爆仓/Hyperliquid"],
            "unavailable_sources": ["自身ETF申赎/持仓：尚缺公开稳定来源", "股票Spot CVD：缺少主动成交方向"],
            "core_market": {"available": len(checks) - len(missing), "total": len(checks), "missing": missing, "pending": []},
            "macro": {"available": 3 - len(macro_missing), "total": 3, "missing": macro_missing,
                      "stale": bool((records.get("macro") or {}).get("stale")), "partial": bool(macro_missing)}}


def build_completeness(payload):
    if (payload.get("instrument") or {}).get("calendar") == "US_EQUITY":
        return build_equity_completeness(payload)
    public_records = payload.get("public_records") or {}
    market = payload.get("market") or {}
    market_24h_delta = payload.get("market_24h_delta") or {}
    delta4h = payload.get("delta4h") or {}
    fear_greed = payload.get("fear_greed") or {}
    price_7d = payload.get("price_7d_pct")
    funding = public_records.get("funding")
    liquidation = public_records.get("liquidation")
    etf = public_records.get("etf")
    spot_cvd = public_records.get("spot_cvd")
    stablecoin = public_records.get("stablecoin")
    macro = public_records.get("macro")
    basis = public_records.get("basis")
    signal_quality = payload.get("signal_data_quality") or {}
    time_reason = (payload.get("market_time_context") or {}).get("ineligible_reason")
    volume_4h_pct = delta4h.get("vol_4h_pct")
    volume_24h_pct = (market_24h_delta.get("metrics") or {}).get("vol24")
    volume_complete = (
        True if volume_4h_pct is not None and volume_24h_pct is not None else None
    )
    spot_volume_complete = (
        True
        if (
            current_record_value(spot_cvd, "volume_4h_pct") is not None
            and current_record_value(spot_cvd, "volume_24h_pct") is not None
        )
        else None
    )
    checks = [
        ("Price", None if str(time_reason or "").startswith("current_quote_") else market.get("price")),
        ("24H/7D", market.get("chg24") if price_7d is not None else None),
        ("F&G", fear_greed.get("value") if fear_greed_is_current(fear_greed) else None),
        ("OI", market.get("oi_usd")),
        ("Volume", volume_complete),
        ("Spot Volume", spot_volume_complete),
        ("Funding", True if all(safe_float(current_record_value(funding, field)) is not None
            for field in ("funding_8h_pct", "funding_24h_pct")) else None),
        ("Liquidation", True if all(safe_float(current_record_value(liquidation, field)) is not None
            for field in ("long_4h_usd", "short_4h_usd", "long_24h_usd", "short_24h_usd")) else None),
        ("ETF Flow", current_record_value(etf, "flow_1d_usd")),
        ("ETF Holding", current_record_value(etf, "holdings_btc")),
        ("Spot CVD", current_record_value(spot_cvd, "cvd_4h_pct")),
        ("Stablecoin", current_record_value(stablecoin, "flow_24h_usd")),
    ]
    missing = []
    pending = []
    for name, value in checks:
        if value is not None:
            continue
        if name == "Liquidation" and liquidation_record_collecting(liquidation):
            pending.append(name)
        else:
            missing.append(name)
    snapshot_notes = []
    if btc_etf.is_holdings_source(etf or {}) and etf.get("flow_record"):
        flow = etf["flow_record"]
        if flow.get("source_stale"):
            snapshot_notes.append(f"ETF流量源日期过期({flow.get('source_date') or '未知'})")
        elif flow.get("status") == "unavailable":
            snapshot_notes.append(f"ETF流量采集失败({flow.get('reason') or 'unknown'})")
        else:
            if flow.get("source_date") and (flow.get("expected_source_date") or "") > flow["source_date"]:
                snapshot_notes.append(f"ETF流量尚未更新至{flow['expected_source_date']}"
                                      f"(当前{flow['source_date']})")
            gaps = [f"{days}D" for days in btc_etf_flow.WINDOWS
                    if current_record_value(etf, f"flow_{days}d_usd") is None]
            if gaps:
                snapshot_notes.append("ETF流量窗口数据不足(" + "/".join(gaps) + ")")
        holding = etf.get("holdings_record") or {}
        if holding.get("source_stale"):
            snapshot_notes.append("ETF持仓源日期过期")
        elif holding.get("status") == "unavailable":
            snapshot_notes.append(f"ETF持仓采集失败({holding.get('reason') or 'unknown'})")
        dates = record_value(etf, "source_date") or {}
        if (current_record_value(etf, "holdings_btc") is not None
                and current_record_value(etf, "flow_1d_usd") is not None
                and dates.get("holdings") != dates.get("flow_1d")):
            snapshot_notes.append(f"ETF流量/持仓日期未对齐({dates.get('flow_1d')}/{dates.get('holdings')})")
    if time_reason:
        snapshot_notes.append(f"当前报价/结构时间对齐不可用({time_reason})")
    if signal_quality.get("usable") is not True:
        reasons = "/".join(signal_quality.get("reasons") or ["unknown"])
        snapshot_notes.append(f"结构数据不可用({reasons})")
    market_source = str(market.get("source") or "")
    if market_source and "Binance" not in market_source:
        snapshot_notes.append(f"行情回退:{market_source}；形态仍用Binance闭合K线")
    labels = {
        "funding": "Funding",
        "liquidation": "爆仓",
        "basis": "Basis",
        "etf": "ETF",
        "spot_cvd": "Spot CVD",
        "stablecoin": "Stablecoin",
        "macro": "Macro",
    }
    for key, record in public_records.items():
        if key == "etf" and record.get("flow_record"):
            continue  # The two ETF data domains are described independently above.
        if key == "etf" and record.get("collection_reason"):
            snapshot_notes.append(f"ETF持仓采集失败({record['collection_reason']})")
        elif key == "etf" and record.get("status") == "unavailable":
            snapshot_notes.append(f"ETF持仓采集失败({record.get('reason') or 'unknown'})")
        if record_has_data(record) and not record_is_current(record):
            label = labels.get(key, key)
            if record.get("source_stale_reason"):
                snapshot_notes.append(f"{label}源日期过期({record.get('source_stale_reason')})")
            else:
                hours = max(0.0, float(record.get("age_sec") or 0) / 3600.0)
                snapshot_notes.append(f"{label}({hours:.0f}h)")
    if liquidation_record_collecting(liquidation):
        coverage = (liquidation or {}).get("coverage") or {}
        hours = float(coverage.get("coverage_hours") or 0.0)
        if not coverage.get("ready_4h"):
            snapshot_notes.append(f"爆仓采集中({hours:.1f}h/4H)")
        elif not coverage.get("ready_24h"):
            snapshot_notes.append(f"爆仓24H采集中({hours:.1f}h/24H)")
    if current_record_value(etf, "holding_7d_status") == "collecting":
        snapshot_notes.append("ETF持仓7D采集中(需7D快照)")
    if (stablecoin or {}).get("status") == "partial":
        coverage = (stablecoin or {}).get("coverage") or {}
        snapshot_notes.append(
            "Stablecoin历史覆盖不足"
            f"(24H {float(coverage.get('day_pct') or 0.0):.1f}% / "
            f"7D {float(coverage.get('week_pct') or 0.0):.1f}%)"
        )
    macro_value = current_record_values(macro)
    macro_checks = [
        ("US10Y", macro_value.get("us10y")),
        ("Real Yield", macro_value.get("real_yield_10y")),
        ("USD Index", macro_value.get("usd_index")),
    ]
    macro_missing = [name for name, value in macro_checks if value is None]
    macro_stale = []
    if record_has_data(macro) and not record_is_current(macro):
        macro_stale.append("Macro")
    optional_notes = []
    if not macro_value.get("fed_policy"):
        optional_notes.append("FedWatch未启用")
    if macro_value.get("fed_target_lower") is None and macro_value.get("effective_fed_funds") is None:
        optional_notes.append("Fed状态不可用")
    optional_notes.append("MVRV/NUPL未启用")
    if not record_has_data(basis):
        optional_notes.append("Basis未启用")
    return {
        "available": len(checks) - len(missing) - len(pending),
        "total": len(checks),
        "missing": missing,
        "pending": pending,
        "snapshot_notes": snapshot_notes,
        "optional_notes": optional_notes,
        "core_market": {
            "available": len(checks) - len(missing) - len(pending),
            "total": len(checks),
            "missing": missing,
            "pending": pending,
        },
        "macro": {
            "available": len(macro_checks) - len(macro_missing),
            "total": len(macro_checks),
            "missing": macro_missing,
            "stale": macro_stale,
            "partial": bool(macro_missing or macro_stale),
        },
    }


def format_market_block(payload):
    market = payload["market"]
    fear_greed = payload.get("fear_greed") or {}
    market_24h_delta = payload.get("market_24h_delta") or {}
    vol_fast = (((payload.get("structure_state") or {}).get("checks") or {}).get("volume_fast_expansion") or {})
    price_7d = payload.get("price_7d_pct")
    fg_text = "NA"
    if fear_greed.get("available"):
        fg_text = f"{int(fear_greed.get('value'))}/100 {fear_greed.get('classification_cn')}"
    vol_delta = ((market_24h_delta.get("metrics") or {}).get("vol24"))
    return [
        (
            f"价格 ${market['price']:,.0f} | "
            f"24h {format_pct_value(market.get('chg24'), 2)} | "
            f"7D {format_pct_value(price_7d, 2)}"
        ),
        f"F&G {fg_text} | 市值 {format_money_compact_plain(market.get('mcap'), 2)}",
        (
            f"24h总量 {format_pct_value(vol_delta, 2)} | "
            f"4H量 {format_ratio_value(vol_fast.get('ratio'))}"
        ),
    ]


def format_component_score(value):
    if value is None:
        return "NA"
    try:
        return f"{float(value):.0f}"
    except Exception:
        return "NA"


def macro_direction_text(value, up_text, down_text, flat_text, threshold):
    number = safe_float(value)
    if number is None:
        return None
    if number >= float(threshold):
        return up_text
    if number <= -float(threshold):
        return down_text
    return flat_text


def macro_reason_parts(value, compact=False):
    parts = []
    real = macro_direction_text(
        value.get("real_yield_10y_30d_pp"),
        "真实利率上行" if compact else "真实利率30D上行",
        "真实利率下行" if compact else "真实利率30D下行",
        None,
        0.10,
    )
    us10y = macro_direction_text(
        value.get("us10y_30d_pp"),
        "利率上行" if compact else "名义利率30D上行",
        "利率下行" if compact else "名义利率30D下行",
        None,
        0.10,
    )
    usd = macro_direction_text(
        value.get("usd_index_30d_pct"),
        "美元走强" if compact else "美元30D走强",
        "美元走弱" if compact else "美元30D走弱",
        None,
        0.50,
    )
    fed = None
    if value.get("fed_policy") == "Hawkish":
        fed = "Fed偏鹰"
    elif value.get("fed_policy") == "Dovish":
        fed = "Fed偏鸽"
    for item in (real, usd, us10y, fed):
        if item and item not in parts:
            parts.append(item)
    return parts


def macro_derivation_line(value):
    parts = macro_reason_parts(value)
    if not parts:
        return "宏观环境输入: 暂无明确方向"
    return "宏观环境输入: " + " + ".join(parts)


def macro_rule_result_text(macro_state):
    if not macro_state:
        return "宏观大环境数据不足。"
    environment = macro_state.get("environment") or {}
    return environment.get("status_text") or "宏观大环境维持观察。"


def format_stale_macro_reference(macro):
    value = (macro or {}).get("value") or {}
    return [
        "宏观数据过期或不可用于当前判断，以下仅作历史参考。" + record_background_label(macro),
        (
            f"历史数值：美债10Y {format_rate_value(value.get('us10y'), 2)}"
            f" | 实际利率 {format_rate_value(value.get('real_yield_10y'), 2)}"
            f" | 美元指数 {format_plain_number(value.get('usd_index'), 2)}"
        ),
        (
            f"历史数值：M2同比 {format_pct_value(value.get('m2_yoy_pct'), 2)}"
            f" | 美联储资产负债表30天 {format_pct_value(value.get('fed_balance_sheet_30d_pct'), 2)}"
        ),
        "解读：宏观数据不足，本次不判断当前利率压力和长期资金方向。",
    ]


def format_macro_block(payload):
    macro = ((payload.get("public_records") or {}).get("macro") or {})
    value = (macro.get("value") or {})
    macro_state = ((payload.get("rule_state") or {}).get("macro") or {})
    if not record_has_data(macro):
        reason = macro.get("reason") or "no_macro_snapshot"
        return [
            "宏观大环境: 采集中 / 源数据过期",
            f"缺失: {reason}",
        ]
    if not record_is_current(macro):
        return format_stale_macro_reference(macro)

    lines = []
    lines.extend(format_macro_pressure_lines(value, macro_state))
    lines.extend(format_long_money_lines(value, macro_state))
    lines.append(format_fed_status_line(value))
    lines.extend(format_cost_of_living_lines(value))
    lines.extend(format_fiscal_lines(value, macro_state))
    lines.extend(format_macro_boundary_lines())
    return lines


def format_common_reference(payload):
    record = ((payload.get("public_records") or {}).get("macro") or {})
    hour = record.get("shared_hour")
    stamp = datetime.fromtimestamp(hour, TOKYO_TZ).strftime("%m-%d %H:00 JST") if hour is not None else "最近一期"
    note = "；宏观输入不可用于当前确认" if not record_is_current(record) else ""
    return f"公共宏观与事件：{stamp}（/macro）{note}"


def build_common_report(record, generated_at=None, event_news=None):
    generated_at = time.time() if generated_at is None else generated_at
    macro = select_public_records({}, {"public": {"macro": record}})["macro"]
    for flag in ("stale", "snapshot_used"):
        if record.get(flag):
            macro[flag] = True
    payload = {"public_records": {"macro": macro}}
    payload["rule_state"] = {"macro": build_macro_rule_state(payload, {})}
    stamp = datetime.fromtimestamp(record.get("shared_hour", common.hour_start(generated_at)), TOKYO_TZ).strftime("%m-%d %H:00 JST")
    value = macro.get("value") or {}
    lines = [f"🏛 公共宏观与事件简报 {stamp}｜辅助观察", "", *format_macro_block(payload), "", "📎 宏观数据状态"]
    lines.append("报告生成：" + datetime.fromtimestamp(generated_at, TOKYO_TZ).strftime("%m-%d %H:%M JST"))
    dates = value.get("source_date") or macro.get("source_date") or {}
    if isinstance(dates, dict) and dates:
        lines.append("源观测日期：" + " | ".join(f"{name} {date}" for name, date in dates.items() if date))
    else:
        lines.append("源观测日期：未提供")
    updated = macro.get("updated_at")
    if updated:
        lines.append("采集时间：" + datetime.fromtimestamp(updated, TOKYO_TZ).strftime("%m-%d %H:%M JST"))
    missing = sorted(set((value.get("missing_reason") or {})) | set(value.get("long_term_missing_reason") or {}))
    if missing:
        lines.append("缺失/未启用：" + "、".join(missing))
    if not record_is_current(macro):
        lines.append("当前宏观输入不足或已过期，相关确认保持待确认。")
    if macro.get("publication_time_missing"):
        lines.append("部分来源未提供发布时间；采集时间不代替发布时间。")
    lines.append("具体标的影响结合各自价格、资金与结构判断。")
    event_news = event_news or {"status": "unavailable", "events": []}
    event_lines = ["📰 公共事件｜华尔街见闻"]
    if event_news.get("status") != "ok":
        event_lines.append("事件新闻暂不可用。")
    elif not event_news.get("events"):
        event_lines.append("无新增事件。")
    else:
        for index, event in enumerate(event_news["events"][:news.EVENT_LIMIT], 1):
            displayed = datetime.fromtimestamp(event["display_time"], TOKYO_TZ).strftime("%m-%d %H:%M JST")
            event_lines.extend([f"{index}. {shorten_title(event['title'])}",
                                f"来源显示时间：{displayed}", event["url"]])
    event_text = "\n".join(event_lines)
    # Reserve space for exactly the events recorded in the outbox; never consume
    # a headline that the generic length limiter removed from the sent message.
    return render_report(lines, budget=TG_SAFE_LEN - utf16_length(event_text) - 2) + "\n\n" + event_text


def format_leverage_block(payload):
    market = payload["market"]
    delta4h = payload.get("delta4h") or {}
    public_records = payload.get("public_records") or {}
    funding = public_records.get("funding") or {}
    liquidation = public_records.get("liquidation") or {}
    basis = public_records.get("basis") or {}
    checks = ((payload.get("structure_state") or {}).get("checks") or {})
    oi_slow = checks.get("oi_slow_confirmation") or {}
    f_mark = record_mark(funding)
    b_mark = record_mark(basis)
    return [
        f"未平仓合约总价值 {format_money_compact_plain(market.get('oi_usd'), 2)}",
        (
            f"未平仓合约量变化：4H {format_pct_value(delta4h.get('oi_4h_pct'), 2)} | "
            f"24H {format_pct_value(oi_slow.get('delta_24h_pct'), 2)} | "
            f"72H {format_pct_value(oi_slow.get('delta_72h_pct'), 2)}"
        ),
        (
            f"Funding 8H {format_pct_value(record_value(funding, 'funding_8h_pct'), 3)}{f_mark} | "
            f"24H {format_pct_value(record_value(funding, 'funding_24h_pct'), 3)}{f_mark}"
        ),
        (
            f"Basis {format_pct_value(record_value(basis, 'basis_pct'), 3)}{b_mark} | "
            f"Mark/Index {format_structure_price(record_value(basis, 'mark_price'))}/"
            f"{format_structure_price(record_value(basis, 'index_price'))}"
        ),
        (
            f"爆仓 4H 多{format_liquidation_window_value(liquidation, 'long_4h_usd', '4h')}/"
            f"空{format_liquidation_window_value(liquidation, 'short_4h_usd', '4h')} | "
            f"24H 多{format_liquidation_window_value(liquidation, 'long_24h_usd', '24h')}/"
            f"空{format_liquidation_window_value(liquidation, 'short_24h_usd', '24h')}"
        ),
    ]


def etf_source_date(record):
    value = (record or {}).get("value") or {}
    source_dates = value.get("source_date") or (record or {}).get("source_date") or {}
    if isinstance(source_dates, dict):
        return (
            source_dates.get("flow_1d")
            or source_dates.get("flow_30d")
            or source_dates.get("holdings")
        )
    return source_dates or None


def etf_raw_data_status_line(etf):
    source_date = etf_source_date(etf) or "未知日期"
    if btc_etf.is_holdings_source(etf):
        if current_record_value(etf, "flow_1d_usd") is not None:
            return f"截至 {source_date}；ETF 仅作日级背景，不参与小时形态判断"
        if not record_has_data(etf):
            return "ETF 流量与持仓缺失，不参与当前判断"
        if not record_is_current(etf):
            return f"截至 {source_date}；ETF 持仓过期，仅作背景；美元流量缺失"
        return f"截至 {source_date}；ETF 持仓可作日级背景；美元流量缺失"
    if not record_has_data(etf):
        return "ETF 数据缺失，不参与当前判断"
    if not record_is_current(etf):
        return f"截至 {source_date}；ETF 数据过期，仅作背景，不参与当前判断"
    return f"截至 {source_date}；ETF 仅作日级背景，不参与小时形态判断"


def format_etf_block(payload):
    etf = (payload.get("public_records") or {}).get("etf") or {}
    if btc_etf.is_holdings_source(etf):
        return format_etf_holdings_only(etf) + [etf_raw_data_status_line(etf)]
    mark = record_mark(etf)
    return [
        (
            f"ETF Flow: 1D {format_signed_money_compact(record_value(etf, 'flow_1d_usd'), 1)} "
            f"({format_pct_value(record_value(etf, 'flow_1d_assets_pct'), 2)}){mark} | "
            f"5D {format_signed_money_compact(record_value(etf, 'flow_5d_usd'), 1)} "
            f"({format_pct_value(record_value(etf, 'flow_5d_assets_pct'), 2)}){mark} | "
            f"30D {format_signed_money_compact(record_value(etf, 'flow_30d_usd'), 1)} "
            f"({format_pct_value(record_value(etf, 'flow_30d_assets_pct'), 2)}){mark}"
        ),
        (
            f"ETF Holding: {format_btc_holding(record_value(etf, 'holdings_btc'))}{mark} | "
            f"占比 {format_pct_value(record_value(etf, 'holdings_market_pct'), 2, signed=False)}{mark} | "
            f"7D {format_etf_holding_7d(etf)}"
        ),
        etf_raw_data_status_line(etf),
    ]


def format_etf_holdings_only(etf):
    flow = etf.get("flow_record") or {}
    current_flows = [current_record_value(etf, f"flow_{days}d_usd") for days in btc_etf_flow.WINDOWS]
    if any(amount is not None for amount in current_flows):
        amounts = [f"{days}D {format_signed_money_change(amount) if amount is not None else '数据不足'}"
                   for days, amount in zip(btc_etf_flow.WINDOWS, current_flows)]
        lines = ["BTC现货ETF：" + " | ".join(amounts)
                 + f"（{flow.get('venue') or '未知来源'}，截至 {flow.get('source_date') or '未知日期'}）"]
        if flow.get("source_date") and (flow.get("expected_source_date") or "") > flow["source_date"]:
            lines.append(f"ETF流量待更新：应更新至 {flow['expected_source_date']}，当前仍截至 {flow['source_date']}")
    else:
        lines = ["BTC现货ETF：美元净流量缺失（1D/5D/30D）"]
    if any(amount is not None for amount in current_flows):
        page = btc_etf_flow.source_page(flow.get("venue"))
        if page:
            lines.append(f"ETF流量来源：{flow['venue']} " + page)
    background = etf.get("flow_background") or {}
    if flow.get("source_stale") and flow.get("value"):
        background = {"venue": flow.get("venue"), "value": flow["value"], "source_date": {
            f"flow_{days}d": flow.get("source_date") for days in btc_etf_flow.WINDOWS}}
    values = background.get("value") or {}
    dates = background.get("source_date") or {}
    if isinstance(dates, dict):
        historical = []
        for days in (1, 5, 30):
            key = f"flow_{days}d"
            amount, day = values.get(key + "_usd"), dates.get(key)
            if safe_float(amount) is not None and day:
                historical.append(f"{days}D {format_signed_money_change(amount)}（{day}）")
        if historical and not any(amount is not None for amount in current_flows):
            lines.append("ETF流量历史：" + " | ".join(historical) + "，仅作背景")
            page = btc_etf_flow.source_page(background.get("venue"))
            if page:
                lines.append(f"ETF流量来源：{background['venue']} " + page)
    amount = safe_float(record_value(etf, "holdings_btc"))
    holding = etf.get("holdings_record") or {}
    if amount is None and holding.get("source_stale"):
        amount = safe_float((holding.get("value") or {}).get("holdings_btc"))
    if amount is None:
        lines.append(f"ETF持仓：数据不足（Bitbo，{etf.get('reason') or 'unknown'}）")
    else:
        quantity = f"{amount:,.8f}".rstrip("0").rstrip(".")
        note = "，已过期，仅作背景" if holding.get("source_stale") or not record_is_current(etf) else ""
        holding_date = (record_value(etf, "source_date") or {}).get("holdings")
        lines.append(f"ETF持仓：{quantity} BTC | 7D {format_etf_holding_7d(etf)}"
                     f"（Bitbo，截至 {holding_date or '未知日期'}{note}）")
    return lines


def format_spot_block(payload):
    public_records = payload.get("public_records") or {}
    spot_cvd = public_records.get("spot_cvd") or {}
    stablecoin = public_records.get("stablecoin") or {}
    cvd_mark = record_mark(spot_cvd)
    stable_mark = record_mark(stablecoin)
    return [
        (
            f"Spot CVD: 4H {format_pct_value(record_value(spot_cvd, 'cvd_4h_pct'), 2)}{cvd_mark} | "
            f"24H {format_pct_value(record_value(spot_cvd, 'cvd_24h_pct'), 2)}{cvd_mark}"
        ),
        (
            f"Stablecoin: 24H {format_signed_money_compact(record_value(stablecoin, 'flow_24h_usd'), 1)}{stable_mark} | "
            f"7D {format_signed_money_compact(record_value(stablecoin, 'flow_7d_usd'), 1)}{stable_mark}"
        ),
    ]


def format_signal_block(payload, include_macro=True):
    lines = format_raw_signal_block(payload)
    return lines if include_macro else lines[:lines.index("宏观阶段：")]


def format_final_view(payload, include_macro=True):
    rule_state = payload.get("rule_state") or {}
    macro = rule_state.get("macro") or {}
    leverage = rule_state.get("leverage") or {}
    etf = rule_state.get("etf") or {}
    spot = rule_state.get("spot") or {}
    levels = rule_state.get("levels") or {}
    macro_value = current_record_values((payload.get("public_records") or {}).get("macro"))
    environment = macro.get("environment") or {}
    long_term = macro.get("long_term_environment") or {}

    lines = [
        f"宏观大环境: {environment.get('status_text') or '宏观压制数据不足。'}",
        f"长期货币层: {long_term.get('status_text') or '长期货币层数据不足，维持观察。'}",
        (
            "长期财政/成本: "
            f"{fiscal_status_cn(macro.get('fiscal_status'))}；"
            f"生活成本/工资代理为{cost_gap_cn(macro.get('cost_of_living_gap'))}；"
            "仅作长期配置背景。"
        ),
        (
            "宏观事实: "
            f"US10Y {format_us10y_directional_band_value(macro_value.get('us10y'), macro_value.get('us10y_30d_pp'))}"
            f"({classify_us10y_band(macro_value.get('us10y'))})；"
            f"Real {format_real_yield_directional_band_value(macro_value.get('real_yield_10y'), macro_value.get('real_yield_10y_30d_pp'))}"
            f"({classify_real_yield_band(macro_value.get('real_yield_10y'))})；"
            f"DXY {format_dxy_directional_band_value(macro_value.get('usd_index'), macro_value.get('usd_index_30d_pct'))}"
            f"({classify_dxy_band(macro_value.get('usd_index'))})。"
        ),
        (
            "杠杆: "
            + (
                "触发 OI 扩张 + Funding负向极端规则。"
                if leverage.get("short_attack_rule") is True
                else "命中 OI 收缩 + Funding/Basis 中性规则；未触发空头主动进攻规则。"
                if leverage.get("deleveraging_rule") is True
                else "杠杆规则数据不足，暂不判断。"
                if leverage.get("deleveraging_rule") is None or leverage.get("short_attack_rule") is None
                else "未命中 OI 收缩 + Funding/Basis 中性规则；未触发空头主动进攻规则。"
            )
        ),
        (
            "杠杆阈值: "
            f"OI收缩 {rule_status_text(leverage.get('oi_contraction_rule'))} | "
            f"Funding中性 {rule_status_text(leverage.get('funding_neutral_rule'))} | "
            f"Basis中性 {rule_status_text(leverage.get('basis_neutral_rule'))} | "
            f"Funding负向极端 {rule_status_text(leverage.get('funding_negative_extreme_rule'))}"
        ),
    ]

    if not include_macro:
        lines = [line for line in lines if not line.startswith(("宏观大环境:", "长期货币层:", "长期财政/成本:", "宏观事实:"))]
    lines.append(plain_etf_flow_line(etf))
    if ((etf.get("consistency") or {}).get("flow_5d_inconsistent")):
        lines.append("ETF日级背景: 5D Flow 连续快照异常跳变，5D值不参与日级规则。")

    stable_text = stable_signal_text(spot)
    if spot.get("cvd_weak_rule") is True:
        lines.append(f"现货/稳定币: Spot CVD 为负，追多确认条件未满足；{stable_text}")
    elif spot.get("cvd_positive_rule") is True:
        lines.append(f"现货/稳定币: Spot CVD 转正；{stable_text}")
    else:
        lines.append(f"现货/稳定币: Spot CVD 规则数据不足或中性；{stable_text}")

    trend = levels.get("price_trend") or {}
    time_summary = (payload.get("market_time_context") or {}).get("summary_text")
    if time_summary:
        lines.append(f"市场结构: {time_summary}")
    elif levels.get("support_breakdown_confirm_rule") is True:
        if trend_support_breakdown_phase(trend) == "confirmation_event":
            lines.append("市场结构: 原支撑本轮跌破确认，风险状态生效。")
        else:
            lines.append("市场结构: 价格仍在原支撑下方，失守状态尚未解除。")
    elif levels.get("support_reclaim_watch_rule") is True:
        lines.append("市场结构: 价格已回到原短线支撑上方，但 4H Spot CVD 尚未转正；保留原锚点并等待资金确认。")
    elif levels.get("support_breakdown_watch_rule") is True:
        lines.append("市场结构: 价格跌破短线支撑，等待连续收盘和资金转弱确认。")
    elif levels.get("breakout_confirm_rule") is True:
        lines.append("市场结构: 价格连续站上短线压力，且资金三选二确认，突破确认规则命中。")
    elif levels.get("post_support_pressure_returned_rule") is True:
        lines.append("市场结构: 支撑首次测试后卖压形态重新出现，关注是否再次测试或跌破支撑。")
    elif levels.get("post_support_consolidation_confirm_rule") is True:
        lines.append("市场结构: 支撑首次测试后的缩量整理已确认，方向仍未选择，尚未形成反转。")
    elif levels.get("post_support_consolidation_watch_rule") is True:
        lines.append("市场结构: 支撑首次测试后进入缩量整理观察，尚未形成反转。")
    elif levels.get("support_tested_rule") is True:
        lines.append("市场结构: 支撑首次测试完成，正在观察下跌是否延续及卖压是否重新出现。")
    elif levels.get("range_rule") is True:
        lines.append("市场结构: 价格处于短线支撑至短线压力区间，区间规则命中；不做方向确认。")
    else:
        lines.append("市场结构: 未命中突破确认或跌破确认规则，按上下沿继续记录。")
    if trend:
        funding = trend.get("funding_confirmation") or {}
        lines.append(
            "结构状态: "
            f"位置 {trend.get('position_text') or '数据不足'} | "
            f"压力 {trend.get('pressure_state_text') or pressure_state_text(trend.get('pressure_state'))} | "
            f"支撑 {trend.get('support_state_text') or support_state_text(trend.get('support_state'))}"
        )
        lines.append(
            "结构计数: "
            f"站上短线压力连续 {int(trend.get('above_pressure_hours') or 0)}H | "
            f"跌破短线支撑连续 {int(trend.get('below_support_hours') or 0)}H | "
            f"上破资金 {int(funding.get('up_hit_count') or 0)}/3 | "
            f"下破资金 {int(funding.get('down_hit_count') or 0)}/3"
        )

    return lines


def format_level_block(payload):
    levels = payload.get("levels") or {}
    rule_levels = ((payload.get("rule_state") or {}).get("levels") or {})
    trend = rule_levels.get("price_trend") or {}
    time_receipt = format_market_time_receipt_line(payload)
    funding = trend.get("funding_confirmation") or {}
    support_anchor = safe_float(trend.get("support_anchor"))
    rolling_support = safe_float(trend.get("rolling_short_support"))
    if rolling_support is None:
        rolling_support = safe_float(levels.get("short_support"))
    lines = []
    if time_receipt:
        lines.append(time_receipt)
    if trend.get("status") == "breakdown_confirmed" and support_anchor is not None:
        roles = confirmed_breakdown_level_roles(trend, levels)
        current_support = roles.get("current_support")
        current_support_text = (
            f"当前支撑 {format_structure_price(current_support)}"
            if current_support is not None
            else "当前支撑尚未形成"
        )
        lines.extend([
            f"{current_support_text} | 短线压力 {format_structure_price(levels.get('short_resistance'))}",
            f"原支撑（收回观察位） {format_structure_price(support_anchor)}",
            f"趋势支撑 {format_structure_price(levels.get('trend_support'))} | 趋势压力 {format_structure_price(levels.get('trend_resistance'))}",
        ])
    elif support_anchor is not None:
        rolling_label = "下一潜在支撑" if rolling_support is not None and rolling_support < support_anchor else "当前滚动支撑"
        anchor_label = "已收回待确认锚点" if trend.get("status") == "support_reclaim_watch" else "跌破观察锚点"
        lines.extend([
            f"{anchor_label} {format_structure_price(support_anchor)} | {rolling_label} {format_structure_price(rolling_support)}",
            f"短线压力 {format_structure_price(levels.get('short_resistance'))}",
            f"趋势支撑 {format_structure_price(levels.get('trend_support'))} | 趋势压力 {format_structure_price(levels.get('trend_resistance'))}",
        ])
    else:
        lines.extend([
            f"短线支撑 {format_structure_price(levels.get('short_support'))} | 短线压力 {format_structure_price(levels.get('short_resistance'))}",
            f"趋势支撑 {format_structure_price(levels.get('trend_support'))} | 趋势压力 {format_structure_price(levels.get('trend_resistance'))}",
        ])
    lines.extend([
        (
            f"{'上一完整小时位置' if time_receipt else '位置'} "
            f"{trend.get('position_text') or levels.get('position_text') or '数据不足'} | "
            f"压力 {rule_levels.get('pressure_state_text') or '数据不足'} | "
            f"支撑 {rule_levels.get('support_state_text') or '数据不足'}"
        ),
        (
            f"上破资金 {int(funding.get('up_hit_count') or 0)}/3 | "
            f"下破资金 {int(funding.get('down_hit_count') or 0)}/3"
        ),
        (
            f"旧调试: 支撑 {format_structure_price(levels.get('support'))} | "
            f"突破位 {format_structure_price(levels.get('breakout_level'))} | "
            f"确认价 {format_structure_price(levels.get('confirmation_price'))}"
        ),
        (
            f"旧调试: 突破 {status_icon(levels.get('breakout_ok'))} | "
            f"确认 {status_icon(levels.get('confirmation_ok'))}"
        ),
    ])
    return lines


def format_news_lines(titles):
    if not titles:
        return ["新闻标题: 无新增"]
    lines = []
    for idx, title in enumerate(titles[:NEWS_MAX_ITEMS], 1):
        lines.append(f"{idx}. {shorten_title(title)}")
    return lines


def format_completeness_block(payload, include_macro=True):
    completeness = payload.get("completeness") or {}
    core = completeness.get("core_market") or {}
    macro = completeness.get("macro") or {}
    missing = core.get("missing") or completeness.get("missing") or []
    macro_missing = macro.get("missing") or []
    macro_stale = macro.get("stale") or []
    snapshots = completeness.get("snapshot_notes") or []
    optional = completeness.get("optional_notes") or []
    lines = [
        (
            f"核心市场 {core.get('available', completeness.get('available', 0))}/"
            f"{core.get('total', completeness.get('total', 0))} | "
            f"宏观 {macro.get('available', 0)}/{macro.get('total', 0)}"
        ),
        "市场缺失: " + (" / ".join(missing) if missing else "无"),
        "宏观缺失: " + (" / ".join(macro_missing) if macro_missing else "无"),
        "宏观过期: " + (" / ".join(macro_stale) if macro_stale else "无"),
        "快照: " + (" / ".join(snapshots) if snapshots else "无"),
        "可选: " + (" / ".join(optional) if optional else "无"),
    ]
    if not include_macro:
        lines[0] = lines[0].split(" | 宏观", 1)[0]
        lines = [line for line in lines if not line.startswith(("宏观缺失:", "宏观过期:"))]
        lines[-2] = "快照: " + (" / ".join(item for item in snapshots if not item.startswith("Macro")) or "无")
        lines[-1] = "可选: " + (" / ".join(item for item in optional if not item.startswith("Fed")) or "无")
    return lines


def status_layer_status(payload, key):
    return (((payload.get("status_layer") or {}).get(key) or {}).get("status")) or "数据不足"


def format_status_layer_macro_block(payload):
    return [
        f"利率环境：{status_layer_status(payload, 'rate_environment')}",
        f"长期流动性：{status_layer_status(payload, 'long_term_liquidity')}",
    ]


def format_status_layer_funds_block(payload):
    return [
        f"ETF资金：{status_layer_status(payload, 'etf_funds')}",
        f"杠杆环境：{status_layer_status(payload, 'leverage_environment')}",
        f"现货资金：{status_layer_status(payload, 'spot_funds')}",
    ]


def format_raw_data_block(payload):
    market = payload["market"]
    public_records = payload.get("public_records") or {}
    macro_value = ((public_records.get("macro") or {}).get("value") or {})
    etf = public_records.get("etf") or {}
    funding = public_records.get("funding") or {}
    basis = public_records.get("basis") or {}
    spot_cvd = public_records.get("spot_cvd") or {}
    stablecoin = public_records.get("stablecoin") or {}
    delta4h = payload.get("delta4h") or {}
    checks = ((payload.get("structure_state") or {}).get("checks") or {})
    oi_slow = checks.get("oi_slow_confirmation") or {}
    buyback = macro_value.get("treasury_buyback") or {}

    return [
        (
            f"BTC：{format_structure_price(market.get('price'))} | "
            f"24H {format_pct_value(market.get('chg24'), 2)} | "
            f"7D {format_pct_value(payload.get('price_7d_pct'), 2)}"
        ),
        (
            f"US10Y：{format_rate_value(macro_value.get('us10y'), 2)} | "
            f"30D {format_pp_value(macro_value.get('us10y_30d_pp'), 2)}"
        ),
        (
            f"Real：{format_rate_value(macro_value.get('real_yield_10y'), 2)} | "
            f"30D {format_pp_value(macro_value.get('real_yield_10y_30d_pp'), 2)}"
        ),
        f"M2：YoY {format_pct_value(macro_value.get('m2_yoy_pct'), 2)}",
        f"Fed Balance Sheet：30D {format_pct_value(macro_value.get('fed_balance_sheet_30d_pct'), 2)}",
        (
            f"Treasury Buyback：次数 {compact_number(buyback.get('recent_count'), 0)} | "
            f"金额 {format_money_compact_plain(buyback.get('total_par_amt_accepted'), 2)}"
        ),
        (
            f"ETF：1D {format_signed_money_compact(record_value(etf, 'flow_1d_usd'), 1)} | "
            f"5D {format_signed_money_compact(record_value(etf, 'flow_5d_usd'), 1)} | "
            f"30D {format_signed_money_compact(record_value(etf, 'flow_30d_usd'), 1)}"
        ),
        (
            f"OI：总量 {format_money_compact_plain(market.get('oi_usd'), 2)} | "
            f"4H {format_pct_value(delta4h.get('oi_4h_pct'), 2)} | "
            f"24H {format_pct_value(oi_slow.get('delta_24h_pct'), 2)} | "
            f"72H {format_pct_value(oi_slow.get('delta_72h_pct'), 2)}"
        ),
        (
            f"Funding：8H {format_pct_value(record_value(funding, 'funding_8h_pct'), 3)} | "
            f"24H {format_pct_value(record_value(funding, 'funding_24h_pct'), 3)}"
        ),
        f"Basis：{format_pct_value(record_value(basis, 'basis_pct'), 3)}",
        (
            f"Spot CVD：4H {format_pct_value(record_value(spot_cvd, 'cvd_4h_pct'), 2)} | "
            f"24H {format_pct_value(record_value(spot_cvd, 'cvd_24h_pct'), 2)}"
        ),
        (
            f"Stablecoin Flow：24H {format_signed_money_compact(record_value(stablecoin, 'flow_24h_usd'), 1)} | "
            f"7D {format_signed_money_compact(record_value(stablecoin, 'flow_7d_usd'), 1)}"
        ),
    ]


def format_market_structure_raw_block(payload):
    levels = payload.get("levels") or {}
    trend = ((((payload.get("rule_state") or {}).get("levels") or {}).get("price_trend")) or {})
    support_anchor = safe_float(trend.get("support_anchor"))
    rolling_support = safe_float(trend.get("rolling_short_support"))
    if rolling_support is None:
        rolling_support = safe_float(levels.get("short_support"))
    lines = [
        f"位置：{levels.get('position_text') or '数据不足'}",
    ]
    if trend.get("status") == "breakdown_confirmed" and support_anchor is not None:
        roles = confirmed_breakdown_level_roles(trend, levels)
        current_support = roles.get("current_support")
        lines.extend([
            (
                f"当前支撑：{format_structure_price(current_support)}"
                if current_support is not None
                else "当前支撑：尚未形成"
            ),
            f"原支撑（收回观察位）：{format_structure_price(support_anchor)}",
        ])
    elif support_anchor is not None:
        rolling_label = "下一潜在支撑" if rolling_support is not None and rolling_support < support_anchor else "当前滚动支撑"
        anchor_label = "已收回待确认锚点" if trend.get("status") == "support_reclaim_watch" else "跌破观察锚点"
        lines.extend([
            f"{anchor_label}：{format_structure_price(support_anchor)}",
            f"{rolling_label}：{format_structure_price(rolling_support)}",
        ])
    else:
        lines.append(f"短线支撑：{format_structure_price(levels.get('short_support'))}")
    lines.extend([
        f"短线压力：{format_structure_price(levels.get('short_resistance'))}",
        f"趋势支撑：{format_structure_price(levels.get('trend_support'))}",
        f"趋势压力：{format_structure_price(levels.get('trend_resistance'))}",
    ])
    return lines


def compact_number(value, decimals=2):
    value = safe_float(value)
    if value is None:
        return "NA"
    try:
        text = f"{float(value):.{int(decimals)}f}"
    except Exception:
        return "NA"
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text in ("", "-0") else text


def compact_money(value, decimals=0):
    value = safe_float(value)
    if value is None:
        return "NA"
    try:
        number = float(value)
    except Exception:
        return "NA"
    sign = "+" if number > 0 else "-" if number < 0 else ""
    abs_value = abs(number)
    units = (
        (1_000_000_000_000, "T"),
        (1_000_000_000, "B"),
        (1_000_000, "M"),
        (1_000, "K"),
    )
    for threshold, suffix in units:
        if abs_value >= threshold:
            return f"{sign}{compact_number(abs_value / threshold, decimals)}{suffix}"
    return f"{sign}{compact_number(abs_value, decimals)}"


def compact_level(value):
    value = safe_float(value)
    if value is None:
        return "NA"
    try:
        return f"{float(value):,.2f}" if assets.current().equity else f"{float(value):,.0f}"
    except Exception:
        return "NA"


def compact_time_label(now_tokyo):
    dated_match = re.search(
        r"\b\d{4}-(\d{2})-(\d{2})\s+(\d{2}:\d{2})\b", now_tokyo or ""
    )
    if dated_match:
        return f"{dated_match.group(1)}-{dated_match.group(2)} {dated_match.group(3)} JST"
    match = re.search(r"\b(\d{2}:\d{2})\b", now_tokyo or "")
    if match:
        return f"{match.group(1)} JST"
    return now_tokyo or "JST"


def format_market_time_ms(value, closed_bar=False):
    try:
        timestamp_ms = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if timestamp_ms <= 0:
        return None
    if closed_bar:
        timestamp_ms += 1
    return datetime.fromtimestamp(timestamp_ms / 1000.0, tz=TOKYO_TZ).strftime("%H:%M JST")


def market_time_sources_compatible(current_price_source, signal_source):
    return (
        current_price_source in ("Binance Spot 24hr", "Binance Futures 24hr")
        and signal_source in (
            "Binance Futures 1h Kline + OI Hist",
            "Binance Futures 1h Kline + Hyperliquid BTC Perp OI",
        )
    )


def market_time_reference_support(trend, levels):
    trend = trend or {}
    levels = levels or {}
    support_anchor = safe_float(trend.get("support_anchor"))
    support_test_anchor = safe_float(trend.get("support_test_anchor"))
    rolling_support = safe_float(trend.get("rolling_short_support"))
    if rolling_support is None:
        rolling_support = safe_float(levels.get("short_support"))
    if support_anchor is not None:
        return support_anchor, "support_anchor"
    if support_test_anchor is not None and trend.get("status") in {
        "support_tested",
        "post_support_consolidation_watch",
        "post_support_consolidation_confirmed",
        "post_support_pressure_returned",
    }:
        return support_test_anchor, "support_test_anchor"
    return rolling_support, "rolling_short_support"


def market_time_ineligible_reason_text(reason):
    return {
        "signal_data_unusable": "上一完整小时数据未对齐",
        "closed_bar_missing": "上一完整小时价格或时间缺失",
        "current_price_missing": "推送时现价缺失",
        "current_quote_time_missing": "报价源时间缺失",
        "current_quote_receipt_missing": "报价抓取时间缺失",
        "current_quote_source_future": "报价源时间异常",
        "current_quote_source_stale": "报价源时间过期",
        "source_mismatch": "现价与结构价位来源不同",
        "time_regression": "报价源时间早于上一完整小时",
        "levels_missing": "支撑或压力数据缺失",
    }.get(reason, "时间或价位数据不足")


def build_market_time_context(payload):
    payload = payload or {}
    market = payload.get("market") or {}
    structure = payload.get("structure_state") or {}
    quality = payload.get("signal_data_quality") or {}
    levels = payload.get("levels") or {}
    rule_levels = ((payload.get("rule_state") or {}).get("levels") or {})
    trend = rule_levels.get("price_trend") or {}

    closed_bar_as_of_ms = market_pattern_bucket(quality.get("latest_close_ms"))
    closed_price = safe_float(structure.get("latest_close"))
    current_quote_at_ms = market_pattern_bucket(market.get("current_quote_at_ms"))
    quote_source_at_ms = epoch_ms(market.get("quote_source_as_of_ms"))
    retrieved = safe_float(market.get("quote_retrieved_at"))
    quote_retrieved_at_ms = epoch_ms(int(retrieved * 1000)) if retrieved is not None else current_quote_at_ms
    current_price = safe_float(market.get("price"))
    current_price_source = market.get("source")
    signal_source = quality.get("source")
    support_test_anchor = safe_float(trend.get("support_test_anchor"))
    support_anchor = safe_float(trend.get("support_anchor"))
    rolling_short_support = safe_float(trend.get("rolling_short_support"))
    if rolling_short_support is None:
        rolling_short_support = safe_float(levels.get("short_support"))
    short_resistance = safe_float(levels.get("short_resistance"))
    if short_resistance is None:
        short_resistance = safe_float((trend.get("levels") or {}).get("short_resistance"))
    reference_support, reference_support_kind = market_time_reference_support(trend, levels)
    breakdown_phase = trend_support_breakdown_phase(trend)
    breakdown_roles = confirmed_breakdown_level_roles(trend, levels)

    ineligible_reason = None
    if quality.get("usable") is not True:
        ineligible_reason = "signal_data_unusable"
    elif closed_bar_as_of_ms is None or closed_price is None:
        ineligible_reason = "closed_bar_missing"
    elif current_price is None:
        ineligible_reason = "current_price_missing"
    elif quote_source_at_ms is None:
        ineligible_reason = "current_quote_time_missing"
    elif quote_retrieved_at_ms is None:
        ineligible_reason = "current_quote_receipt_missing"
    elif quote_source_at_ms > quote_retrieved_at_ms:
        ineligible_reason = "current_quote_source_future"
    elif quote_retrieved_at_ms - quote_source_at_ms > HYPERLIQUID_CTX_STALE_SEC * 1000:
        ineligible_reason = "current_quote_source_stale"
    elif not market_time_sources_compatible(current_price_source, signal_source):
        ineligible_reason = "source_mismatch"
    elif quote_source_at_ms < closed_bar_as_of_ms:
        ineligible_reason = "time_regression"
    elif reference_support is None or short_resistance is None:
        ineligible_reason = "levels_missing"

    relation_eligible = ineligible_reason is None
    support_distance_usd = None
    support_distance_pct = None
    resistance_distance_usd = None
    resistance_distance_pct = None
    current_relation = "unavailable"
    if relation_eligible:
        support_distance_usd = current_price - reference_support
        support_distance_pct = pct(current_price, reference_support)
        resistance_distance_usd = short_resistance - current_price
        resistance_distance_pct = (
            resistance_distance_usd / short_resistance * 100.0
            if short_resistance
            else None
        )
        if current_price < reference_support:
            current_relation = "below_support"
        elif breakdown_phase is not None and reference_support_kind == "support_anchor":
            current_relation = "temporarily_reclaimed_former_support"
        elif current_price >= short_resistance:
            current_relation = "above_resistance"
        else:
            current_relation = "inside_range"

    market_patterns = payload.get("market_patterns") or {}
    down_pattern_states = {
        key: ((market_patterns.get(key) or {}).get("state"))
        for key in ("spot_active_sell", "new_short_pressure", "deleveraging_down")
    }
    return {
        "schema": "btc_market_time_context",
        "revision": 2,
        "closed_bar_as_of_ms": closed_bar_as_of_ms,
        "closed_price": closed_price,
        "current_quote_at_ms": None if assets.current().equity else current_quote_at_ms,
        "quote_source_as_of_ms": quote_source_at_ms,
        "quote_retrieved_at_ms": quote_retrieved_at_ms,
        "current_price": current_price,
        "current_price_source": current_price_source,
        "signal_source": signal_source,
        "support_anchor": support_anchor,
        "support_test_anchor": support_test_anchor,
        "rolling_short_support": rolling_short_support,
        "reference_support": reference_support,
        "reference_support_kind": reference_support_kind,
        "short_resistance": short_resistance,
        "structure_status": trend.get("status"),
        "structure_bucket": trend.get("bucket"),
        "support_breakdown_confirmed_bucket": trend.get("support_breakdown_confirmed_bucket"),
        "breakdown_phase": breakdown_phase,
        "former_support": breakdown_roles.get("former_support"),
        "current_support": breakdown_roles.get("current_support"),
        "above_pressure_hours": int(trend.get("above_pressure_hours") or 0),
        "required_confirm_hours": assets.current().parameter("PRICE_TREND_CONFIRM_HOURS", PRICE_TREND_CONFIRM_HOURS),
        "down_pattern_states": down_pattern_states,
        "down_patterns_clear": all(value == "NO" for value in down_pattern_states.values()),
        "relation_eligible": relation_eligible,
        "ineligible_reason": ineligible_reason,
        "current_relation": current_relation,
        "support_distance_usd": support_distance_usd,
        "support_distance_pct": support_distance_pct,
        "resistance_distance_usd": resistance_distance_usd,
        "resistance_distance_pct": resistance_distance_pct,
    }


def market_time_history_clause(context):
    context = context or {}
    closed_time = format_market_time_ms(context.get("closed_bar_as_of_ms"), closed_bar=True)
    closed_price = safe_float(context.get("closed_price"))
    status = context.get("structure_status")
    support = safe_float(context.get("reference_support"))
    pressure = safe_float(context.get("short_resistance"))
    if closed_time is None or closed_price is None:
        return "截至上一完整小时，结构数据不足"

    closed_text = compact_level(closed_price)
    if status in {
        "support_tested",
        "post_support_consolidation_watch",
        "post_support_consolidation_confirmed",
        "post_support_pressure_returned",
    } and support is not None:
        clause = (
            f"截至 {closed_time}，{compact_level(support)} 支撑已完成首次回测，"
            f"上一完整小时收于 {closed_text}"
        )
        clause += "，未跌破支撑" if closed_price >= support else "，已收于支撑下方"
        if status == "post_support_consolidation_watch":
            clause += "；回测后处于缩量整理观察"
        elif status == "post_support_consolidation_confirmed":
            clause += "；回测后的缩量整理已确认"
        elif status == "post_support_pressure_returned":
            clause += "；回测后下行形态重新出现"
        return clause
    if status == "breakdown_confirmed" and support is not None:
        if context.get("breakdown_phase") == "confirmation_event":
            return (
                f"截至 {closed_time}，上一完整小时收于 {closed_text}，"
                f"{compact_level(support)} 原支撑本轮跌破确认"
            )
        return (
            f"截至 {closed_time}，上一完整小时收于 {closed_text}，"
            f"仍在 {compact_level(support)} 原支撑下方"
        )
    if status == "breakdown_watch" and support is not None:
        return (
            f"截至 {closed_time}，上一完整小时收于 {closed_text}，"
            f"{compact_level(support)} 支撑处于跌破观察"
        )
    if status == "support_reclaim_watch" and support is not None:
        return (
            f"截至 {closed_time}，上一完整小时收于 {closed_text}，"
            f"已回到 {compact_level(support)} 原支撑上方，但资金确认尚未完成"
        )
    if status in ("breakout_attempt", "breakout_hold", "trend_confirmed", "pullback_watch") and pressure is not None:
        status_text = {
            "breakout_attempt": "突破尝试",
            "breakout_hold": "连续站上但延续尚未确认",
            "trend_confirmed": "突破确认",
            "pullback_watch": "突破后的回踩观察",
        }[status]
        return (
            f"截至 {closed_time}，上一完整小时收于 {closed_text}，"
            f"{compact_level(pressure)} 压力处于{status_text}"
        )
    if support is not None and pressure is not None:
        return (
            f"截至 {closed_time}，上一完整小时收于 {closed_text}，"
            f"仍在 {compact_level(support)} 支撑与 {compact_level(pressure)} 压力之间"
        )
    return f"截至 {closed_time}，上一完整小时收于 {closed_text}"


def market_time_current_clause(context):
    context = context or {}
    quote_time = format_market_time_ms(context.get("current_quote_at_ms"))
    current_price = safe_float(context.get("current_price"))
    if current_price is None:
        return "推送时现价数据不足"
    time_text = quote_time or "推送时"
    price_text = compact_level(current_price)
    if context.get("relation_eligible") is not True:
        reason = market_time_ineligible_reason_text(context.get("ineligible_reason"))
        return f"{time_text} 现价 {price_text}；{reason}，本次不判断与关键位的关系"

    relation = context.get("current_relation")
    support = safe_float(context.get("reference_support"))
    pressure = safe_float(context.get("short_resistance"))
    support_usd = abs(safe_float(context.get("support_distance_usd")) or 0.0)
    support_pct = abs(safe_float(context.get("support_distance_pct")) or 0.0)
    resistance_usd = abs(safe_float(context.get("resistance_distance_usd")) or 0.0)
    resistance_pct = abs(safe_float(context.get("resistance_distance_pct")) or 0.0)
    if relation == "below_support":
        if context.get("breakdown_phase") is not None:
            return (
                f"{time_text} 现价 {price_text}，仍低于 {compact_level(support)} 原支撑 "
                f"{compact_level(support_usd)}（{support_pct:.2f}%）"
            )
        return (
            f"{time_text} 现价 {price_text}，暂时低于 {compact_level(support)} 支撑 "
            f"{compact_level(support_usd)}（{support_pct:.2f}%），等待完整 1H 收盘确认"
        )
    if relation == "temporarily_reclaimed_former_support":
        return (
            f"{time_text} 现价 {price_text}，暂时回到 {compact_level(support)} 原支撑上方，"
            "等待完整 1H 收盘确认收回"
        )
    if relation == "above_resistance":
        return (
            f"{time_text} 现价 {price_text}，暂时高于 {compact_level(pressure)} 压力 "
            f"{compact_level(resistance_usd)}（{resistance_pct:.2f}%），等待完整 1H 收盘确认"
        )
    return (
        f"{time_text} 现价 {price_text}，较支撑高 {compact_level(support_usd)}"
        f"（+{support_pct:.2f}%），距 {compact_level(pressure)} 压力尚差 "
        f"{compact_level(resistance_usd)}（{resistance_pct:.2f}%）"
    )


def market_time_confirmation_clause(context):
    context = context or {}
    status = context.get("structure_status")
    if status == "trend_confirmed":
        return "短线突破已由完整小时确认"
    if status == "breakdown_confirmed":
        if context.get("breakdown_phase") == "confirmation_event":
            return "本轮支撑跌破已由完整小时确认"
        return "跌破事件已发生，当前仍未收回原支撑"
    if status == "support_reclaim_watch":
        return "价格收回不等于资金确认，原锚点暂不解除"
    if context.get("relation_eligible") is not True:
        return "当前关系数据不足，不新增结构判断"
    if context.get("current_relation") in ("above_resistance", "below_support"):
        return "现价穿越仅作临时位置记录，不提前改变完整小时状态"
    required = max(1, int(context.get("required_confirm_hours") or assets.current().parameter("PRICE_TREND_CONFIRM_HOURS", PRICE_TREND_CONFIRM_HOURS)))
    required_text = "两个" if required == 2 else f"{required} 个"
    if status in ("breakout_attempt", "breakout_hold"):
        completed = max(0, int(context.get("above_pressure_hours") or 0))
        return f"已连续 {completed} 个完整小时收于压力上方，尚未满足连续{required_text}完整小时确认"
    return f"尚无连续{required_text}完整小时收于压力上方，短线突破未确认"


def build_market_time_summary(context):
    return "；".join([
        market_time_history_clause(context),
        market_time_current_clause(context),
        market_time_confirmation_clause(context),
    ]) + "。"


def format_market_time_receipt_line(payload):
    context = (payload or {}).get("market_time_context") or {}
    closed_time = format_market_time_ms(context.get("closed_bar_as_of_ms"), closed_bar=True)
    quote_time = format_market_time_ms(context.get("current_quote_at_ms"))
    closed_price = compact_level(context.get("closed_price"))
    current_price = compact_level(context.get("current_price"))
    if closed_time and quote_time and closed_price != "NA" and current_price != "NA":
        return (
            f"时间对齐：上一完整小时 {closed_time} 收盘 {closed_price} | "
            f"推送现价 {quote_time} {current_price}"
        )
    if closed_time and closed_price != "NA":
        return f"时间对齐：上一完整小时 {closed_time} 收盘 {closed_price} | 推送现价数据不足"
    return None


def compact_fear_greed_line(fear_greed):
    if not (fear_greed or {}).get("available"):
        return "😐 F&G NA"
    value = safe_float(fear_greed.get("value"))
    if value is None:
        return "😐 F&G NA"
    emoji = "😨" if value <= 25 else "🤑" if value >= 75 else "😐"
    return f"{emoji} F&G {int(value)}"


def format_plain_short_fear_greed_line(fear_greed):
    if not fear_greed_is_current(fear_greed):
        return "市场情绪：数据不足"
    value = safe_float(fear_greed.get("value"))
    if value is None:
        return "市场情绪：数据不足"
    classification = fear_greed.get("classification_cn") or "状态未知"
    return f"市场情绪：{classification}（{int(value)}/100）"


def compact_macro_status(macro_state):
    environment = (macro_state or {}).get("environment") or {}
    text = environment.get("status_text")
    return text[:-1] if text and text.endswith("。") else text or "数据不足"


def fed_proxy_text(fed_proxy):
    rule = (fed_proxy or {}).get("easing_proxy_rule")
    if rule is True:
        return "Fed降息代理：命中"
    if rule is False:
        return "Fed未转鸽代理：未确认"
    return "Fed代理：数据不足"


def easing_trade_text(rule):
    if rule is True:
        return "确认"
    if rule is False:
        return "未确认"
    return "数据不足"


def stable_signal_text(spot, with_icon=False):
    prefix_ok = "✅ " if with_icon else ""
    prefix_warn = "⚠ " if with_icon else ""
    prefix_no = "❌ " if with_icon else ""
    prefix_unknown = "⚪ " if with_icon else ""
    cvd_4h = safe_float((spot or {}).get("cvd_4h_pct"))
    if (spot or {}).get("stablecoin_sustained_rule") is True:
        if cvd_4h is None:
            return f"{prefix_warn}稳定币供给增加，是否转化为 BTC 买盘待确认"
        if cvd_4h > 0:
            return f"{prefix_ok}稳定币供给增加，BTC 4H 现货买盘同步转正"
        return f"{prefix_warn}稳定币供给增加，但尚未转化为 BTC 现货买盘"
    if (spot or {}).get("stablecoin_short_term_rule") is True:
        return f"{prefix_warn}稳定币供给短期增加，7D 尚未确认"
    if (spot or {}).get("stablecoin_24h_positive_rule") is False:
        return f"{prefix_no}稳定币供给增加未确认"
    return f"{prefix_unknown}稳定币供给数据不足"


def compact_signal_line(hit, label):
    if hit is True:
        return f"✅ {label}"
    if hit is False:
        return f"❌ {label}"
    return f"⚪ {label}"


def compact_breakout_line(levels):
    breakout = (levels or {}).get("breakout_ok")
    if breakout is True:
        return "✅ 已突破"
    if breakout is False:
        return "❌ 未突破"
    return "⚪ 突破数据不足"


def compact_action_line(payload):
    levels = (((payload.get("rule_state") or {}).get("levels")) or {})
    if levels.get("breakdown_risk_rule") is True:
        return "⚠ 追多关闭"
    if levels.get("breakout_confirm_rule") is True:
        return "✅ 突破确认"
    return "❌ 追多确认不足"


def plain_stable_reason(spot):
    return stable_signal_text(spot, with_icon=False)


def plain_stable_signal_line(spot):
    return stable_signal_text(spot, with_icon=True)


def plain_etf_flow_line(etf):
    flow_1d = safe_float((etf or {}).get("flow_1d_usd"))
    flow_5d = safe_float((etf or {}).get("flow_5d_usd"))
    source_date = (etf or {}).get("source_date") or "未知日期"
    if (etf or {}).get("eligible_for_rules") is False:
        if (etf or {}).get("status") == "data_stale":
            return f"⚪ ETF 数据过期，仅作背景，不参与当前判断（截至 {source_date}）"
        return "⚪ ETF 流量数据不足"
    if (etf or {}).get("flow_outflow_rule") is True:
        return f"❌ ETF 最近交易日偏流出（截至 {source_date}）"
    if (etf or {}).get("flow_outflow_rule") is False:
        if rule_gt(flow_1d, 0.0) is True and rule_lt(flow_5d, 0.0) is True:
            return f"⚠ ETF 最近交易日偏流入，5D 仍偏流出（截至 {source_date}）"
        if rule_any(rule_gt(flow_1d, 0.0), rule_gt(flow_5d, 0.0)) is True:
            return f"✅ ETF 最近交易日偏流入（截至 {source_date}）"
        return f"⚪ ETF 最近交易日流向中性（截至 {source_date}）"
    return "⚪ ETF 日级背景数据不足"


def plain_leverage_signal_line(leverage):
    if (leverage or {}).get("short_attack_rule") is True:
        return "⚠ 杠杆扩张但Funding偏空"
    oi_72h_context = (leverage or {}).get("oi_72h_context")
    if (leverage or {}).get("oi_mixed_rule") is True:
        return "⚠ 杠杆短期方向分化"
    if (leverage or {}).get("oi_contraction_rule") is True:
        if oi_72h_context == "accumulating":
            return "⚠ 杠杆短线降温，72H累计仓位仍偏高"
        return "❌ 杠杆资金偏收缩"
    if (leverage or {}).get("oi_expansion_rule") is True:
        if oi_72h_context == "declining":
            return "⚠ 杠杆短线回升，72H累计仓位仍偏低"
        return "✅ 杠杆资金偏扩张"
    if (leverage or {}).get("oi_short_term_status") == "stable":
        if oi_72h_context == "accumulating":
            return "⚪ 杠杆短线稳定，72H累计仓位仍偏高"
        if oi_72h_context == "declining":
            return "⚪ 杠杆短线稳定，72H累计仓位仍偏低"
        return "⚪ 杠杆资金短线稳定"
    return "⚪ 杠杆资金数据不足"


def plain_spot_cvd_signal_line(spot):
    if (spot or {}).get("cvd_positive_rule") is True:
        return "✅ 现货买盘转正"
    if (spot or {}).get("cvd_weak_rule") is True:
        return "❌ 现货买盘偏弱"
    return "⚪ 现货买盘数据不足或中性"


def plain_breakout_line(levels):
    status = (levels or {}).get("status")
    pressure = (levels or {}).get("pressure_state")
    support = (levels or {}).get("support_state")
    position = (levels or {}).get("position_state")
    trend = (levels or {}).get("price_trend") or {}
    if support == "breakdown_confirmed":
        if trend_support_breakdown_phase(trend) == "confirmation_event":
            return "❌ 原支撑本轮跌破确认"
        return "❌ 仍在原支撑下方"
    if support == "support_reclaim_watch":
        return "⚠ 价格已收回原短线支撑，等待现货资金确认"
    if support == "breakdown_watch":
        return "⚠ 原短线支撑已失守，正在测试下一潜在支撑"
    if pressure == "breakout_confirmed":
        return "✅ 短线压力突破确认"
    if pressure == "breakout_hold":
        return "⚠ 已站稳短线压力，等待资金确认"
    if pressure == "breakout_attempt":
        return "⚠ 突破短线压力，等待连续确认"
    if status == "post_support_pressure_returned":
        return "⚠ 支撑首次测试后卖压重新出现"
    if status == "post_support_consolidation_confirmed":
        return "⚪ 支撑首次测试后的缩量整理已确认"
    if status == "post_support_consolidation_watch":
        return "⚪ 支撑首次测试后进入缩量整理观察"
    if status == "support_tested":
        return "⚪ 短线支撑首次测试完成"
    if support == "near_support":
        return "⚠ 价格接近短线支撑"
    if position == "near_upper":
        return "⚠ 价格接近短线压力"
    if position == "near_lower":
        return "⚠ 价格接近短线支撑"
    if position == "mid_range":
        return "⚪ 价格在短线区间中部"
    return "⚪ 结构位置数据不足"


def plain_action_line(payload):
    levels = (((payload.get("rule_state") or {}).get("levels")) or {})
    trend = levels.get("price_trend") or {}
    status = trend.get("status") or levels.get("status")
    if status == "breakdown_confirmed":
        if trend_support_breakdown_phase(trend) == "confirmation_event":
            return "❌ 本轮跌破确认，风险状态生效"
        return "❌ 原支撑失守状态仍在，风险尚未解除"
    if status == "support_reclaim_watch":
        return "⚠ 价格已收回锚点，资金确认尚未完成"
    if status == "breakdown_watch":
        return "⚠ 跌破观察，等待连续确认"
    if status == "trend_confirmed":
        return "✅ 突破确认，趋势保持"
    if status == "pullback_watch":
        return "⚠ 突破后回踩观察，暂未取消"
    if status == "breakout_hold":
        return "⚠ 已站稳压力位，等待延续确认"
    if status == "breakout_attempt":
        return "⚠ 突破尝试中，等待连续确认"
    if status == "post_support_pressure_returned":
        return "⚠ 卖压重新出现，关注再次测试或跌破"
    if status == "post_support_consolidation_confirmed":
        return "⚪ 缩量整理已确认，方向仍未选择"
    if status == "post_support_consolidation_watch":
        return "⚪ 缩量整理观察中，尚未形成反转"
    if status == "support_tested":
        if (payload.get("market_time_context") or {}).get("summary_text"):
            return "⚪ 支撑测试已完成，等待完整小时确认压力突破"
        return "⚪ 首次测试完成，继续观察卖压"
    return "⚪ 结构方向未确认"


def format_market_stage_line(payload):
    rule_state = payload.get("rule_state") or {}
    leverage = rule_state.get("leverage") or {}
    levels = rule_state.get("levels") or {}
    trend = levels.get("price_trend") or {}

    if trend.get("status") == "breakdown_confirmed":
        if trend_support_breakdown_phase(trend) == "confirmation_event":
            return "原支撑本轮跌破确认，风险状态生效"
        return "仍在原支撑下方，风险状态保持"
    if trend.get("status") == "support_reclaim_watch":
        return "价格已收回原短线支撑，等待现货资金确认"
    if trend.get("status") == "breakdown_watch":
        return "短线支撑跌破观察中，等待连续确认"
    if trend.get("status") == "trend_confirmed":
        return "短线压力突破确认，趋势保持"
    if trend.get("status") == "pullback_watch":
        return "突破后回踩观察中，暂不取消确认状态"
    if trend.get("status") == "breakout_hold":
        return "已站稳压力位，等待资金和成交确认"
    if trend.get("status") == "breakout_attempt":
        return "突破尝试中，等待连续站稳"
    if trend.get("status") == "post_support_pressure_returned":
        return "支撑首次测试后卖压重新出现，关注再次测试或跌破"
    if trend.get("status") == "post_support_consolidation_confirmed":
        return "支撑首次测试后的缩量整理已确认，方向仍未选择"
    if trend.get("status") == "post_support_consolidation_watch":
        return "支撑首次测试后进入缩量整理观察，尚未形成反转"
    if trend.get("status") == "support_tested":
        return "支撑首次测试完成，继续观察下跌是否延续"
    if leverage.get("oi_contraction_rule") is True:
        return "资金收缩中，走势仍偏观察"
    return "区间观察中，等待价格和资金确认"


def format_market_reason_line(payload):
    rule_state = payload.get("rule_state") or {}
    leverage = rule_state.get("leverage") or {}
    spot = rule_state.get("spot") or {}

    if leverage.get("oi_contraction_rule") is True:
        leverage_text = "杠杆资金在减少"
    elif leverage.get("oi_contraction_rule") is False:
        leverage_text = "杠杆资金没有明显减少"
    else:
        leverage_text = "杠杆资金数据不足"

    return f"{leverage_text}，{plain_stable_reason(spot)}。"


def format_market_key_line(payload):
    rule_state = payload.get("rule_state") or {}
    levels = rule_state.get("levels") or {}
    trend = levels.get("price_trend") or {}

    if trend.get("status") == "breakdown_confirmed":
        if trend_support_breakdown_phase(trend) == "confirmation_event":
            return "本轮原支撑跌破已确认，风险状态生效。"
        return "价格仍在原支撑下方，失守状态尚未解除。"
    if trend.get("status") == "support_reclaim_watch":
        return "价格已回到原短线支撑上方，但 4H Spot CVD 尚未转正，原锚点暂不解除。"
    if trend.get("status") == "breakdown_watch":
        return "价格跌破短线支撑，等待连续收盘和资金确认。"
    if trend.get("status") == "trend_confirmed":
        return "价格连续站上短线压力，且资金确认已跟上。"
    if trend.get("status") == "pullback_watch":
        return "突破后短线回踩，确认状态暂未取消。"
    if trend.get("status") == "breakout_hold":
        return "价格已连续站上压力位，但还缺少延续确认。"
    if trend.get("status") == "breakout_attempt":
        return "价格正在尝试突破，等待连续站稳。"
    if trend.get("status") == "post_support_pressure_returned":
        return "支撑首次测试后卖压形态重新出现，关注是否再次测试或跌破支撑。"
    if trend.get("status") == "post_support_consolidation_confirmed":
        return "支撑首次测试后的缩量整理已确认，方向仍未选择，尚未形成反转。"
    if trend.get("status") == "post_support_consolidation_watch":
        return "支撑首次测试后进入缩量整理观察，卖压变化仍需逐整点确认。"
    if trend.get("status") == "support_tested":
        return "价格已完成支撑首次测试，正在观察下跌是否延续及卖压是否重新出现。"
    return "价格仍在短线支撑和短线压力之间，暂时不做阶段确认。"


def format_compact_market_stage_block(payload):
    rule_state = payload.get("rule_state") or {}
    etf = rule_state.get("etf") or {}
    leverage = rule_state.get("leverage") or {}
    spot = rule_state.get("spot") or {}
    levels = rule_state.get("levels") or {}
    return [
        "资金/价格：",
        plain_etf_flow_line(etf),
        plain_leverage_signal_line(leverage),
        plain_spot_cvd_signal_line(spot),
        plain_stable_signal_line(spot),
        plain_breakout_line(levels),
        plain_action_line(payload),
    ]


def format_plain_macro_environment_line(environment, dxy_pressure_rule):
    high_real_rate = environment.get("high_real_rate_pressure_rule")
    if high_real_rate is True and dxy_pressure_rule is True:
        return "利率和美元仍偏压制"
    if high_real_rate is True:
        return "利率仍偏压制，美元压力不明显"
    if dxy_pressure_rule is True:
        return "美元偏强，外部压力仍在"
    if high_real_rate is False and dxy_pressure_rule is False:
        return "利率和美元压力暂时缓和"
    return "利率和美元数据不足，维持观察"


def format_plain_macro_long_term_line(long_term, liquidity_improved_rule):
    status = long_term.get("status")
    if liquidity_improved_rule is True or status == "liquidity_conversion":
        return "货币宽松开始转成流动性改善"
    if status in ("expectation_exists", "exists_not_dominant"):
        return "货币宽松逻辑存在，但还没真正转成流动性改善"
    if status == "watching":
        return "宽松逻辑还在观察中"
    return "长期资金环境数据不足，维持观察"


def format_raw_signal_block(payload):
    rule_state = payload.get("rule_state") or {}
    etf = rule_state.get("etf") or {}
    leverage = rule_state.get("leverage") or {}
    spot = rule_state.get("spot") or {}
    levels = rule_state.get("levels") or {}
    macro = rule_state.get("macro") or {}
    environment = macro.get("environment") or {}
    long_term = macro.get("long_term_environment") or {}
    dxy_pressure_rule = rule_any(
        environment.get("dxy_stage_strength_rule"),
        environment.get("dxy_strong_rule"),
    )
    long_term_expectation_stage_rule = True if long_term.get("status") == "expectation_exists" else (
        False if long_term.get("status") else None
    )
    liquidity_stage_rule = True if long_term.get("status") == "liquidity_conversion" else (
        False if long_term.get("status") else None
    )
    liquidity_improved_rule = rule_all(
        long_term.get("real_pressure_easing_rule"),
        rule_any(
            long_term.get("m2_expansion_rule"),
            long_term.get("fed_balance_sheet_expansion_rule"),
        ),
    )
    return [
        "资金/结构：",
        plain_etf_flow_line(etf),
        compact_signal_line(leverage.get("oi_contraction_rule"), "OI收缩"),
        stable_signal_text(spot, with_icon=True),
        f"压力状态：{levels.get('pressure_state_text') or '数据不足'}",
        f"支撑状态：{levels.get('support_state_text') or '数据不足'}",
        "宏观阶段：",
        compact_signal_line(environment.get("high_real_rate_pressure_rule"), "高成本压制阶段"),
        compact_signal_line(dxy_pressure_rule, "美元偏强阶段"),
        compact_signal_line(long_term_expectation_stage_rule, "长期放水预期阶段"),
        compact_signal_line(liquidity_stage_rule, "放水转流动性阶段"),
        compact_signal_line(liquidity_improved_rule, "流动性改善信号"),
    ]


def format_compact_macro_stage_block(payload):
    rule_state = payload.get("rule_state") or {}
    macro = rule_state.get("macro") or {}
    environment = macro.get("environment") or {}
    long_term = macro.get("long_term_environment") or {}
    dxy_pressure_rule = rule_any(
        environment.get("dxy_stage_strength_rule"),
        environment.get("dxy_strong_rule"),
    )
    liquidity_improved_rule = rule_all(
        long_term.get("real_pressure_easing_rule"),
        rule_any(
            long_term.get("m2_expansion_rule"),
            long_term.get("fed_balance_sheet_expansion_rule"),
        ),
    )
    environment_text = format_plain_macro_environment_line(environment, dxy_pressure_rule)
    long_term_text = format_plain_macro_long_term_line(long_term, liquidity_improved_rule)
    return [f"宏观背景：{environment_text}；{long_term_text}。"]


def format_compact_macro_stage_checklist(payload):
    rule_state = payload.get("rule_state") or {}
    macro = rule_state.get("macro") or {}
    environment = macro.get("environment") or {}
    long_term = macro.get("long_term_environment") or {}
    dxy_pressure_rule = rule_any(
        environment.get("dxy_stage_strength_rule"),
        environment.get("dxy_strong_rule"),
    )
    long_term_expectation_stage_rule = True if long_term.get("status") == "expectation_exists" else (
        False if long_term.get("status") else None
    )
    liquidity_stage_rule = True if long_term.get("status") == "liquidity_conversion" else (
        False if long_term.get("status") else None
    )
    liquidity_improved_rule = rule_all(
        long_term.get("real_pressure_easing_rule"),
        rule_any(
            long_term.get("m2_expansion_rule"),
            long_term.get("fed_balance_sheet_expansion_rule"),
        ),
    )
    return [
        "完整周期：",
        compact_signal_line(environment.get("high_real_rate_pressure_rule"), "高利率成本仍在压制"),
        compact_signal_line(dxy_pressure_rule, "美元偏强仍在压制"),
        compact_signal_line(long_term_expectation_stage_rule, "长期宽松预期还没成为主线"),
        compact_signal_line(liquidity_stage_rule, "宽松还没转成市场流动性"),
        compact_signal_line(liquidity_improved_rule, "流动性改善还未确认"),
    ]


def compact_support_anchor_followup(trend):
    support_anchor = safe_float((trend or {}).get("support_anchor"))
    rolling_support = safe_float((trend or {}).get("rolling_short_support"))
    if rolling_support is None:
        return "下一潜在支撑尚未形成"
    if support_anchor is not None and rolling_support < support_anchor:
        return f"下一潜在支撑 {compact_level(rolling_support)}"
    if support_anchor is not None and abs(rolling_support - support_anchor) <= max(abs(support_anchor), 1.0) * 1e-9:
        return f"当前滚动支撑仍与原锚点重合（{compact_level(rolling_support)}）"
    return f"当前滚动支撑 {compact_level(rolling_support)}"


def format_compact_current_view(payload):
    rule_state = payload.get("rule_state") or {}
    levels = rule_state.get("levels") or {}
    trend = levels.get("price_trend") or {}
    time_summary = (payload.get("market_time_context") or {}).get("summary_text")
    if time_summary:
        return f"当前理解：{time_summary}"
    support_anchor = compact_level(trend.get("support_anchor"))
    anchor_followup = compact_support_anchor_followup(trend)

    if trend.get("status") == "breakdown_confirmed":
        if trend_support_breakdown_phase(trend) == "confirmation_event":
            return (
                f"当前理解：原短线支撑 {support_anchor} 本轮跌破确认，"
                f"{anchor_followup}，风险状态生效。"
            )
        return (
            f"当前理解：价格仍在原短线支撑 {support_anchor} 下方，"
            f"{anchor_followup}，风险状态尚未解除。"
        )
    if trend.get("status") == "support_reclaim_watch":
        return (
            f"当前理解：价格已收回原短线支撑 {support_anchor}，"
            "但 4H Spot CVD 尚未转正；暂保留原锚点，等待资金确认。"
        )
    if trend.get("status") == "breakdown_watch":
        return (
            f"当前理解：原短线支撑 {support_anchor} 已失守，"
            f"{anchor_followup}。"
        )
    if trend.get("status") == "trend_confirmed":
        return "当前理解：短线压力突破确认，趋势保持，继续观察回踩质量。"
    if trend.get("status") == "pullback_watch":
        return "当前理解：突破后处于回踩观察，短线回落暂不等于确认取消。"
    if trend.get("status") == "breakout_hold":
        return "当前理解：价格已连续站上短线压力，等待资金三选二确认。"
    if trend.get("status") == "breakout_attempt":
        return "当前理解：价格正在尝试突破短线压力，还需要连续站稳。"
    if trend.get("status") == "post_support_pressure_returned":
        return (
            f"当前理解：价格已完成对 {compact_level(trend.get('support_test_anchor'))} "
            "附近支撑的首次测试；当前卖压形态重新出现，"
            "关注是否再次测试或跌破支撑，尚未形成反转。"
        )
    if trend.get("status") == "post_support_consolidation_confirmed":
        return (
            f"当前理解：价格已完成对 {compact_level(trend.get('support_test_anchor'))} "
            "附近支撑的首次测试；缩量整理已确认，方向仍未选择，尚未形成反转。"
        )
    if trend.get("status") == "post_support_consolidation_watch":
        sell_pressure_change = trend.get("spot_sell_pressure_change") or {}
        if (
            sell_pressure_change.get("available") is True
            and sell_pressure_change.get("easing_rule") is True
        ):
            pressure_text = (
                "Binance 4H现货主动卖出占优幅度较上一可用整点缩小，"
                "但尚未形成反转。"
            )
        else:
            pressure_text = "卖压是否继续衰减待确认，尚未形成反转。"
        return (
            f"当前理解：价格已完成对 {compact_level(trend.get('support_test_anchor'))} "
            f"附近支撑的首次测试；当前进入缩量整理观察，{pressure_text}"
        )
    if trend.get("status") == "support_tested":
        return (
            f"当前理解：价格已完成对 {compact_level(trend.get('support_test_anchor'))} "
            "附近支撑的首次测试；正在观察下跌是否延续及卖压是否重新出现，"
            "尚未形成反转。"
        )
    if (trend.get("position_state") or levels.get("position_state")) == "near_upper":
        return "当前理解：价格接近短线压力，重点观察是否收盘突破。"
    if (trend.get("position_state") or levels.get("position_state")) == "near_lower":
        return "当前理解：价格接近短线支撑，重点观察支撑是否有效。"
    return "当前理解：短线仍在观察，等待价格和资金进一步确认。"


def format_compact_signal_block(payload):
    return [
        *format_compact_macro_stage_checklist(payload),
        "",
        *format_compact_market_stage_block(payload),
        "",
        format_compact_current_view(payload),
    ]


def format_compact_macro_block(payload):
    macro = ((payload.get("public_records") or {}).get("macro") or {})
    value = macro.get("value") or {}
    macro_state = ((payload.get("rule_state") or {}).get("macro") or {})
    if not record_has_data(macro):
        return ["宏观大环境采集中 / 源数据过期"]
    if not record_is_current(macro):
        return format_stale_macro_reference(macro)
    lines = []
    lines.extend(format_macro_pressure_lines(value, macro_state))
    lines.extend(format_compact_long_money_lines(value, macro_state))
    return lines


def format_compact_macro_cycle_block(payload):
    macro_cycle = ((payload.get("rule_state") or {}).get("macro_cycle") or {})
    return [
        f"阶段：{macro_cycle.get('stage_text') or '数据不足'}",
        f"长期状态：{macro_cycle.get('long_logic_status') or '观察中'}",
    ]


def format_change_in_words(value, increase_text="增加", decrease_text="减少", decimals=2):
    number = safe_float(value)
    if number is None:
        return "数据不足"
    amount = format_pct_value(abs(number), decimals, signed=False)
    if number > 0:
        return f"{increase_text}{amount}"
    if number < 0:
        return f"{decrease_text}{amount}"
    return "基本不变"


def format_spot_balance_in_words(value, decimals=2):
    return format_change_in_words(
        value,
        increase_text="主动买入占优",
        decrease_text="主动卖出占优",
        decimals=decimals,
    )


def format_funding_in_words(value):
    number = safe_float(value)
    if number is None:
        return "持仓费用数据不足"
    amount = format_pct_value(abs(number), 3, signed=False)
    if number > 0:
        return f"看涨方向每8小时约支付{amount}给看跌方向"
    if number < 0:
        return f"看跌方向每8小时约支付{amount}给看涨方向"
    return "看涨与看跌方向之间的持仓费用接近0%"


def format_basis_in_words(value):
    number = safe_float(value)
    if number is None:
        return "合约与现货价格差数据不足"
    amount = format_pct_value(abs(number), 3, signed=False)
    if number > 0:
        return f"合约价格比现货高{amount}"
    if number < 0:
        return f"合约价格比现货低{amount}"
    return "合约与现货价格基本相同"


def format_money_flow_in_words(value, decimals=0):
    number = safe_float(value)
    if number is None:
        return "数据不足"
    amount = compact_money(abs(number), decimals).lstrip("+")
    if number > 0:
        return f"净流入 {amount}"
    if number < 0:
        return f"净流出 {amount}"
    return "流入流出基本平衡"


def format_compact_funds_block(payload):
    public_records = payload.get("public_records") or {}
    market = payload.get("market") or {}
    market_24h_delta = payload.get("market_24h_delta") or {}
    funding = public_records.get("funding") or {}
    basis = public_records.get("basis") or {}
    etf = public_records.get("etf") or {}
    spot_cvd = public_records.get("spot_cvd") or {}
    stablecoin = public_records.get("stablecoin") or {}
    delta4h = payload.get("delta4h") or {}
    checks = ((payload.get("structure_state") or {}).get("checks") or {})
    oi_slow = checks.get("oi_slow_confirmation") or {}
    volume_24h_pct = (market_24h_delta.get("metrics") or {}).get("vol24")
    return [
        "未平仓合约：",
        f"总价值 {format_money_compact_plain(market.get('oi_usd'), 2)}",
        (
            f"数量变化：过去4小时{format_change_in_words(delta4h.get('oi_4h_pct'))} | "
            f"24小时{format_change_in_words(oi_slow.get('delta_24h_pct'))} | "
            f"72小时{format_change_in_words(oi_slow.get('delta_72h_pct'))}"
        ),
        "",
        "合约成交额变化：",
        (
            f"过去4小时{format_change_in_words(delta4h.get('vol_4h_pct'))} | "
            f"24小时{format_change_in_words(volume_24h_pct)}"
        ),
        "",
        "现货成交额变化：",
        (
            f"过去4小时{format_change_in_words(record_value(spot_cvd, 'volume_4h_pct'))}{record_mark(spot_cvd)} | "
            f"24小时{format_change_in_words(record_value(spot_cvd, 'volume_24h_pct'))}{record_mark(spot_cvd)}"
        ),
        "",
        "合约持仓费用和价格差：",
        f"{format_funding_in_words(record_value(funding, 'funding_8h_pct'))}{record_mark(funding)}",
        f"{format_basis_in_words(record_value(basis, 'basis_pct'))}{record_mark(basis)}",
        "",
        "场外资金流向：",
        (
            f"ETF 1D {compact_money(record_value(etf, 'flow_1d_usd'), 0)}{record_mark(etf)} | "
            f"5D {compact_money(record_value(etf, 'flow_5d_usd'), 0)}{record_mark(etf)} | "
            f"30D {compact_money(record_value(etf, 'flow_30d_usd'), 0)}{record_mark(etf)}"
        ),
        etf_raw_data_status_line(etf),
        "",
        "现货主动买卖：",
        (
            f"过去4小时{format_spot_balance_in_words(record_value(spot_cvd, 'cvd_4h_pct'))}{record_mark(spot_cvd)} | "
            f"24小时{format_spot_balance_in_words(record_value(spot_cvd, 'cvd_24h_pct'))}{record_mark(spot_cvd)}"
        ),
        "",
        "稳定币资金变化：",
        (
            f"24小时{format_money_flow_in_words(record_value(stablecoin, 'flow_24h_usd'))}{record_mark(stablecoin)} | "
            f"7天{format_money_flow_in_words(record_value(stablecoin, 'flow_7d_usd'))}{record_mark(stablecoin)}"
        ),
    ]


MARKET_PATTERN_METRIC_TEXT = {
    "price.change_4h_pct": "价格变化",
    "derivatives.oi_change_4h_pct": "未平仓合约量变化",
    "derivatives.volume_change_4h_pct": "合约成交额变化",
    "spot.cvd_4h_pct": "现货主动买卖情况",
    "spot.volume_change_4h_pct": "现货成交额变化",
    "structure.in_range": "近期高低位",
}


def market_pattern_reason_text(item):
    reason = str((item or {}).get("reason") or "")
    if assets.current().equity:
        unavailable = []
        if reason == "spot_hourly_flow_unavailable" or "spot.cvd_4h_pct" in reason:
            unavailable.append("股票接口不提供主动买卖方向")
        if assets.current().derivatives_relation == "proxy" and "derivatives.oi_change_4h_pct" in reason:
            unavailable.append("SPY的OI仅作参考，不作为VOO自身资金确认")
        if unavailable:
            return "；".join(unavailable) + "，相关确认不适用"
    if reason == "signal_data_unusable":
        reason_text = {
            "no_closed_bars": "缺少已经结束的完整小时数据",
            "latest_bar_stale": "最近一个完整小时的数据过旧",
            "recent_bars_not_contiguous": "最近几个完整小时的数据不连续",
            "oi_not_aligned": "价格和未平仓合约数据的统计时间不一致",
            "hyperliquid_oi_warmup": "Hyperliquid未平仓合约影子样本尚未覆盖72小时",
        }
        quality_reasons = ((item or {}).get("evidence") or {}).get("quality_reasons") or []
        translated = [reason_text.get(value) for value in quality_reasons if reason_text.get(value)]
        if translated:
            return "；".join(dict.fromkeys(translated))
        return "最近几个完整小时的数据不可用"
    if reason == "structure_history_unavailable":
        return "连续历史价格不足，无法可靠判断近期高低位"
    if reason == "spot_hourly_flow_unavailable":
        return "现货逐小时主动买卖数据缺失或与结构小时未对齐"
    if reason == "condition_unknown":
        return "判断所需数据无法计算"
    if reason == "missing_pattern_item":
        return "内部判断结果缺失"
    if reason == "duplicate_pattern_item":
        return "内部判断结果重复"
    if reason == "unknown_pattern_state":
        return "内部判断状态异常"
    if reason == "invalid_snapshot_wait":
        return "内部判断状态异常"
    if reason.startswith("missing_or_unaligned:"):
        metrics = [
            MARKET_PATTERN_METRIC_TEXT.get(metric, metric)
            for metric in reason.split(":", 1)[1].split(",")
            if metric
        ]
        return "、".join(metrics) + "缺失或统计时间不一致"
    return "所需数据不足，暂时无法判断"


MARKET_PATTERN_BIAS_TEXT = {
    "positive": "🟢 表面偏利好",
    "negative": "🔴 表面偏利空",
    "neutral": "⚪ 表面方向不明",
}

MARKET_PATTERN_CATEGORY_TEXT = {
    "price_oi": "价格和未平仓合约量",
    "spot_flow": "现货主动买卖",
    "key_level_event": "近期高低位事件",
    "activity_state": "持续平静状态",
}


def market_pattern_bias_text(item):
    if (item or {}).get("state") != "YES":
        return None
    bias = (item or {}).get("surface_bias") or (item or {}).get("surface_bias_on_confirmation")
    text = MARKET_PATTERN_BIAS_TEXT.get(bias)
    if not text:
        return None
    if (item or {}).get("surface_bias_horizon") == "event_time" and bias != "neutral":
        return text.replace("表面", "发生时表面", 1)
    return text


def market_pattern_wait_text(item):
    item = item or {}
    key = item.get("key")
    evidence = item.get("evidence") or {}
    remaining = max(1, int(evidence.get("remaining_confirm_bars") or 1))
    anchor = compact_level(evidence.get("anchor"))
    level_text = f" {anchor}" if anchor != "NA" else ""
    if key == "support_liquidity_sweep":
        return (
            f"价格已经跌破近期低位{level_text}，正在观察能否重新回到该价位上方；"
            f"还可观察{remaining}个完整小时｜🟡 当前偏弱，最终形态未确认"
        )
    if key == "resistance_false_breakout":
        return (
            f"价格已经涨过近期高位{level_text}，正在观察是否会重新跌回该价位下方；"
            f"还可观察{remaining}个完整小时｜🟡 当前偏强，最终形态未确认"
        )
    if key == "volume_breakout":
        return (
            f"合约成交额明显增加时，价格已经涨过近期高位{level_text}，正在观察能否继续保持；"
            f"还可观察{remaining}个完整小时｜🟡 当前偏强，持续性未确认"
        )
    if key == "resistance_volume_stall":
        return (
            f"价格已放量接近触碰前的压力{level_text}，但推进不足；"
            f"还需{remaining}个完整小时确认｜🟡 只观察成交与价格表现，不确认卖压原因"
        )
    if key == "low_volume_range":
        required = int(item.get("required_consecutive") or 3)
        consecutive = max(1, int(item.get("consecutive_hits") or 1))
        remaining_checks = max(0, required - consecutive)
        return (
            f"程序已连续{consecutive}次整点检测到价格变化不大、成交额减少，"
            f"还需{remaining_checks}次整点确认｜🟡 表面方向不明，仍在确认"
        )
    return "这项情况正在确认｜🟡 暂不判断方向"


def market_pattern_short_wait_text(item):
    item = item or {}
    key = item.get("key")
    evidence = item.get("evidence") or {}
    remaining = max(1, int(evidence.get("remaining_confirm_bars") or 1))
    anchor = compact_level(evidence.get("anchor"))
    level_text = f" {anchor}" if anchor != "NA" else ""
    if key == "support_liquidity_sweep":
        return (
            f"跌破支撑后的收回正在确认：观察{level_text or '近期低位'}，"
            f"还需{remaining}个完整小时｜当前偏弱"
        )
    if key == "resistance_false_breakout":
        return (
            f"冲高回落正在确认：观察{level_text or '近期高位'}，"
            f"还需{remaining}个完整小时｜当前偏强"
        )
    if key == "volume_breakout":
        return (
            f"放量冲高正在确认：观察{level_text or '近期高位'}，"
            f"还需{remaining}个完整小时｜当前偏强，持续性未确认"
        )
    if key == "resistance_volume_stall":
        return (
            f"阻力附近放量但推进不足正在确认：观察{level_text or '触碰前压力'}，"
            f"还需{remaining}个完整小时｜方向不明确"
        )
    if key == "low_volume_range":
        required = int(item.get("required_consecutive") or 3)
        consecutive = max(1, int(item.get("consecutive_hits") or 1))
        remaining_checks = max(0, required - consecutive)
        return (
            f"低成交整理正在确认：已连续{consecutive}次，"
            f"还需{remaining_checks}次整点确认｜方向不明确"
        )
    return "形态正在确认｜暂不判断方向"


def normalized_market_pattern_items(patterns):
    config = load_status_rules_config().get("market_patterns") or {}
    definitions = config.get("patterns") or []
    raw_items = [
        item
        for item in ((patterns or {}).get("items") or [])
        if isinstance(item, dict) and item.get("key")
    ]
    items_by_key = {}
    duplicate_keys = set()
    for item in raw_items:
        key = item.get("key")
        if key in items_by_key:
            duplicate_keys.add(key)
            continue
        items_by_key[key] = item
    normalized = []
    for definition in definitions:
        key = definition.get("key")
        raw_item = items_by_key.get(key)
        if key in duplicate_keys:
            item = market_pattern_result(definition, "NA", reason="duplicate_pattern_item")
        elif raw_item is None:
            item = market_pattern_result(definition, "NA", reason="missing_pattern_item")
        else:
            state = raw_item.get("state")
            reason = raw_item.get("reason")
            if state not in MARKET_PATTERN_STATE_TEXT:
                state = "NA"
                reason = "unknown_pattern_state"
            elif definition.get("kind") == "snapshot" and state == "WAIT":
                state = "NA"
                reason = "invalid_snapshot_wait"
            item = market_pattern_result(
                definition,
                state,
                evidence=raw_item.get("evidence"),
                reason=reason,
                raw_hit=raw_item.get("raw_hit"),
            )
            for dynamic_key in ("required_consecutive", "consecutive_hits"):
                if dynamic_key in raw_item:
                    item[dynamic_key] = raw_item.get(dynamic_key)
        normalized.append(item)
    return normalized, config


MARKET_PATTERN_CONCLUSION_SCHEMA = "btc_market_pattern_conclusion"
MARKET_PATTERN_CONCLUSION_REVISION = 1


def market_pattern_conflicted_categories(items, config):
    exclusive_categories = {
        group.get("key")
        for group in (config.get("groups") or [])
        if group.get("exclusive") is True
    }
    return {
        category
        for category in exclusive_categories
        if sum(
            item.get("state") == "YES" and item.get("category") == category
            for item in items
        ) > 1
    }


def market_pattern_bucket(value):
    if isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return None


def build_market_pattern_current_structure(payload):
    levels = ((payload.get("rule_state") or {}).get("levels") or {})
    trend = (levels.get("price_trend") or {})
    time_summary = (payload.get("market_time_context") or {}).get("summary_text")
    status = trend.get("status")
    position = trend.get("position_state")
    support = trend.get("support_state")
    pressure = trend.get("pressure_state")
    base = {
        "status": status,
        "position_state": position,
        "support_state": support,
        "pressure_state": pressure,
        "data_sufficient": trend.get("data_sufficient") is True,
    }
    if trend.get("data_sufficient") is not True:
        return {
            **base,
            "phase": "insufficient",
            "confirmation": "insufficient",
            "conclusion_key": "structure_insufficient",
            "text": "当前结构数据不足",
        }

    breakdown_text = (
        "当前原支撑本轮跌破确认"
        if trend_support_breakdown_phase(trend) == "confirmation_event"
        else "当前仍在原支撑下方，失守状态尚未解除"
    )
    status_mapping = {
        "breakdown_confirmed": (
            "negative",
            "confirmed",
            "breakdown_confirmed",
            breakdown_text,
        ),
        "support_reclaim_watch": (
            "range",
            "watch",
            "support_reclaim_watch",
            "当前价格已收回原短线支撑，但现货资金尚未确认，原锚点暂不解除",
        ),
        "breakdown_watch": (
            "negative",
            "watch",
            "breakdown_watch",
            "当前已跌破短线支撑，仍在等待确认",
        ),
        "trend_confirmed": (
            "positive",
            "confirmed",
            "trend_confirmed",
            "当前短线压力突破已确认",
        ),
        "pullback_watch": (
            "positive",
            "watch",
            "pullback_watch",
            "当前处于突破后回踩，原确认状态暂未取消",
        ),
        "breakout_hold": (
            "positive",
            "watch",
            "breakout_hold",
            "当前已连续站上短线压力，仍在等待延续确认",
        ),
        "breakout_attempt": (
            "positive",
            "watch",
            "breakout_attempt",
            "当前正在尝试突破短线压力，尚未连续站稳",
        ),
        "post_support_pressure_returned": (
            "negative",
            "watch",
            "post_support_pressure_returned",
            "当前支撑首次测试后卖压形态重新出现，关注是否再次测试或跌破支撑",
        ),
        "post_support_consolidation_confirmed": (
            "range",
            "confirmed",
            "post_support_consolidation_confirmed",
            "当前支撑首次测试后的缩量整理已确认，方向仍未选择，尚未形成反转",
        ),
        "post_support_consolidation_watch": (
            "range",
            "watch",
            "post_support_consolidation_watch",
            "当前支撑首次测试后进入缩量整理观察，尚未形成反转",
        ),
        "support_tested": (
            "range",
            "watch",
            "support_tested",
            "当前已完成支撑首次测试，正在观察下跌是否延续及卖压是否重新出现",
        ),
    }
    mapped = status_mapping.get(status)
    if mapped:
        phase, confirmation, conclusion_key, text = mapped
        return {
            **base,
            "phase": phase,
            "confirmation": confirmation,
            "conclusion_key": conclusion_key,
            "text": time_summary or text,
        }

    if status != "range_observe":
        return {
            **base,
            "phase": "insufficient",
            "confirmation": "insufficient",
            "conclusion_key": "structure_unknown",
            "text": "当前结构数据不足",
        }
    if support == "near_support" or position == "near_lower":
        return {
            **base,
            "phase": "range",
            "confirmation": "unconfirmed",
            "conclusion_key": "near_support",
            "text": time_summary or "当前接近短线支撑，跌破尚未确认",
        }
    if position == "near_upper":
        return {
            **base,
            "phase": "range",
            "confirmation": "unconfirmed",
            "conclusion_key": "near_resistance",
            "text": time_summary or "当前接近短线压力，突破尚未确认",
        }
    if status == "range_observe" or position == "mid_range":
        return {
            **base,
            "phase": "range",
            "confirmation": "unconfirmed",
            "conclusion_key": "range_observe",
            "text": time_summary or "当前仍在短线区间内，结构方向未确认",
        }
    return {
        **base,
        "phase": "insufficient",
        "confirmation": "insufficient",
        "conclusion_key": "structure_unknown",
        "text": "当前结构数据不足",
    }


def market_pattern_conclusion_text(current_window, latest_event, current_structure, relation):
    current_bias = current_window.get("bias")
    current_integrity = current_window.get("integrity")
    current_occurrence = current_window.get("occurrence")
    latest_labels = latest_event.get("confirmed_labels") or []
    event_text = "；".join(latest_labels)
    clauses = []

    if relation == "conflict":
        clauses.append("4H形态结果存在冲突，本次不做综合方向判断")
    elif current_bias in ("positive", "negative"):
        direction_text = "偏强" if current_bias == "positive" else "偏弱"
        if current_integrity == "PARTIAL":
            clauses.append(f"基于可用数据，过去4小时已确认形态表面{direction_text}")
        else:
            clauses.append(f"过去4小时已确认形态表面{direction_text}")
    elif current_bias == "neutral":
        clauses.append("过去4小时价格变化不大、成交额持续减少，表面方向不明")
    elif current_occurrence == "WAIT":
        clauses.append("过去4小时尚无新增已确认主形态，仍有项目正在确认")
    elif current_occurrence == "NA":
        clauses.append("过去4小时主形态数据不足")
    else:
        clauses.append("当前窗口没有可用于组合的已确认形态")

    if relation == "aligned" and event_text:
        clauses.append(f"期间曾发生“{event_text}”，与当前窗口已确认形态方向一致，但不代表后续一定延续")
    elif relation == "offsetting" and event_text:
        clauses.append(
            f"期间曾发生“{event_text}”，与当前窗口已确认形态方向不同，"
            "说明窗口内并非单边延续，但不代表当前方向已经改变"
        )
    elif relation == "mixed_events":
        clauses.append("最近同一时点出现正反结构事件，关键价位表现反复")
    elif relation == "event_only" and event_text:
        clauses.append(f"期间曾发生“{event_text}”，但当前窗口没有已确认方向")

    if latest_event.get("waiting_keys"):
        clauses.append("另有结构事件仍在确认")
    if latest_event.get("occurrence") == "NA":
        clauses.append("近期结构事件数据不足，未纳入综合判断")
    pattern_partial = (
        current_window.get("integrity") == "PARTIAL"
        or latest_event.get("integrity") == "PARTIAL"
    )
    if pattern_partial and not any("数据不足" in clause for clause in clauses):
        clauses.append("部分形态数据不足，未纳入综合判断")
    clauses.append(current_structure.get("text") or "当前结构数据不足")
    text = "；".join(clauses)
    return text if text.endswith("。") else text + "。"


def build_market_pattern_conclusion(payload, items=None, config=None):
    patterns = payload.get("market_patterns") or {}
    if items is None or config is None:
        items, config = normalized_market_pattern_items(patterns)
    conflicted_categories = market_pattern_conflicted_categories(items, config)

    current_items = [
        item for item in items
        if item.get("surface_bias_horizon") == "current_window"
    ]
    current_confirmed = [
        item for item in current_items
        if item.get("state") == "YES" and item.get("category") not in conflicted_categories
    ]
    current_waiting = [item for item in current_items if item.get("state") == "WAIT"]
    current_unavailable = [item for item in current_items if item.get("state") == "NA"]
    current_biases = {
        item.get("surface_bias_on_confirmation")
        for item in current_confirmed
        if item.get("surface_bias_on_confirmation") in ("positive", "negative")
    }
    neutral_confirmed = [
        item for item in current_confirmed
        if item.get("surface_bias_on_confirmation") == "neutral"
    ]
    current_conflict = bool(conflicted_categories) or len(current_biases) > 1 or bool(
        current_biases and neutral_confirmed
    )
    if current_conflict:
        current_bias = "none"
        current_integrity = "CONFLICT"
    else:
        current_bias = next(iter(current_biases), "neutral" if neutral_confirmed else "none")
        current_integrity = "PARTIAL" if current_unavailable else "OK"
    if current_confirmed:
        current_occurrence = "YES"
    elif current_waiting:
        current_occurrence = "WAIT"
    elif current_unavailable:
        current_occurrence = "NA"
    else:
        current_occurrence = "NO"
    current_window = {
        "occurrence": current_occurrence,
        "integrity": current_integrity,
        "bias": current_bias,
        "confirmed_keys": [item.get("key") for item in current_confirmed],
        "waiting_keys": [item.get("key") for item in current_waiting],
        "unavailable_keys": [item.get("key") for item in current_unavailable],
        "conflicted_keys": [item.get("key") for item in current_confirmed] if current_conflict else [],
    }

    event_items = [item for item in items if item.get("surface_bias_horizon") == "event_time"]
    event_waiting = [item for item in event_items if item.get("state") == "WAIT"]
    event_unavailable = [item for item in event_items if item.get("state") == "NA"]
    data_quality_meta = patterns.get("data_quality") or {}
    current_bucket = market_pattern_bucket(data_quality_meta.get("bucket"))
    if current_bucket is None:
        as_of_ms = market_pattern_bucket(patterns.get("as_of_ms"))
        current_bucket = as_of_ms // (3600 * 1000) if as_of_ms is not None else None
    active_hours = int(config.get("event_active_hours") or 4)
    valid_events = []
    invalid_event_keys = []
    for item in event_items:
        if item.get("state") != "YES":
            continue
        confirmed_bucket = market_pattern_bucket((item.get("evidence") or {}).get("confirmed_bucket"))
        age = current_bucket - confirmed_bucket if None not in (current_bucket, confirmed_bucket) else None
        if age is None or age < 0 or age >= active_hours:
            invalid_event_keys.append(item.get("key"))
            continue
        valid_events.append((confirmed_bucket, item))
    valid_events.sort(key=lambda pair: pair[0], reverse=True)
    latest_bucket = valid_events[0][0] if valid_events else None
    latest_items = [item for bucket, item in valid_events if bucket == latest_bucket]
    older_items = [item for bucket, item in valid_events if bucket != latest_bucket]
    latest_biases = {
        item.get("surface_bias_on_confirmation")
        for item in latest_items
        if item.get("surface_bias_on_confirmation") in ("positive", "negative")
    }
    if len(latest_biases) > 1:
        event_bias = "mixed"
    else:
        event_bias = next(iter(latest_biases), "none")
    all_event_unavailable_keys = [item.get("key") for item in event_unavailable] + invalid_event_keys
    if latest_items:
        event_occurrence = "YES"
    elif event_waiting:
        event_occurrence = "WAIT"
    elif all_event_unavailable_keys:
        event_occurrence = "NA"
    else:
        event_occurrence = "NO"
    latest_event = {
        "occurrence": event_occurrence,
        "integrity": "PARTIAL" if all_event_unavailable_keys else "OK",
        "bias": event_bias,
        "confirmed_bucket": latest_bucket,
        "confirmed_keys": [item.get("key") for item in latest_items],
        "confirmed_labels": [item.get("label") for item in latest_items if item.get("label")],
        "older_confirmed_keys": [item.get("key") for item in older_items],
        "waiting_keys": [item.get("key") for item in event_waiting],
        "unavailable_keys": all_event_unavailable_keys,
        "invalid_confirmed_keys": invalid_event_keys,
    }

    if current_integrity == "CONFLICT":
        relation = "conflict"
    elif event_bias == "mixed":
        relation = "mixed_events"
    elif current_bias in ("positive", "negative") and event_bias in ("positive", "negative"):
        relation = "aligned" if current_bias == event_bias else "offsetting"
    elif current_bias in ("positive", "negative"):
        relation = "current_only"
    elif event_bias in ("positive", "negative"):
        relation = "event_only"
    elif current_bias == "neutral":
        relation = "neutral"
    elif current_integrity == "PARTIAL" or latest_event.get("integrity") == "PARTIAL":
        relation = "partial"
    else:
        relation = "none"

    current_structure = build_market_pattern_current_structure(payload)
    if relation == "conflict":
        data_quality = "conflict"
    elif (
        current_occurrence in ("NA", "NO")
        and event_occurrence in ("NA", "NO")
        and (current_unavailable or all_event_unavailable_keys)
    ):
        data_quality = "insufficient"
    elif (
        current_integrity == "PARTIAL"
        or latest_event.get("integrity") == "PARTIAL"
        or current_structure.get("phase") == "insufficient"
    ):
        data_quality = "partial"
    else:
        data_quality = "complete"
    conclusion_key = "data_conflict" if relation == "conflict" else (
        "data_insufficient" if data_quality == "insufficient" else relation
    )
    text = market_pattern_conclusion_text(
        current_window,
        latest_event,
        current_structure,
        relation,
    )
    return {
        "schema": MARKET_PATTERN_CONCLUSION_SCHEMA,
        "revision": MARKET_PATTERN_CONCLUSION_REVISION,
        "current_window": current_window,
        "latest_event": latest_event,
        "current_structure": current_structure,
        "relation": relation,
        "data_quality": data_quality,
        "conclusion_key": conclusion_key,
        "text": text,
        "source": "rules",
    }


def format_market_patterns_block(payload):
    patterns = payload.get("market_patterns") or {}
    items, config = normalized_market_pattern_items(patterns)
    if not items:
        structure = build_market_pattern_current_structure(payload)
        return [
            "程序固定判断：没有可用的形态规则，暂时无法判断。",
            "",
            f"综合结论：缺少可用形态规则；{structure.get('text') or '当前结构数据不足'}。",
        ]

    conflicted_categories = market_pattern_conflicted_categories(items, config)
    conclusion = build_market_pattern_conclusion(payload, items=items, config=config)
    invalid_confirmed_keys = set(
        (conclusion.get("latest_event") or {}).get("invalid_confirmed_keys") or []
    )
    cross_current_conflict = (
        (conclusion.get("current_window") or {}).get("integrity") == "CONFLICT"
        and not conflicted_categories
    )
    confirmed = [
        item for item in items
        if (
            item.get("state") == "YES"
            and item.get("category") not in conflicted_categories
            and item.get("key") not in invalid_confirmed_keys
            and not (
                cross_current_conflict
                and item.get("surface_bias_horizon") == "current_window"
            )
        )
    ]
    waiting = [item for item in items if item.get("state") == "WAIT"]
    unavailable = [item for item in items if item.get("state") == "NA"]
    not_seen = [item for item in items if item.get("state") == "NO"]

    lines = []
    explanatory_items = confirmed + waiting + unavailable
    if any(
        item.get("category") in ("price_oi", "activity_state")
        or item.get("key") == "spot_led_rise"
        for item in explanatory_items
    ):
        lines.append("“未平仓合约量”指市场里还有多少份合约没有关闭。")
    if confirmed:
        lines.extend(["", "已观察到："])
        for item in confirmed:
            bias_text = market_pattern_bias_text(item)
            suffix = f"｜{bias_text}" if bias_text else ""
            lines.append(f"• {item.get('label') or '已满足固定条件'}{suffix}")

    if waiting:
        lines.extend(["", "正在确认："])
        lines.extend(f"• {market_pattern_wait_text(item)}" for item in waiting)

    data_issue_lines = []
    for category in sorted(conflicted_categories):
        category_text = MARKET_PATTERN_CATEGORY_TEXT.get(category, "相关形态")
        data_issue_lines.append(f"• {category_text}出现互相冲突的结果，本次不判断。")
    if cross_current_conflict:
        data_issue_lines.append("• 当前窗口形态出现互相冲突的方向，本次不判断。")
    if invalid_confirmed_keys:
        data_issue_lines.append("• 近期高低位事件：确认时间缺失或已过有效期，本次暂不判断。")
    seen_unavailable = set()
    for item in unavailable:
        category_text = MARKET_PATTERN_CATEGORY_TEXT.get(item.get("category"), "相关形态")
        reason_text = market_pattern_reason_text(item)
        marker = (category_text, reason_text)
        if marker in seen_unavailable:
            continue
        seen_unavailable.add(marker)
        data_issue_lines.append(f"• {category_text}：{reason_text}，本次暂不判断。")
    if data_issue_lines:
        lines.extend(["", "数据不足："])
        lines.extend(data_issue_lines)

    if len(not_seen) == len(items):
        lines.extend(["", "所有可判断的常见情况均未出现。"])
    elif not_seen:
        lines.extend(["", "其余可判断的常见情况未出现。"])
    lines.extend(["", f"综合结论：{conclusion.get('text') or '数据不足，无法形成综合结论。'}"])
    if lines and not lines[0]:
        lines.pop(0)
    return lines

def format_compact_level_block(payload):
    levels = payload.get("levels") or {}
    rule_levels = ((payload.get("rule_state") or {}).get("levels") or {})
    time_receipt = format_market_time_receipt_line(payload)
    position_text = rule_levels.get("position_text") or levels.get("position_text") or "数据不足"
    pressure_text = rule_levels.get("pressure_state_text") or "数据不足"
    support_text = rule_levels.get("support_state_text") or "数据不足"
    trend = rule_levels.get("price_trend") or {}
    support_anchor = safe_float(trend.get("support_anchor"))
    rolling_support = safe_float(trend.get("rolling_short_support"))
    if rolling_support is None:
        rolling_support = safe_float(levels.get("short_support"))
    lines = []
    if time_receipt:
        lines.append(time_receipt)
    if trend.get("status") == "breakdown_confirmed" and support_anchor is not None:
        roles = confirmed_breakdown_level_roles(trend, levels)
        current_support = roles.get("current_support")
        lines.extend([
            (
                f"当前支撑 {compact_level(current_support)}"
                if current_support is not None
                else "当前支撑尚未形成"
            ),
            f"原支撑（收回观察位） {compact_level(support_anchor)}",
        ])
    elif support_anchor is not None:
        rolling_label = "下一潜在支撑" if rolling_support is not None and rolling_support < support_anchor else "当前滚动支撑"
        anchor_label = "已收回待确认锚点" if trend.get("status") == "support_reclaim_watch" else "跌破观察锚点"
        lines.extend([
            f"{anchor_label} {compact_level(support_anchor)}",
            f"{rolling_label} {compact_level(rolling_support)}",
        ])
    else:
        lines.append(f"短线支撑 {compact_level(levels.get('short_support'))}")
    lines.extend([
        f"短线压力 {compact_level(levels.get('short_resistance'))}",
        f"趋势支撑 {compact_level(levels.get('trend_support'))}",
        "",
        "📍 当前状态",
        f"{'上一完整小时位置' if time_receipt else '位置'}：{position_text}",
        f"压力：{pressure_text}",
        f"支撑：{support_text}",
    ])
    return lines


def format_compact_completeness_alert(payload, include_macro=True):
    completeness = payload.get("completeness") or {}
    core = completeness.get("core_market") or {}
    macro = completeness.get("macro") or {}
    missing = list(core.get("missing") or completeness.get("missing") or [])
    pending = list(core.get("pending") or completeness.get("pending") or [])
    macro_missing = list(macro.get("missing") or [])
    macro_stale = list(macro.get("stale") or [])
    snapshots = list(completeness.get("snapshot_notes") or [])
    core_available = core.get("available", completeness.get("available", 0))
    core_total = core.get("total", completeness.get("total", 0))
    macro_available = macro.get("available", 0)
    macro_total = macro.get("total", 0)
    if not include_macro:
        macro_available = macro_total = 0
        macro_missing, macro_stale = [], []
        snapshots = [item for item in snapshots if not item.startswith("Macro")]
    has_alert = (
        core_available != core_total
        or macro_available != macro_total
        or missing
        or pending
        or macro_missing
        or macro_stale
        or snapshots
    )
    if not has_alert:
        return []

    lines = [f"核心 {core_available}/{core_total}" + (f" | 宏观 {macro_available}/{macro_total}" if include_macro else "")]
    if missing:
        lines.append("市场缺失: " + " / ".join(missing))
    if pending:
        lines.append("采集中: " + " / ".join(pending))
    if macro_missing:
        lines.append("宏观缺失: " + " / ".join(macro_missing))
    if macro_stale:
        lines.append("宏观过期: " + " / ".join(macro_stale))
    if snapshots:
        lines.append("快照: " + " / ".join(snapshots))
    return lines


def format_compact_gpt_block(payload):
    completeness = payload.get("completeness") or {}
    core = completeness.get("core_market") or {}
    macro = completeness.get("macro") or {}
    core_available = core.get("available", completeness.get("available", 0))
    core_total = core.get("total", completeness.get("total", 0))
    macro_available = macro.get("available", 0)
    macro_total = macro.get("total", 0)
    alerts = format_compact_completeness_alert(payload)
    if not alerts:
        return [f"数据完整 | 核心 {core_available}/{core_total} | 宏观 {macro_available}/{macro_total}"]
    return alerts


def format_plain_short_macro_block(payload):
    macro = ((payload.get("public_records") or {}).get("macro") or {})
    value = macro.get("value") or {}
    macro_state = ((payload.get("rule_state") or {}).get("macro") or {})
    environment = macro_state.get("environment") or {}
    long_term = macro_state.get("long_term_environment") or {}
    if not record_has_data(macro):
        return ["外部环境数据正在采集，或者已有数据已经过期。"]
    if not record_is_current(macro):
        return format_stale_macro_reference(macro)

    high_rate_pressure = environment.get("high_real_rate_pressure_rule")
    dxy_pressure = rule_any(
        environment.get("dxy_stage_strength_rule"),
        environment.get("dxy_strong_rule"),
    )
    fiscal = value.get("fiscal_proxy") or {}
    long_status = long_term.get("status")
    buyback = value.get("treasury_buyback") or {}
    accepted = safe_float(buyback.get("total_par_amt_accepted"))
    rate_icon = "🔴" if high_rate_pressure is True else "🟢" if high_rate_pressure is False else "⚪"
    dollar_icon = "🔴" if dxy_pressure is True else "⚪"
    money_icon = "🟢" if long_status == "liquidity_conversion" else "⚪"
    us10y_change = format_pp_value(value.get("us10y_30d_pp"), 2)
    real_change = format_pp_value(value.get("real_yield_10y_30d_pp"), 2)

    lines = [
        (
            f"{rate_icon} 美债10Y "
            f"{format_us10y_directional_band_value(value.get('us10y'), value.get('us10y_30d_pp'))}"
            f"（30天 {us10y_change}）"
            " | 实际利率 "
            f"{format_real_yield_directional_band_value(value.get('real_yield_10y'), value.get('real_yield_10y_30d_pp'))}"
            f"（30天 {real_change}）"
        ),
        (
            f"{dollar_icon} 美元指数 "
            f"{format_dxy_directional_band_value(value.get('usd_index'), value.get('usd_index_30d_pct'))}"
            f"（30天 {format_pct_value(value.get('usd_index_30d_pct'), 2)}）"
        ),
        "注：当前/下一档；pp=百分点。",
        (
            f"{money_icon} 长期资金：货币供应量（M2）同比 "
            f"{format_pct_value(value.get('m2_yoy_pct'), 2)}"
            f" | 美联储资产负债表30天 {format_pct_value(value.get('fed_balance_sheet_30d_pct'), 2)}"
        ),
        (
            f"财政：压力{fiscal_pressure_text(fiscal)} | 债务成本{debt_cost_text(fiscal)}"
            f" | 国债回购 {format_money_compact_plain(accepted, 2)}"
        ),
    ]

    if high_rate_pressure is True and long_status in ("expectation_exists", "exists_not_dominant"):
        interpretation = "高实际利率仍在压制；长期资金有改善，但尚未明显传导到比特币。"
    elif high_rate_pressure is True:
        interpretation = "高实际利率仍在压制，长期资金改善尚未确认。"
    elif long_status == "liquidity_conversion":
        interpretation = "利率压力缓和，长期资金改善开始传导到市场。"
    else:
        interpretation = "利率和长期资金方向仍需继续观察。"
    lines.append(f"解读：{interpretation}")
    return lines


def format_signed_money_change(value, decimals=0):
    number = safe_float(value)
    if number is None:
        return "NA"
    sign = "+" if number > 0 else "-" if number < 0 else ""
    return f"{sign}${compact_money(abs(number), decimals).lstrip('+-')}"


def format_plain_short_funds_block(payload):
    public_records = payload.get("public_records") or {}
    market = payload.get("market") or {}
    market_24h_delta = payload.get("market_24h_delta") or {}
    funding = public_records.get("funding") or {}
    basis = public_records.get("basis") or {}
    etf = public_records.get("etf") or {}
    spot_cvd = public_records.get("spot_cvd") or {}
    stablecoin = public_records.get("stablecoin") or {}
    delta4h = payload.get("delta4h") or {}
    checks = ((payload.get("structure_state") or {}).get("checks") or {})
    oi_slow = checks.get("oi_slow_confirmation") or {}
    volume_24h_pct = (market_24h_delta.get("metrics") or {}).get("vol24")
    funding_value = safe_float(record_value(funding, "funding_8h_pct"))
    if funding_value is None:
        funding_text = "持仓费用 NA"
    elif funding_value > 0:
        funding_text = (
            "多头过去8H累计支付"
            f"{format_pct_value(abs(funding_value), 3, signed=False)}"
        )
    elif funding_value < 0:
        funding_text = (
            "空头过去8H累计支付"
            f"{format_pct_value(abs(funding_value), 3, signed=False)}"
        )
    else:
        funding_text = "双方持仓费用接近0%"

    etf_date = etf_source_date(etf) or "未知日期"
    if record_has_data(etf):
        etf_note = "数据过期，仅作背景" if not record_is_current(etf) else "仅日级背景"
        etf_line = (
            f"BTC现货ETF：1D {format_signed_money_change(record_value(etf, 'flow_1d_usd'))}"
            f" | 5D {format_signed_money_change(record_value(etf, 'flow_5d_usd'))}"
            f" | 30D {format_signed_money_change(record_value(etf, 'flow_30d_usd'))}"
            f"（截至 {etf_date}，{etf_note}）"
        )
    else:
        etf_line = "BTC现货ETF：数据不足（仅日级背景）"

    lines = [
        (
            f"合约未平仓：{format_money_compact_plain(market.get('oi_usd'), 2)}"
            f" | 4H {format_pct_value(delta4h.get('oi_4h_pct'), 2)}"
            f" | 24H {format_pct_value(oi_slow.get('delta_24h_pct'), 2)}"
            f" | 72H {format_pct_value(oi_slow.get('delta_72h_pct'), 2)}"
        ),
        (
            f"成交额：合约 4H {format_pct_value(delta4h.get('vol_4h_pct'), 2)}"
            f" / 24H {format_pct_value(volume_24h_pct, 2)}"
            f" | 现货 4H {format_pct_value(record_value(spot_cvd, 'volume_4h_pct'), 2)}"
            f" / 24H {format_pct_value(record_value(spot_cvd, 'volume_24h_pct'), 2)}"
            f"{record_mark(spot_cvd)}"
        ),
        (
            f"现货主动买卖：4H {format_spot_balance_in_words(record_value(spot_cvd, 'cvd_4h_pct'))}"
            f" | 24H {format_spot_balance_in_words(record_value(spot_cvd, 'cvd_24h_pct'))}"
            f"{record_mark(spot_cvd)}"
        ),
        (
            f"稳定币供给变化：24H {format_signed_money_change(record_value(stablecoin, 'flow_24h_usd'))}"
            f" | 7D {format_signed_money_change(record_value(stablecoin, 'flow_7d_usd'))}"
            f"{record_mark(stablecoin)}"
        ),
        *(format_etf_holdings_only(etf) if btc_etf.is_holdings_source(etf) else [etf_line]),
        (
            f"合约成本：{funding_text}{record_mark(funding)}"
            f" | 合约价较现货 {format_pct_value(record_value(basis, 'basis_pct'), 3)}{record_mark(basis)}"
        ),
    ]

    interpretation = []
    if (
        rule_gt(delta4h.get("vol_4h_pct"), 0.0) is True
        and rule_gt(current_record_value(spot_cvd, "volume_4h_pct"), 0.0) is True
        and rule_gt(current_record_value(spot_cvd, "cvd_4h_pct"), 0.0) is True
    ):
        interpretation.append("4H成交和买入回升")
    if rule_any(
        rule_lt(oi_slow.get("delta_24h_pct"), 0.0),
        rule_lt(oi_slow.get("delta_72h_pct"), 0.0),
    ) is True:
        interpretation.append("中期仓位减少")
    stable_outflow = rule_lt(current_record_value(stablecoin, "flow_24h_usd"), 0.0) is True
    if stable_outflow:
        interpretation.append("稳定币供给减少，不能据此判断交易所净入金")
    for record in (funding, basis, spot_cvd, stablecoin):
        if record_has_data(record) and not record_is_current(record):
            lines.append(str(record.get("source") or "采集数据") + record_background_label(record))
    lines.append(
        "解读：" + ("；".join(interpretation) if interpretation else "资金方向仍需继续观察") + "。"
    )
    return lines


def format_plain_short_patterns_block(payload):
    items, config = normalized_market_pattern_items(payload.get("market_patterns") or {})
    if not items:
        return ["形态规则数据不足，暂时无法判断。"]

    conclusion = build_market_pattern_conclusion(payload, items=items, config=config)
    full_lines = format_market_patterns_block(payload)
    replacements = {
        "“未平仓合约量”指市场里还有多少份合约没有关闭。": (
            "“还没有关闭的合约仓位”表示合约市场中仍在场内的资金规模。"
        ),
        "｜🟢 表面偏利好": "｜发生时对价格有利",
        "｜🔴 表面偏利空": "｜发生时对价格不利",
        "｜🔴 发生时表面偏利空": "｜发生时对价格不利",
        "｜🟢 发生时表面偏利好": "｜发生时对价格有利",
        "｜⚪ 表面方向不明": "｜当时方向不明确",
    }
    short_wait_lines = {
        f"• {market_pattern_wait_text(item)}": f"• {market_pattern_short_wait_text(item)}"
        for item in items
        if item.get("state") == "WAIT"
    }
    fact_lines = []
    for line in full_lines:
        if not line.startswith("• "):
            continue
        if "本次暂不判断" in line or "本次不判断" in line:
            continue
        plain_line = short_wait_lines.get(line, line)
        for old, new in replacements.items():
            plain_line = plain_line.replace(old, new)
        fact_lines.append(plain_line)

    current_window = conclusion.get("current_window") or {}
    relation = conclusion.get("relation")
    bias = current_window.get("bias")
    occurrence = current_window.get("occurrence")
    integrity = current_window.get("integrity")
    if integrity == "CONFLICT" or relation == "conflict":
        current_text = "当前：形态结果互相冲突，方向不明确。"
    elif bias == "positive":
        current_text = "当前：过去4小时出现的情况整体偏强。"
    elif bias == "negative":
        current_text = "当前：过去4小时出现的情况整体偏弱。"
    elif bias == "neutral":
        current_text = "当前：过去4小时处于低成交整理，方向不明确。"
    elif occurrence == "WAIT":
        current_text = "当前：仍有形态正在确认，方向不明确。"
    elif occurrence == "NA" or conclusion.get("data_quality") in ("partial", "insufficient"):
        current_text = "当前：方向不明确。"
    elif relation == "event_only":
        current_text = "当前：方向不明确。"
    else:
        current_text = "当前：过去4小时没有形成明确方向。"

    if fact_lines:
        result = fact_lines
    elif conclusion.get("data_quality") in ("partial", "insufficient"):
        equity_note = ("股票复权口径未确认，价格趋势及形态确认暂停。"
                       if 'stock_adjustment_unverified' in (payload.get('signal_data_quality') or {}).get('reasons', [])
                       else "可用价格与股数成交量继续参与判断；依赖缺失指标的形态保持待确认。")
        result = ([equity_note]
                  if assets.current().equity else ["数据不足：部分价格、合约仓位或成交额数据缺失或未对齐，本次不判断4H形态。"])
    else:
        result = ["过去4小时没有出现已定义的常见形态。"]
    result.append(current_text)
    return result


def format_plain_short_current_view_block(payload):
    context = (payload.get("market_time_context") or {})
    if not context:
        return [format_compact_current_view(payload).replace("当前理解：", "", 1)]

    closed_time = format_market_time_ms(context.get("closed_bar_as_of_ms"), closed_bar=True)
    quote_time = format_market_time_ms(context.get("current_quote_at_ms"))
    closed_price = safe_float(context.get("closed_price"))
    current_price = safe_float(context.get("current_price"))
    support = safe_float(context.get("reference_support"))
    pressure = safe_float(context.get("short_resistance"))
    status = context.get("structure_status")
    relation = context.get("current_relation")
    breakdown_phase = context.get("breakdown_phase")
    current_support = safe_float(context.get("current_support"))
    relation_eligible = context.get("relation_eligible") is True
    lines = []

    if closed_time is None or closed_price is None:
        lines.append("上一完整小时：数据不足，暂时无法确认价格阶段。")
    elif status in {
        "support_tested",
        "post_support_consolidation_watch",
        "post_support_consolidation_confirmed",
        "post_support_pressure_returned",
    } and support is not None:
        hold_text = "本次守住" if closed_price >= support else "已收在支撑下方"
        phase_text = {
            "post_support_consolidation_watch": "，回测后进入整理观察",
            "post_support_consolidation_confirmed": "，回测后整理已确认",
            "post_support_pressure_returned": "，回测后卖出压力重新出现",
        }.get(status, "")
        lines.append(
            f"{closed_time} 收 {compact_level(closed_price)}"
            f" | {compact_level(support)} 支撑{hold_text}{phase_text}"
        )
    elif status == "breakdown_confirmed" and support is not None:
        if breakdown_phase == "confirmation_event":
            lines.append(
                f"{closed_time} 收 {compact_level(closed_price)}"
                f" | {compact_level(support)} 原支撑本轮跌破确认"
            )
        else:
            lines.append(
                f"{closed_time} 收 {compact_level(closed_price)}"
                f" | 仍在 {compact_level(support)} 原支撑下方"
            )
    elif status == "breakdown_watch" and support is not None:
        lines.append(
            f"{closed_time} 收 {compact_level(closed_price)}"
            f" | {compact_level(support)} 支撑暂时跌破，等待确认"
        )
    elif status == "support_reclaim_watch" and support is not None:
        lines.append(
            f"{closed_time} 收 {compact_level(closed_price)}"
            f" | 已回到原支撑 {compact_level(support)} 上方，买入资金待确认"
        )
    elif status in ("breakout_attempt", "breakout_hold", "trend_confirmed", "pullback_watch") and pressure is not None:
        state_text = {
            "breakout_attempt": "尝试突破",
            "breakout_hold": "连续站上，持续性待确认",
            "trend_confirmed": "突破已确认",
            "pullback_watch": "突破后回落观察",
        }[status]
        lines.append(
            f"{closed_time} 收 {compact_level(closed_price)}"
            f" | {compact_level(pressure)} 压力{state_text}"
        )
    elif support is not None and pressure is not None:
        lines.append(
            f"{closed_time} 收 {compact_level(closed_price)}"
            f" | 仍在 {compact_level(support)}–{compact_level(pressure)} 区间"
        )
    else:
        lines.append(f"{closed_time} 收 {compact_level(closed_price)}")

    if current_price is None:
        lines.append("当前价格：数据不足。")
    elif not relation_eligible:
        reason = market_time_ineligible_reason_text(context.get("ineligible_reason"))
        lines.append(
            f"{quote_time or '当前'} 现价 {compact_level(current_price)}"
            f" | {reason}，不比较关键位"
        )
    elif relation == "below_support":
        support_usd = abs(safe_float(context.get("support_distance_usd")) or 0.0)
        support_pct = abs(safe_float(context.get("support_distance_pct")) or 0.0)
        if breakdown_phase is not None:
            lines.append(
                f"{quote_time or '当前'} 现价 {compact_level(current_price)}"
                f" | 仍低于 {compact_level(support)} 原支撑 "
                f"{compact_level(support_usd)}（{support_pct:.2f}%）"
            )
        else:
            lines.append(
                f"{quote_time or '当前'} 现价 {compact_level(current_price)}"
                f" | 暂低于 {compact_level(support)} 支撑 "
                f"{compact_level(support_usd)}（{support_pct:.2f}%）"
            )
    elif relation == "temporarily_reclaimed_former_support":
        lines.append(
            f"{quote_time or '当前'} 现价 {compact_level(current_price)}"
            f" | 暂回到 {compact_level(support)} 原支撑上方，等待完整小时收盘确认收回"
        )
    elif relation == "above_resistance":
        resistance_usd = abs(safe_float(context.get("resistance_distance_usd")) or 0.0)
        resistance_pct = abs(safe_float(context.get("resistance_distance_pct")) or 0.0)
        lines.append(
            f"{quote_time or '当前'} 现价 {compact_level(current_price)}"
            f" | 暂高于 {compact_level(pressure)} 压力 "
            f"{compact_level(resistance_usd)}（{resistance_pct:.2f}%）"
        )
    else:
        resistance_usd = abs(safe_float(context.get("resistance_distance_usd")) or 0.0)
        resistance_pct = abs(safe_float(context.get("resistance_distance_pct")) or 0.0)
        lines.append(
            f"{quote_time or '当前'} 现价 {compact_level(current_price)}"
            f" | 距 {compact_level(pressure)} 压力 "
            f"{compact_level(resistance_usd)}（{resistance_pct:.2f}%）"
        )

    required = max(1, int(context.get("required_confirm_hours") or assets.current().parameter("PRICE_TREND_CONFIRM_HOURS", PRICE_TREND_CONFIRM_HOURS)))
    required_text = "两个" if required == 2 else f"{required}个"
    if closed_time is None or closed_price is None or (not relation_eligible and not assets.current().equity):
        conclusion = "数据缺失或未对齐，本次不新增突破或跌破判断。"
    elif status == "trend_confirmed":
        conclusion = "上方突破已经得到完整小时确认。"
    elif status == "breakdown_confirmed":
        current_support_text = (
            f"{compact_level(current_support)} 当前支撑是否守住"
            if current_support is not None
            else "新的当前支撑形成"
        )
        if breakdown_phase == "confirmation_event":
            conclusion = (
                f"本轮支撑跌破已确认；关注能否收回 {compact_level(support)}，"
                f"以及 {current_support_text}。"
            )
        else:
            conclusion = (
                f"原支撑失守状态仍在；关注能否收回 {compact_level(support)}，"
                f"以及 {current_support_text}。"
            )
    elif status == "support_reclaim_watch":
        conclusion = "价格已回到原支撑上方，但买入资金还没跟上。"
    elif relation_eligible and relation in ("above_resistance", "below_support"):
        conclusion = "现价只是临时越过关键位，仍需等待完整小时收盘确认。"
    elif status == "post_support_pressure_returned":
        conclusion = "支撑本次曾守住，但卖出压力重新出现。"
    elif status in ("post_support_consolidation_watch", "post_support_consolidation_confirmed"):
        conclusion = "支撑本次守住，价格正在整理，方向尚未确认。"
    elif status in {
        "support_tested",
    }:
        resistance_pct = abs(safe_float(context.get("resistance_distance_pct")) or 0.0)
        if relation_eligible and relation == "inside_range" and resistance_pct <= 1.0:
            conclusion = "已接近压力，但尚未确认突破。"
        else:
            conclusion = "支撑测试已完成，方向尚未确认。"
    elif status in ("breakout_attempt", "breakout_hold"):
        completed = max(0, int(context.get("above_pressure_hours") or 0))
        conclusion = (
            f"已有 {completed} 个完整小时站上压力，"
            f"需要连续{required_text}完整小时确认突破。"
        )
    else:
        conclusion = "价格仍在支撑和压力之间，方向尚未确认。"
    lines.append(f"结论：{conclusion}")
    return lines


def format_plain_short_level_block(payload):
    levels = payload.get("levels") or {}
    rule_levels = ((payload.get("rule_state") or {}).get("levels") or {})
    trend = rule_levels.get("price_trend") or {}
    context = payload.get("market_time_context") or {}
    support = safe_float(context.get("reference_support"))
    if support is None:
        support = safe_float(levels.get("short_support"))
    pressure = safe_float(context.get("short_resistance"))
    if pressure is None:
        pressure = safe_float(levels.get("short_resistance"))
    trend_support = safe_float(levels.get("trend_support"))
    status = trend.get("status") or rule_levels.get("status")

    support_text = {
        "support_tested": "已测试",
        "post_support_consolidation_watch": "已测试，整理中",
        "post_support_consolidation_confirmed": "已测试，整理确认",
        "post_support_pressure_returned": "曾守住，卖压再现",
        "support_reclaim_watch": "已收回，资金待确认",
        "breakdown_watch": "暂时跌破，等待确认",
        "breakdown_confirmed": "跌破确认",
    }.get(status, rule_levels.get("support_state_text") or "状态待确认")
    pressure_text = {
        "trend_confirmed": "突破确认",
        "pullback_watch": "突破后回踩",
        "breakout_hold": "已站上，持续性待确认",
        "breakout_attempt": "尝试突破",
    }.get(status, rule_levels.get("pressure_state_text") or "尚未突破")

    level_parts = []
    if support is not None:
        level_parts.append(f"{compact_level(support)} 支撑（{support_text}）")
    if pressure is not None:
        level_parts.append(f"{compact_level(pressure)} 压力（{pressure_text}）")
    lines = [" | ".join(level_parts) if level_parts else "关键位置数据不足"]
    if trend_support is not None:
        lines.append(f"{compact_level(trend_support)} 趋势支撑")

    support_anchor = safe_float(trend.get("support_anchor"))
    rolling_support = safe_float(trend.get("rolling_short_support"))
    if status == "breakdown_confirmed" and support_anchor is not None:
        roles = confirmed_breakdown_level_roles(trend, levels)
        current_support = roles.get("current_support")
        level_parts = [
            (
                f"{compact_level(current_support)} 当前支撑"
                if current_support is not None
                else "当前支撑尚未形成"
            )
        ]
        if pressure is not None:
            level_parts.append(f"{compact_level(pressure)} 压力（{pressure_text}）")
        lines = [" | ".join(level_parts)]
        lines.append(f"{compact_level(support_anchor)} 原支撑（收回观察位）")
        if trend_support is not None:
            lines.append(f"{compact_level(trend_support)} 趋势支撑")
        return lines
    if (
        support_anchor is not None
        and rolling_support is not None
        and rolling_support < support_anchor
    ):
        lines.append(f"{compact_level(rolling_support)} 下一支撑")
    return lines


def format_plain_short_data_block(payload):
    alerts = format_compact_completeness_alert(payload, include_macro=False)
    receipt = payload.get("hyperliquid_derivatives") or {}
    if not BTC_PUSH_HYPERLIQUID_ENABLED:
        derivatives_text = "Binance现货/Hyperliquid禁用"
    elif receipt.get("mode") == "primary":
        derivatives_text = (
            "Binance现货/Hyperliquid主源"
            + ("就绪" if receipt.get("primary_ready") else "不足")
        )
    else:
        derivatives_text = (
            "Binance现货/Hyperliquid影子"
            f"{int(receipt.get('continuous_hours') or 0)}/"
            f"{HYPERLIQUID_PRIMARY_MIN_HOURS}H"
        )
    if not alerts:
        return [f"本标的采集自检通过（策略仍需独立核验）｜{derivatives_text}"]
    return [
        "数据存在以下缺口，本次结论已降低确定性。",
        *alerts,
        f"来源：{derivatives_text}",
    ]


def build_equity_report(now_tokyo, payload, forced=False, query=False, full=False, limit_length=True):
    identity = payload["instrument"]
    asset, symbol = identity["asset_id"], identity["derivatives_symbol"]
    proxy = identity["derivatives_relation"] == "proxy"
    market, records = payload["market"], payload.get("public_records") or {}
    funding, basis = records.get("funding") or {}, records.get("basis") or {}
    derivatives = payload.get("derivatives_market") or {}
    quality = payload.get("signal_data_quality") or {}
    volume = market.get("session_volume_shares")
    title = f"📊 {asset} {compact_time_label(now_tokyo)}" + ("（QUERY）" if query else "（FORCE）" if forced else "")
    phase = {"REGULAR": "正常盘", "PRE": "盘前", "POST": "盘后", "OVERNIGHT": "夜盘", "CLOSED": "休市", "UNKNOWN": "交易状态待确认"}.get((payload.get("market_session") or {}).get("phase"), "待确认")
    price_text = f"{market['price']:,.2f}" if market.get("price") is not None else "NA"
    lines = [title + "｜辅助观察", "", f"💰 {price_text} USD/股 | {phase}",
             f"来源 {identity['price_symbol']}；成交量 {compact_number(volume, 0) if volume is not None else 'NA'} 股（源接口当日口径）",
             "股票价格 USD，支付/结算 USDC；合约计价 USDT。",
             ("报价采集失败，当前价格待确认。" if market.get("quote_collection_error") else
              ("报价源时间 " + format_market_time_ms(market.get("quote_source_as_of_ms")) if market.get("quote_source_as_of_ms") else
               "报价源时间未提供；抓取时间 " + datetime.fromtimestamp(market.get("quote_retrieved_at", time.time()), TOKYO_TZ).strftime("%m-%d %H:%M JST"))),
             f"公开流闭合K线：{format_market_time_ms(quality.get('latest_close_ms'), closed_bar=True) or '积累中'}。",
             "", format_common_reference(payload)]
    lines += ["", "📍 合约观察", (f"{symbol}：标普500衍生品参考（代理，非VOO自身合约）" if proxy else f"{symbol}：QQQ直接合约参考")]
    lines += [f"OI {format_money_compact_plain(derivatives.get('oi_usd'), 2)} USDT | 合约数量 {compact_number(derivatives.get('oi_quantity'), 2)}",
              f"普通Funding：8H {format_pct_value(record_value(funding, 'funding_8h_pct'), 3)} | 24H {format_pct_value(record_value(funding, 'funding_24h_pct'), 3)}",
              f"{symbol} 标记价/自身指数基差 {format_pct_value(record_value(basis, 'basis_pct'), 3)}"]
    special = (funding.get("value") or {}).get("special_settlements") or []
    regular_time = record_value(funding, "latest_funding_time_ms")
    if regular_time:
        regular_stamp = datetime.fromtimestamp(regular_time / 1000, TOKYO_TZ).strftime("%m-%d %H:%M JST")
        lines.append("普通结算截至 " + regular_stamp + ("；已过期，仅作历史参考。" if funding.get("source_stale") else "。"))
    if special:
        latest = special[-1]
        stamp = datetime.fromtimestamp(latest["time"] / 1000, TOKYO_TZ).strftime("%m-%d %H:%M JST")
        lines.append(f"股息调整 Special：最近 {format_pct_value(latest.get('rate') * 100 if latest.get('rate') is not None else None, 4)}（{stamp}），不计入普通Funding。")
    elif "special_settlements" in (funding.get("value") or {}):
        lines.append("股息调整 Special：当前返回窗口无记录。")
    else:
        lines.append("股息调整 Special：采集不可用，暂不能确认。")
    if proxy:
        lines.append("VOO价位仅使用EQ_VOO；不计算VOO/SPY跨基金基差，SPY不作为VOO自身资金确认。")
    if quality.get("usable"):
        delta = payload.get("delta4h") or {}
        lines += ["", "📐 过去4小时发生了什么",
                  f"价格 {format_pct_value(delta.get('price_4h_pct'), 2)} | 股数成交量 {format_pct_value(delta.get('vol_4h_pct'), 2)}"]
        pattern_lines = format_market_patterns_block(payload) if full else format_plain_short_patterns_block(payload)
        lines.extend(line.replace("成交额", "股数成交量").replace("数据不足：", "适用性与待确认项：") for line in pattern_lines)
        lines += ["", "🎯 现在怎么看"]
        lines.extend(format_plain_short_current_view_block(payload))
        lines += ["", "📌 市场结构"]
        lines.extend(line for line in (format_level_block(payload) if full else format_plain_short_level_block(payload))
                     if not line.startswith("旧调试:"))
    completeness = payload.get("completeness") or {}
    lines += ["", "🤖 数据状态"]
    if not quality.get("usable"):
        lines.append("股票历史与复权口径尚待确认，趋势及关键位确认暂停；原始观察、覆盖窗口和具体缺口见附件。")
    lines.append("自身ETF申赎/持仓：尚缺公开稳定来源；股票Spot CVD：缺少主动成交方向。")
    # Keep the full reasons in evidence; repeated empty analysis sections do not
    # add information while the public price series is warming up.
    if quality.get("snapshot_used"):
        lines.append("本次小时线采集失败，附件保留此前观察及原始时间。")
    if quality.get("usable") and completeness.get("missing"):
        lines.append("采集缺失/窗口不足：" + "、".join(completeness["missing"]))
    return render_report(lines, budget=TG_SAFE_LEN) if limit_length else "\n".join(lines)


def build_full_report_message(now_tokyo, is_new_day, today, push_count, payload, forced=False, query=False, limit_length=True):
    if (payload.get("instrument") or {}).get("calendar") == "US_EQUITY":
        with assets.asset_scope(payload["instrument"]["asset_id"]):
            return build_equity_report(now_tokyo, payload, forced, query, True, limit_length)
    lines = []
    title = f"📊 BTC 整点推送 {now_tokyo}"
    if forced:
        title += "（FORCE）"
    elif query:
        title += "（QUERY）"
    lines.append(title + "｜辅助观察")
    if is_new_day:
        lines.append(f"📅 新的一天：{today}（东京时间进度已更新）")
    lines.append(f"🧾 今日进度：第 {push_count} 次推送（JST）")
    lines.append("")
    lines.append("📌 市场快照")
    lines.extend(format_market_block(payload))
    lines.append("")
    lines.append(format_common_reference(payload))
    lines.append("")
    lines.append("📍 OI / 杠杆")
    lines.extend(format_leverage_block(payload))
    lines.append("")
    lines.append("🏦 ETF / 持仓")
    lines.extend(format_etf_block(payload))
    lines.append("")
    lines.append("💰 现货 / 稳定币")
    lines.extend(format_spot_block(payload))
    lines.extend(seller_exhaustion.format_lines(payload))
    lines.append("")
    lines.append("📐 过去4小时发生了什么")
    lines.extend(format_market_patterns_block(payload))
    lines.append("")
    lines.append("🎯 信号")
    lines.extend(format_signal_block(payload, include_macro=False))
    lines.append("")
    lines.append("🧭 规则报告")
    lines.extend(format_final_view(payload, include_macro=False))
    lines.append("")
    lines.append("📌 市场结构")
    lines.extend(format_level_block(payload))
    lines.append("")
    lines.append("📰 新闻标题")
    lines.extend(format_news_lines(payload.get("news_titles") or []))
    lines.append("")
    lines.append("📎 采集自检（策略仍需独立核验）")
    lines.extend(format_completeness_block(payload, include_macro=False))
    return render_report(lines, budget=TG_SAFE_LEN) if limit_length else "\n".join(lines)


def build_report_message(now_tokyo, is_new_day, today, push_count, payload, forced=False, query=False):
    if (payload.get("instrument") or {}).get("calendar") == "US_EQUITY":
        with assets.asset_scope(payload["instrument"]["asset_id"]):
            return build_equity_report(now_tokyo, payload, forced, query)
    market = payload["market"]
    lines = []
    title = f"📊 BTC {compact_time_label(now_tokyo)}"
    if forced:
        title += "（FORCE）"
    elif query:
        title += "（QUERY）"
    lines.append(title + "｜辅助观察")
    lines.append("")
    lines.append(f"💰 {format_structure_price(market.get('price'))}")
    lines.append(f"24H {format_pct_value(market.get('chg24'), 2)} | 7D {format_pct_value(payload.get('price_7d_pct'), 2)}")
    lines.append(format_plain_short_fear_greed_line(payload.get("fear_greed") or {}))
    lines.append("")
    lines.append(format_common_reference(payload))
    lines.append("")
    lines.append("📍 资金")
    lines.extend(format_plain_short_funds_block(payload))
    lines.extend(seller_exhaustion.format_lines(payload))
    lines.append("")
    lines.append("📐 过去4小时发生了什么")
    lines.extend(format_plain_short_patterns_block(payload))
    lines.append("")
    lines.append("🎯 现在怎么看")
    lines.extend(format_plain_short_current_view_block(payload))
    lines.append("")
    lines.append("📌 市场结构")
    lines.extend(format_plain_short_level_block(payload))

    news_titles = payload.get("news_titles") or []
    lines.append("")
    lines.append("📰 新闻")
    lines.extend(format_news_lines(news_titles) if news_titles else ["本小时无新增新闻。"])

    lines.append("")
    lines.append("🤖 数据状态")
    lines.extend(format_plain_short_data_block(payload))

    return render_report(lines, budget=TG_SAFE_LEN)


def get_current_derivatives_market(state, raw_signal_bars, display_price):
    if assets.current().equity:
        return get_equity_derivatives_market(state)
    if HYPERLIQUID_MODE == "primary":
        receipt = build_hyperliquid_oi_receipt(state)
        latest_ctx = ((state.get("hyperliquid_derivatives") or {}).get("latest_ctx") or {})
        if receipt.get("primary_ready") and receipt.get("latest_ctx_current"):
            return {
                "mark_price": safe_float(latest_ctx.get("mark_px")),
                "oi_usd": safe_float(latest_ctx.get("open_interest_usd")),
                "oi_btc": safe_float(latest_ctx.get("open_interest_btc")),
                "source": HYPERLIQUID_SOURCE,
            }
        return {
            "mark_price": None,
            "oi_usd": None,
            "oi_btc": None,
            "source": HYPERLIQUID_SOURCE,
            "reason": receipt.get("reason") or "primary_not_ready",
        }
    try:
        oi_btc, mark_price, oi_usd = get_btc_oi()
        return {
            "mark_price": mark_price,
            "oi_usd": oi_usd,
            "oi_btc": oi_btc,
            "source": "Binance Futures openInterest",
        }
    except Exception:
        log_exception("current open interest skipped; fallback to latest Binance OI Hist")
        latest_bar = raw_signal_bars[-1] if raw_signal_bars else {}
        return {
            "mark_price": display_price,
            "oi_usd": safe_float(latest_bar.get("oi_value")),
            "oi_btc": safe_float(latest_bar.get("oi")),
            "source": "Binance openInterestHist fallback",
        }


def collect_report_payload(state, require_usable_signal_data=False):
    started = time.perf_counter()
    merge_latest_hyperliquid_state(state)
    raw_signal_bars, core_metrics = public_io.measure(lambda: get_signal_bars(state))
    signal_data_quality = build_signal_data_quality(raw_signal_bars)
    if assets.current().equity and state.get("stock_history_error"):
        signal_data_quality.update(snapshot_used=True, collection_error=state["stock_history_error"])
    target = (assets.latest_expected_close(time.time() * 1000) if assets.current().equity
              else int(time.time() // 3600) * 3600000 - 1)
    signal_data_quality["target_close_ms"] = target
    if abs(int(signal_data_quality.get("latest_close_ms") or 0) - target) > 1000:
        signal_data_quality["usable"] = False
        signal_data_quality["reasons"] = list(dict.fromkeys(
            list(signal_data_quality.get("reasons") or []) + ["target_hour_missing"]))
    if require_usable_signal_data and not signal_data_quality.get("usable") and not assets.current().equity:
        log_line("core_wait_metrics " + json.dumps(core_metrics, ensure_ascii=False))
        raise SignalDataNotReadyError("signal data not ready: " + ",".join(signal_data_quality.get("reasons") or []))
    try:
        display_market, market_metrics = public_io.measure(get_display_market)
    except Exception as exc:
        if not assets.current().equity:
            raise
        display_market = {"price": None, "mcap": None, "chg24": None, "vol24": None,
                          "quote_collection_error": type(exc).__name__, "quote_source_time_missing": True,
                          "source": "Binance Stocks " + assets.current().price_symbol}
        market_metrics = {"failure": type(exc).__name__}
    current_quote_at_ms = int(time.time() * 1000)
    signal_bars = raw_signal_bars if signal_data_quality.get("usable") else []
    derivatives_market = get_current_derivatives_market(
        state,
        raw_signal_bars,
        display_market["price"],
    )
    mark_price = derivatives_market.get("mark_price")
    oi_usd = derivatives_market.get("oi_usd") if assets.current().derivatives_relation == "direct" else None
    fear_greed, fear_metrics = public_io.measure(
        (lambda: {**assets.not_applicable("crypto_fear_greed"), "available": False})
        if assets.current().equity else get_fear_greed_index)
    if not fear_greed_is_current(fear_greed):
        fear_greed = {**fear_greed, "available": False, "source_stale": True}

    price = display_market["price"]
    mcap = display_market["mcap"]
    chg24 = display_market["chg24"]
    vol24 = display_market["vol24"]

    snap = {
        "ts": int(time.time()),
        "tokyo": tokyo_now().strftime("%Y-%m-%d %H:%M"),
        "price": price,
        "mcap": mcap,
        "chg24": chg24,
        "oi_usd": oi_usd,
        "oi_btc": derivatives_market.get("oi_btc"),
        "oi_source": derivatives_market.get("source"),
        "mark_price": mark_price,
        "vol24": vol24,
        "source": display_market.get("source"),
        "current_quote_at_ms": None if assets.current().equity else current_quote_at_ms,
    }
    append_snapshot(state, snap)
    recycle_state(state)

    structure_state = analyze_structure(
        signal_bars,
        lookback=assets.current().parameter("STRUCTURE_LOOKBACK_H", STRUCTURE_LOOKBACK_H),
        buffer_pct=assets.current().parameter("STRUCTURE_BREAKOUT_BUFFER_PCT", STRUCTURE_BREAKOUT_BUFFER_PCT),
        buffer_atr_mult=assets.current().parameter("STRUCTURE_BREAKOUT_BUFFER_ATR", STRUCTURE_BREAKOUT_BUFFER_ATR),
        oi_fast_window_h=assets.current().parameter("STRUCTURE_OI_FAST_WINDOW_H", STRUCTURE_OI_FAST_WINDOW_H),
        oi_fast_min_delta_pct=assets.current().parameter("STRUCTURE_OI_FAST_MIN_DELTA_PCT", STRUCTURE_OI_FAST_MIN_DELTA_PCT),
        oi_fast_min_rising_steps=assets.current().parameter("STRUCTURE_OI_FAST_MIN_RISING_STEPS", STRUCTURE_OI_FAST_MIN_RISING_STEPS),
        oi_slow_day_h=assets.current().parameter("STRUCTURE_OI_SLOW_DAY_H", STRUCTURE_OI_SLOW_DAY_H),
        oi_slow_trend_h=assets.current().parameter("STRUCTURE_OI_SLOW_TREND_H", STRUCTURE_OI_SLOW_TREND_H),
        oi_slow_day_min_delta_pct=assets.current().parameter("STRUCTURE_OI_SLOW_DAY_MIN_DELTA_PCT", STRUCTURE_OI_SLOW_DAY_MIN_DELTA_PCT),
        oi_slow_trend_min_delta_pct=assets.current().parameter("STRUCTURE_OI_SLOW_TREND_MIN_DELTA_PCT", STRUCTURE_OI_SLOW_TREND_MIN_DELTA_PCT),
        volume_fast_window_h=assets.current().parameter("STRUCTURE_VOLUME_FAST_WINDOW_H", STRUCTURE_VOLUME_FAST_WINDOW_H),
        volume_fast_baseline_h=assets.current().parameter("STRUCTURE_VOLUME_FAST_BASELINE_H", STRUCTURE_VOLUME_FAST_BASELINE_H),
        volume_fast_multiplier=assets.current().parameter("STRUCTURE_VOLUME_FAST_MULTIPLIER", STRUCTURE_VOLUME_FAST_MULTIPLIER),
        volume_slow_day_h=assets.current().parameter("STRUCTURE_VOLUME_SLOW_DAY_H", STRUCTURE_VOLUME_SLOW_DAY_H),
        volume_slow_baseline_days=assets.current().parameter("STRUCTURE_VOLUME_SLOW_BASELINE_DAYS", STRUCTURE_VOLUME_SLOW_BASELINE_DAYS),
        volume_slow_multiplier=assets.current().parameter("STRUCTURE_VOLUME_SLOW_MULTIPLIER", STRUCTURE_VOLUME_SLOW_MULTIPLIER),
        pullback_atr_mult=assets.current().parameter("STRUCTURE_PULLBACK_ATR", STRUCTURE_PULLBACK_ATR),
        fail_atr_mult=assets.current().parameter("STRUCTURE_FAIL_ATR", STRUCTURE_FAIL_ATR),
    )
    delta4h = calc_delta_4h_from_bars(signal_bars)
    if assets.current().equity and not signal_data_quality.get("natural_window_current", True):
        delta4h = None
    model_snapshots = bars_to_model_snapshots(signal_bars)
    asset_model = MarketModel(parameter=assets.current().parameter) if assets.current().equity else model
    model_state = asset_model.build_state_from_delta(delta4h, snapshots=model_snapshots)
    model_state = apply_structure_gate(model_state, structure_state)
    market = {
        "price": price,
        "mcap": mcap,
        "chg24": chg24,
        "vol24": vol24,
        "oi_usd": oi_usd,
        "oi_btc": derivatives_market.get("oi_btc"),
        "oi_source": derivatives_market.get("source"),
        "mark_price": mark_price,
        "source": display_market.get("source"),
        "mcap_source": display_market.get("mcap_source"),
        **{key: display_market.get(key) for key in (
            "quote_source_as_of_ms", "quote_window_start_ms", "quote_retrieved_at",
            "quote_endpoint_url", "mcap_source_as_of_ms", "mcap_retrieved_at", "mcap_endpoint_url",
            "mcap_cache_used", "mcap_cache_age_sec",
            "quote_source_time_missing", "price_currency", "volume_unit", "session_volume_shares", "previous_close",
            "quote_collection_error",
        )},
        "current_quote_at_ms": None if assets.current().equity else current_quote_at_ms,
    }
    market_24h_delta = build_market_24h_delta(state, market, signal_bars=signal_bars)
    seven_day_comparison = build_7d_comparison(state, market, model_state, fear_greed)

    merge_latest_liquidation_ws_state(state)
    public_snapshot = collect_public_snapshot(state)
    append_public_snapshot(state, public_snapshot)
    public_records = select_public_records(state, public_snapshot)
    hyperliquid_receipt = (assets.not_applicable("crypto_derivatives") if assets.current().equity
                            else build_hyperliquid_oi_receipt(state))
    news_titles = []
    if not assets.current().equity:
        try:
            titles = get_news_titles(30)
        except Exception:
            titles = []
        news_titles = dedupe_news_titles(state, [title for title in titles
                                               if is_cn_news_title(title) and is_relevant_cn_news_title(title)])

    payload = {
        "instrument": assets.current().identity(),
        "derivatives_market": derivatives_market,
        "derivative_bars": state.get("derivative_bars") if assets.current().equity else None,
        "market_session": assets.market_session() if assets.current().equity else {"open": True, "phase": "24/7"},
        "calendar_periods": assets.calendar_observation() if assets.current().equity else None,
        "history_revision_event": state.get("history_revision_event"),
        "market": market,
        "raw_signal_bars": raw_signal_bars,
        "market_24h_delta": market_24h_delta,
        "fear_greed": fear_greed,
        "seven_day_comparison": seven_day_comparison,
        "structure_state": structure_state,
        "model_state": model_state,
        "delta4h": delta4h,
        "signal_data_quality": signal_data_quality,
        "price_7d_pct": calc_bars_delta_pct(signal_bars, 168),
        "levels": build_market_levels(signal_bars, structure_state),
        "public_snapshot": public_snapshot,
        "public_records": public_records,
        "hyperliquid_derivatives": hyperliquid_receipt,
        "news_titles": news_titles,
    }
    try:
        payload["public_history"] = market_observations.collect(
            assets.current().asset_id, market_history.history_path(STATE_FILE))
    except Exception as exc:
        payload["public_history"] = {"history": {"status": "unavailable", "reason": type(exc).__name__,
                                                "rows": [], "values": {}}}
    payload["market_patterns"] = build_market_patterns(payload, signal_bars, state)
    payload["rule_state"] = build_rule_state(payload, state)
    payload["market_time_context"] = build_market_time_context(payload)
    payload["market_time_context"]["summary_text"] = build_market_time_summary(
        payload["market_time_context"]
    )
    payload["status_layer"] = build_status_layer(payload)
    payload["market_patterns"]["conclusion"] = build_market_pattern_conclusion(payload)
    payload["signals"] = build_signals(delta4h, structure_state, public_records, fear_greed)
    payload["signals"]["etf_outflow"] = payload["rule_state"]["etf"].get("flow_outflow_rule") is True
    payload["completeness"] = build_completeness(payload)
    payload["collector_metrics"] = {
        "duration_ms": round((time.perf_counter() - started) * 1000, 3),
        "core": core_metrics, "market": market_metrics, "fear_greed": fear_metrics,
        "sources": {key: record.get("collector_metrics") for key, record in public_snapshot["public"].items()},
    }
    log_line("collection_metrics " + json.dumps(payload["collector_metrics"], ensure_ascii=False))
    return payload


def append_success_log(state, today, payload, forced):
    logs = state.get("event_logs") or []
    etf_rule = ((payload.get("rule_state") or {}).get("etf") or {})
    logs.append(compact_event_log({
        "ts": int(time.time()),
        "day": today,
        "forced": bool(forced),
        "schema": REPORT_SCHEMA,
        "revision": REPORT_SCHEMA_REVISION,
        "fear_greed": payload["fear_greed"],
        "seven_day_comparison": payload["seven_day_comparison"],
        "structure_state": payload.get("structure_state"),
        "market_24h_delta": payload.get("market_24h_delta"),
        "public_snapshot": payload.get("public_snapshot"),
        "completeness": payload.get("completeness"),
        "signal_data_quality": payload.get("signal_data_quality"),
        "market_time_context": payload.get("market_time_context"),
        "rule_state": payload.get("rule_state"),
        "status_layer": payload.get("status_layer"),
        "market_patterns": payload.get("market_patterns"),
        "etf_rule_eligible": etf_rule.get("etf_rule_eligible"),
        "etf_source_date": etf_rule.get("source_date"),
        "etf_rule_ineligible_reason": etf_rule.get("ineligible_reason"),
        "macro": ((payload.get("public_records") or {}).get("macro") or {}),
        "liquidation_ws": state.get("liquidation_ws"),
        "hyperliquid_derivatives": payload.get("hyperliquid_derivatives"),
    }))
    state["event_logs"] = logs
    recycle_state(state)


_DELIVERY_PROTECTED_KEYS = frozenset({
    "applied_report_ids", "repair_markers", "last_repair_command_id", "repaired_ts",
    "tg_update_offset", "day_tokyo", "day_push_count", "day_first_push_tokyo",
    "event_logs", "last_success_ts", "last_success_tokyo", "last_query_ts", "last_query_tokyo",
    "report_generated_at", "snapshots", "public_snapshots",
})


def scheduled_report_id(timestamp):
    prefix = "" if assets.current().asset_id == "BTC" else assets.current().asset_id.lower() + ":"
    return f"{prefix}hour:{int(timestamp) // 3600 * 3600}"


def next_scheduled_push_ts():
    # seconds_until_next_hour_tokyo 返回截断的整数秒；向上取整避免提前跨桶。
    return math.ceil(time.time() + max(1, seconds_until_next_hour_tokyo()))


def build_delivery_document(payload, report_text, report_id, generated_at, full_report_text=None):
    # Deploy the V2 reader before reloading this producer. Partial data must
    # degrade individual datasets, never silently disable an asset's attachment.
    if not BTC_PUSH_EVIDENCE_ENABLED:
        return None
    payload = {**payload, "evidence_version": 2}
    from btc_evidence import build_bundle, encode_bundle
    stamp = datetime.fromtimestamp(generated_at, TOKYO_TZ).isoformat()
    return encode_bundle(build_bundle(payload, report_text, report_id, stamp,
                                      full_report_text=full_report_text))


def _merge_delivery_snapshots(state, commit):
    snapshot = commit.get("snapshot")
    if isinstance(snapshot, dict) and int(snapshot.get("ts") or 0) > 0:
        timestamp = int(snapshot["ts"])
        hour = snapshot_hour_key(timestamp)
        newer = any(int(row.get("ts") or 0) >= timestamp
                    and snapshot_hour_key(int(row["ts"])) == hour
                    for row in state.get("snapshots") or [] if int(row.get("ts") or 0) > 0)
        if not newer:
            append_snapshot(state, copy.deepcopy(snapshot))
    public = commit.get("public_snapshot")
    if isinstance(public, dict) and int(public.get("ts") or 0) > 0:
        # 已保存的同时间快照优先；旧投递不能覆盖后来采集的同一小时数据。
        merged = {int(public["ts"]): copy.deepcopy(public)}
        merged.update({int(row["ts"]): row for row in state.get("public_snapshots") or []
                       if int(row.get("ts") or 0) > 0})
        ordered = [merged[key] for key in sorted(merged)][-PUBLIC_SNAPSHOT_MAX:]
        state["public_snapshots"] = compact_public_snapshots(ordered)


_DELIVERY_CACHE_KEYS = frozenset({"macro_cache", "ollama_summary_cache", "news_seen", "news_title_cn_cache"})


def _merge_delivery_caches(latest, candidate):
    for key, time_key in (("macro_cache", "updated_at"), ("ollama_summary_cache", "ts")):
        incoming = candidate.get(key)
        current = latest.get(key) or {}
        if not isinstance(incoming, dict):
            continue
        if key == "macro_cache" and incoming.get("schema_version") != MACRO_CACHE_SCHEMA_REVISION:
            continue
        if int(incoming.get(time_key) or 0) > int(current.get(time_key) or 0):
            latest[key] = copy.deepcopy(incoming)
    seen = dict(latest.get("news_seen") or {})
    for key, stamp in (candidate.get("news_seen") or {}).items():
        if int(stamp or 0) > int(seen.get(key) or 0):
            seen[key] = stamp
    latest["news_seen"] = seen
    titles = copy.deepcopy(latest.get("news_title_cn_cache") or {})
    for key, incoming in (candidate.get("news_title_cn_cache") or {}).items():
        if not isinstance(incoming, dict) or incoming.get("ver") != NEWS_TRANSLATION_CACHE_VERSION:
            continue
        current = titles.get(key) or {}
        if current.get("ver") != NEWS_TRANSLATION_CACHE_VERSION or int(incoming.get("ts") or 0) > int(current.get("ts") or 0):
            titles[key] = copy.deepcopy(incoming)
    latest["news_title_cn_cache"] = titles


def apply_delivery_to_state(report_id, commit, receipt):
    with assets.asset_scope(commit.get("asset_id", "BTC")):
        return _apply_delivery_to_state(report_id, commit, receipt)


def _apply_delivery_to_state(report_id, commit, receipt):
    """在 worker 锁内重新读主状态；一次摘要回执只提交一次计数。"""
    with STATE_LOCK:
        latest = load_state()
        applied = dict(latest.get("applied_report_ids") or {})
        if report_id in applied:
            return
        sent_ts = receipt.get("date")
        if type(sent_ts) is not int or sent_ts <= 0:
            sent_ts = int(time.time())
        sent_time = datetime.fromtimestamp(sent_ts, TOKYO_TZ)
        sent_day = sent_time.strftime("%Y-%m-%d")
        sent_label = sent_time.strftime("%Y-%m-%d %H:%M JST")
        if commit.get("kind") not in ("repair_ack", "cached_query"):
            generated_at = float(commit.get("generated_at") or 0)
            same_repair = (commit.get("repair_marker") == latest.get("last_repair_command_id")
                           and int(commit.get("repaired_ts") or 0) == int(latest.get("repaired_ts") or 0))
            candidate = commit.get("candidate") or {}
            current_revision = int(latest.get("price_history_revision") or 0)
            incoming_revision = int(candidate.get("price_history_revision") or 0)
            same_price_basis = not assets.current().equity or incoming_revision >= current_revision
            if same_repair:
                if same_price_basis:
                    if assets.current().equity and incoming_revision > current_revision:
                        for key in assets.PRICE_STATE_KEYS:
                            latest.pop(key, None)
                    _merge_delivery_snapshots(latest, commit)
                _merge_delivery_caches(latest, candidate)
            # 后送达的旧报告不能覆盖更新报告，也不能复活 repair 清除的缓存。
            if (generated_at >= float(latest.get("report_generated_at") or 0)
                    and same_repair and same_price_basis):
                candidate = commit.get("candidate") or {}
                latest.update({k: copy.deepcopy(v) for k, v in candidate.items()
                               if k not in _DELIVERY_PROTECTED_KEYS and k not in _DELIVERY_CACHE_KEYS and k not in DOMAINS})
                latest["report_generated_at"] = generated_at
            if commit.get("query"):
                if sent_ts >= int(latest.get("last_query_ts") or 0):
                    latest.update(last_query_ts=sent_ts, last_query_tokyo=sent_label)
            else:
                # 补旧日的本地回执时不回退当前日计数。
                if not latest.get("day_tokyo") or latest["day_tokyo"] < sent_day:
                    latest.update(day_tokyo=sent_day, day_push_count=0,
                                  day_first_push_tokyo=sent_label)
                if latest.get("day_tokyo") == sent_day:
                    latest["day_push_count"] = int(latest.get("day_push_count") or 0) + 1
                if sent_ts >= int(latest.get("last_success_ts") or 0):
                    latest.update(last_success_ts=sent_ts, last_success_tokyo=sent_label)
                event = copy.deepcopy(commit.get("event") or {})
                event.update(report_id=report_id, ts=sent_ts, day=sent_day)
                latest["event_logs"] = list(latest.get("event_logs") or []) + [event]
        applied[report_id] = sent_ts
        latest["applied_report_ids"] = applied
        recycle_state(latest)
        # 新 save_state 在锁内保留较新的侧车；回执不写入旧采集域快照。
        for domain in DOMAINS:
            latest.pop(domain, None)
        save_state(latest)
        log_line(f"delivery state applied report_id={report_id} "
                 f"generated_to_sent_sec={max(0, sent_ts-float(commit.get('generated_at') or sent_ts)):.3f} "
                 f"sent_to_persist_sec={max(0, time.time()-sent_ts):.3f}")


def pump_pending_deliveries(max_parts=2, report_id=None):
    store = get_delivery_store()
    results = []
    spec = assets.current()
    store.apply_pending(apply_delivery_to_state, asset_id=spec.asset_id)
    if report_id is None:
        if spec.equity and not assets.market_session()["open"]:
            store.expire_scheduled(spec.asset_id)
            return results
        store.expire_scheduled(spec.asset_id, int(time.time() // 3600) * 3600)
    for _ in range(max_parts):
        send_document = (lambda document, reply_to_message_id: tg_send_document(
            document, reply_to_message_id, filename=spec.asset_id.lower() + "-evidence.json")) if spec.equity else tg_send_document
        result = store.deliver_one(tg_send, send_document, asset_id=spec.asset_id, report_id=report_id)
        if result is None:
            break
        results.append(result)
        log_line(f"delivery report_id={result['report_id']} part={result['part']} status={result['status']}")
        store.apply_pending(apply_delivery_to_state, asset_id=spec.asset_id)
    return results


def delivery_next_due_at():
    return get_delivery_store().next_due_at()


def _report_delivery_result(report_id):
    status = (get_delivery_store().get_report(report_id) or {}).get("summary_status")
    return {"sent": PUSH_RESULT_SENT, "failed": PUSH_RESULT_FAILED,
            "unknown": PUSH_RESULT_UNKNOWN}.get(status, PUSH_RESULT_QUEUED)


def _finish_queued_command(command, report_id):
    if isinstance(command, dict):
        get_delivery_store().finish_report_command(command["update_id"], report_id)


def _enqueue_repair(report_id, command):
    store = get_delivery_store()
    with store.worker_lock(assets.current().asset_id):
        state = load_state()
        markers = dict(state.get("repair_markers") or {})
        if report_id not in markers:
            do_repair(state)
            state["news_seen"] = {}
            state.pop("ollama_summary_cache", None)
            state.pop("news_title_cn_cache", None)
            markers[report_id] = int(time.time())
            state["repair_markers"] = markers
            state["last_repair_command_id"] = report_id
            save_state(state, reset_collectors=True)
            if not assets.current().equity:
                _persisted_hyperliquid_minutes.pop(os.path.abspath(STATE_FILE), None)
            else:
                store.expire_scheduled(assets.current().asset_id)
        store.enqueue_report(report_id, f"✅ {assets.current().asset_id} 已执行 /repair：状态与缓存已重置。",
                             None, {"kind": "repair_ack", "asset_id": assets.current().asset_id},
                             command_id=command["update_id"] if isinstance(command, dict) else None)


_asset_report_locks = {asset: threading.RLock() for asset in assets.INSTRUMENTS}
_common_report_lock = threading.RLock()


def pump_common_deliveries(report_id=None):
    store = get_delivery_store()
    apply = lambda identity, commit, receipt: common.apply_receipt(store, identity, commit, receipt)
    store.apply_pending(apply, asset_id="COMMON")
    store.expire_scheduled("COMMON", common.hour_start(time.time()))
    result = store.deliver_one(tg_send, tg_send_document, asset_id="COMMON", report_id=report_id)
    if result:
        log_line(f"delivery report_id={result['report_id']} part={result['part']} status={result['status']}")
    store.apply_pending(apply, asset_id="COMMON")
    return result


def send_common_push(dry_run=False, cmd=None, check_commands=False,
                     require_usable_signal_data=False, report_id=None):
    with _common_report_lock:
        store = get_delivery_store() if not dry_run else None
        if isinstance(cmd, dict):
            report_id = f"tg:{cmd['update_id']}"
        report_id = report_id or ("manual:common:" + uuid.uuid4().hex if cmd else common.report_id(time.time()))
        if store and store.get_report(report_id) is not None:
            _finish_queued_command(cmd, report_id)
            pump_common_deliveries(report_id)
            return _report_delivery_result(report_id)
        if cmd:
            latest = (store.load_metadata("common:latest_report") or {}) if store else {}
            if latest:
                stamp = datetime.fromtimestamp(latest["generated_at"], TOKYO_TZ).strftime("%m-%d %H:%M JST")
                text = f"最近公共简报：{stamp}（查询原报告）\n\n" + latest["text"]
            else:
                text = "🏛 公共宏观与事件简报：尚无已发送的报告，等待当前或下一整点生成。"
            commit = {"kind": "common_query", "asset_id": "COMMON"}
        else:
            record = fetch_macro_record({})
            event_news = news.fetch_events()
            now = time.time()
            if store:
                diagnostic = common.record_news_collection(store, report_id, event_news, now)
                log_line("common_news " + json.dumps(diagnostic, ensure_ascii=False, sort_keys=True))
            # If preparation spans an hour, expire this scheduled attempt instead
            # of publishing a missed snapshot under a current-looking title.
            if report_id.startswith("common:hour:") and report_id != common.report_id(now):
                return "expired"
            if store:
                store.expire_scheduled("COMMON", common.hour_start(now))
            event_news = common.select_events(store, event_news, now)
            text = build_common_report(record, now, event_news)
            commit = {"kind": "common_report", "asset_id": "COMMON", "text": text,
                      "hour": record["shared_hour"], "generated_at": now,
                      "news_events": event_news["events"]}
        text = render_report([text], budget=TG_SAFE_LEN)
        if dry_run:
            print(text)
            return
        store.enqueue_report(report_id, text, None, commit,
                             command_id=cmd["update_id"] if isinstance(cmd, dict) else None)
        pump_common_deliveries(report_id)
        return _report_delivery_result(report_id)


def safe_send_common_push(**kwargs):
    try:
        return send_common_push(**kwargs)
    except ReceiptPersistenceError as exc:
        log_line(f"common receipt pending local save report_id={exc.claim['report_id']}")
        return PUSH_RESULT_UNKNOWN
    except Exception:
        log_exception("common push failed; asset schedules continue")
        return PUSH_RESULT_FAILED


def send_push(dry_run=False, cmd=None, check_commands=True, require_usable_signal_data=False, report_id=None):
    if cmd is None and check_commands and not dry_run:
        cmd = poll_tg_command()
    command = cmd
    asset = assets.current().asset_id
    if cmd is not None:
        raw = cmd["command"] if isinstance(cmd, dict) else cmd
        action, asset = assets.command_identity(raw)
        cmd = {**cmd, "command": action} if isinstance(cmd, dict) else action
        if asset == "COMMON":
            return send_common_push(dry_run=dry_run, cmd=cmd, report_id=report_id)
    with assets.asset_scope(asset), _asset_report_locks[asset]:
        return _send_push(dry_run, cmd, False, require_usable_signal_data, report_id)


def cached_equity_observations(state, session):
    """Read raw public observations without granting price confirmation eligibility.

    Queries never advance trading state, reuse retired price history or require
    fresh network requests. Source timestamps remain those of each observation.
    """
    spec = assets.current()
    cached = (state.get("latest_observation_report") or state.get("latest_valid_report") or {}) if state.get("price_history_origin") == "binance_public_ws" else {}
    payload = copy.deepcopy(cached.get("payload") or {})
    path = market_history.history_path(STATE_FILE)
    payload.update(instrument=spec.identity(), market_session=session, news_titles=[])
    payload.setdefault("public_records", copy.deepcopy((state.get("public_snapshots") or [{}])[-1].get("public") or {}))
    payload.setdefault("market", {"price": None, "mcap": None, "chg24": None, "vol24": None})
    payload.setdefault("raw_signal_bars", [])
    payload.setdefault("derivative_bars", [])
    try:
        payload["raw_signal_bars"] = market_history.read_rows("stock_1h", spec.asset_id, path=path, limit=1000)
        quote = market_history.read_meta("stock_price:" + spec.asset_id, path=path, default={})
        if quote:
            payload["market"].update(price=quote.get("price"), previous_close=quote.get("previous_close"),
                quote_source_as_of_ms=quote.get("source_as_of_ms"), quote_source_time_missing=quote.get("source_as_of_ms") is None,
                quote_retrieved_at=quote["retrieved_at_ms"]/1000, quote_endpoint_url=quote.get("endpoint_url"))
        oi = market_history.read_rows("derivative_oi", spec.derivatives_symbol, path=path, limit=1000)
        if oi:
            payload["derivative_bars"] = [{"oi_timestamp": r["observed_ms"], "oi": r.get("quantity"),
                "oi_value": r.get("notional_value"), "oi_retrieved_at_ms": r["retrieved_at_ms"],
                "oi_endpoint_url": (r.get("provenance") or {}).get("endpoint_url")} for r in oi]
        payload["public_history"] = market_observations.collect(spec.asset_id, path)
    except Exception as exc:
        payload["public_history"] = {"history": {"status": "unavailable", "reason": type(exc).__name__, "rows": [], "values": {}}}
    payload["signal_data_quality"] = assets.stock_quality(payload["raw_signal_bars"])
    return payload, cached


def send_cached_equity_query(state, session, report_id, command, dry_run=False, full=False):
    spec = assets.current()
    payload, cached = cached_equity_observations(state, session)
    original_time = cached.get("generated_at")
    stamp = datetime.fromtimestamp(original_time, TOKYO_TZ).strftime("%Y-%m-%d %H:%M JST") if original_time else "无"
    note = f"⏸ {spec.asset_id} 当前休市、停牌或交易状态待确认。最近观察报告：{stamp}。\n"
    if cached.get("payload"):
        # Re-render the historical payload, retaining its price/time. Legacy
        # cached text may contain the discontinued recommendation feed.
        payload["news_titles"] = []
        body = build_equity_report(stamp, payload, query=True, full=full, limit_length=not full)
        full_body = build_equity_report(stamp, payload, query=True, full=True, limit_length=False)
    else:
        body = f"📊 {spec.asset_id}｜公开股票行情积累中，复权口径待确认。旧接口历史不参与新分析。"
        body += "\n已采集的原始观察及各项覆盖情况见结构化附件。"
        full_body = body
    text = note + body if full else render_report([note, body], budget=TG_SAFE_LEN)
    if dry_run:
        print(text)
        return
    document = build_delivery_document(payload, text, report_id, time.time(), note + full_body)
    get_delivery_store().enqueue_report(report_id, text, document,
        {"kind": "cached_query", "asset_id": spec.asset_id},
        command_id=command["update_id"] if isinstance(command, dict) else None)
    pump_pending_deliveries(report_id=report_id)
    return _report_delivery_result(report_id)


def _send_push(
    dry_run=False,
    cmd=None,
    check_commands=True,
    require_usable_signal_data=False,
    report_id=None,
):
    if cmd is None and check_commands and not dry_run:
        cmd = poll_tg_command()
    command = cmd
    if isinstance(cmd, dict):
        report_id = f"tg:{cmd['update_id']}"
        cmd = cmd["command"]
    report_id = report_id or (scheduled_report_id(time.time()) if cmd is None else "manual:" + uuid.uuid4().hex)
    if not dry_run:
        state_store().require_ready()
        existing = get_delivery_store().get_report(report_id)
        if existing is not None:
            _finish_queued_command(command, report_id)
            if not isinstance(command, dict):
                pump_pending_deliveries()
            return _report_delivery_result(report_id)
    if cmd == "REPAIR":
        if dry_run:
            print("/repair：重置状态与缓存；dry-run 未执行。")
            return
        _enqueue_repair(report_id, command)
        pump_pending_deliveries(report_id=report_id)
        return _report_delivery_result(report_id)

    state = load_state()
    recycle_state(state)
    if assets.current().equity:
        session = assets.market_session()
        if not session["open"]:
            if cmd is None:
                if not dry_run:
                    get_delivery_store().expire_scheduled(assets.current().asset_id)
                return "market_closed"
            return send_cached_equity_query(state, session, report_id, command, dry_run)

    forced = (cmd == "FORCE_PUSH")
    query = (cmd == "QUERY_PUSH")
    today = tokyo_day_str()
    last_day = state.get("day_tokyo")
    is_new_day = (last_day != today)
    if is_new_day and not query:
        state["day_tokyo"] = today
        state["day_push_count"] = 0
        state["day_first_push_tokyo"] = tokyo_now().strftime("%Y-%m-%d %H:%M JST")

    push_count = int(state.get("day_push_count", 0)) + (0 if query else 1)
    payload = (collect_report_payload(state, require_usable_signal_data=True)
               if require_usable_signal_data else collect_report_payload(state))
    if assets.current().equity and not assets.market_session()["open"]:
        # Collection may cross the closing boundary. Do not queue a closed-hour
        # report or commit its candidate state; manual queries use the saved copy.
        if cmd is None:
            return "market_closed"
        return send_cached_equity_query(load_state(), assets.market_session(), report_id, command, dry_run)
    signal_quality = payload.get("signal_data_quality") or {}
    if require_usable_signal_data and not signal_quality.get("usable") and not assets.current().equity:
        reasons = signal_quality.get("reasons") or ["unknown"]
        raise SignalDataNotReadyError(
            "signal data not ready: " + ",".join(str(item) for item in reasons)
        )
    merge_latest_liquidation_ws_state(state)
    merge_latest_hyperliquid_state(state)
    state["report"] = {
        "schema": REPORT_SCHEMA,
        "revision": REPORT_SCHEMA_REVISION,
        "instrument": payload.get("instrument"),
        "derivatives_market": payload.get("derivatives_market"),
        "collector_metrics": payload.get("collector_metrics"),
        "market": payload["market"],
        "market_24h_delta": payload["market_24h_delta"],
        "fear_greed": payload["fear_greed"],
        "seven_day_comparison": payload["seven_day_comparison"],
        "structure_state": payload["structure_state"],
        "delta4h": payload["delta4h"],
        "signal_data_quality": payload["signal_data_quality"],
        "levels": payload["levels"],
        "signals": payload["signals"],
        "rule_state": payload["rule_state"],
        "status_layer": payload["status_layer"],
        "market_patterns": payload["market_patterns"],
        "public_records": payload["public_records"],
        "completeness": payload["completeness"],
        "liquidation_ws": state.get("liquidation_ws"),
        "hyperliquid_derivatives": payload.get("hyperliquid_derivatives"),
        "news_seen_count": len(state.get("news_seen") or {}),
    }
    state["last_market"] = payload["market"]
    state["last_market_24h_delta"] = payload["market_24h_delta"]
    state["last_fear_greed"] = payload["fear_greed"]
    state["last_seven_day_comparison"] = payload["seven_day_comparison"]
    state["last_structure_state"] = payload["structure_state"]

    now_tokyo = tokyo_now().strftime("%Y-%m-%d %H:%M JST")
    final_msg = build_report_message(
        now_tokyo=now_tokyo,
        is_new_day=is_new_day,
        today=today,
        push_count=push_count,
        payload=payload,
        forced=forced,
        query=query,
    )

    if dry_run:
        print(final_msg)
        return

    generated_at = time.time()
    if assets.current().equity:
        state["latest_observation_report"] = {"text": final_msg, "payload": payload, "generated_at": generated_at,
            "full_text": build_full_report_message(now_tokyo, is_new_day, today, push_count, payload, forced, query, limit_length=False)}
        if signal_quality.get("usable"):
            state["latest_valid_report"] = copy.deepcopy(state["latest_observation_report"])
    event = None
    if not query:
        append_success_log(state, today, payload, forced)
        event = state["event_logs"][-1]
    # outbox 只固化本次快照增量，不为每个失败投递重复复制整段历史。
    candidate = compact_state({k: v for k, v in state.items()
                               if k not in _DELIVERY_PROTECTED_KEYS and k not in DOMAINS})
    snapshot = (state.get("snapshots") or [None])[-1]
    public_snapshot = (state.get("public_snapshots") or [None])[-1]
    full_text = build_full_report_message(now_tokyo, is_new_day, today, push_count,
                                          payload, forced, query, limit_length=False) if BTC_PUSH_EVIDENCE_ENABLED else None
    document = build_delivery_document(payload, final_msg, report_id, generated_at, full_text)
    get_delivery_store().enqueue_report(report_id, final_msg, document, {
        "kind": "report", "asset_id": assets.current().asset_id, "query": query, "generated_at": generated_at,
        "repair_marker": state.get("last_repair_command_id"),
        "repaired_ts": state.get("repaired_ts"), "candidate": candidate, "event": event,
        "snapshot": snapshot, "public_snapshot": public_snapshot,
    }, command_id=command["update_id"] if isinstance(command, dict) else None)
    pump_pending_deliveries(report_id=report_id if cmd is not None else None)
    return _report_delivery_result(report_id)


def run_print(full=False):
    state = load_state()
    recycle_state(state)
    if assets.current().equity and not assets.market_session()["open"]:
        return send_cached_equity_query(state, assets.market_session(), "print", None, True, full=full)
    payload = collect_report_payload(state)
    builder = build_full_report_message if full else build_report_message
    print(builder(
        now_tokyo=tokyo_now().strftime("%Y-%m-%d %H:%M JST"),
        is_new_day=False,
        today=tokyo_day_str(),
        push_count=int(state.get("day_push_count", 0)) + 1,
        payload=payload,
    ))


def safe_send_push(cmd=None, check_commands=True, require_usable_signal_data=False, report_id=None):
    try:
        if cmd is None and check_commands:
            cmd = poll_tg_command()
            check_commands = False
        return send_push(
            cmd=cmd,
            check_commands=check_commands,
            require_usable_signal_data=require_usable_signal_data,
            report_id=report_id,
        )
    except SignalDataNotReadyError as exc:
        log_line(str(exc))
        return PUSH_RESULT_RETRYABLE
    except ReceiptPersistenceError as exc:
        log_line(f"delivery receipt pending local save report_id={exc.claim['report_id']} part={exc.claim['part']}")
        return PUSH_RESULT_UNKNOWN
    except Exception as exc:
        if isinstance(cmd, dict):
            store = get_delivery_store()
            try:
                queued_id = f"tg:{cmd['update_id']}"
                if store.get_report(queued_id) is not None:
                    try:
                        _finish_queued_command(cmd, queued_id)
                    except (OSError, sqlite3.OperationalError):
                        # claim_command 会依据持久报告补齐，不重做报告准备或网络投递。
                        log_line(f"command completion pending update_id={cmd['update_id']}")
                    return _report_delivery_result(queued_id)
                else:
                    if isinstance(exc, (OSError, sqlite3.OperationalError)):
                        status = store.defer_command(cmd["update_id"], {"stage": "prepare_local_retry"})
                        if status == "pending":
                            log_line(f"command preparation delayed update_id={cmd['update_id']}")
                            return PUSH_RESULT_QUEUED
                    else:
                        store.finish_command(cmd["update_id"], {"stage": "prepare_failed"}, status="failed")
            except (OSError, sqlite3.OperationalError):
                # Keep running plus an in-memory retry; restart also restores this command.
                store.pending_command_requeues[cmd["update_id"]] = {"stage": "prepare_local_retry"}
                log_line(f"command local recovery pending update_id={cmd['update_id']}")
        log_exception("send_push failed; scheduler will continue")
        return PUSH_RESULT_FAILED


def run_forever(immediate_first=False):
    state_store().require_ready()
    initialize_delivery()
    market_public.start_history_worker(market_history.history_path(STATE_FILE), log_line)
    market_streams.start_streams(market_history.history_path(STATE_FILE), log_line)
    log_line(
        f"starting {APP_ID}; schema={REPORT_SCHEMA}; revision={REPORT_SCHEMA_REVISION}; "
        f"ollama_enabled={BTC_PUSH_OLLAMA_ENABLED}; "
        f"news_translate_ollama={BTC_PUSH_NEWS_TRANSLATE_WITH_OLLAMA}; "
        f"fear_greed_enabled={BTC_PUSH_FEAR_GREED_ENABLED}; "
        f"public_snapshot_enabled=1; liquidation_ws_enabled={BTC_PUSH_LIQUIDATION_WS_ENABLED}; "
        f"hyperliquid_enabled={BTC_PUSH_HYPERLIQUID_ENABLED}; "
        f"hyperliquid_mode={HYPERLIQUID_MODE}; "
        f"immediate_first={immediate_first}"
    )
    start_liquidation_ws_collector()
    start_hyperliquid_ws_collector()
    if immediate_first:
        safe_send_push(check_commands=False)
    next_push_ts = next_scheduled_push_ts()
    retry_push_ts = None
    retry_attempt = 0
    active_report_id = None
    while True:
        try:
            pump_pending_deliveries()
        except Exception as exc:
            log_line(f"delivery pump paused error={type(exc).__name__}")
        now_ts = time.time()
        if now_ts >= next_push_ts:
            active_report_id = scheduled_report_id(next_push_ts)
            result = safe_send_push(
                check_commands=False,
                require_usable_signal_data=True,
                report_id=active_report_id,
            )
            next_push_ts = next_scheduled_push_ts()
            retry_push_ts = None
            retry_attempt = 0
            if result == PUSH_RESULT_RETRYABLE:
                if BTC_PUSH_SCHEDULE_MAX_RETRIES > 0:
                    retry_attempt = 1
                    retry_push_ts = time.time() + BTC_PUSH_SCHEDULE_RETRY_DELAY_SEC
                    log_line(
                        "scheduled push waiting for signal data; "
                        f"retry={retry_attempt}/{BTC_PUSH_SCHEDULE_MAX_RETRIES}; "
                        f"delay_sec={BTC_PUSH_SCHEDULE_RETRY_DELAY_SEC}"
                    )
                else:
                    safe_send_push(
                        check_commands=False,
                        require_usable_signal_data=False,
                        report_id=active_report_id,
                    )
            continue

        if retry_push_ts is not None and now_ts >= retry_push_ts:
            result = safe_send_push(
                check_commands=False,
                require_usable_signal_data=True,
                report_id=active_report_id,
            )
            if result == PUSH_RESULT_SENT:
                log_line(f"scheduled push retry succeeded attempt={retry_attempt}")
                retry_push_ts = None
                retry_attempt = 0
            elif (
                result == PUSH_RESULT_RETRYABLE
                and retry_attempt < BTC_PUSH_SCHEDULE_MAX_RETRIES
            ):
                retry_attempt += 1
                retry_push_ts = time.time() + BTC_PUSH_SCHEDULE_RETRY_DELAY_SEC
                log_line(
                    "scheduled push signal data still unavailable; "
                    f"retry={retry_attempt}/{BTC_PUSH_SCHEDULE_MAX_RETRIES}; "
                    f"delay_sec={BTC_PUSH_SCHEDULE_RETRY_DELAY_SEC}"
                )
            else:
                retry_push_ts = None
                if result == PUSH_RESULT_RETRYABLE:
                    log_line(
                        "scheduled push retries exhausted; "
                        "sending data-insufficient fallback"
                    )
                    fallback_result = safe_send_push(
                        check_commands=False,
                        require_usable_signal_data=False,
                        report_id=active_report_id,
                    )
                    log_line(f"scheduled push fallback result={fallback_result}")
                retry_attempt = 0
            continue

        try:
            cmd = poll_tg_command()
            if cmd:
                safe_send_push(cmd=cmd, check_commands=False)
        except Exception as exc:
            log_line(f"command poll paused error={type(exc).__name__}")

        wake_ts = min(
            next_push_ts,
            retry_push_ts if retry_push_ts is not None else next_push_ts,
        )
        try:
            delivery_due = delivery_next_due_at()
            if delivery_due is not None:
                wake_ts = min(wake_ts, delivery_due)
        except Exception as exc:
            log_line(f"delivery schedule paused error={type(exc).__name__}")
        sleep_s = max(1, min(wake_ts - time.time(), BTC_PUSH_TG_POLL_INTERVAL_SEC))
        time.sleep(sleep_s)


def run_collect_public():
    state = load_state()
    recycle_state(state)
    snapshot = collect_public_snapshot(state)
    append_public_snapshot(state, snapshot)
    merge_latest_liquidation_ws_state(state)
    merge_latest_hyperliquid_state(state)
    save_state(state)
    print(json.dumps(snapshot, ensure_ascii=False, indent=2))


def run_markets(immediate_first=False):
    """Instruments share hourly rules and an outbox, with isolated report state."""
    initialize_delivery()
    store = get_delivery_store()
    start_liquidation_ws_collector()
    start_hyperliquid_ws_collector()
    market_public.start_history_worker(market_history.history_path(STATE_FILE), log_line)
    market_streams.start_streams(market_history.history_path(STATE_FILE), log_line)
    schedules = {asset: AssetSchedule(asset, store.load_schedule(asset), time.time(),
                 immediate=immediate_first, retries=BTC_PUSH_SCHEDULE_MAX_RETRIES,
                 retry_delay=BTC_PUSH_SCHEDULE_RETRY_DELAY_SEC) for asset in assets.INSTRUMENTS}
    common_schedule = common.CommonSchedule(store.load_schedule("COMMON"), time.time(),
        immediate=immediate_first, retries=BTC_PUSH_SCHEDULE_MAX_RETRIES,
        retry_delay=BTC_PUSH_SCHEDULE_RETRY_DELAY_SEC)
    def common_tick():
        with _common_report_lock:
            try:
                common_schedule.tick(time.time(), send=safe_send_common_push, pump=pump_common_deliveries,
                    expire=lambda before: store.expire_scheduled("COMMON", before),
                    save=lambda value: store.save_schedule("COMMON", value))
            except Exception as exc:
                log_line(f"COMMON schedule paused error={type(exc).__name__}")
    def tick(asset):
        with assets.asset_scope(asset), _asset_report_locks[asset]:
            try:
                schedules[asset].tick(time.time(), session=assets.market_session(), send=safe_send_push,
                    pump=pump_pending_deliveries, expire=lambda: store.expire_scheduled(asset),
                    save=lambda value: store.save_schedule(asset, value), report_identity=scheduled_report_id)
            except Exception as exc:
                log_line(f"{asset} schedule paused error={type(exc).__name__}")
    with ThreadPoolExecutor(max_workers=4, thread_name_prefix="market-report") as workers, \
            ThreadPoolExecutor(max_workers=4, thread_name_prefix="market-command") as commands:
        active = {}
        command_jobs = set()
        while True:
            if "COMMON" not in active or active["COMMON"].done():
                active["COMMON"] = workers.submit(common_tick)
            for asset in assets.INSTRUMENTS:
                if asset not in active or active[asset].done():
                    active[asset] = workers.submit(tick, asset)
            command_jobs = {job for job in command_jobs if not job.done()}
            try:
                command = poll_tg_command()
                if command:
                    command_jobs.add(commands.submit(safe_send_push, cmd=command, check_commands=False))
            except Exception as exc:
                log_line(f"command poll paused error={type(exc).__name__}")
            time.sleep(max(1, BTC_PUSH_TG_POLL_INTERVAL_SEC))


def assert_migration_daemon_stopped():
    """旧 daemon 不认识新版 writer 锁，正式默认状态迁移需另查存活进程。"""
    default_state = os.path.abspath(os.path.expanduser("~/.btc_push_state_tokyo.json"))
    if os.path.abspath(STATE_FILE) != default_state:
        return
    result = subprocess.run(["ps", "-axo", "pid=,command="], check=True,
                            capture_output=True, text=True)
    active = []
    pattern = re.compile(r"(?:^|[\s/])(?:btc_push(?:_local_ollama[^\s/]*)?\.py|run_btc_push\.sh)(?:\s|$)")
    for line in result.stdout.splitlines():
        parts = line.strip().split(None, 1)
        if len(parts) == 2 and parts[0].isdigit() and int(parts[0]) != os.getpid() and pattern.search(parts[1]):
            active.append(int(parts[0]))
    if active:
        raise RuntimeError(f"state migration requires stopped BTC collectors; active_pids={active}")


def main(argv):
    selected = "BTC"
    if "--asset" in argv:
        index = argv.index("--asset")
        if index + 1 >= len(argv) or argv[index + 1].upper() not in assets.INSTRUMENTS:
            raise ValueError("--asset requires BTC, QQQ or VOO")
        selected = argv[index + 1].upper()
        argv = argv[:index] + argv[index + 2:]
    with assets.asset_scope(selected):
        return run_main(argv)


def run_main(argv):
    if "--history-status" in argv:
        path = market_history.history_path(STATE_FILE)
        if not os.path.exists(path):
            print(json.dumps({"status": "not_started", "path": path}))
            return
        from contextlib import closing
        with closing(market_history.HistoryStore(path, readonly=True)) as history:
            print(json.dumps(history.status(), ensure_ascii=False, indent=2))
        return
    if "--collect-history" in argv:
        market_public.run_once(market_history.history_path(STATE_FILE), log=log_line)
        return
    if "--print-full" in argv:
        run_print(full=True)
        return
    if "--print" in argv:
        run_print()
        return
    if "--once" in argv and env_bool("BTC_PUSH_DRY_RUN", False):
        send_push(dry_run=True)
        return
    with state_store().writer_lock():
        if "--migrate-state" in argv:
            assert_migration_daemon_stopped()
            print(json.dumps(state_store().migrate(normalize=migrate_state_schema), ensure_ascii=False))
            return
        state_store().require_ready()
        if "--collect-public" in argv:
            run_collect_public()
        elif "--once" in argv:
            initialize_delivery()
            send_push()
        else:
            with ExitStack() as writers:
                for asset in assets.INSTRUMENTS:
                    if asset == assets.current().asset_id:
                        continue
                    with assets.asset_scope(asset):
                        writers.enter_context(state_store().writer_lock())
                        state_store().require_ready()
                with assets.asset_scope("BTC"):
                    run_markets(immediate_first=env_bool("BTC_PUSH_IMMEDIATE_FIRST", False))


if __name__ == "__main__":
    main(sys.argv[1:])
