"""BTC 机器人稳定运行时能力。

本模块只负责环境配置、时间、Telegram、基础行情与新闻标题采集。
正式主链不得再从带版本号的历史脚本导入运行能力。
"""

import json
import os
import re
import time
from datetime import datetime, timedelta, timezone

import feedparser
import requests
import math
import btc_collection_io as public_io

from btc_delivery import DeliveryFailure


BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHAT_ID_RAW = os.getenv("CHAT_ID", "").strip()
OLLAMA_API = os.getenv("OLLAMA_API", "http://127.0.0.1:11434/api/generate").strip()
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:14b").strip()

TOKYO_TZ = timezone(timedelta(hours=9))
STATE_FILE = os.path.expanduser(os.getenv("STATE_FILE", "~/.btc_push_state_tokyo.json"))
MAX_SNAPSHOTS = int(os.getenv("MAX_SNAPSHOTS", "240"))
MAX_EVENT_LOG_DAYS = int(os.getenv("MAX_EVENT_LOG_DAYS", "7"))
TG_SAFE_LEN = int(os.getenv("TG_SAFE_LEN", "3800"))

COINGECKO_BTC_URL = (
    "https://api.coingecko.com/api/v3/coins/bitcoin"
    "?localization=false&tickers=false&market_data=true&community_data=false"
    "&developer_data=false&sparkline=false"
)
BINANCE_OI_URL = "https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT"
BINANCE_MARK_PRICE_URL = "https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT"
RSS_FEEDS = [
    "https://feeds.coindesk.com/Coindesk",
    "https://cointelegraph.com/rss",
    "https://www.theblock.co/rss.xml",
]


def tokyo_now():
    return datetime.now(TOKYO_TZ)


def tokyo_day_str():
    return tokyo_now().strftime("%Y-%m-%d")


def seconds_until_next_hour_tokyo():
    now = tokyo_now()
    next_hour = now.replace(minute=0, second=0, microsecond=0)
    if next_hour <= now:
        next_hour += timedelta(hours=1)
    return int((next_hour - now).total_seconds())


def do_repair(state):
    keep = {
        "tg_update_offset": state.get("tg_update_offset"),
        "day_tokyo": state.get("day_tokyo"),
        "day_push_count": state.get("day_push_count"),
        "day_first_push_tokyo": state.get("day_first_push_tokyo"),
        "event_logs": state.get("event_logs"),
        "applied_report_ids": state.get("applied_report_ids"),
        "repair_markers": state.get("repair_markers"),
        "last_repair_command_id": state.get("last_repair_command_id"),
    }
    state.clear()
    for key, value in keep.items():
        if value is not None:
            state[key] = value
    state["repaired_ts"] = int(time.time())


def _tg_chat_id():
    if not BOT_TOKEN or not CHAT_ID_RAW:
        raise DeliveryFailure("permanent")
    try:
        return int(CHAT_ID_RAW)
    except (TypeError, ValueError) as exc:
        raise DeliveryFailure("permanent") from exc


def _tg_post(method, *, timeout=(5, 25), **kwargs):
    try:
        response = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/{method}", timeout=timeout, **kwargs
        )
    except requests.ConnectTimeout as exc:
        # Requests 明确的连接超时发生于请求发送前；普通断连/读取超时不作此推断。
        raise DeliveryFailure("connect_timeout") from exc
    except requests.RequestException as exc:
        raise DeliveryFailure("unknown") from exc
    try:
        body = response.json()
    except (ValueError, TypeError) as exc:
        raise DeliveryFailure("unknown") from exc
    if not isinstance(body, dict):
        raise DeliveryFailure("unknown")
    if body.get("ok") is False:
        if body.get("error_code", response.status_code) == 429:
            parameters = body.get("parameters") or {}
            retry_after = parameters.get("retry_after") if isinstance(parameters, dict) else None
            raise DeliveryFailure("rate_limited", retry_after)
        raise DeliveryFailure("permanent")
    receipt = body.get("result")
    if (response.status_code != 200 or body.get("ok") is not True
            or not isinstance(receipt, dict) or type(receipt.get("message_id")) is not int
            or receipt["message_id"] <= 0):
        raise DeliveryFailure("unknown")
    return receipt


def tg_send(text):
    return _tg_post(
        "sendMessage",
        json={
            "chat_id": _tg_chat_id(),
            "text": text,
            "disable_web_page_preview": True,
        },
    )


def tg_send_document(document, reply_to_message_id, filename="btc-evidence.json"):
    if not isinstance(document, bytes) or type(reply_to_message_id) is not int or reply_to_message_id <= 0:
        raise DeliveryFailure("permanent")
    return _tg_post(
        "sendDocument",
        # Evidence uploads are much larger than the short text message. Keep
        # their bounded waiting budget independent; unknown outcomes stay unknown.
        timeout=(30, 60),
        data={"chat_id": _tg_chat_id(),
              "reply_parameters": json.dumps({"message_id": reply_to_message_id})},
        files={"document": (filename, document, "application/json")},
    )


def _finite_number(value):
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (ValueError, TypeError, OverflowError):
        return None


def get_btc_market():
    response = public_io.get(COINGECKO_BTC_URL, timeout=(3, 20))
    response.raise_for_status()
    market_data = (response.json() or {}).get("market_data") or {}
    price = _finite_number((market_data.get("current_price") or {}).get("usd"))
    market_cap = _finite_number((market_data.get("market_cap") or {}).get("usd"))
    change_24h = _finite_number(market_data.get("price_change_percentage_24h"))
    volume_24h = _finite_number((market_data.get("total_volume") or {}).get("usd"))
    return price, market_cap, change_24h, volume_24h


def get_btc_oi():
    oi_response = public_io.get(BINANCE_OI_URL, timeout=20)
    oi_response.raise_for_status()
    oi_contracts = float((oi_response.json() or {}).get("openInterest") or 0.0)

    price_response = public_io.get(BINANCE_MARK_PRICE_URL, timeout=20)
    price_response.raise_for_status()
    mark_price = float((price_response.json() or {}).get("markPrice") or 0.0)
    oi_usd = oi_contracts * mark_price if mark_price > 0 else 0.0
    return oi_contracts, mark_price, oi_usd


def get_news_titles(n=30):
    titles = []
    for feed_url in RSS_FEEDS:
        try:
            response = public_io.get(feed_url, timeout=(3, 8))
            response.raise_for_status()
            feed = feedparser.parse(response.content)
            for entry in getattr(feed, "entries", [])[:max(n, 50)]:
                title = (getattr(entry, "title", "") or "").strip()
                if not title:
                    continue
                title = re.sub(r"\s+[-|–]\s+[^-|–]{2,30}$", "", title).strip()
                titles.append(title)
            if len(titles) >= n:
                break
        except Exception:
            continue

    seen = set()
    result = []
    for title in titles:
        key = title.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(title)
        if len(result) >= n:
            break
    return result


__all__ = [
    "BOT_TOKEN",
    "CHAT_ID_RAW",
    "MAX_EVENT_LOG_DAYS",
    "MAX_SNAPSHOTS",
    "OLLAMA_API",
    "OLLAMA_MODEL",
    "STATE_FILE",
    "TG_SAFE_LEN",
    "TOKYO_TZ",
    "do_repair",
    "get_btc_market",
    "get_btc_oi",
    "get_news_titles",
    "seconds_until_next_hour_tokyo",
    "tg_send",
    "tokyo_day_str",
    "tokyo_now",
]
