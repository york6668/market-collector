"""Public Chinese event headlines; no recommendations or generated commentary."""
import hashlib
import html
import math
import re
import time
from urllib.parse import urlsplit

import btc_collection_io as io

EVENT_URL = "https://api-one-wscn.awtmt.com/apiv1/content/lives"
EVENT_PARAMS = {"channel": "us-stock-channel", "client": "pc", "limit": 100}
EVENT_SOURCE = "华尔街见闻"
NEWS_TTL = 48 * 3600
EVENT_MAX_AGE = 24 * 3600
EVENT_LIMIT = 3

# Generic ETF, dollar, rate, exchange and contract words alone are not crypto topics.
CRYPTO_TERMS = (
    "比特币", "以太坊", "加密", "虚拟货币", "数字资产", "区块链", "稳定币", "链上",
    "代币", "巨鲸", "矿企", "矿工", "币安", "欧易", "BTC", "Bitcoin", "ETH",
    "Ethereum", "SOL", "Solana", "XRP", "Web3", "DeFi", "Binance", "Coinbase",
    "OKX", "Bitget", "Tether", "USDT", "USDC", "BitMine", "MicroStrategy",
    "Aptos", "Polkadot",
)
PROMOTION = re.compile(
    r"(?:^|[【\[\s（(])(?:广告|推广|赞助内容|商业合作)(?:[】\]\s：:）)]|$)|"
    r"限时优惠|订阅解锁|付费解锁|扫码|二维码|加群|入群|"
    r"微信小程序|点击.{0,12}(?:领取|购买|订阅|解锁)|立即.{0,8}(?:购买|订阅|领取)|"
    r"开户链接|返佣|荐股|推荐买入|值得买|我会买|应该买|最值得|必买|免费领取|"
    r"只[能买投选].{0,15}(?:一|1|只).{0,15}(?:ETF|基金|股票)|"
    r"百万美元.{0,12}(?:组合|退休)|致富|财富自由|稳赚|躺赚|翻倍机会|"
    r"投资者.{0,15}(?:需要|应该).{0,12}(?:做|买|准备)|"
    r"更多上涨空间|投资组合.{0,20}(?:添加|加入)|"
    r"sponsored|advertorial|motley\s*fool|stocks?\s+to\s+buy",
    re.I,
)
# Keep concrete disclosures/actions. Forecasts, calendars, wrap-ups and standalone
# price ticks are not new event reports, even when the publisher tags them as US news.
NON_EVENT = re.compile(r"要闻汇总|日内.{0,6}(?:关注|提醒)|早餐|晚报|收盘综述|复盘|"
                       r"投资策略|投资机会|目标价|评级|预测|有望|可能.{0,8}(?:上涨|下跌|纳入)|"
                       r"市场信号|是否.{0,12}(?:值得|买入)|抵御.{0,12}风险")
EVENT_ACTION = re.compile(
    r"宣布|发布|公布|披露|批准|通过|否决|签署|决定|维持|上调|下调|调降|降息|加息|"
    r"降准|加征|豁免|禁令|制裁|关税|法案|立法|监管|起诉|诉讼|调查|罚款|处罚|"
    r"财报|业绩|营收|净利|利润|盈利|亏损|EPS|GDP|CPI|PCE|PMI|ADP|非农|"
    r"失业|就业|通胀|零售销售|耐用品|消费者信心|消费者物价|生产者物价|"
    r"收购|并购|裁员|破产|违约|停产|复产|量产|投产|扩产|外包|召回|"
    r"出口|进口|产量|销量|出货|订单|达成|签订|协议|停火|袭击|爆炸|地震|"
    r"遇袭|劫机|中断|罢工|关闭|重启|恢复|取消|推迟|辞职|任命|"
    r"结束.{0,15}(?:行动|战争)|谈判|选举|投票", re.I)


def clean_text(value):
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", "", value or ""))).strip()


def is_promotion(text):
    return bool(PROMOTION.search(text or ""))


def is_crypto_news(title):
    for term in CRYPTO_TERMS:
        pattern = re.escape(term)
        if term.isascii():
            pattern = r"(?<![a-z0-9])" + pattern + r"(?![a-z0-9])"
        if re.search(pattern, title or "", re.I):
            return True
    return False


def event_keys(event):
    title = re.sub(r"[^\w\s]+", " ", event["title"].lower())
    title = re.sub(r"\s+", " ", title).strip()
    return ("wscn:" + event["id"], "title:" + hashlib.sha256(title.encode()).hexdigest())


def parse_events(rows, now):
    if not isinstance(rows, list):
        raise ValueError("event_news_items_invalid")
    events, keys = [], set()
    for row in rows:
        if not isinstance(row, dict) or row.get("type") != "live" or row.get("is_priced") is not False:
            continue
        raw_time = row.get("display_time")
        if isinstance(raw_time, bool) or not isinstance(raw_time, (int, float)):
            continue
        if not math.isfinite(raw_time) or not 0 <= now - raw_time <= EVENT_MAX_AGE:
            continue
        if "us-stock-channel" not in (row.get("channels") or []):
            continue
        raw_body = row.get("content_text") or ""
        if not isinstance(raw_body, str) or not isinstance(row.get("title") or "", str):
            continue
        paragraphs = [p for p in re.split(r"[\r\n]+", raw_body) if p.strip()]
        title = clean_text(row.get("title") or (paragraphs[0] if paragraphs else ""))
        body = clean_text(raw_body)
        # Inspect the full source text, including a promotional footer omitted by a title.
        if (not re.search(r"[\u4e00-\u9fff]", title) or is_promotion(title + " " + body)
                or NON_EVENT.search(title) or not EVENT_ACTION.search(title)
                or is_crypto_news(title)):
            continue
        identity = str(row.get("id") or "")
        uri = row.get("uri")
        if not identity.isdigit() or not isinstance(uri, str):
            continue
        try:
            url = urlsplit(uri)
        except ValueError:
            continue
        if (url.scheme != "https" or url.netloc != "wallstreetcn.com"
                or url.path != "/livenews/" + identity):
            continue
        event = {"id": identity, "title": title, "source": EVENT_SOURCE,
                 "display_time": raw_time, "url": "https://wallstreetcn.com/livenews/" + identity}
        dedupe = event_keys(event)
        if keys.intersection(dedupe):
            continue
        keys.update(dedupe)
        events.append(event)
    return sorted(events, key=lambda event: (event["display_time"], event["id"]), reverse=True)


def fetch_events(now=None):
    now = time.time() if now is None else now
    def fetch():
        response = io.get(EVENT_URL, params=EVENT_PARAMS, timeout=(3, 8))
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict) or data.get("code") != 20000 or not isinstance(data.get("data"), dict):
            raise ValueError("event_news_envelope_invalid")
        rows = data["data"].get("items")
        if not isinstance(rows, list):
            raise ValueError("event_news_items_invalid")
        return {"rows": rows, "retrieved_at": time.time()}
    try:
        receipt = io.cached_call(("public_event_news", EVENT_URL, tuple(sorted(EVENT_PARAMS.items())), "zh-CN"), 300, fetch)
        return {"status": "ok", "events": parse_events(receipt["rows"], now),
                "source": EVENT_SOURCE, "retrieved_at": receipt["retrieved_at"]}
    except Exception as exc:
        return {"status": "unavailable", "events": [], "source": EVENT_SOURCE, "reason": type(exc).__name__}
