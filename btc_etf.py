"""Anonymous US spot BTC ETF holdings; USD net flows remain unavailable."""
import copy
import json
import math
import re
from datetime import datetime, timedelta
from decimal import Decimal
from html.parser import HTMLParser

ENDPOINT = "https://bitbo.io/treasuries/us-etfs/"
VENUE = "Bitbo"
FUNDS = tuple(sorted("IBIT FBTC GBTC BTC BITB ARKB HODL MSBT BRRR EZBC BTCO BTCW".split()))
EXCLUDED = {"BITA", "DEFI"}
FLOW_FIELDS = tuple("flow_1d_usd flow_5d_usd flow_30d_usd flow_1d_assets_pct flow_5d_assets_pct flow_30d_assets_pct".split())


def is_holdings_source(record):
    return record.get("venue") == VENUE and record.get("endpoint_url") == ENDPOINT


def validate_holdings(scope, rows, total):
    """Require the complete agreed fund scope, including genuine zero holdings."""
    if scope != list(FUNDS) or not isinstance(rows, list) or len(rows) != len(FUNDS):
        raise ValueError("holdings_scope_gap")
    seen, amounts = set(), []
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("invalid_fund_row")
        symbol, amount = row.get("symbol"), row.get("holdings_btc")
        if not isinstance(symbol, str) or symbol not in FUNDS or symbol in seen:
            raise ValueError("holdings_scope_gap")
        if type(amount) not in (int, float) or not math.isfinite(amount) or amount < 0:
            raise ValueError("invalid_fund_holdings")
        seen.add(symbol)
        amounts.append(amount)
    try:
        expected = math.fsum(amounts)
    except OverflowError:
        raise ValueError("invalid_holdings_total") from None
    if (type(total) not in (int, float) or not math.isfinite(total) or total < 0
            or not math.isclose(total, expected, rel_tol=1e-12, abs_tol=1e-8)):
        raise ValueError("invalid_holdings_total")


class _Page(HTMLParser):
    def __init__(self):
        super().__init__()
        self.scripts, self.dates = [], []
        self.script, self.date = None, None

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "script":
            self.script = []
        elif tag == "td" and attrs.get("data-title") == "Last Updated":
            self.date = []

    def handle_data(self, text):
        if self.script is not None:
            self.script.append(text)
        if self.date is not None:
            self.date.append(text)

    def handle_endtag(self, tag):
        if tag == "script" and self.script is not None:
            self.scripts.append("".join(self.script))
            self.script = None
        elif tag == "td" and self.date is not None:
            self.dates.append(" ".join("".join(self.date).split()))
            self.date = None


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate_holdings_field")
        result[key] = value
    return result


def parse_page(text):
    """Read the fixed data literal, never execute scripts or use Value Today."""
    if not isinstance(text, str) or len(text) > 2 * 1024 * 1024:
        raise ValueError("invalid_holdings_page")
    page = _Page()
    page.feed(text)
    literals = [m.group(1) for script in page.scripts
                for m in re.finditer(r"\bconst\s+data\s*=\s*(\[.*?\])\s*;", script, re.S)]
    if len(literals) != 1:
        raise ValueError("holdings_page_changed")
    # Only the provider's three known unquoted property names are accepted.
    literal = re.sub(r'([{,]\s*)(name|symbol|btc)\s*:', r'\1"\2":', literals[0])
    try:
        raw = json.loads(literal, parse_float=Decimal, parse_int=Decimal,
                         object_pairs_hook=_unique_object)
    except (ValueError, TypeError):
        raise ValueError("invalid_holdings_literal") from None
    if not isinstance(raw, list):
        raise ValueError("invalid_holdings_literal")
    holdings, seen = {}, set()
    for item in raw:
        if not isinstance(item, dict):
            raise ValueError("invalid_fund_row")
        symbol, amount = item.get("symbol"), item.get("btc")
        if not isinstance(symbol, str) or symbol not in set(FUNDS) | EXCLUDED:
            raise ValueError("unknown_fund")
        if symbol in seen:
            raise ValueError("duplicate_fund")
        seen.add(symbol)
        if not isinstance(amount, Decimal) or not amount.is_finite() or amount < 0:
            raise ValueError("invalid_fund_holdings")
        if not math.isfinite(float(amount)):
            raise ValueError("invalid_fund_holdings")
        if symbol in FUNDS:
            holdings[symbol] = amount
    if set(holdings) != set(FUNDS):
        raise ValueError("missing_fund")
    if not page.dates:
        raise ValueError("missing_holdings_date")
    try:
        dates = {datetime.strptime(day, "%B %d, %Y").date().isoformat() for day in page.dates}
    except ValueError:
        raise ValueError("invalid_holdings_date") from None
    if len(dates) != 1:
        raise ValueError("conflicting_holdings_dates")
    result = {"holdings_btc": float(sum(holdings.values(), Decimal(0))),
              "source_date": dates.pop(), "holdings_scope": list(FUNDS),
              "holdings_rows": [{"symbol": symbol, "holdings_btc": float(holdings[symbol])}
                                for symbol in FUNDS]}
    validate_holdings(result["holdings_scope"], result["holdings_rows"], result["holdings_btc"])
    return result


def seven_day_basis(state, source_date, now, tolerance):
    target_day = (datetime.strptime(source_date, "%Y-%m-%d").date() - timedelta(days=7)).isoformat()
    candidates = []
    for snap in state.get("public_snapshots") or []:
        record = (snap.get("public") or {}).get("etf") or {}
        value = record.get("value") or {}
        if (not is_holdings_source(record)
                or record.get("holdings_scope") != list(FUNDS)
                or record.get("status") not in ("ok", "partial")
                or any(record.get(key) for key in ("stale", "source_stale", "snapshot_used"))
                or (value.get("source_date") or {}).get("holdings") != target_day):
            continue
        amount, stamp = value.get("holdings_btc"), snap.get("ts")
        if (type(amount) not in (int, float) or not math.isfinite(amount) or amount < 0
                or type(stamp) not in (int, float) or not math.isfinite(stamp)):
            continue
        try:
            validate_holdings(record.get("holdings_scope"), record.get("holdings_rows"), amount)
        except ValueError:
            continue
        drift = abs(stamp - (now - 7 * 86400))
        if drift <= tolerance:
            candidates.append((drift, -stamp, record))
    return min(candidates, key=lambda row: row[:2])[2] if candidates else None


def flow_background(state):
    """Keep historical display separate from all current measurement values."""
    for snap in reversed(state.get("public_snapshots") or []):
        record = (snap.get("public") or {}).get("etf") or {}
        previous = record.get("flow_background")
        if isinstance(previous, dict):
            return copy.deepcopy(previous)
        value = record.get("value") or {}
        if not any(type(value.get(key)) in (int, float) for key in FLOW_FIELDS[:3]):
            continue
        return {"source": record.get("source"), "venue": record.get("venue"),
                "endpoint_url": record.get("endpoint_url"),
                "value": {key: value.get(key) for key in FLOW_FIELDS[:3]},
                "source_date": copy.deepcopy(value.get("source_date") or record.get("source_date") or {}),
                "analysis_eligible": False}
    return None
