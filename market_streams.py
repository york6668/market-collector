"""Public stock and Bybit streams, reusing the bot's RFC6455 transport."""
import hashlib
import json
import socket
import threading
import time
import uuid
from contextlib import closing

from btc_websocket import connect_websocket
from market_history import HistoryStore, HOUR, canonical
from market_public import STOCK_WS, BYBIT_WS, number, provenance

_threads = {}
_lock = threading.Lock()
STOCK_TOPICS = ["price", "calendar"] + [f"{symbol}@{kind}" for symbol in ("QQQ", "VOO")
    for kind in ("quote", "kline_5m", "kline_1h", "tradingStatus", "tradability")]


def stock_message(store, message, received_ms):
    event = message.get("e")
    if event == "price":
        for item in message.get("rates", []):
            symbol = item.get("s")
            if symbol not in ("QQQ", "VOO"):
                continue
            if item.get("ac") != "EQ_" + symbol:
                raise ValueError("stock_symbol_mismatch")
            stamp = item.get("t")
            if stamp is not None and (not isinstance(stamp, int) or not 0 < stamp <= received_ms):
                raise ValueError("stock_price_time_invalid")
            data = {"price": number(item["p"], 0), "source_as_of_ms": stamp,
                    "source_time_missing": stamp is None, "phase": item.get("mp"),
                    "previous_close": number(item["pc"], 0) if item.get("pc") is not None else None,
                    "retrieved_at_ms": received_ms, "endpoint_url": STOCK_WS}
            old = store.get_meta("stock_price:" + symbol, {})
            if stamp is not None and (old.get("source_as_of_ms") or 0) > stamp:
                continue
            store.set_meta("stock_price:" + symbol, data)
            # Quotes update the live cache continuously; durable price snapshots
            # are sampled once per minute, not saved as repeated tick revisions.
            if store.get_meta("stock_price_minute:" + symbol) != received_ms // 60000:
                store.put("stock_price", symbol, [{**data, "observed_ms": stamp if stamp is not None else received_ms,
                    "item_key": str(received_ms // 60000)}], provenance(STOCK_WS, "USD/share", source_time_missing=stamp is None), received_ms=received_ms)
                store.set_meta("stock_price_minute:" + symbol, received_ms // 60000)
    elif event == "kline":
        symbol, bar = message.get("s"), message.get("k") or {}
        if symbol not in ("QQQ", "VOO") or bar.get("x") is not True:
            return
        if bar.get("s") != symbol or bar.get("i") not in ("5m", "1h"):
            raise ValueError("stock_kline_identity_invalid")
        interval = HOUR if bar["i"] == "1h" else 300000
        opening, end = int(bar["t"]), int(bar["ct"])
        o, h, l, c = (number(bar[k], 0) for k in ("o", "h", "l", "c"))
        if opening % interval or end != opening + interval - 1 or end >= received_ms or not 0 < l <= min(o, c) <= max(o, c) <= h:
            raise ValueError("stock_kline_invalid")
        row = {"observed_ms": end, "open_time": opening, "close_time": end,
               "open": o, "high": h, "low": l, "close": c, "volume": number(bar["v"], 0),
               "symbol": "EQ_" + symbol, "adjustment_mode": "UNKNOWN", "volume_unit": "shares",
               "price_currency": "USD", "calendar": "US_EQUITY", "endpoint_url": STOCK_WS,
               "signal_source": "Binance Stocks public WS " + symbol,
               "vw": number(bar["vw"], 0) if bar.get("vw") is not None else None}
        store.put("stock_" + bar["i"], symbol, [row], provenance(STOCK_WS, "USD/share", interval_ms=interval,
                  adjustment_mode="UNKNOWN"), received_ms=received_ms)
    elif event == "quote":
        symbol = message.get("s")
        if symbol in ("QQQ", "VOO"):
            store.set_meta("stock_quote:" + symbol, {"bid": number(message["bp"], 0),
                "ask": number(message["ap"], 0), "source_as_of_ms": message.get("T"), "retrieved_at_ms": received_ms})
    elif event == "calendar":
        store.set_meta("stock_calendar", {"from": message.get("from"), "to": message.get("to"),
                                         "source_as_of_ms": message.get("ts"), "retrieved_at_ms": received_ms})
    elif event in ("tradingStatus", "tradability"):
        symbol = message.get("symbol")
        if symbol in ("QQQ", "VOO"):
            key = "stock_" + event + ":" + symbol
            old = store.get_meta(key, {})
            if (message.get("t") or message.get("time") or 0) >= (old.get("t") or old.get("time") or 0):
                store.set_meta(key, {**message, "retrieved_at_ms": received_ms})


def bybit_message(store, message, received_ms):
    if message.get("topic") != "allLiquidation.BTCUSDT":
        return
    # The stream has no per-liquidation ID. Deduplicate repeated envelopes while
    # preserving identical entries at different positions inside one envelope.
    digest = hashlib.sha256(canonical(message).encode()).hexdigest()
    rows = []
    for index, item in enumerate(message.get("data") or []):
        if item.get("s") != "BTCUSDT" or item.get("S") not in ("Buy", "Sell"):
            raise ValueError("liquidation_identity_invalid")
        stamp = int(item["T"])
        qty, price = number(item["v"], 0), number(item["p"], 0)
        if qty <= 0 or price <= 0:
            raise ValueError("liquidation_amount_invalid")
        rows.append({"observed_ms": stamp, "item_key": digest + ":" + str(index),
                     "side": "long" if item["S"] == "Buy" else "short", "quantity": qty,
                     "bankruptcy_price": price, "estimated_notional": qty * price})
    store.put("bybit_liquidation", "BTCUSDT", rows, provenance(BYBIT_WS, "USDT", quantity_unit="BTC",
              notional_basis="bankruptcy_price_estimate"), received_ms=received_ms)


def run_stream(path, kind, log=lambda _: None, stop=None):
    stop = stop or threading.Event()
    bybit = kind == "bybit"
    url = BYBIT_WS if bybit else STOCK_WS + "/ws"
    topics = ["allLiquidation.BTCUSDT"] if bybit else STOCK_TOPICS
    series = "bybit_liquidation" if bybit else "stock_stream"
    symbols = ["BTCUSDT"] if bybit else ["QQQ", "VOO"]
    backoff = 1
    while not stop.is_set():
        sock = None
        try:
            sock = connect_websocket(url, connect_timeout=10, read_timeout=5)
            request = {"op": "subscribe", "args": topics, "req_id": "market"} if bybit else {
                "method": "SUBSCRIBE", "params": topics, "id": 1}
            sock.send_frame(1, json.dumps(request).encode())
            connection = uuid.uuid4().hex
            last_response = last_ping = time.monotonic()
            connected_mono = last_response
            last_saved = 0
            last_health_minute = None
            subscribed, start = False, None
            with closing(HistoryStore(path)) as store:
                while not stop.is_set():
                    current = time.monotonic()
                    if current - last_response >= 60:
                        raise TimeoutError("public_stream_server_silent")
                    if not subscribed and current - connected_mono >= 20:
                        raise TimeoutError("public_stream_subscription_unconfirmed")
                    if current - last_ping >= 20:
                        sock.send_frame(1, b'{"op":"ping"}') if bybit else sock.send_frame(9, b"market")
                        last_ping = current
                    try:
                        opcode, payload = sock.read_frame()
                    except socket.timeout:
                        continue
                    now = int(time.time() * 1000)
                    if opcode == 8:
                        raise ConnectionError("public_stream_closed")
                    if opcode == 9:
                        sock.send_frame(10, payload)
                    message = json.loads(payload.decode()) if opcode == 1 else {}
                    ack = (message.get("op") == "subscribe" and message.get("success") is True) if bybit else (
                        message.get("id") == 1 and "result" in message and message["result"] is None)
                    if ack and not subscribed:
                        subscribed, start = True, now
                    if message.get("success") is False or "error" in message:
                        raise ValueError("public_stream_subscription_rejected")
                    (bybit_message if bybit else stock_message)(store, message, now)
                    last_response = time.monotonic()
                    if subscribed and now - last_saved >= 10000:
                        for symbol in symbols:
                            store.cover(series, symbol, connection, start, now)
                        store.set_meta("stream:" + kind, {"connected": True, "last_response_ms": now,
                            "connection": connection, "started_ms": start})
                        last_saved = now
                        if last_health_minute != now // 60000:
                            store.health_sample('stream:' + kind, now, True)
                            last_health_minute = now // 60000
                    if subscribed and now - start > 60000:
                        backoff = 1
        except Exception as exc:
            log(f"public stream {kind}: {type(exc).__name__}")
            try:
                with closing(HistoryStore(path)) as store:
                    store.set_meta("stream:" + kind, {"connected": False, "error": type(exc).__name__,
                        "disconnected_ms": int(time.time() * 1000)})
                    store.health_sample('stream:' + kind, int(time.time()*1000), False, type(exc).__name__)
            except Exception:
                pass
        finally:
            if sock is not None:
                sock.close()
            if stop.is_set():
                with closing(HistoryStore(path)) as store:
                    state = store.get_meta('stream:' + kind, {})
                    store.set_meta('stream:' + kind, {**state, 'connected':False,
                                   'disconnected_ms':int(time.time()*1000)})
        stop.wait(backoff)
        backoff = min(60, backoff * 2)


def start_streams(path, log=lambda _: None):
    with _lock:
        for kind in ("stocks", "bybit"):
            key = (path, kind)
            if key not in _threads or not _threads[key].is_alive():
                thread = threading.Thread(target=run_stream, args=(path, kind, log), name="market-" + kind, daemon=True)
                _threads[key] = thread
                thread.start()
