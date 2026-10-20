"""Bounded public collection, reusable per-thread HTTP sessions and timings."""
import copy
import socket
import struct
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from contextvars import copy_context
from urllib.parse import urlsplit, urlunsplit

import requests
from urllib3.connection import HTTPSConnection
from urllib3.connectionpool import HTTPSConnectionPool
from urllib3.exceptions import ReadTimeoutError

_local = threading.local()
_cache = {}
_cache_lock = threading.RLock()
_fetch_locks = [threading.RLock() for _ in range(32)]
_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="btc-public")
# Reuse connections within a collection burst, not across hourly idle periods.
MAX_CONNECTION_IDLE_SEC = 60


def _socket_addresses(sock):
    try:
        return {"peer_ip": sock.getpeername()[0], "local_ip": sock.getsockname()[0]}
    except OSError:
        return {}  # Diagnostics must not turn a concurrently closed socket into a new failure.


def _tcp_state(sock):
    # Darwin's public TCP_CONNECTION_INFO, as defined by netinet/tcp.h.
    # Observe only this client's socket; no packet capture or elevated access.
    if sys.platform != "darwin" or sock is None:
        return None
    try:
        data = sock.getsockopt(socket.IPPROTO_TCP, 0x106, 112)
        if len(data) < 104:
            return None
        return {"state": data[0], "send_buffer_bytes": struct.unpack_from("I", data, 32)[0],
                "rtt_ms": struct.unpack_from("I", data, 44)[0],
                **dict(zip(("tx_packets", "tx_bytes", "tx_retransmitted_bytes", "rx_packets",
                            "rx_bytes", "rx_out_of_order_bytes"), struct.unpack_from("6Q", data, 56)))}
    except (OSError, ValueError, struct.error):
        return None


class _ObservedHTTPSConnection(HTTPSConnection):
    def _new_conn(self):
        event = getattr(self, "_collection_event", None)
        started = time.perf_counter()
        if event is not None:
            event["phase"] = "dns_tcp_connect"
        try:
            sock = super()._new_conn()
            if event is not None:
                event.update(_socket_addresses(sock), connection_mode="new", phase="tls_handshake")
            return sock
        finally:
            if event is not None:
                event["dns_tcp_ms"] = round((time.perf_counter() - started) * 1000, 3)

    def connect(self):
        event = getattr(self, "_collection_event", None)
        started = time.perf_counter()
        try:
            result = super().connect()
            if event is not None:
                event.update(phase="request_send", tls_version=self.sock.version())
            return result
        finally:
            if event is not None:
                event["connect_ms"] = round((time.perf_counter() - started) * 1000, 3)

    def request(self, *args, **kwargs):
        event = getattr(self, "_collection_event", None)
        if event is not None:
            event["phase"] = "request_send"
        result = super().request(*args, **kwargs)
        if event is not None:
            event["phase"] = "response_headers"
        return result

    def getresponse(self):
        before = _tcp_state(self.sock)
        try:
            result = super().getresponse()
        except Exception:
            event = getattr(self, "_collection_event", None)
            if event is not None:
                event.update(tcp_before_response=before, tcp_at_failure=_tcp_state(self.sock))
            raise
        self._last_headers_at = time.monotonic()
        event = getattr(self, "_collection_event", None)
        if event is not None:
            event["phase"] = "response_body"
        return result


class _ObservedHTTPSPool(HTTPSConnectionPool):
    ConnectionCls = _ObservedHTTPSConnection

    def _get_conn(self, timeout=None):
        conn = super()._get_conn(timeout)
        last = getattr(conn, "_last_headers_at", None)
        idle = time.monotonic() - last if last is not None else None
        expired = conn.sock is not None and idle is not None and idle > MAX_CONNECTION_IDLE_SEC
        if expired:
            conn.close()
        events = getattr(_local, "active_transport", None)
        if events is not None:
            event = {"connection_mode": "reused" if conn.sock is not None else "new",
                     "phase": "connection_checkout",
                     "idle_since_headers_sec": round(idle, 3) if idle is not None else None,
                     "connection_reset_reason": "idle_expired" if expired else None}
            if conn.sock is not None:
                event.update(_socket_addresses(conn.sock))
            events.append(event)
            conn._collection_event = event
        else:
            conn._collection_event = None
        return conn


class _ObservedAdapter(requests.adapters.HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        super().init_poolmanager(*args, **kwargs)
        # PoolManager defaults reference a module-wide dictionary. Keep other
        # requests clients untouched, including Telegram and private providers.
        self.poolmanager.pool_classes_by_scheme = dict(self.poolmanager.pool_classes_by_scheme,
                                                       https=_ObservedHTTPSPool)


def _new_session():
    value = requests.Session()
    adapter = _ObservedAdapter(pool_connections=8, pool_maxsize=8)
    value.mount("https://", adapter)
    return value


def session():
    if getattr(_local, "session", None) is None:
        _local.session = _new_session()
    return _local.session


def _read_timeout(exc):
    # Requests wraps a timeout while consuming the body in ConnectionError.
    return isinstance(exc, requests.ReadTimeout) or (
        isinstance(exc, requests.ConnectionError)
        and any(isinstance(cause, ReadTimeoutError) for cause in exc.args))


def _request_once(client, method, url, attempt, **kwargs):
    started = time.time()
    timer = time.perf_counter()
    status, failure = None, None
    phase, headers_ms, response_headers = "response_headers", None, {}
    previous_transport = getattr(_local, "active_transport", None)
    transport = []
    _local.active_transport = transport
    hooks = dict(kwargs.pop("hooks", None) or {})
    callbacks = hooks.get("response") or []
    if callable(callbacks):
        callbacks = [callbacks]

    def received_headers(response, *args, **unused):
        nonlocal status, phase, headers_ms, response_headers
        status = response.status_code
        headers_ms = round((time.perf_counter() - timer) * 1000, 3)
        phase = "response_body"
        response_headers = {key: response.headers[key] for key in (
            "Date", "CF-Ray", "CF-Cache-Status") if key in response.headers}

    hooks["response"] = [received_headers, *callbacks]
    try:
        response = client.request(method, url, hooks=hooks, **kwargs)
        status = response.status_code
        phase = "complete"
        for event in transport:
            event["phase"] = "complete"
        if status >= 400:
            failure = "http_" + str(status)
        return response
    except Exception as exc:
        failure = type(exc).__name__
        raise
    finally:
        _local.active_transport = previous_transport
        trace = getattr(_local, "trace", None)
        if trace is not None:
            parsed = urlsplit(url)
            # Public collector URLs only; credentials and query strings are never copied.
            endpoint = urlunsplit((parsed.scheme, parsed.hostname or "", parsed.path, "", ""))
            trace.append({
                "endpoint_url": endpoint, "method": method.upper(),
                "started_at": started, "retrieved_at": time.time(),
                "duration_ms": round((time.perf_counter() - timer) * 1000, 3),
                "http_status": status, "failure": failure,
                "attempt": attempt, "session_mode": "fresh" if attempt > 1 else "pooled",
                "connection_mode": transport[-1]["connection_mode"] if transport else "unobserved",
                "response_headers_ms": headers_ms,
                "failure_phase": ((transport[-1]["phase"] if transport else phase)
                                  if failure and phase != "complete" else None),
                "response_headers": response_headers,
                "transport": transport,
            })


def request(method, url, retry_read_timeout=False, **kwargs):
    if retry_read_timeout and method.upper() != "GET":
        raise ValueError("read_timeout_retry_requires_get")
    attempts = 2 if retry_read_timeout else 1
    for attempt in range(1, attempts + 1):
        client = session() if attempt == 1 else _new_session()
        try:
            return _request_once(client, method, url, attempt, **kwargs)
        except requests.RequestException as exc:
            if attempt == attempts or not _read_timeout(exc):
                raise
        finally:
            if attempt > 1:
                client.close()


@contextmanager
def trace_requests():
    previous = getattr(_local, "trace", None)
    current = []
    _local.trace = current
    try:
        yield current
    finally:
        _local.trace = previous


def get(url, **kwargs):
    return request("GET", url, **kwargs)


def post(url, **kwargs):
    return request("POST", url, **kwargs)


def collect_sources(fetchers, unavailable):
    """Caller must give state-mutating sources private state snapshots."""
    def one(item):
        key, fetcher = item
        started = time.perf_counter()
        with trace_requests() as trace:
            try:
                record = fetcher()
                if not isinstance(record, dict):
                    raise TypeError("collector_record_not_object")
                failure = None
            except Exception as exc:
                failure = type(exc).__name__
                record = unavailable(key, failure)
        record = dict(record)
        # A cache hit retains original retrieval provenance.
        if trace:
            record["source_requests"] = trace
        metric = {
            "duration_ms": round((time.perf_counter() - started) * 1000, 3),
            "request_count": len(trace),
            "failure": failure,
            "status": record.get("status"),
            "data_age_sec": (max(0, time.time() - record["source_as_of_ms"] / 1000)
                             if isinstance(record.get("source_as_of_ms"), (int, float))
                             else None),
        }
        record["collector_metrics"] = metric
        return key, record
    futures = [_executor.submit(copy_context().run, one, item) for item in fetchers.items()]
    return dict(future.result() for future in futures)


def measure(fetcher):
    """Time a dependency stage without changing its return value or failure."""
    started = time.perf_counter()
    with trace_requests() as trace:
        value = fetcher()
    return value, {"duration_ms": round((time.perf_counter() - started) * 1000, 3),
                   "request_count": len(trace), "requests": trace}


def cache_get(key, max_age, now=None):
    now = time.time() if now is None else now
    with _cache_lock:
        item = _cache.get(key)
        if item is None or not 0 <= now - item[0] < max_age:
            return None
        return {"value": copy.deepcopy(item[1]), "retrieved_at": item[0],
                "age_sec": now - item[0]}


def cache_put(key, value, now=None):
    with _cache_lock:
        _cache[key] = (time.time() if now is None else now, copy.deepcopy(value))
        # Hourly dependency keys are bounded even during a long daemon lifetime.
        if len(_cache) > 64:
            for old in sorted(_cache, key=lambda k: _cache[k][0])[:-48]:
                del _cache[old]


def clear_cache():
    with _cache_lock:
        _cache.clear()


def cached_call(key, max_age, fetcher):
    """Share public dependencies without concurrent duplicate requests or cross-key data."""
    with _fetch_locks[hash(key) % len(_fetch_locks)]:
        cached = cache_get(key, max_age)
        if cached is not None:
            return cached["value"]
        value = fetcher()
        cache_put(key, value)
        return copy.deepcopy(value)
