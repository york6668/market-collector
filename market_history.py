"""Durable public observations. Import and read-only opens never create files.

Revisions retain their first retrieval time: replay cannot see a correction or a
backfilled CFTC observation before it was actually collected. JSON state remains
the bot's operational cache; this database is not pruned with that cache.
"""
import hashlib
import json
import os
import sqlite3
import time
from contextlib import closing
from pathlib import Path

HOUR = 3600000
DAY = 24 * HOUR


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def history_path(state_file=None):
    state_file = state_file or os.getenv("STATE_FILE", "~/.btc_push_state_tokyo.json")
    return os.path.abspath(os.path.expanduser(os.getenv("MARKET_HISTORY_DB", state_file + ".market.sqlite3")))


class HistoryStore:
    def __init__(self, path, *, readonly=False):
        self.path = os.path.abspath(os.path.expanduser(str(path)))
        self.readonly = readonly
        if readonly:
            self.db = sqlite3.connect(Path(self.path).as_uri() + "?mode=ro", uri=True, timeout=5)
        else:
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
            self.db = sqlite3.connect(self.path, timeout=10)
            self.db.execute("PRAGMA journal_mode=WAL")
            self.db.execute("PRAGMA synchronous=FULL")
            self.db.executescript("""
                CREATE TABLE IF NOT EXISTS observations (
                    revision INTEGER PRIMARY KEY, series TEXT NOT NULL, symbol TEXT NOT NULL,
                    item_key TEXT NOT NULL, observed_ms INTEGER NOT NULL, known_ms INTEGER NOT NULL,
                    digest TEXT NOT NULL, payload TEXT NOT NULL, provenance TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS observation_lookup
                    ON observations(series,symbol,observed_ms,known_ms);
                CREATE INDEX IF NOT EXISTS observation_identity
                    ON observations(series,symbol,item_key,revision);
                CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS coverage (
                    series TEXT, symbol TEXT, connection TEXT, start_ms INTEGER, end_ms INTEGER,
                    PRIMARY KEY(series,symbol,connection));
                CREATE TABLE IF NOT EXISTS health_samples (
                    checked_ms INTEGER, source TEXT, ok INTEGER, detail TEXT,
                    PRIMARY KEY(checked_ms,source));
            """)
        self.db.row_factory = sqlite3.Row

    def close(self):
        self.db.close()

    def put(self, series, symbol, rows, provenance, *, received_ms=None):
        received_ms = int(time.time() * 1000 if received_ms is None else received_ms)
        changed = 0
        # Commit the whole source response, including revisions, atomically.
        with self.db:
            for item in rows:
                item = dict(item)
                observed = int(item["observed_ms"])
                if observed > received_ms or observed < 0:
                    raise ValueError("observation_after_retrieval")
                key = str(item.get("item_key", observed))
                body = canonical(item)
                digest = hashlib.sha256(body.encode()).hexdigest()
                previous = self.db.execute("SELECT digest FROM observations WHERE series=? AND symbol=? "
                    "AND item_key=? ORDER BY revision DESC LIMIT 1", (series, symbol, key)).fetchone()
                if previous and previous[0] == digest:
                    continue
                self.db.execute("INSERT INTO observations(series,symbol,item_key,observed_ms,known_ms,"
                    "digest,payload,provenance) VALUES(?,?,?,?,?,?,?,?)",
                    (series, symbol, key, observed, received_ms, digest, body, canonical(provenance)))
                changed += 1
        return changed

    def rows(self, series, symbol, *, start_ms=0, end_ms=None, known_ms=None, limit=None):
        end_ms = int(time.time() * 1000 if end_ms is None else end_ms)
        known_ms = int(time.time() * 1000 if known_ms is None else known_ms)
        query = """SELECT o.* FROM observations o JOIN (
            SELECT MAX(revision) revision FROM observations
            WHERE series=? AND symbol=? AND observed_ms>=? AND observed_ms<=? AND known_ms<=?
            GROUP BY item_key) latest ON o.revision=latest.revision ORDER BY o.observed_ms DESC,o.item_key DESC"""
        args = [series, symbol, int(start_ms), end_ms, known_ms]
        if limit is not None:
            query += " LIMIT ?"
            args.append(int(limit))
        result = []
        for row in self.db.execute(query, args):
            item = json.loads(row["payload"])
            item.update(retrieved_at_ms=row["known_ms"], provenance=json.loads(row["provenance"]))
            result.append(item)
        return list(reversed(result))

    def get_meta(self, key, default=None):
        row = self.db.execute("SELECT payload FROM metadata WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def set_meta(self, key, value):
        with self.db:
            self.db.execute("INSERT INTO metadata VALUES(?,?) ON CONFLICT(key) DO UPDATE SET payload=excluded.payload",
                            (key, canonical(value)))

    def cover(self, series, symbol, connection, start_ms, end_ms):
        if not 0 <= start_ms <= end_ms:
            raise ValueError("invalid_coverage_interval")
        with self.db:
            self.db.execute("INSERT INTO coverage VALUES(?,?,?,?,?) ON CONFLICT(series,symbol,connection) "
                "DO UPDATE SET end_ms=MAX(end_ms,excluded.end_ms)",
                (series, symbol, connection, int(start_ms), int(end_ms)))

    def coverage(self, series, symbol, start_ms, end_ms):
        intervals = self.db.execute("SELECT start_ms,end_ms FROM coverage WHERE series=? AND symbol=? "
            "AND end_ms>? AND start_ms<? ORDER BY start_ms", (series, symbol, start_ms, end_ms)).fetchall()
        cursor, covered, gaps = start_ms, 0, []
        for a, b in intervals:
            a, b = max(start_ms, a), min(end_ms, b)
            if a > cursor:
                gaps.append([cursor, a])
            covered += max(0, b - max(cursor, a))
            cursor = max(cursor, b)
        if cursor < end_ms:
            gaps.append([cursor, end_ms])
        return {"start_ms": start_ms, "end_ms": end_ms, "covered_ms": covered,
                "complete": not gaps, "gaps": gaps}

    def health_sample(self, source, checked_ms, ok, detail=None):
        with self.db:
            self.db.execute('INSERT OR REPLACE INTO health_samples VALUES(?,?,?,?)',
                            (int(checked_ms), source, int(ok), detail))

    def status(self, now_ms=None):
        from datetime import date
        from market_assets import calendar_session, expected_hour_closes, trading_day
        now_ms = int(time.time()*1000 if now_ms is None else now_ms)
        first = self.db.execute('SELECT MIN(checked_ms) FROM health_samples').fetchone()[0]
        sources = [dict(row) for row in self.db.execute('SELECT source,count(*) checks,sum(ok) successes,'
                  'max(checked_ms) last_check_ms FROM health_samples GROUP BY source')]
        jobs = {row['key'][4:]:json.loads(row['payload']) for row in self.db.execute("SELECT * FROM metadata WHERE key LIKE 'job:%'")}
        stock_days = {}
        stock_coverage = {}
        for symbol in ('QQQ','VOO'):
            rows = self.rows('stock_1h',symbol, start_ms=first or now_ms, end_ms=now_ms, known_ms=now_ms)
            days = {date.fromisoformat(calendar_session(row['observed_ms']/1000)['trading_date']) for row in rows}
            stock_days[symbol] = len([d for d in days if trading_day(d)])
            present = {row['observed_ms'] for row in rows}
            expected = expected_hour_closes(first or now_ms, now_ms//HOUR*HOUR-1)
            stock_coverage[symbol] = {'expected_hours_since_start':len(expected),
                'observed_hours':len(present), 'missing_close_ms':[stamp for stamp in expected if stamp not in present]}
        updates = 0
        initial = self.db.execute("SELECT min(known_ms) FROM observations WHERE series='cftc'").fetchone()[0]
        if initial is not None:
            baseline = self.db.execute("SELECT max(observed_ms) FROM observations WHERE series='cftc' AND known_ms=?",(initial,)).fetchone()[0]
            updates = self.db.execute("SELECT count(DISTINCT observed_ms) FROM observations WHERE series='cftc' AND observed_ms>? AND known_ms>?",(baseline,initial)).fetchone()[0]
        elapsed = (now_ms-first)/DAY if first is not None else 0
        streams = {kind:self.get_meta('stream:'+kind,{}) for kind in ('stocks','bybit')}
        for state in streams.values():
            state['responding'] = state.get('connected') is True and 0 <= now_ms-state.get('last_response_ms',0) <= 60000
        return {'started_ms':first,'elapsed_days':elapsed,'stock_trading_days':stock_days,
                'stock_hour_coverage':stock_coverage, 'streams':streams,
                'bybit_14d_coverage':self.coverage('bybit_liquidation','BTCUSDT',now_ms//HOUR*HOUR-14*DAY,now_ms//HOUR*HOUR),
                'cftc_new_reporting_dates':updates,'observation_period_complete':elapsed>=14 and min(stock_days.values())>=10 and updates>=1,
                'stability_accepted':False, 'note':'Observation duration does not prove source availability or gap recovery; inspect source health and coverage.',
                'sources':sources,'jobs':jobs}


def read_rows(series, symbol, *, path=None, **kwargs):
    path = path or history_path()
    if not os.path.exists(path):
        return []
    with closing(HistoryStore(path, readonly=True)) as store:
        return store.rows(series, symbol, **kwargs)


def read_meta(key, *, path=None, default=None):
    path = path or history_path()
    if not os.path.exists(path):
        return default
    with closing(HistoryStore(path, readonly=True)) as store:
        return store.get_meta(key, default)


def flow_summary(rows, end_ms):
    """Each amount is a fixed-time sum; a missing hour invalidates that window."""
    rows = sorted((r for r in rows if r["close_time"] < end_ms), key=lambda r: r["open_time"])
    end = end_ms // HOUR * HOUR
    windows = {}
    for days in (1, 7, 14):
        selected = [r for r in rows if end - days * DAY <= r["open_time"] < end]
        expected = list(range(end - days * DAY, end, HOUR))
        complete = [r["open_time"] for r in selected] == expected
        valid = complete and all(0 <= r["taker_buy_quote"] <= r["quote_volume"] for r in selected)
        windows[str(days) + "d"] = {"status": "ok" if valid else "partial" if selected else "collecting",
            "net_quote": sum(2 * r["taker_buy_quote"] - r["quote_volume"] for r in selected) if valid else None,
            "observed_count": len(selected), "expected_count": len(expected)}
    curve, total, segment_start, previous = [], 0.0, None, None
    for row in rows:
        if previous is None or row["open_time"] != previous + HOUR:
            total, segment_start = 0.0, row["open_time"]
        total += 2 * row["taker_buy_quote"] - row["quote_volume"]
        curve.append({"time": row["close_time"], "value": total, "start_ms": segment_start})
        previous = row["open_time"]
    return {"windows": windows, "curve": curve, "unit": "USDT", "as_of_ms": rows[-1]["close_time"] if rows else None}
