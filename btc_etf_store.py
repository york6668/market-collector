"""Dated ETF observations in the existing local market history database.

Dates are index labels, not invented publication times. HistoryStore preserves
when each revision first became known; successful refresh times stay separate.
"""
import copy
import sqlite3
from datetime import datetime, timezone

import btc_etf
import btc_etf_flow
from market_history import HistoryStore


def _day_ms(day):
    btc_etf_flow.source_day(day)
    return int(datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)


class ETFStore:
    def __init__(self, path):
        self.history = HistoryStore(path)
        self.errors = []

    def close(self):
        self.history.close()

    def call(self, method, *args):
        # A disk/DB failure degrades persistence, not an otherwise valid source.
        try:
            return getattr(self, method)(*args)
        except (sqlite3.Error, OSError) as exc:
            self.errors.append(type(exc).__name__)
            return None

    def _refresh(self, key, value):
        old = self.history.get_meta(key) or {}
        if value["retrieved_at"] >= old.get("retrieved_at", 0):
            self.history.set_meta(key, value)

    def save_holdings(self, parsed, retrieved_at):
        btc_etf.validate_holdings(parsed["holdings_scope"], parsed["holdings_rows"], parsed["holdings_btc"])
        row = {key: copy.deepcopy(parsed[key]) for key in
               ("source_date", "holdings_scope", "holdings_rows", "holdings_btc")}
        row["observed_ms"] = _day_ms(row["source_date"])
        self.history.put("etf_holdings", btc_etf.VENUE, [row],
                         {"venue": btc_etf.VENUE, "endpoint_url": btc_etf.ENDPOINT,
                          "time_basis": "observation_date"}, received_ms=int(retrieved_at * 1000))
        self._refresh("etf:holdings", {"source_date": row["source_date"], "retrieved_at": retrieved_at})

    def holdings(self, day, now):
        rows = self.history.rows("etf_holdings", btc_etf.VENUE,
            start_ms=_day_ms(day) if day else 0,
            end_ms=_day_ms(day) if day else int(now * 1000), known_ms=int(now * 1000), limit=1)
        if not rows:
            return None
        row = rows[-1]
        if not btc_etf.is_holdings_source(row["provenance"]):
            return None
        btc_etf.validate_holdings(row["holdings_scope"], row["holdings_rows"], row["holdings_btc"])
        parsed = {key: copy.deepcopy(row[key]) for key in
                  ("source_date", "holdings_scope", "holdings_rows", "holdings_btc")}
        refresh = self.history.get_meta("etf:holdings") or {}
        parsed["retrieved_at"] = row["retrieved_at_ms"] / 1000
        if refresh.get("source_date") == parsed["source_date"] and refresh.get("retrieved_at", now + 1) <= now:
            parsed["retrieved_at"] = refresh["retrieved_at"]
        return parsed

    def save_flow(self, record):
        if not btc_etf_flow.is_flow_source(record):
            raise ValueError("unknown_flow_source")
        rows = [{**copy.deepcopy(row), "observed_ms": _day_ms(row["date"])} for row in record["history"]]
        self.history.put("etf_flow", record["venue"], rows,
            {"venue": record["venue"], "endpoint_url": record["endpoint_url"],
             "time_basis": "observation_date"}, received_ms=int(record["retrieved_at"] * 1000))
        self._refresh("etf:flow:" + record["venue"],
            {"source_date": record["source_date"], "retrieved_at": record["retrieved_at"],
             "sample_dates": [row["date"] for row in record["history"]]})

    def flow(self, venue, now):
        refresh = self.history.get_meta("etf:flow:" + venue)
        if not refresh or refresh["retrieved_at"] > now:
            return None
        rows = self.history.rows("etf_flow", venue, start_ms=_day_ms(min(refresh["sample_dates"])),
                                 end_ms=_day_ms(refresh["source_date"]), known_ms=int(now * 1000))
        rows = [row for row in rows if row["date"] in refresh["sample_dates"]]
        if not rows or rows[-1]["date"] != refresh["source_date"]:
            return None
        history = [{key: copy.deepcopy(row[key]) for key in ("date", "totalNetInflow", "perEtfUsd")}
                   for row in rows]
        return {**btc_etf_flow._parsed(history, refresh["source_date"], venue),
                "retrieved_at": refresh["retrieved_at"]}

    def seed(self, state):
        """Import provable old observations once, without changing operational state."""
        if self.history.get_meta("etf:seeded"):
            return
        for snap in state.get("public_snapshots") or []:
            record = (snap.get("public") or {}).get("etf") or {}
            value = record.get("value") or {}
            retrieved = record.get("retrieved_at") or snap.get("ts")
            if not retrieved or not btc_etf.is_holdings_source(record):
                continue
            holding = record.get("holdings_record") or record
            hv = holding.get("value") or value
            day = (hv.get("source_date") or {}).get("holdings")
            if day and not holding.get("cache_used"):
                try:
                    self.save_holdings({"source_date": day, "holdings_btc": hv.get("holdings_btc"),
                        "holdings_scope": record.get("holdings_scope"), "holdings_rows": record.get("holdings_rows")},
                        holding.get("retrieved_at") or retrieved)
                except (ValueError, TypeError, KeyError):
                    pass  # Old partial/legacy records are not evidence of a complete balance.
            flow = record.get("flow_record") or {}
            history = value.get("history")
            if btc_etf_flow.is_flow_source(flow) and isinstance(history, list) and history:
                try:
                    self.save_flow({**flow, "history": history})
                except (ValueError, TypeError, KeyError):
                    pass
        self.history.set_meta("etf:seeded", True)
