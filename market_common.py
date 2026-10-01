"""Hourly macro snapshots and delivery state, independent of trading instruments."""
import copy

import btc_collection_io as io
import market_news as news
from market_scheduler import AssetSchedule


def hour_start(now):
    return int(now) // 3600 * 3600


def report_id(now):
    return f"common:hour:{hour_start(now)}"


def macro_snapshot(store, identity, legacy, fetch, now):
    """One immutable snapshot per hour, including across daemon restarts.

    With no store, previews only use process-local caching. Fetch receives a
    private legacy cache and must retain actual source dates on cache hits.
    """
    hour = hour_start(now)
    key = ("common_macro", tuple(identity), store.path if store else "preview", hour)

    def collect():
        saved = store.load_metadata("common:macro") if store else None
        if saved and saved.get("identity") == list(identity):
            if saved.get("hour") == hour:
                return saved["record"]
            seed = saved.get("record") or legacy
        else:
            seed = legacy
        record = fetch(copy.deepcopy(seed or {}))
        if not record.get("value") and (seed or {}).get("value"):
            # Shared historical fallback, with its original timestamps. All
            # consumers see the same degraded snapshot instead of falling back
            # independently to their instrument's private report history.
            failure = record.get("reason") or "macro_collection_unavailable"
            record = copy.deepcopy(seed)
            record.update(source_stale=True, source_stale_reason=failure)
        record["shared_hour"] = hour
        if store:
            store.save_metadata("common:macro", {"identity": list(identity), "hour": hour, "record": record})
        return record

    return io.cached_call(key, 3600, collect)


def select_events(store, record, now):
    """Only displayed events are consumed; prepared and unknown sends reserve them."""
    seen = (store.load_metadata("common:news_seen") or {}) if store else {}
    blocked = {key for key, stamp in seen.items() if now - stamp < news.NEWS_TTL}
    for row in store.unapplied_report_commits("COMMON") if store else []:
        commit, status = row["commit_payload"], row["summary_status"]
        if commit.get("kind") != "common_report":
            continue
        if status == "expired" or (status == "failed" and commit.get("hour", 0) < hour_start(now)):
            continue
        for event in commit.get("news_events") or []:
            blocked.update(news.event_keys(event))
    selected = []
    for event in record.get("events") or []:
        keys = news.event_keys(event)
        if blocked.intersection(keys):
            continue
        selected.append(copy.deepcopy(event))
        blocked.update(keys)
        if len(selected) == news.EVENT_LIMIT:
            break
    return {**record, "events": selected}


def apply_receipt(store, identity, commit, receipt):
    if commit.get("kind") != "common_report":
        return
    # Merge even a late receipt: it must not replace the latest report, but its
    # events were delivered and must not be selected again. Replays are idempotent.
    now = store.clock()
    seen = {key: stamp for key, stamp in (store.load_metadata("common:news_seen") or {}).items()
            if now - stamp < news.NEWS_TTL}
    for event in commit.get("news_events") or []:
        for key in news.event_keys(event):
            seen[key] = max(seen.get(key, 0), receipt["date"])
    store.save_metadata("common:news_seen", seen)
    latest = store.load_metadata("common:latest_report") or {}
    if float(latest.get("generated_at") or 0) > commit["generated_at"]:
        return
    store.save_metadata("common:latest_report", {
        "report_id": identity, "text": commit["text"], "hour": commit["hour"],
        "generated_at": commit["generated_at"], "sent_at": receipt["date"],
        "message_id": receipt["message_id"],
    })


class CommonSchedule(AssetSchedule):
    def __init__(self, saved, now, **kwargs):
        super().__init__("COMMON", copy.deepcopy(saved), now, **kwargs)
        # Stale preparation retries never recollect or send a missed hour.
        if self.state.get("retry_at") is not None and self.state.get("report_id") != report_id(now):
            self.state.update(retry_at=None, retry_attempt=0, report_id=None,
                              next_push=min(self.state["next_push"], now))
        self.state["market_open"] = True

    def tick(self, now, *, send, pump, expire, save):
        expire(hour_start(now))
        return super().tick(now, session={"open": True}, send=send, pump=pump,
                            expire=lambda: None, save=save, report_identity=report_id)
