"""Shared hourly scheduling with recoverable state per instrument."""
import copy
import math


class AssetSchedule:
    def __init__(self, asset_id, saved, now, *, immediate=False, retries=2, retry_delay=15):
        self.asset_id = asset_id
        self.retries, self.retry_delay = retries, retry_delay
        self.state = copy.deepcopy(saved) if saved else {
            "next_push": now if immediate else self.next_hour(now),
            "retry_at": None, "retry_attempt": 0, "report_id": None, "market_open": None}
        self._restoring = bool(saved)

    @staticmethod
    def next_hour(now):
        return (int(now) // 3600 + 1) * 3600

    def tick(self, now, *, session, send, pump, expire, save, report_identity):
        previous = copy.deepcopy(self.state)
        state = self.state
        reopened = state.get("market_open") is False
        identity = report_identity(now)
        retired = ":resume:" in (state.get("report_id") or "")
        if retired or (state.get("retry_at") is not None and state.get("report_id") != identity):
            state.update(retry_at=None, retry_attempt=0, report_id=None)
        if self.asset_id != "COMMON" and (self._restoring or reopened or retired
                                          or state["next_push"] < int(now) // 3600 * 3600):
            # Recovery is not a trigger. Keep a due boundary only when it is
            # exactly now; otherwise wait until the next hour. A persisted
            # preparation retry may still finish the same hourly report.
            state["next_push"] = max(math.ceil(now / 3600) * 3600,
                                     math.ceil(state["next_push"] / 3600) * 3600)
        if not session["open"]:
            expire()
            state.update(next_push=self.next_hour(now), retry_at=None, retry_attempt=0,
                         report_id=None, market_open=False)
            if state != previous or self._restoring:
                save(state)
            self._restoring = False
            return "market_closed"
        state["market_open"] = True
        if state != previous or self._restoring:
            save(state)
        self._restoring = False
        pump()
        scheduled = now >= state["next_push"]
        retry = state.get("retry_at") is not None and now >= state["retry_at"]
        if not scheduled and not retry:
            return None
        if scheduled:
            state.update(next_push=self.next_hour(now), retry_attempt=0, report_id=identity)
        # Persist retry intent before external work. A crash resumes the same ID;
        # the outbox decides whether it is pending, sent or unknown.
        state["retry_at"] = now + self.retry_delay
        save(state)
        result = send(require_usable_signal_data=True, report_id=state["report_id"], check_commands=False)
        if result == "retryable":
            if state["retry_attempt"] < self.retries:
                state["retry_attempt"] += 1
            else:
                result = send(require_usable_signal_data=False, report_id=state["report_id"], check_commands=False)
                state.update(retry_at=None, retry_attempt=0)
        else:
            state.update(retry_at=None, retry_attempt=0)
        save(state)
        return result
