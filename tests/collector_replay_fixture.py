"""Synthetic, offline inputs for the real collector export and downstream replay.

No live market observations, Telegram requests, configuration, or state are read.
Call build_case(name, evidence_module) to reuse payload/text/bundle/encoded bytes.
The exporter builds every bundle from raw inputs; bundles are never patched.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import socket
import sys
from datetime import datetime, timezone

HOUR_MS = 3_600_000
END_MS = int(datetime(2026, 9, 4, 6, tzinfo=timezone.utc).timestamp() * 1000)
GENERATED_AT = "2026-09-04T06:00:10+00:00"
CASES = ("valid", "missing_spot", "half_hour_spot", "misaligned_spot")


def make_payload(case="valid"):
    """Build 240 source bars; OI timestamps remain actual synthetic source stamps."""
    if case not in CASES:
        raise ValueError(f"unknown fixture: {case}")
    bars, spot = [], []
    spot_shift = {"half_hour_spot": -HOUR_MS // 2,
                  "misaligned_spot": -HOUR_MS}.get(case, 0)
    for index in range(240):
        opened = END_MS - (240 - index) * HOUR_MS
        closed = opened + HOUR_MS - 1  # Binance inclusive close receipt.
        price, volume, oi = 60_000 + index, 10 + index % 7, 100_000 + index
        quote = price * volume
        flow = dict(open_time=opened, close_time=closed,
                    open=price, high=price + 100, low=price - 100,
                    close=price + 20, volume=volume, quote_volume=quote,
                    taker_buy_quote_volume=quote * 0.6)
        bars.append(dict(flow, endpoint_url="https://fapi.binance.com/fapi/v1/klines",
            retrieved_at_ms=END_MS + 1000,
            oi=oi, oi_value=oi * (price + 20), oi_timestamp=closed + 1,
            oi_aligned=True, oi_drift_ms=1, oi_time_basis="source_observation",
            oi_source="Binance openInterestHist",
            oi_endpoint_url="https://fapi.binance.com/futures/data/openInterestHist",
            oi_source_as_of_ms=closed + 1, oi_retrieved_at_ms=END_MS + 1000))
        spot.append(dict(flow, open_time=opened + spot_shift,
                         close_time=closed + spot_shift))
    spot_value = {"hourly_flow": spot}
    for hours in (1, 4, 24):
        total = sum(row["quote_volume"] for row in spot[-hours:])
        net = sum(2 * row["taker_buy_quote_volume"] - row["quote_volume"]
                  for row in spot[-hours:])
        spot_value[f"net_{hours}h_quote"] = net
        spot_value[f"cvd_{hours}h_pct"] = net / total * 100
    records = {"spot_cvd": dict(status="ok", source="Binance Spot klines",
        venue="Binance", endpoint_url="https://api.binance.com/api/v3/klines",
        retrieved_at=(END_MS + 1000) / 1000,
        source_as_of_ms=spot[-1]["close_time"], time_basis="source_observation",
        source_time_missing=False, stale=False, value=spot_value)}
    if case == "missing_spot":
        records.pop("spot_cvd")
    return {"raw_signal_bars": bars, "public_records": records,
            "signal_data_quality": {"synthetic_fixture": True}}


def build_case(case="valid", evidence_module=None):
    """Return the exact raw payload, text, exporter bundle, and roundtrip bytes."""
    if evidence_module is None:
        import btc_evidence as evidence_module
    evidence = evidence_module
    payload = make_payload(case)
    text = f"📊 BTC 离线联合回放（合成样本）\ncase={case}\n仅供开发验证，不代表真实市场。"
    bundle = evidence.build_bundle(payload, text, f"fixture:{case}", GENERATED_AT)
    encoded = evidence.encode_bundle(bundle)
    decoded = evidence.decode_bundle(encoded)
    if decoded != bundle:
        raise AssertionError("collector encode/decode changed the bundle")
    validation = evidence.validate_bundle(decoded, text)
    expected = "valid_auxiliary" if case == "valid" else "degraded_auxiliary"
    if validation["status"] != expected:
        raise AssertionError((case, expected, validation))
    expected_reason = {"missing_spot": "missing_dataset",
                       "half_hour_spot": "invalid_utc_hour_boundary",
                       "misaligned_spot": "core_hour_alignment_mismatch"}.get(case)
    if expected_reason and expected_reason not in validation["dataset_reasons"].get("spot_cvd", []):
        raise AssertionError((case, expected_reason, validation))
    if case == "valid":
        assert not validation["dataset_reasons"]
        for name in ("futures_1h", "oi_1h", "spot_cvd"):
            assert len(decoded["datasets"][name]["rows"]) == 240
            assert decoded["datasets"][name]["window"]["complete"] is True
        for source, projected in zip(payload["raw_signal_bars"], decoded["datasets"]["oi_1h"]["rows"]):
            assert projected["timestamp"] == source["oi_timestamp"]
            assert projected["drift_ms"] == source["oi_drift_ms"] == 1
    return dict(payload=payload, text=text, bundle=decoded, encoded=encoded,
                validation=validation)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--collector-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--output-dir", type=Path, default=Path(__file__).with_suffix(""))
    args = parser.parse_args()
    # Block network before importing production code; retain no permanent patches.
    network_attempts = []
    def denied(*_args, **_kwargs):
        network_attempts.append("blocked")
        raise AssertionError("fixture network forbidden")
    socket.create_connection = denied
    socket.socket.connect = denied
    sys.path.insert(0, str(args.collector_root.resolve()))
    import btc_evidence as evidence
    args.output_dir.mkdir(parents=True, exist_ok=True)
    digest = lambda data: hashlib.sha256(data).hexdigest()
    receipt = {"scope": "synthetic raw payload -> actual build_bundle -> encode/decode/validate",
        "helper_sha256": digest(Path(__file__).read_bytes()),
        "exporter_path": str(Path(evidence.__file__).resolve()),
        "exporter_sha256": digest(Path(evidence.__file__).read_bytes()), "cases": {}}
    for case in CASES:
        result = build_case(case, evidence)
        artifacts = {"payload.json": evidence.canonical_bytes(result["payload"]),
                     "txt": result["text"].encode("utf-8"),
                     "bundle.json": result["encoded"],
                     "validation.json": evidence.canonical_bytes(result["validation"])}
        files = {}
        for suffix, data in artifacts.items():
            path = args.output_dir / f"{case}.{suffix}"
            path.write_bytes(data)
            files[suffix] = {"path": str(path.resolve()), "sha256": digest(data), "bytes": len(data)}
        receipt["cases"][case] = {"status": result["validation"]["status"],
            "dataset_reasons": result["validation"]["dataset_reasons"], "files": files}
    assert not network_attempts
    receipt.update(passed_cases=len(CASES), network_attempts=len(network_attempts),
                   production_state_access=False, telegram_access=False)
    receipt_path = args.output_dir / "fixture-results.json"
    receipt_path.write_text(json.dumps(receipt, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"receipt": str(receipt_path), "passed_cases": len(CASES),
                     "helper_sha256": receipt["helper_sha256"],
                     "exporter_sha256": receipt["exporter_sha256"],
                     "network_attempts": len(network_attempts)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
