#!/usr/bin/env python3
"""OECD Dashboard ETL — stdlib only (no pip installs needed).

Reads config/datasets.json, pulls each dataset from the OECD SDMX API
(CSV format), and writes data/<id>.json + data/meta.json.
Safety rule: a dataset file is only overwritten after a successful fetch
with at least one observation; on failure the previous file is kept.
"""
import csv
import io
import json
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
API = "https://sdmx.oecd.org/public/rest/data"
HISTORY_MONTHS = 37  # ~3 years + 1 for year-over-year context


def fetch_csv(dataflow: str, filt: str) -> list[dict]:
    url = f"{API}/{dataflow}/{filt}?format=csvfile"
    # NOTE: no startPeriod parameter at all — an empty "startPeriod="
    # makes the API return 403 (verified 2026-11-09). We fetch the full
    # series and trim locally to HISTORY_MONTHS.
    req = urllib.request.Request(url, headers={"User-Agent": "oecd-dashboard-etl"})
    # OECD throttles bursts (HTTP 429 seen 2026-11-10 when the registry grew
    # to 73 series): pace requests and retry throttled/5xx responses with
    # backoff instead of failing the dataset for the day.
    text = None
    for attempt, wait in enumerate((0, 5, 15, 30, 60)):
        if wait:
            time.sleep(wait)
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                text = resp.read().decode("utf-8")
            break
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504) and attempt < 4:
                continue
            raise
    time.sleep(1.2)
    if "NoRecordsFound" in text:
        return []
    rows = list(csv.DictReader(io.StringIO(text)))
    rows.sort(key=lambda r: r["TIME_PERIOD"])
    return rows[-(HISTORY_MONTHS):] if len(rows) > HISTORY_MONTHS else rows


def summarise(ds: dict, rows: list[dict]) -> dict:
    series = [{"period": r["TIME_PERIOD"], "value": float(r["OBS_VALUE"])} for r in rows]
    latest = series[-1]
    prev = series[-2] if len(series) > 1 else None
    yoy_ref = series[-13] if len(series) > 12 else None
    return {
        "id": ds["id"],
        "title": ds["title"],
        "subtitle": ds["subtitle"],
        "source": ds["source"],
        "unit": ds["unit"],
        "decimals": ds["decimals"],
        "direction_good": ds["direction_good"],
        "latest": latest,
        "change_vs_prev": (latest["value"] - prev["value"]) if prev else None,
        "change_vs_year_ago": (latest["value"] - yoy_ref["value"]) if yoy_ref else None,
        "series": series,
    }


def main() -> int:
    config = json.loads((ROOT / "config" / "datasets.json").read_text(encoding="utf-8"))
    out_dir = ROOT / "data"
    out_dir.mkdir(exist_ok=True)
    results, failures = [], []
    for ds in config["datasets"]:
        try:
            rows = fetch_csv(ds["dataflow"], ds["filter"])
            if not rows:
                raise ValueError("empty result (NoRecordsFound)")
            payload = summarise(ds, rows)
            (out_dir / f"{ds['id']}.json").write_text(
                json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8"
            )
            results.append({"id": ds["id"], "latest_period": payload["latest"]["period"],
                            "latest_value": payload["latest"]["value"]})
            print(f"OK   {ds['id']}: {payload['latest']['period']} = {payload['latest']['value']}")
        except Exception as exc:  # keep last good file, report, continue
            failures.append({"id": ds["id"], "error": str(exc)})
            print(f"FAIL {ds['id']}: {exc} (previous data file kept)", file=sys.stderr)
    meta = {
        "updated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "succeeded": results,
        "failed": failures,
    }
    (out_dir / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=1), encoding="utf-8")
    return 0 if results else 1


if __name__ == "__main__":
    raise SystemExit(main())
