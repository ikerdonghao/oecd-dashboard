#!/usr/bin/env python3
"""OECD Dashboard ETL — stdlib only (no pip installs needed).

Reads config/datasets.json, pulls datasets from the OECD SDMX API (CSV
format), and writes data/<id>.json + data/meta.json.
Safety rule: a dataset file is only overwritten after a successful fetch
with at least one observation; on failure the previous file is kept.

Refresh strategy (changed 2026-11-10 at Iker's request):
- "fill" mode (default): only fetch datasets whose data/<id>.json file
  does not exist yet and which are not on the confirmed-empty registry.
  New indicators/countries added to the config are picked up automatically
  by this rule. A run with nothing missing makes zero API requests.
- "full" mode: fetch everything (absorbs OECD historical revisions and
  re-tests the confirmed-empty registry).
- Mode selection: --mode full|fill, else $ETL_MODE, else automatic:
  full on Mondays (UTC), fill on other days.
- Confirmed-empty vs failed is kept strictly separate: a successful
  response with zero observations records the id in data/known_empty.json
  (fill runs then skip it until a full run re-tests it); an HTTP/network
  failure is NOT recorded and is retried on the next fill run.
- Requests are paced (1.2s apart) and 429/5xx responses are retried with
  backoff — the OECD API throttles bursts (seen 2026-11-10 at 73 series).
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
EMPTY_REGISTRY = "known_empty.json"


def fetch_csv(dataflow: str, filt: str) -> list[dict]:
    """Return observation rows; [] means OECD answered successfully but
    has no data for this key (confirmed empty). Raises on HTTP/network
    errors after retries (a failure — not evidence of emptiness)."""
    url = f"{API}/{dataflow}/{filt}?format=csvfile"
    # NOTE: no startPeriod parameter at all — an empty "startPeriod="
    # makes the API return 403 (verified 2026-11-09). We fetch the full
    # series and trim locally to HISTORY_MONTHS.
    req = urllib.request.Request(url, headers={"User-Agent": "oecd-dashboard-etl"})
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
            if e.code == 404:
                # OECD answers "no data for this key" as HTTP 404 with a
                # NoRecordsFound body — that is a confirmed empty, not a
                # failure (verified 2026-11-10).
                body = e.read().decode("utf-8", errors="replace")
                if "NoRecordsFound" in body:
                    return []
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


def choose_mode() -> str:
    if "--mode" in sys.argv:
        return sys.argv[sys.argv.index("--mode") + 1]
    import os
    if os.environ.get("ETL_MODE") in ("full", "fill"):
        return os.environ["ETL_MODE"]
    return "full" if datetime.now(timezone.utc).weekday() == 0 else "fill"


def main() -> int:
    mode = choose_mode()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    config = json.loads((ROOT / "config" / "datasets.json").read_text(encoding="utf-8"))
    out_dir = ROOT / "data"
    out_dir.mkdir(exist_ok=True)

    registry_path = out_dir / EMPTY_REGISTRY
    known_empty = {}
    if registry_path.exists():
        reg = json.loads(registry_path.read_text(encoding="utf-8"))
        known_empty = {e["id"]: e.get("since", "") for e in reg.get("empty", [])}

    results, failures, confirmed_now = [], [], []
    skipped_existing = skipped_known = 0
    for ds in config["datasets"]:
        ds_id = ds["id"]
        if mode == "fill":
            if (out_dir / f"{ds_id}.json").exists():
                skipped_existing += 1
                continue
            if ds_id in known_empty:
                skipped_known += 1
                continue
        try:
            rows = fetch_csv(ds["dataflow"], ds["filter"])
        except Exception as exc:  # failure: keep last good file, retry next fill run
            failures.append({"id": ds_id, "error": str(exc)})
            print(f"FAIL {ds_id}: {exc} (previous data file kept)", file=sys.stderr)
            continue
        if not rows:  # confirmed empty: record it; fill runs skip until a full run
            if ds_id not in known_empty:
                known_empty[ds_id] = today
            confirmed_now.append(ds_id)
            print(f"EMPTY {ds_id}: OECD has no data for this key (recorded)")
            continue
        payload = summarise(ds, rows)
        (out_dir / f"{ds_id}.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8"
        )
        known_empty.pop(ds_id, None)  # data appeared for a formerly empty key
        results.append({"id": ds_id, "latest_period": payload["latest"]["period"],
                        "latest_value": payload["latest"]["value"]})
        print(f"OK   {ds_id}: {payload['latest']['period']} = {payload['latest']['value']}")

    registry = {
        "updated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "empty": [{"id": k, "since": v} for k, v in sorted(known_empty.items())],
    }
    registry_path.write_text(json.dumps(registry, ensure_ascii=False, indent=1), encoding="utf-8")

    attempted = len(results) + len(failures) + len(confirmed_now)
    meta = {
        "updated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "mode": mode,
        "attempted": attempted,
        "skipped_existing": skipped_existing,
        "skipped_known_empty": skipped_known,
        "confirmed_empty_this_run": confirmed_now,
        "succeeded": results,
        "failed": failures,
    }
    (out_dir / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"mode={mode} attempted={attempted} ok={len(results)} "
          f"empty={len(confirmed_now)} failed={len(failures)} "
          f"skipped_existing={skipped_existing} skipped_known_empty={skipped_known}")
    return 0 if (results or attempted == 0) else 1


if __name__ == "__main__":
    raise SystemExit(main())
