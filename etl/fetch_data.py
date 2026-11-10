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
    rows = [r for r in csv.DictReader(io.StringIO(text)) if r.get("OBS_VALUE") not in (None, "")]
    rows.sort(key=lambda r: r["TIME_PERIOD"])
    return rows[-(HISTORY_MONTHS):] if len(rows) > HISTORY_MONTHS else rows


IMF_API = "https://api.imf.org/external/sdmx/3.0/data/dataflow/IMF.STA/GFS_COFOG/+/"


def fetch_imf(filt: str) -> list[dict]:
    """Fetch one IMF GFS_COFOG series (SDMX 3.0, SDMX-JSON) and return
    rows in the same shape as fetch_csv. Keyless — but the URL version
    segment must be '+' (an explicit version returns an empty result),
    and dimensionAtObservation=TIME_PERIOD is required (verified
    2026-11-10). [] means the flow answered with no matching series."""
    url = IMF_API + filt + "?dimensionAtObservation=TIME_PERIOD"
    req = urllib.request.Request(url, headers={"User-Agent": "oecd-dashboard-etl",
                                               "Accept": "application/json"})
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
    doc = json.loads(text)
    datasets = doc.get("data", {}).get("dataSets", [])
    series_map = datasets[0].get("series", {}) if datasets else {}
    if not series_map:
        return []
    structure = doc["data"]["structures"][0]
    dim_values = [[v.get("id") for v in d["values"]] for d in structure["dimensions"]["series"]]
    time_values = [v.get("value") for v in structure["dimensions"]["observation"][0]["values"]]
    wanted = filt.split(".")
    rows = []
    for key, sdata in series_map.items():
        actual = [dim_values[i][int(p)] for i, p in enumerate(key.split(":"))]
        if actual != wanted:
            continue
        for idx, obs in sdata.get("observations", {}).items():
            if obs and obs[0] is not None:
                rows.append({"TIME_PERIOD": time_values[int(idx)], "OBS_VALUE": str(obs[0])})
    rows.sort(key=lambda r: r["TIME_PERIOD"])
    return rows[-HISTORY_MONTHS:] if len(rows) > HISTORY_MONTHS else rows


PERIODS_PER_YEAR = {"M": 12, "Q": 4, "A": 1}


def build_payload(ds: dict, series: list[dict]) -> dict:
    """Assemble the per-dataset JSON from a finished series. The
    year-ago comparison uses the dataset's own frequency (an annual
    series compares 1 period back, a quarterly one 4, monthly 12)."""
    ppy = PERIODS_PER_YEAR.get(ds.get("freq", "M"), 12)
    latest = series[-1]
    prev = series[-2] if len(series) > 1 else None
    yoy_ref = series[-(ppy + 1)] if len(series) > ppy else None
    return {
        "id": ds["id"],
        "title": ds["title"],
        "subtitle": ds["subtitle"],
        "source": ds["source"],
        "unit": ds["unit"],
        "decimals": ds["decimals"],
        "direction_good": ds["direction_good"],
        "freq": ds.get("freq", "M"),
        "latest": latest,
        "change_vs_prev": (latest["value"] - prev["value"]) if prev else None,
        "change_vs_year_ago": (latest["value"] - yoy_ref["value"]) if yoy_ref else None,
        "series": series,
    }


def summarise(ds: dict, rows: list[dict]) -> dict:
    series = [{"period": r["TIME_PERIOD"], "value": float(r["OBS_VALUE"])} for r in rows]
    return build_payload(ds, series)


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

    by_id = {d["id"]: d for d in config["datasets"]}
    rows_cache: dict[str, list[dict]] = {}

    def get_rows(ds: dict) -> list[dict]:
        if ds["id"] not in rows_cache:
            if ds.get("provider") == "imf":
                rows_cache[ds["id"]] = fetch_imf(ds["filter"])
            else:
                rows_cache[ds["id"]] = fetch_csv(ds["dataflow"], ds["filter"])
        return rows_cache[ds["id"]]

    def series_of(ds_id: str):
        """Finished series of another dataset: this run's fetch if any,
        else its existing data file, else a fresh fetch of it."""
        if ds_id in rows_cache:
            rows = rows_cache[ds_id]
            return [{"period": r["TIME_PERIOD"], "value": float(r["OBS_VALUE"])} for r in rows] if rows else []
        path = out_dir / f"{ds_id}.json"
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))["series"]
        rows = get_rows(by_id[ds_id])
        return [{"period": r["TIME_PERIOD"], "value": float(r["OBS_VALUE"])} for r in rows] if rows else []

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
            rows = [] if "derive_sum_of" in ds else get_rows(ds)
            if "derive_sum_of" in ds:
                comp = [{p["period"]: p["value"] for p in series_of(cid)} for cid in ds["derive_sum_of"]]
                if any(not c for c in comp):
                    raise ValueError("derived sum: a component series is empty")
                common = sorted(set(comp[0]).intersection(*[set(c) for c in comp[1:]]))
                if not common:
                    raise ValueError("derived sum: components share no periods")
                summed = [{"period": p, "value": sum(c[p] for c in comp)} for p in common]
                payload = build_payload(ds, summed)
                (out_dir / f"{ds_id}.json").write_text(
                    json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8"
                )
                known_empty.pop(ds_id, None)
                results.append({"id": ds_id, "latest_period": payload["latest"]["period"],
                                "latest_value": payload["latest"]["value"]})
                print(f"OK   {ds_id}: {payload['latest']['period']} = {payload['latest']['value']} (sum of {len(comp)} components)")
                continue
            if rows and "derive_share_of" in ds:
                den = {p["period"]: p["value"] for p in series_of(ds["derive_share_of"])}
                num = {r["TIME_PERIOD"]: float(r["OBS_VALUE"]) for r in rows}
                common = sorted(p for p in num if p in den and den[p] != 0)
                if not common:
                    raise ValueError("derived share: no overlapping periods with denominator")
                ratio = [{"period": p, "value": num[p] / den[p] * 100} for p in common]
                payload = build_payload(ds, ratio)
                (out_dir / f"{ds_id}.json").write_text(
                    json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8"
                )
                known_empty.pop(ds_id, None)
                results.append({"id": ds_id, "latest_period": payload["latest"]["period"],
                                "latest_value": payload["latest"]["value"]})
                print(f"OK   {ds_id}: {payload['latest']['period']} = {payload['latest']['value']} (share of {ds['derive_share_of']})")
                continue
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
        try:
            payload = summarise(ds, rows)
        except Exception as exc:
            failures.append({"id": ds_id, "error": str(exc)})
            print(f"FAIL {ds_id}: {exc} (previous data file kept)", file=sys.stderr)
            continue
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
