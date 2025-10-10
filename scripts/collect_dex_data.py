#!/usr/bin/env python3


"""
scripts/collect_dex_data.py
Pulls live Solana DEX pair data from DexScreener and appends
a timestamped snapshot to daily CSVs under ./data/.

Usage:
    python scripts/collect_dex_data.py                    # one-shot
    python scripts/collect_dex_data.py --interval 300     # loop every 5 minutes
    python scripts/collect_dex_data.py --outdir ./data    # custom output dir
"""

import argparse
import csv
import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Any

import requests

DEXSCREENER_PAIRS_URL = "https://api.dexscreener.com/latest/dex/pairs/solana"
USER_AGENT = "CapstoneDataCollector/1.0 (contact: student-researcher)"
REQUEST_TIMEOUT = 20
DEFAULT_INTERVAL = 0
DEFAULT_OUTDIR = "./data"


def fetch_solana_pairs() -> Dict[str, Any]:
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    resp = requests.get(DEXSCREENER_PAIRS_URL, headers=headers, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def _get_nested(d: Dict[str, Any], path: str, default=None):
    cur = d
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur


def normalize_records(raw: Dict[str, Any], snapshot_ts: str) -> List[Dict[str, Any]]:
    pairs = raw.get("pairs", []) or []
    out: List[Dict[str, Any]] = []
    for p in pairs:
        rec = {
            "snapshot_ts": snapshot_ts,
            "pairAddress": p.get("pairAddress"),
            "chainId": p.get("chainId"),
            "dexId": p.get("dexId"),
            "url": p.get("url"),
            "baseToken_symbol": _get_nested(p, "baseToken.symbol"),
            "baseToken_address": _get_nested(p, "baseToken.address"),
            "quoteToken_symbol": _get_nested(p, "quoteToken.symbol"),
            "quoteToken_address": _get_nested(p, "quoteToken.address"),
            "priceNative": p.get("priceNative"),
            "priceUsd": p.get("priceUsd"),
            "priceChange_h1": _get_nested(p, "priceChange.h1"),
            "priceChange_h6": _get_nested(p, "priceChange.h6"),
            "priceChange_h24": _get_nested(p, "priceChange.h24"),
            "txns_m5_buys": _get_nested(p, "txns.m5.buys"),
            "txns_m5_sells": _get_nested(p, "txns.m5.sells"),
            "txns_h1_buys": _get_nested(p, "txns.h1.buys"),
            "txns_h1_sells": _get_nested(p, "txns.h1.sells"),
            "txns_h6_buys": _get_nested(p, "txns.h6.buys"),
            "txns_h6_sells": _get_nested(p, "txns.h6.sells"),
            "txns_h24_buys": _get_nested(p, "txns.h24.buys"),
            "txns_h24_sells": _get_nested(p, "txns.h24.sells"),
            "volume_m5": _get_nested(p, "volume.m5"),
            "volume_h1": _get_nested(p, "volume.h1"),
            "volume_h6": _get_nested(p, "volume.h6"),
            "volume_h24": _get_nested(p, "volume.h24"),
            "liquidity_base": _get_nested(p, "liquidity.base"),
            "liquidity_quote": _get_nested(p, "liquidity.quote"),
            "liquidity_usd": _get_nested(p, "liquidity.usd"),
            "pairCreatedAt": p.get("pairCreatedAt"),
        }
        out.append(rec)
    return out


def ensure_dir(path: str) -> None:
    if not os.path.isdir(path):
        os.makedirs(path, exist_ok=True)


def _daily_csv_path(outdir: str, date_utc: datetime) -> str:
    return os.path.join(outdir, f"dexscreener_solana_{date_utc.strftime('%Y-%m-%d')}.csv")


def read_existing_rows_indexed(csv_path: str) -> set:
    keyset = set()
    if not os.path.isfile(csv_path):
        return keyset
    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        try:
            header = next(f)
        except StopIteration:
            return keyset
        for line in f:
         
            pass
    # Fall back to DictReader (safe path)
    import csv as _csv
    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        reader = _csv.DictReader(f)
        for row in reader:
            keyset.add((row.get("pairAddress"), row.get("snapshot_ts")))
    return keyset


def write_rows(csv_path: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return

    fieldnames = [
        "snapshot_ts","pairAddress","chainId","dexId","url",
        "baseToken_symbol","baseToken_address","quoteToken_symbol","quoteToken_address",
        "priceNative","priceUsd",
        "priceChange_h1","priceChange_h6","priceChange_h24",
        "txns_m5_buys","txns_m5_sells","txns_h1_buys","txns_h1_sells","txns_h6_buys","txns_h6_sells",
        "txns_h24_buys","txns_h24_sells",
        "volume_m5","volume_h1","volume_h6","volume_h24",
        "liquidity_base","liquidity_quote","liquidity_usd",
        "pairCreatedAt",
    ]

    file_exists = os.path.isfile(csv_path)
    existing_keys = read_existing_rows_indexed(csv_path)

    deduped = []
    for r in rows:
        key = (r.get("pairAddress"), r.get("snapshot_ts"))
        if key not in existing_keys:
            deduped.append(r)
            existing_keys.add(key)

    if not deduped:
        return

    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(deduped)


def snapshot_once(outdir: str) -> int:
    now_utc = datetime.now(timezone.utc)
    snapshot_ts = now_utc.isoformat(timespec="seconds")

    try:
        raw = fetch_solana_pairs()
    except requests.HTTPError as e:
        print(f"[ERROR] HTTP {e.response.status_code}: {e}", file=sys.stderr)
        return 0
    except requests.RequestException as e:
        print(f"[ERROR] Request failed: {e}", file=sys.stderr)
        return 0
    except Exception as e:
        print(f"[ERROR] Unexpected: {e}", file=sys.stderr)
        return 0

    rows = normalize_records(raw, snapshot_ts)
    ensure_dir(outdir)
    csv_path = _daily_csv_path(outdir, now_utc)

    pre_count = 0
    if os.path.isfile(csv_path):
        with open(csv_path, "r", encoding="utf-8") as f:
            pre_count = max(sum(1 for _ in f) - 1, 0)

    write_rows(csv_path, rows)

    post_count = 0
    if os.path.isfile(csv_path):
        with open(csv_path, "r", encoding="utf-8") as f:
            post_count = max(sum(1 for _ in f) - 1, 0)

    added = max(post_count - pre_count, 0)
    print(f"[OK] {snapshot_ts} UTC — wrote {added} rows to {csv_path}")
    return added


def main():
    ap = argparse.ArgumentParser(description="Collect Solana DEX pair snapshots from DexScreener.")
    ap.add_argument("--interval", type=int, default=DEFAULT_INTERVAL,
                    help="Seconds between snapshots (0 = run once and exit).")
    ap.add_argument("--outdir", type=str, default=DEFAULT_OUTDIR,
                    help="Output directory for daily CSVs.")
    args = ap.parse_args()

    if args.interval <= 0:
        snapshot_once(args.outdir)
        return

    print(f"[START] Looping every {args.interval}s, writing to {os.path.abspath(args.outdir)}")
    try:
        while True:
            snapshot_once(args.outdir)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\n[STOP] Interrupted by user.")


if __name__ == "__main__":
    main()
