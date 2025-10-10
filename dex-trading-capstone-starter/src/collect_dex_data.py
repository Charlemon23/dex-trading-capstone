#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
collect_dex_data.py
---------------------------------
Collect Solana DEX pair snapshots from DexScreener.

Two collection modes:
1) Search mode (default): queries /latest/dex/search?q=... with a set of queries,
   merges & de-duplicates results, and appends to daily CSV.
2) Exact-pairs mode: if --pair-ids is provided, fetches those specific pairs via
   /latest/dex/pairs/{chainId}/{pairId}.

Examples:
    # default search mode with built-in queries
    python src/data_collection/collect_dex_data.py

    # custom search queries (comma-separated)
    python src/data_collection/collect_dex_data.py --queries "solana,raydium solana,SOL/USDC"

    # exact pairs by ID
    python src/data_collection/collect_dex_data.py --pair-ids "PAIR_ID_1,PAIR_ID_2"

    # loop every 5 minutes
    python src/data_collection/collect_dex_data.py --interval 300
"""

import argparse
import csv
import os
import sys
import time
import urllib.parse
from datetime import datetime, timezone
from typing import Dict, List, Any, Iterable

try:
    import requests
except ImportError:
    sys.stderr.write(
        "[ERROR] Missing dependency 'requests'. Install with:\n"
        "    pip install 'requests>=2.32.0'\n"
    )
    raise

# ---- DexScreener endpoints (per official docs) ----
DEX_SEARCH_URL = "https://api.dexscreener.com/latest/dex/search"
DEX_PAIRS_URL_TMPL = "https://api.dexscreener.com/latest/dex/pairs/{chainId}/{pairId}"

USER_AGENT = "CapstoneDataCollector/1.1 (student-researcher)"
REQUEST_TIMEOUT = 20  # seconds
DEFAULT_INTERVAL = 0  # 0 = run once and exit
DEFAULT_OUTDIR = "./data"

# Sensible Solana-focused default queries to capture a wide slice of pairs
DEFAULT_QUERIES = [
    "solana",
    "raydium solana",
    "orca solana",
    "meteora solana",
    "SOL/USDC",
]


# ---------------- HTTP helpers ----------------

def http_get_json(url: str, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    resp = requests.get(url, headers=headers, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def fetch_pairs_by_search_queries(queries: Iterable[str]) -> List[Dict[str, Any]]:
    """Call /latest/dex/search for each query and merge results (dedup by pairAddress)."""
    seen = set()
    merged: List[Dict[str, Any]] = []
    for q in queries:
        q = q.strip()
        if not q:
            continue
        data = http_get_json(DEX_SEARCH_URL, params={"q": q})
        for p in data.get("pairs", []) or []:
            pid = p.get("pairAddress")
            if pid and pid not in seen:
                seen.add(pid)
                merged.append(p)
    return merged


def fetch_pairs_by_ids(chain_id: str, pair_ids_csv: str) -> List[Dict[str, Any]]:
    """Fetch specific pairs using /latest/dex/pairs/{chainId}/{pairId} (comma-separated allowed)."""
    out: List[Dict[str, Any]] = []
    for pid in [x.strip() for x in pair_ids_csv.split(",") if x.strip()]:
        url = DEX_PAIRS_URL_TMPL.format(chainId=chain_id, pairId=urllib.parse.quote(pid))
        data = http_get_json(url)
        out.extend(data.get("pairs", []) or [])
    # dedupe by pairAddress
    dedup, seen = [], set()
    for p in out:
        pa = p.get("pairAddress")
        if pa and pa not in seen:
            seen.add(pa)
            dedup.append(p)
    return dedup


# ---------------- Transform & I/O ----------------

def _get_nested(d: Dict[str, Any], path: str, default=None):
    cur = d
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur


def normalize_records(pairs: List[Dict[str, Any]], snapshot_ts: str) -> List[Dict[str, Any]]:
    """Flatten API objects into a consistent row schema."""
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


def read_existing_keys(csv_path: str) -> set:
    keys = set()
    if not os.path.isfile(csv_path):
        return keys
    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            keys.add((row.get("pairAddress"), row.get("snapshot_ts")))
    return keys


def write_rows(csv_path: str, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0

    fieldnames = [
        "snapshot_ts","pairAddress","chainId","dexId","url",
        "baseToken_symbol","baseToken_address","quoteToken_symbol","quoteToken_address",
        "priceNative","priceUsd",
        "priceChange_h1","priceChange_h6","priceChange_h24",
        "txns_m5_buys","txns_m5_sells","txns_h1_buys","txns_h1_sells",
        "txns_h6_buys","txns_h6_sells","txns_h24_buys","txns_h24_sells",
        "volume_m5","volume_h1","volume_h6","volume_h24",
        "liquidity_base","liquidity_quote","liquidity_usd",
        "pairCreatedAt",
    ]

    file_exists = os.path.isfile(csv_path)
    existing = read_existing_keys(csv_path)

    deduped = []
    for r in rows:
        key = (r.get("pairAddress"), r.get("snapshot_ts"))
        if key not in existing:
            deduped.append(r)
            existing.add(key)

    if not deduped:
        return 0

    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(deduped)
    return len(deduped)


# ---------------- Orchestration ----------------

def do_snapshot(outdir: str, queries: List[str], chain_id: str, pair_ids_csv: str | None) -> int:
    now_utc = datetime.now(timezone.utc)
    snapshot_ts = now_utc.isoformat(timespec="seconds")

    try:
        if pair_ids_csv:
            pairs = fetch_pairs_by_ids(chain_id=chain_id, pair_ids_csv=pair_ids_csv)
        else:
            pairs = fetch_pairs_by_search_queries(queries)
    except requests.HTTPError as e:
        print(f"[ERROR] HTTP {e.response.status_code}: {e}", file=sys.stderr)
        return 0
    except requests.RequestException as e:
        print(f"[ERROR] Request failed: {e}", file=sys.stderr)
        return 0
    except Exception as e:
        print(f"[ERROR] Unexpected: {e}", file=sys.stderr)
        return 0

    rows = normalize_records(pairs, snapshot_ts)
    ensure_dir(outdir)
    csv_path = _daily_csv_path(outdir, now_utc)

    before = 0
    if os.path.isfile(csv_path):
        with open(csv_path, "r", encoding="utf-8") as f:
            before = max(sum(1 for _ in f) - 1, 0)

    added = write_rows(csv_path, rows)

    after = 0
    if os.path.isfile(csv_path):
        with open(csv_path, "r", encoding="utf-8") as f:
            after = max(sum(1 for _ in f) - 1, 0)

    print(f"[OK] {snapshot_ts} UTC — wrote {added} rows to {csv_path} (total {after})")
    return added


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Collect Solana DEX pair snapshots from DexScreener.")
    ap.add_argument("--interval", type=int, default=DEFAULT_INTERVAL,
                    help="Seconds between snapshots (0 = run once and exit).")
    ap.add_argument("--outdir", type=str, default=DEFAULT_OUTDIR,
                    help="Output directory for daily CSVs.")
    ap.add_argument("--queries", type=str, default=",".join(DEFAULT_QUERIES),
                    help="Comma-separated search queries for /latest/dex/search.")
    ap.add_argument("--chain-id", type=str, default="solana",
                    help="Chain ID for exact pair lookups.")
    ap.add_argument("--pair-ids", type=str, default=None,
                    help="Comma-separated pair IDs to fetch via /latest/dex/pairs/{chainId}/{pairId}. "
                         "If provided, overrides search mode.")
    return ap.parse_args()


def main():
    args = parse_args()
    queries = [q.strip() for q in args.queries.split(",") if q.strip()]

    if args.interval <= 0:
        do_snapshot(args.outdir, queries, args.chain_id, args.pair_ids)
        return

    print(f"[START] Looping every {args.interval}s, writing to {os.path.abspath(args.outdir)}")
    try:
        while True:
            do_snapshot(args.outdir, queries, args.chain_id, args.pair_ids)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\n[STOP] Interrupted by user.")


if __name__ == "__main__":
    main()
