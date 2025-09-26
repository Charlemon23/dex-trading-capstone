import argparse
from datetime import datetime
from .clients.dexscreener import search_pairs_by_query, normalize_pairs_to_df
from .config import settings
from .utils.io import save_df_csv

def collect(query: str, limit: int = 100) -> str:
    pairs = search_pairs_by_query(query)
    if limit and isinstance(limit, int):
        pairs = pairs[:limit]
    df = normalize_pairs_to_df(pairs)
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    fname = f"dexscreener_{query.replace(' ','_').lower()}_{ts}.csv"
    out_path = save_df_csv(df, settings.output_dir, fname)
    return out_path

def main():
    parser = argparse.ArgumentParser(description="Collect Solana DEX market pairs by query using DexScreener API.")
    parser.add_argument("--query", required=True, help="Free text query (e.g., token symbol or name).")
    parser.add_argument("--limit", type=int, default=50, help="Max pairs to save (default: 50).")
    args = parser.parse_args()

    out = collect(args.query, args.limit)
    print(f"Saved CSV: {out}")

if __name__ == "__main__":
    main()
