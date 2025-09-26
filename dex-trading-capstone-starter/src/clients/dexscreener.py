from typing import Dict, Any, List, Optional
import httpx
import pandas as pd
from ..config import settings

def _get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = settings.dexscreener_base_url.rstrip('/') + '/' + path.lstrip('/')
    with httpx.Client(timeout=20) as client:
        r = client.get(url, params=params)
        r.raise_for_status()
        return r.json()

def search_pairs_by_query(query: str) -> List[Dict[str, Any]]:
    """Search markets by free-text query (token symbol/name/addr) across chains."""
    data = _get("search", params={"q": query})
    # The API returns a structure with a 'pairs' list.
    return data.get("pairs", []) if isinstance(data, dict) else []

def normalize_pairs_to_df(pairs: List[Dict[str, Any]]) -> pd.DataFrame:
    if not pairs:
        return pd.DataFrame(columns=[
            "chainId","dexId","pairAddress","baseToken","quoteToken","priceUsd",
            "fdv","liquidityUsd","txns_m5_buys","txns_m5_sells","txns_h1_buys","txns_h1_sells",
            "volume_m5","volume_h1","pairCreatedAt"
        ])
    rows = []
    for p in pairs:
        rows.append({
            "chainId": p.get("chainId"),
            "dexId": p.get("dexId"),
            "pairAddress": p.get("pairAddress"),
            "baseToken": p.get("baseToken", {}).get("symbol"),
            "quoteToken": p.get("quoteToken", {}).get("symbol"),
            "priceUsd": p.get("priceUsd"),
            "fdv": p.get("fdv"),
            "liquidityUsd": (p.get("liquidity") or {}).get("usd"),
            "txns_m5_buys": ((p.get("txns") or {}).get("m5") or {}).get("buys"),
            "txns_m5_sells": ((p.get("txns") or {}).get("m5") or {}).get("sells"),
            "txns_h1_buys": ((p.get("txns") or {}).get("h1") or {}).get("buys"),
            "txns_h1_sells": ((p.get("txns") or {}).get("h1") or {}).get("sells"),
            "volume_m5": ((p.get("volume") or {}).get("m5")),
            "volume_h1": ((p.get("volume") or {}).get("h1")),
            "pairCreatedAt": p.get("pairCreatedAt"),
        })
    df = pd.DataFrame(rows)
    # Cast numerics where possible
    for col in ["priceUsd","fdv","liquidityUsd","txns_m5_buys","txns_m5_sells","txns_h1_buys","txns_h1_sells","volume_m5","volume_h1"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df
