# Capstone Project: Automated Trading on DEXs (Solana)

This repository is the starter implementation for the capstone project **Analyzing the Impact of Automated Trading on Decentralized Exchanges**.

## What’s included
- Minimal **data collector** for DexScreener API (Solana pairs)
- Simple **normalization** to CSV
- Config management via `.env`
- Basic project structure to grow into analysis & visualization

## Quickstart
1. Create and populate a `.env` file.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Collect sample data for a query (e.g., BONK on Solana):
   ```bash
   python -m src.collect --query "bonk" --limit 50
   ```
4. Output CSV will appear in `data/raw/`.

## Roadmap
- Enrich collector with transaction-level feeds (on-chain, solana-py)
- Heuristic-based bot labeling
- Statistical analysis (slippage, spreads, liquidity depth)
- Jupyter dashboards/notebooks
