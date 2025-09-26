from dataclasses import dataclass
import os
from dotenv import load_dotenv

load_dotenv()

@dataclass
class Settings:
    dexscreener_base_url: str = os.getenv("DEXSCREENER_BASE_URL", "https://api.dexscreener.com/latest/dex")
    output_dir: str = os.getenv("OUTPUT_DIR", "./data/raw")

settings = Settings()
