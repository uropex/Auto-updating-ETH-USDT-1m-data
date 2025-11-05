import os, json, pandas as pd, requests
from datetime import datetime, timezone, timedelta

JST = timezone(timedelta(hours=9))
BASE = "https://api.binance.com/api/v3/klines"
SYMBOL = "ETHUSDT"
INTERVAL = "1m"
LIMIT = 1000

OUT_DIR = "docs"
CSV_PATH = f"{OUT_DIR}/eth_klines_1m.csv"
META_PATH = f"{OUT_DIR}/eth_meta.json"

def fetch_klines():
    url = f"{BASE}?symbol={SYMBOL}&interval={INTERVAL}&limit={LIMIT}"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    data = r.json()
    cols = ["open_time","open","high","low","close","volume","close_time",
            "quote_volume","trades","taker_base","taker_quote","ignore"]
    df = pd.DataFrame(data, columns=cols)
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True).dt.tz_convert(JST)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True).dt.tz_convert(JST)
    for c in ["open","high","low","close","volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    os.makedirs(OUT_DIR, exist_ok=True)
    df.to_csv(CSV_PATH, index=False)

    meta = {
        "symbol": SYMBOL,
        "interval": INTERVAL,
        "rows": len(df),
        "generated_at_jst": datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S"),
    }
    with open(META_PATH, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)

if __name__ == "__main__":
    fetch_klines()
    print("✅ CSV updated:", CSV_PATH)
