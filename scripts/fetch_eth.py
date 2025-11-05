import os, json
from datetime import datetime, timezone, timedelta
import requests
import pandas as pd

JST = timezone(timedelta(hours=9))

OUT_DIR = "docs"
CSV_PATH = f"{OUT_DIR}/eth_klines_1m.csv"
META_PATH = f"{OUT_DIR}/eth_meta.json"

UA = {"User-Agent": "Mozilla/5.0 (GitHubActions ETH bot)"}

def fetch_binance_1m(limit=1000):
    base = "https://api.binance.com/api/v3/klines"
    url = f"{base}?symbol=ETHUSDT&interval=1m&limit={limit}"
    r = requests.get(url, timeout=25, headers=UA)
    r.raise_for_status()
    arr = r.json()
    cols = ["open_time","open","high","low","close","volume","close_time",
            "quote_volume","trades","taker_base","taker_quote","ignore"]
    df = pd.DataFrame(arr, columns=cols)
    for c in ["open","high","low","close","volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["open_time"]  = pd.to_datetime(df["open_time"],  unit="ms", utc=True).dt.tz_convert(JST)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True).dt.tz_convert(JST)
    df["source"] = "binance"
    return df

def fetch_coinbase_1m():
    """Coinbase API（鍵不要）から直近の1分足相当データを取得"""
    url = "https://api.exchange.coinbase.com/products/ETH-USD/candles?granularity=60"
    r = requests.get(url, timeout=25, headers=UA)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list) or len(data) == 0:
        raise RuntimeError("Coinbase returned no data")

    # time, low, high, open, close, volume（逆順）
    df = pd.DataFrame(data, columns=["time","low","high","open","close","volume"])
    df = df.sort_values("time").reset_index(drop=True)

    df["open_time"]  = pd.to_datetime(df["time"], unit="s", utc=True).dt.tz_convert(JST)
    df["close_time"] = (pd.to_datetime(df["time"], unit="s", utc=True) + pd.Timedelta(minutes=1) - pd.Timedelta(milliseconds=1)).dt.tz_convert(JST)

    df = df[["open_time","close_time","open","high","low","close","volume"]]
    df["quote_volume"] = None
    df["trades"] = None
    df["taker_base"] = None
    df["taker_quote"] = None
    df["source"] = "coinbase"
    return df

def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    try:
        df = fetch_binance_1m(limit=1000)
        used = "binance"
    except Exception as e:
        print("Binance fetch failed, fallback to Coinbase:", repr(e))
        df = fetch_coinbase_1m()
        used = "coinbase"

    df.to_csv(CSV_PATH, index=False)
    meta = {
        "rows": len(df),
        "generated_at_jst": datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S"),
        "source": used,
    }
    with open(META_PATH, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)
    print("✅ CSV updated:", CSV_PATH, "source:", used)

if __name__ == "__main__":
    main()
