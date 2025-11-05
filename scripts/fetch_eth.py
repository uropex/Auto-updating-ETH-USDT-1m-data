import os, json
from datetime import datetime, timezone, timedelta
import requests
import pandas as pd

JST = timezone(timedelta(hours=9))

OUT_DIR = "docs"
CSV_PATH = f"{OUT_DIR}/eth_klines_1m.csv"
META_PATH = f"{OUT_DIR}/eth_meta.json"

UA = {"User-Agent": "Mozilla/5.0 (GitHubActions ETH bot)"}
CG_KEY = os.getenv("COINGECKO_API_KEY", "").strip()

def fetch_binance_1m(limit=1000):
    base = "https://api.binance.com/api/v3/klines"
    url = f"{base}?symbol=ETHUSDT&interval=1m&limit={limit}"
    r = requests.get(url, timeout=25, headers=UA)
    r.raise_for_status()
    arr = r.json()
    cols = ["open_time","open","high","low","close","volume","close_time",
            "quote_volume","trades","taker_base","taker_quote","ignore"]
    df = pd.DataFrame(arr, columns=cols)
    for c in ["open","high","low","close","volume","quote_volume","taker_base","taker_quote"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["open_time"]  = pd.to_datetime(df["open_time"],  unit="ms", utc=True).dt.tz_convert(JST)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True).dt.tz_convert(JST)
    df["source"] = "binance"
    return df

def fetch_coinbase_1m(limit_minutes=1440):
    """
    Coinbase（鍵不要）: /products/ETH-USD/candles?granularity=60
    返り値: [time, low, high, open, close, volume] の逆時系列
    """
    base = "https://api.exchange.coinbase.com/products/ETH-USD/candles"
    # granularity=60 は 1分 ではなく 60秒=1分 ではなく、Coinbaseは秒単位 → 60=1分
    params = {"granularity": 60}
    r = requests.get(base, params=params, timeout=25, headers=UA)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list) or len(data) == 0:
        raise RuntimeError("Coinbase returned no data")

    df = pd.DataFrame(data, columns=["time","low","high","open","close","volume"])
    # time は UNIX秒（UTC）。Coinbaseは新しい足が下に来るとは限らない→昇順に並べ直し
    df = df.sort_values("time").reset_index(drop=True)
    # 直近 limit_minutes 本だけに絞る（多すぎ防止）
    df = df.tail(limit_minutes)

    # Binance互換に整形
    df["open_time"]  = pd.to_datetime(df["time"], unit="s", utc=True).dt.tz_convert(JST)
    df["close_time"] = (pd.to_datetime(df["time"], unit="s", utc=True) + pd.Timedelta(minutes=1) - pd.Timedelta(milliseconds=1)).dt.tz_convert(JST)
    df.rename(columns={"open":"open","high":"high","low":"low","close":"close","volume":"volume"}, inplace=True)
    # CoinbaseはUSDTではなくUSDだが、短期予測・指標計算には問題なし
    df["quote_volume"] = None
    df["trades"] = None
    df["taker_base"] = None
    df["taker_quote"] = None
    df["source"] = "coinbase"
    return df[["open_time","close_time","open","high","low","close","volume","quote_volume","trades","taker_base","taker_quote","source"]]

def fetch_coingecko_1m(days=1):
    if not CG_KEY:
        raise RuntimeError("COINGECKO_API_KEY not set")
    base = "https://api.coingecko.com/api/v3/coins/ethereum/market_chart"
    url = f"{base}?vs_currency=usd&days={days}&interval=minute"
    hdr = {"x-cg-pro-api-key": CG_KEY, **UA}
    r = requests.get(url, timeout=30, headers=hdr)
    r.raise_for_status()
    obj = r.json()
    prices = pd.DataFrame(obj.get("prices", []), columns=["ts_ms","price"])
    vols   = pd.DataFrame(obj.get("total_volumes", []), columns=["ts_ms","quote_volume_cum"])
    if prices.empty:
        raise RuntimeError("CoinGecko returned no data")

    prices["ts"] = pd.to_datetime(prices["ts_ms"], unit="ms", utc=True)
    prices["minute"] = prices["ts"].dt.floor("T")
    o = prices.groupby("minute")["price"].first()
    h = prices.groupby("minute")["price"].max()
    l = prices.groupby("minute")["price"].min()
    c = prices.groupby("minute")["price"].last()
    ohlc = pd.concat([o.rename("open"), h.rename("high"), l.rename("low"), c.rename("close")], axis=1).reset_index()

    if not vols.empty:
        vols["ts"] = pd.to_datetime(vols["ts_ms"], unit="ms", utc=True)
        vols["minute"] = vols["ts"].dt.floor("T")
        v = vols.groupby("minute")["quote_volume_cum"].last().diff().rename("quote_volume")
        ohlc = ohlc.merge(v, on="minute", how="left")
    else:
        ohlc["quote_volume"] = None

    df = pd.DataFrame()
    df["open_time"]  = ohlc["minute"].dt.tz_convert(JST)
    df["close_time"] = (ohlc["minute"] + pd.Timedelta(minutes=1) - pd.Timedelta(milliseconds=1)).dt.tz_convert(JST)
    df["open"]  = ohlc["open"]; df["high"]=ohlc["high"]; df["low"]=ohlc["low"]; df["close"]=ohlc["close"]
    df["volume"] = None
    df["quote_volume"] = ohlc["quote_volume"]
    df["trades"] = None; df["taker_base"]=None; df["taker_quote"]=None
    df["source"] = "coingecko"
    df = df.sort_values("open_time").tail(1500).reset_index(drop=True)
    return df[["open_time","close_time","open","high","low","close","volume","quote_volume","trades","taker_base","taker_quote","source"]]

def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    used = None
    try:
        df = fetch_binance_1m(limit=1000); used = "binance"
    except Exception as e:
        print("Binance failed →", repr(e))
        try:
            df = fetch_coinbase_1m(limit_minutes=1440); used = "coinbase"
        except Exception as e2:
            print("Coinbase failed →", repr(e2))
            df = fetch_coingecko_1m(days=1); used = "coingecko"

    df.to_csv(CSV_PATH, index=False)
    meta = {
        "rows": len(df),
        "generated_at_jst": datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S"),
        "order": ["binance","coinbase","coingecko"],
        "used": used
    }
    with open(META_PATH, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)
    print("✅ CSV updated:", CSV_PATH, "source:", used)

if __name__ == "__main__":
    main()
