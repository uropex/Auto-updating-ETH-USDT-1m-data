import os, json, time
from datetime import datetime, timezone, timedelta
import requests
import pandas as pd

JST = timezone(timedelta(hours=9))

OUT_DIR = "docs"
CSV_PATH = f"{OUT_DIR}/eth_klines_1m.csv"
META_PATH = f"{OUT_DIR}/eth_meta.json"

UA = {"User-Agent": "Mozilla/5.0 (GitHubActions ETH bot)"}

def fetch_binance_1m(limit=1000):
    """Binance 1m klines を取得（失敗時は例外を投げる）"""
    base = "https://api.binance.com/api/v3/klines"
    url = f"{base}?symbol=ETHUSDT&interval=1m&limit={limit}"
    r = requests.get(url, timeout=25, headers=UA)
    r.raise_for_status()  # ここで451などは例外に
    arr = r.json()
    cols = ["open_time","open","high","low","close","volume","close_time",
            "quote_volume","trades","taker_base","taker_quote","ignore"]
    df = pd.DataFrame(arr, columns=cols)
    # 型変換
    for c in ["open","high","low","close","volume","quote_volume","taker_base","taker_quote"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["open_time"]  = pd.to_datetime(df["open_time"],  unit="ms", utc=True).dt.tz_convert(JST)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True).dt.tz_convert(JST)
    df["source"] = "binance"
    return df

def fetch_coingecko_1m(days=1):
    """
    CoinGecko market_chart (1分粒度相当) から OHLC を再構成。
    prices: [ts_ms, price], total_volumes: [ts_ms, quoteVol]
    """
    base = "https://api.coingecko.com/api/v3/coins/ethereum/market_chart"
    url = f"{base}?vs_currency=usd&days={days}&interval=minute"
    r = requests.get(url, timeout=30, headers=UA)
    r.raise_for_status()
    obj = r.json()

    prices = pd.DataFrame(obj.get("prices", []), columns=["ts_ms","price"])
    vols   = pd.DataFrame(obj.get("total_volumes", []), columns=["ts_ms","quote_volume_cum"])

    if prices.empty:
        raise RuntimeError("CoinGecko returned no data")

    # 1分単位に丸め
    prices["ts"] = pd.to_datetime(prices["ts_ms"], unit="ms", utc=True)
    prices["minute"] = prices["ts"].dt.floor("T")

    # OHLC 再構成
    o = prices.groupby("minute")["price"].first()
    h = prices.groupby("minute")["price"].max()
    l = prices.groupby("minute")["price"].min()
    c = prices.groupby("minute")["price"].last()
    ohlc = pd.concat([o.rename("open"), h.rename("high"), l.rename("low"), c.rename("close")], axis=1).reset_index()

    # 出来高（USDT換算累積→差分）
    if not vols.empty:
        vols["ts"] = pd.to_datetime(vols["ts_ms"], unit="ms", utc=True)
        vols["minute"] = vols["ts"].dt.floor("T")
        v = vols.groupby("minute")["quote_volume_cum"].last().diff().rename("quote_volume")
        ohlc = ohlc.merge(v, on="minute", how="left")
    else:
        ohlc["quote_volume"] = None

    # Binance互換の最低列構成に整形
    df = pd.DataFrame()
    df["open_time"]  = ohlc["minute"].dt.tz_convert(JST)
    df["close_time"] = (ohlc["minute"] + pd.Timedelta(minutes=1) - pd.Timedelta(milliseconds=1)).dt.tz_convert(JST)
    df["open"]  = ohlc["open"]
    df["high"]  = ohlc["high"]
    df["low"]   = ohlc["low"]
    df["close"] = ohlc["close"]
    df["volume"] = None  # ETHボリュームは不明（必要なら近似計算可）
    df["quote_volume"] = ohlc["quote_volume"]
    df["trades"] = None
    df["taker_base"] = None
    df["taker_quote"] = None
    df["source"] = "coingecko"
    # 直近 ~24h 分だけに制限（過剰データ防止）
    df = df.sort_values("open_time").tail(1500).reset_index(drop=True)
    return df

def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    # まず Binance を試す → ダメなら CoinGecko
    try:
        df = fetch_binance_1m(limit=1000)
    except Exception as e:
        print("Binance fetch failed, fallback to CoinGecko:", repr(e))
        df = fetch_coingecko_1m(days=1)

    # 出力
    df_out = df[[
        "open_time","close_time","open","high","low","close",
        "volume","quote_volume","trades","taker_base","taker_quote","source"
    ]]
    df_out.to_csv(CSV_PATH, index=False)

    meta = {
        "rows": len(df_out),
        "generated_at_jst": datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S"),
        "primary": "binance",
        "fallback": "coingecko",
        "used": df_out["source"].iloc[-1],
    }
    with open(META_PATH, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)

    print("✅ CSV updated:", CSV_PATH, "source:", meta["used"])

if __name__ == "__main__":
    main()

