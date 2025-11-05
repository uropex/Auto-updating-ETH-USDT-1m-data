# scripts/analyze_and_publish.py
import json, math, os
from datetime import datetime, timezone, timedelta
import pandas as pd

JST = timezone(timedelta(hours=9))
CSV_PATH  = "docs/eth_klines_1m.csv"
OUT_JSON  = "docs/eth_summary.json"
OUT_HTML  = "docs/index.html"   # 既存index.htmlを上書き（見やすいダッシュボード）

def ema(s, span): return s.ewm(span=span, adjust=False).mean()

def rsi(close, period=14):
    diff = close.diff()
    up = diff.clip(lower=0)
    down = -diff.clip(upper=0)
    rs = up.rolling(period).mean() / down.rolling(period).mean()
    return 100 - (100 / (1 + rs))

def macd(close, fast=12, slow=26, signal=9):
    efast, eslow = ema(close, fast), ema(close, slow)
    line = efast - eslow
    sig  = ema(line, signal)
    hist = line - sig
    return line, sig, hist

def atr(df, period=14):
    h, l, c = df["high"], df["low"], df["close"]
    prev_c = c.shift(1)
    tr = pd.concat([(h-l).abs(), (h-prev_c).abs(), (l-prev_c).abs()], axis=1).max(axis=1)
    return tr.rolling(period).mean()

def drift_vol(close, span=60):
    ret = (close).pct_change().replace([float("inf"), float("-inf")], float("nan"))
    mu = ret.ewm(span=span, adjust=False).mean().iloc[-1]
    var = (ret**2).ewm(span=span, adjust=False).mean().iloc[-1] - mu**2
    sigma = max(var, 0)**0.5
    return float(mu), float(sigma)

def project_price(p0, mu, sigma, n):
    # 幾何BM近似（離散）：log(価格)の平均と分散で1σ帯
    import math
    mean_log = math.log(p0) + n * math.log(1.0 + mu)
    stdev = sigma * (n ** 0.5)
    mid = math.exp(mean_log)
    lo  = math.exp(mean_log - stdev)
    hi  = math.exp(mean_log + stdev)
    return float(mid), float(lo), float(hi)

def main():
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(CSV_PATH)
    df = pd.read_csv(CSV_PATH)

    # 列名の正規化
    # 期待列: open_time, close_time, open, high, low, close
    for col in ["open","high","low","close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    # タイムスタンプ
    if "close_time" in df.columns:
        try:
            ts = pd.to_datetime(df["close_time"]).iloc[-1]
        except Exception:
            ts = pd.Timestamp.now(tz=JST)
    else:
        ts = pd.Timestamp.now(tz=JST)

    # 指標（1m足）
    df["EMA9"]  = ema(df["close"], 9)
    df["EMA20"] = ema(df["close"], 20)
    df["EMA50"] = ema(df["close"], 50)
    df["RSI14"] = rsi(df["close"], 14)
    macd_line, sig_line, hist = macd(df["close"])
    df["MACD"] = macd_line; df["MACDsig"] = sig_line; df["MACDhist"] = hist
    df["ATR14"] = atr(df, 14)

    price = float(df["close"].iloc[-1])
    ema9, ema20, ema50 = map(lambda x: float(df[x].iloc[-1]), ["EMA9","EMA20","EMA50"])
    rsi14 = float(df["RSI14"].iloc[-1])
    macd_v = float(df["MACD"].iloc[-1]); macd_s = float(df["MACDsig"].iloc[-1]); macd_h = float(df["MACDhist"].iloc[-1])
    atr14 = float(df["ATR14"].iloc[-1])

    # ドリフト＆ボラ（1m）
    mu1, s1 = drift_vol(df["close"], span=60)
    # 15m / 1h は簡易的にウィンドウ長で近似（必要十分）
    mu15, s15 = drift_vol(df["close"].rolling(15).mean().dropna(), span=40) if len(df)>=60 else (mu1, s1)
    mu60, s60 = drift_vol(df["close"].rolling(60).mean().dropna(), span=30) if len(df)>=240 else (mu1, s1)

    horizons = [
        ("1m", 1,  mu1, s1),
        ("5m", 5,  mu1, s1),
        ("15m",15, mu1, s1),
        ("30m",30, mu1, s1),
        ("1h", 60//15*1, mu15, s15),   # 4本分相当
        ("12h",12, mu60, s60),         # 12本の1h
    ]

    fcst = []
    for label, n, mu, sigma in horizons:
        mid, lo, hi = project_price(price, mu, sigma, n)
        fcst.append({
            "horizon": label,
            "point": round(mid, 2),
            "ci_low": round(lo, 2),
            "ci_high": round(hi, 2)
        })

    summary = {
        "timestamp_jst": pd.to_datetime(ts).tz_convert(JST).strftime("%Y-%m-%d %H:%M:%S"),
        "price": round(price, 2),
        "ema": {"ema9": round(ema9,2), "ema20": round(ema20,2), "ema50": round(ema50,2)},
        "rsi14": round(rsi14, 2),
        "macd": {"line": round(macd_v,2), "signal": round(macd_s,2), "hist": round(macd_h,2)},
        "atr14": round(atr14, 2),
        "drift_sigma": {
            "m1":  {"mu": round(mu1,6),  "sigma": round(s1,6)},
            "m15": {"mu": round(mu15,6), "sigma": round(s15,6)},
            "h1":  {"mu": round(mu60,6), "sigma": round(s60,6)}
        },
        "forecast": fcst,
    }

    # JSON出力
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    # シンプルなダッシュボードHTML（Pagesプレビュー用）
    html = f"""<!doctype html><meta charset="utf-8">
<title>ETH Auto Summary</title>
<h1>ETH/USDT 1m Summary</h1>
<p>Updated (JST): {summary["timestamp_jst"]}</p>
<ul>
  <li>Price: {summary["price"]}</li>
  <li>EMA9/20/50: {summary["ema"]["ema9"]} / {summary["ema"]["ema20"]} / {summary["ema"]["ema50"]}</li>
  <li>RSI14: {summary["rsi14"]}</li>
  <li>MACD: {summary["macd"]["line"]} (Signal {summary["macd"]["signal"]}, Hist {summary["macd"]["hist"]})</li>
  <li>ATR14: {summary["atr14"]}</li>
</ul>
<h2>Forecast</h2>
<pre>{json.dumps(summary["forecast"], ensure_ascii=False, indent=2)}</pre>
<p><a href="./eth_klines_1m.csv">eth_klines_1m.csv</a> / <a href="./eth_summary.json">eth_summary.json</a></p>
"""
    with open(OUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)

    import shutil
shutil.copyfile(OUT_JSON, "eth_summary.json")


    print("✅ Wrote:", OUT_JSON, "and", OUT_HTML)

if __name__ == "__main__":
    main()
