"""
optimize_thresholds.py
Grid-search signal thresholds to maximize cumulative strategy return
minus buy-and-hold over the trailing 1-year daily OHLCV bars.

Indicator structure (mirrors backtest.py):
  Gap      : (Open - prev_Close) / prev_Close * 100
  Momentum : (Close - Open) / Open * 100
  VWAP     : (Close - typical) / typical * 100  [typical = (H+L+C)/3]
  Volume   : if vol >= avg_vol * ratio -> inherits momentum vote, else 0
  Score    : sum of four votes  (range -4 to +4)
  BUY  if score >= buy_thr
  SELL if score <= -buy_thr
"""

import warnings
warnings.filterwarnings("ignore")

import sys
import time
from itertools import product

import numpy as np
import pandas as pd
import yfinance as yf

# ── Representative S&P 500 sample ─────────────────────────────────────────────
TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA",
    "JPM",  "JNJ",  "XOM",  "BAC",  "UNH",  "V",    "HD",
    "PG",   "MA",   "ABBV", "CVX",  "LLY",  "MRK",
    "COST", "WMT",  "NFLX", "INTC", "AMD",
]

VOL_WINDOW   = 20
TARGET_DAYS  = 252   # 1-year window

# ── Parameter grid ────────────────────────────────────────────────────────────
GAP_THRESHOLDS  = [0.25, 0.5, 0.75, 1.0, 1.5, 2.0]
MOM_THRESHOLDS  = [0.1,  0.2, 0.3,  0.5, 0.75]
VWAP_THRESHOLDS = [0.05, 0.1, 0.15, 0.2]
VOL_RATIOS      = [1.0,  1.2, 1.5,  2.0, 2.5]
BUY_THRESHOLDS  = [1, 2, 3]

MIN_SIGNALS_PER_STOCK = 5
MIN_STOCKS_REQUIRED   = 12


def download_stocks(tickers: list) -> dict:
    print(f"Downloading 2-year daily bars for {len(tickers)} tickers...")
    out = {}
    for t in tickers:
        try:
            h = yf.Ticker(t).history(period="2y", interval="1d", auto_adjust=True)
            if h is not None and not h.empty:
                h = h[["Open", "High", "Low", "Close", "Volume"]].dropna()
                if len(h) >= VOL_WINDOW + TARGET_DAYS + 2:
                    out[t] = h
                    print(f"  {t}: {len(h)} bars OK")
                else:
                    print(f"  {t}: too short ({len(h)} bars)")
        except Exception as e:
            print(f"  {t}: FAILED — {e}")
    return out


def precompute_features(datasets: dict) -> dict:
    """
    For each stock extract the 1-year target window and pre-compute
    indicator values as numpy arrays. These don't depend on thresholds
    so we compute them once and reuse across all grid iterations.
    """
    features = {}
    for ticker, hist in datasets.items():
        window = hist.iloc[-(TARGET_DAYS + 1):]   # +1 row provides prev_close for row 1

        o = window["Open"].values
        h = window["High"].values
        l = window["Low"].values
        c = window["Close"].values
        v = window["Volume"].values

        prev_c = np.empty_like(c)
        prev_c[0] = np.nan
        prev_c[1:] = c[:-1]

        gap_pct  = (o - prev_c) / prev_c * 100
        mom_pct  = (c - o) / o * 100
        typical  = (h + l + c) / 3
        vs_vwap  = (c - typical) / typical * 100

        avg_vol_full = (
            pd.Series(hist["Volume"].values)
            .rolling(VOL_WINDOW)
            .mean()
            .shift(1)
            .values
        )
        avg_vol_window = avg_vol_full[-(TARGET_DAYS + 1):]
        vol_ratio = np.where(avg_vol_window > 0, v / avg_vol_window, np.nan)

        fwd = np.empty_like(c)
        fwd[:-1] = c[1:] / c[:-1] - 1
        fwd[-1]  = np.nan

        # Drop warmup row (index 0) — it only exists to supply prev_close
        features[ticker] = {
            "gap":       gap_pct[1:],
            "mom":       mom_pct[1:],
            "vwap":      vs_vwap[1:],
            "vol_ratio": vol_ratio[1:],
            "fwd":       fwd[1:],
            "close":     c,   # full window including warmup row for B&H
        }
    return features


def eval_params(features, gap_t, mom_t, vwap_t, vol_r, buy_thr):
    sell_thr = -buy_thr
    alphas, accuracies, sig_counts = [], [], []

    for ticker, f in features.items():
        fwd       = f["fwd"]
        valid     = ~np.isnan(fwd) & ~np.isnan(f["vol_ratio"])

        v_gap  = np.where(f["gap"]  >= gap_t,  1, np.where(f["gap"]  <= -gap_t,  -1, 0))
        v_mom  = np.where(f["mom"]  >= mom_t,  1, np.where(f["mom"]  <= -mom_t,  -1, 0))
        v_vwap = np.where(f["vwap"] >= vwap_t, 1, np.where(f["vwap"] <= -vwap_t, -1, 0))
        v_vol  = np.where(f["vol_ratio"] >= vol_r, v_mom, 0)

        score = v_gap + v_mom + v_vwap + v_vol

        buy_m  = (score >= buy_thr)  & valid
        sell_m = (score <= sell_thr) & valid
        n_sigs = int(buy_m.sum() + sell_m.sum())
        if n_sigs < MIN_SIGNALS_PER_STOCK:
            continue

        strat = np.where(buy_m,  fwd,
                np.where(sell_m, -fwd, 0.0))
        strat = np.where(np.isnan(strat), 0.0, strat)
        cum_strat = float(np.prod(1 + strat) - 1)

        close = f["close"]
        bh    = float(close[-1] / close[1] - 1)   # skip warmup row

        alphas.append(cum_strat - bh)
        sig_counts.append(n_sigs)

        n_buy  = int(buy_m.sum());  n_sell = int(sell_m.sum())
        bh_    = np.mean(fwd[buy_m]  > 0) if n_buy  > 0 else 0.0
        sh_    = np.mean(fwd[sell_m] < 0) if n_sell > 0 else 0.0
        if n_buy > 0 and n_sell > 0:
            accuracies.append((bh_ + sh_) / 2)
        elif n_buy > 0:
            accuracies.append(bh_)
        else:
            accuracies.append(sh_)

    if len(alphas) < MIN_STOCKS_REQUIRED:
        return None

    return {
        "avg_alpha":      float(np.mean(alphas)),
        "med_alpha":      float(np.median(alphas)),
        "pct_beats_bh":   float(np.mean(np.array(alphas) > 0)),
        "avg_accuracy":   float(np.mean(accuracies)),
        "avg_n_signals":  float(np.mean(sig_counts)),
        "n_stocks":       len(alphas),
    }


def main():
    t0 = time.time()

    datasets = download_stocks(TICKERS)
    print(f"\nLoaded {len(datasets)} stocks in {time.time()-t0:.0f}s\n")

    print("Pre-computing indicator features…")
    features = precompute_features(datasets)

    combos = list(product(GAP_THRESHOLDS, MOM_THRESHOLDS, VWAP_THRESHOLDS,
                          VOL_RATIOS, BUY_THRESHOLDS))
    total = len(combos)
    print(f"Grid-searching {total} parameter combinations…\n")

    rows = []
    for i, (gap_t, mom_t, vwap_t, vol_r, buy_thr) in enumerate(combos):
        r = eval_params(features, gap_t, mom_t, vwap_t, vol_r, buy_thr)
        if r:
            rows.append(dict(gap_t=gap_t, mom_t=mom_t, vwap_t=vwap_t,
                             vol_r=vol_r, buy_thr=buy_thr, sell_thr=-buy_thr, **r))
        if (i + 1) % 200 == 0:
            print(f"  {i+1}/{total} done…")

    if not rows:
        print("No valid parameter sets found.")
        return

    df = pd.DataFrame(rows).sort_values("avg_alpha", ascending=False)

    # ── Baseline (current production values) ─────────────────────────────────
    baseline = eval_params(features, 1.0, 0.3, 0.1, 1.5, 2)
    print("\n" + "="*72)
    print("CURRENT (baseline)  gap=1.0  mom=0.3  vwap=0.1  vol_r=1.5  buy_thr=2")
    if baseline:
        print(f"  avg_alpha    : {baseline['avg_alpha']:+.4f}")
        print(f"  med_alpha    : {baseline['med_alpha']:+.4f}")
        print(f"  pct_beats_bh : {baseline['pct_beats_bh']:.1%}")
        print(f"  avg_accuracy : {baseline['avg_accuracy']:.1%}")
        print(f"  avg_n_signals: {baseline['avg_n_signals']:.1f}")

    print("\n" + "="*72)
    print("TOP 20 parameter sets  (sorted by avg strategy alpha vs buy-and-hold)")
    print("="*72)
    cols = ["gap_t","mom_t","vwap_t","vol_r","buy_thr",
            "avg_alpha","med_alpha","pct_beats_bh","avg_accuracy","avg_n_signals"]
    pd.set_option("display.width", 120)
    pd.set_option("display.float_format", lambda x: f"{x:+.4f}" if isinstance(x, float) else str(x))
    print(df[cols].head(20).to_string(index=False))

    best = df.iloc[0]
    print("\n" + "="*72)
    print("RECOMMENDED PARAMETERS:")
    print(f"  _GAP_PCT_THRESHOLD      = {best['gap_t']}")
    print(f"  _MOMENTUM_PCT_THRESHOLD = {best['mom_t']}")
    print(f"  _VWAP_PCT_THRESHOLD     = {best['vwap_t']}")
    print(f"  _VOL_HIGH_RATIO         = {best['vol_r']}")
    print(f"  _BUY_THRESHOLD          = {int(best['buy_thr'])}")
    print(f"  _SELL_THRESHOLD         = {int(best['sell_thr'])}")
    print(f"\n  avg alpha vs B&H : {best['avg_alpha']:+.4f}")
    print(f"  pct beats B&H    : {best['pct_beats_bh']:.1%}")
    print(f"  avg accuracy     : {best['avg_accuracy']:.1%}")
    print(f"  avg signals/yr   : {best['avg_n_signals']:.1f}")
    print(f"\nTotal runtime: {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
