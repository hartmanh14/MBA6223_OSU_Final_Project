"""
optimize_thresholds.py  (v2 — long-only strategy)
---------------------------------------------------
Grid-search signal thresholds to maximise cumulative strategy return
minus buy-and-hold over trailing 1-year daily OHLCV bars.

Strategy: BUY = long next day, SELL/HOLD = flat (no short positions).
Objective: avg(strategy_return - buy_and_hold) across sample stocks.
"""

import warnings
warnings.filterwarnings("ignore")

import time
from itertools import product

import numpy as np
import pandas as pd
import yfinance as yf

# ── Diversified S&P 500 sample ────────────────────────────────────────────────
TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA",
    "JPM",  "JNJ",  "XOM",  "BAC",  "UNH",  "V",    "HD",
    "PG",   "MA",   "ABBV", "CVX",  "LLY",  "MRK",
    "COST", "WMT",  "NFLX", "INTC", "AMD",
]

VOL_WINDOW  = 20
TARGET_DAYS = 252

# ── Expanded parameter grid ───────────────────────────────────────────────────
GAP_THRESHOLDS  = [0.25, 0.5, 0.75, 1.0, 1.5, 2.0]
MOM_THRESHOLDS  = [0.1,  0.15, 0.2, 0.3, 0.5]
VWAP_THRESHOLDS = [0.05, 0.1,  0.15, 0.2, 0.3]
VOL_RATIOS      = [1.0,  1.2,  1.5,  2.0, 2.5]
BUY_THRESHOLDS  = [1, 2, 3]

MIN_BUY_SIGNALS = 10   # at least 10 BUY days per stock
MIN_STOCKS      = 12   # at least 12 stocks must qualify


def download_stocks(tickers):
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


def precompute_features(datasets):
    """Pre-compute per-bar indicator values once; thresholds applied in the loop."""
    features = {}
    for ticker, hist in datasets.items():
        window = hist.iloc[-(TARGET_DAYS + 1):]

        o  = window["Open"].values
        h  = window["High"].values
        l  = window["Low"].values
        c  = window["Close"].values
        v  = window["Volume"].values

        prev_c      = np.empty_like(c); prev_c[0] = np.nan; prev_c[1:] = c[:-1]
        gap_pct     = (o - prev_c) / prev_c * 100
        mom_pct     = (c - o) / o * 100
        typical     = (h + l + c) / 3
        vs_vwap     = (c - typical) / typical * 100

        avg_vol_full = (
            pd.Series(hist["Volume"].values)
            .rolling(VOL_WINDOW).mean().shift(1).values
        )
        avg_vol_w = avg_vol_full[-(TARGET_DAYS + 1):]
        vol_ratio = np.where(avg_vol_w > 0, v / avg_vol_w, np.nan)

        fwd        = np.empty_like(c)
        fwd[:-1]   = c[1:] / c[:-1] - 1
        fwd[-1]    = np.nan

        # Strip warmup row (index 0 is prev_close donor only)
        features[ticker] = {
            "gap":       gap_pct[1:],
            "mom":       mom_pct[1:],
            "vwap":      vs_vwap[1:],
            "vol_ratio": vol_ratio[1:],
            "fwd":       fwd[1:],
            "close":     c,
        }
    return features


def eval_params(features, gap_t, mom_t, vwap_t, vol_r, buy_thr):
    sell_thr = -buy_thr
    alphas, avg_buy_rets, n_buy_list = [], [], []

    for ticker, f in features.items():
        fwd   = f["fwd"]
        valid = ~np.isnan(fwd) & ~np.isnan(f["vol_ratio"])

        v_gap  = np.where(f["gap"]  >=  gap_t, 1, np.where(f["gap"]  <= -gap_t,  -1, 0))
        v_mom  = np.where(f["mom"]  >=  mom_t, 1, np.where(f["mom"]  <= -mom_t,  -1, 0))
        v_vwap = np.where(f["vwap"] >= vwap_t, 1, np.where(f["vwap"] <= -vwap_t, -1, 0))
        v_vol  = np.where(f["vol_ratio"] >= vol_r, v_mom, 0)

        score  = v_gap + v_mom + v_vwap + v_vol
        buy_m  = (score >= buy_thr) & valid
        n_buy  = int(buy_m.sum())

        if n_buy < MIN_BUY_SIGNALS:
            continue

        # Long-only: BUY = long, everything else = flat
        strat = np.where(buy_m, fwd, 0.0)
        strat = np.where(np.isnan(strat), 0.0, strat)
        cum_strat = float(np.prod(1 + strat) - 1)

        close = f["close"]
        bh    = float(close[-1] / close[1] - 1)

        alphas.append(cum_strat - bh)
        avg_buy_rets.append(float(np.mean(fwd[buy_m])))
        n_buy_list.append(n_buy)

    if len(alphas) < MIN_STOCKS:
        return None

    return {
        "avg_alpha":     float(np.mean(alphas)),
        "med_alpha":     float(np.median(alphas)),
        "pct_beats_bh":  float(np.mean(np.array(alphas) > 0)),
        "avg_buy_ret":   float(np.mean(avg_buy_rets)),
        "avg_n_buy":     float(np.mean(n_buy_list)),
        "n_stocks":      len(alphas),
    }


def main():
    t0 = time.time()

    datasets = download_stocks(TICKERS)
    print(f"\nLoaded {len(datasets)} stocks in {time.time()-t0:.0f}s\n")

    print("Pre-computing indicator features…")
    features = precompute_features(datasets)

    combos = list(product(
        GAP_THRESHOLDS, MOM_THRESHOLDS, VWAP_THRESHOLDS,
        VOL_RATIOS, BUY_THRESHOLDS,
    ))
    print(f"Grid-searching {len(combos)} parameter combinations…\n")

    rows = []
    for i, (gap_t, mom_t, vwap_t, vol_r, buy_thr) in enumerate(combos):
        r = eval_params(features, gap_t, mom_t, vwap_t, vol_r, buy_thr)
        if r:
            rows.append(dict(
                gap_t=gap_t, mom_t=mom_t, vwap_t=vwap_t,
                vol_r=vol_r, buy_thr=buy_thr, sell_thr=-buy_thr,
                **r,
            ))
        if (i + 1) % 500 == 0:
            print(f"  {i+1}/{len(combos)} done…")

    if not rows:
        print("No valid parameter sets found.")
        return

    df = pd.DataFrame(rows).sort_values("avg_alpha", ascending=False)

    # ── Baseline ──────────────────────────────────────────────────────────────
    baseline = eval_params(features, 1.0, 0.3, 0.2, 1.2, 2)
    print("\n" + "="*80)
    print("CURRENT (post-v1 optimisation)  gap=1.0 mom=0.3 vwap=0.2 vol_r=1.2 buy_thr=2")
    if baseline:
        for k, v in baseline.items():
            print(f"  {k:<18}: {v:+.4f}" if isinstance(v, float) else f"  {k:<18}: {v}")

    print("\n" + "="*80)
    print("TOP 25 — long-only strategy (sorted by avg alpha vs buy-and-hold)")
    print("="*80)
    cols = ["gap_t","mom_t","vwap_t","vol_r","buy_thr",
            "avg_alpha","med_alpha","pct_beats_bh","avg_buy_ret","avg_n_buy"]
    pd.set_option("display.width", 140)
    pd.set_option("display.float_format", lambda x: f"{x:+.4f}")
    print(df[cols].head(25).to_string(index=False))

    best = df.iloc[0]
    print("\n" + "="*80)
    print("RECOMMENDED PARAMETERS (long-only strategy):")
    print(f"  _GAP_PCT_THRESHOLD      = {best['gap_t']}")
    print(f"  _MOMENTUM_PCT_THRESHOLD = {best['mom_t']}")
    print(f"  _VWAP_PCT_THRESHOLD     = {best['vwap_t']}")
    print(f"  _VOL_HIGH_RATIO         = {best['vol_r']}")
    print(f"  _BUY_THRESHOLD          = {int(best['buy_thr'])}")
    print(f"  _SELL_THRESHOLD         = {int(best['sell_thr'])}")
    print(f"\n  avg alpha vs B&H : {best['avg_alpha']:+.4f}")
    print(f"  pct beats B&H    : {best['pct_beats_bh']:.1%}")
    print(f"  avg BUY return   : {best['avg_buy_ret']:+.5f}  (per day)")
    print(f"  avg BUY days/yr  : {best['avg_n_buy']:.1f}")
    print(f"\nTotal runtime: {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
