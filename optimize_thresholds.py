"""
optimize_thresholds.py  (v3 — 8-indicator long-only)
------------------------------------------------------
Grid-search signal thresholds to maximise cumulative strategy return
minus buy-and-hold over trailing 1-year daily OHLCV bars.

Indicators (7 in backtest; live model adds macro vote):
  1. Gap          ±gap_t %
  2. Momentum     ±mom_t %
  3. VWAP proxy   ±vwap_t %
  4. Volume       vol_r × avg  (confirms momentum)
  5. RSI-14       > 50 = +1, < 50 = -1
  6. MA-50        close > MA50 = +1, else -1
  7. Sector ETF   ETF return > 0 = +1, < 0 = -1

VIX gate: if VIX ≥ vix_gate → force HOLD regardless of score.
Strategy: BUY = long next bar, SELL/HOLD = flat (no shorts).
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

SECTOR_ETF = {
    **{t: "XLK" for t in ["AAPL","MSFT","NVDA","INTC","AMD"]},
    **{t: "XLY" for t in ["AMZN","TSLA","HD","COST"]},
    **{t: "XLC" for t in ["GOOGL","META","NFLX"]},
    **{t: "XLF" for t in ["JPM","BAC","V","MA"]},
    **{t: "XLV" for t in ["JNJ","UNH","ABBV","LLY","MRK"]},
    **{t: "XLE" for t in ["XOM","CVX"]},
    **{t: "XLP" for t in ["PG","WMT"]},
}
ALL_ETFS = list(set(SECTOR_ETF.values()))

VOL_WINDOW  = 20
RSI_WINDOW  = 14
MA_WINDOW   = 50
TARGET_DAYS = 252

# ── Parameter grid ────────────────────────────────────────────────────────────
GAP_THRESHOLDS  = [0.1, 0.25, 0.5, 0.75, 1.0]
MOM_THRESHOLDS  = [0.1, 0.2,  0.3, 0.5]
VWAP_THRESHOLDS = [0.1, 0.2,  0.3]
VOL_RATIOS      = [1.0, 1.2,  1.5, 2.0]
BUY_THRESHOLDS  = [1, 2, 3, 4]
VIX_GATES       = [20.0, 25.0, 30.0, None]   # None = no gate

MIN_BUY_SIGNALS = 10
MIN_STOCKS      = 12


def _compute_rsi(close: pd.Series, window: int = RSI_WINDOW) -> pd.Series:
    delta    = close.diff()
    gain     = delta.clip(lower=0)
    loss     = (-delta).clip(lower=0)
    avg_gain = gain.ewm(com=window - 1, min_periods=window).mean()
    avg_loss = loss.ewm(com=window - 1, min_periods=window).mean()
    rs       = avg_gain / avg_loss.replace(0, np.nan)
    return 100.0 - (100.0 / (1.0 + rs))


def download_stocks(tickers):
    print(f"Downloading 2y daily bars for {len(tickers)} tickers…")
    out = {}
    for t in tickers:
        try:
            h = yf.Ticker(t).history(period="2y", interval="1d", auto_adjust=True)
            if h is not None and not h.empty:
                h = h[["Open","High","Low","Close","Volume"]].dropna()
                if len(h) >= max(VOL_WINDOW, MA_WINDOW) + TARGET_DAYS + 2:
                    out[t] = h
                    print(f"  {t}: {len(h)} bars")
                else:
                    print(f"  {t}: too short ({len(h)})")
        except Exception as e:
            print(f"  {t}: FAILED — {e}")
    return out


def download_aux(fetch_period="2y"):
    """Download VIX and sector ETFs."""
    print("Downloading VIX and sector ETFs…")
    aux = {}
    for sym in ["^VIX"] + ALL_ETFS:
        try:
            h = yf.Ticker(sym).history(period=fetch_period, interval="1d", auto_adjust=True)
            if h is not None and not h.empty:
                aux[sym] = h["Close"].dropna()
        except Exception as e:
            print(f"  {sym}: FAILED — {e}")
    return aux


def precompute_features(datasets, aux):
    """Pre-compute all indicator values for each stock's 1-year window."""
    features = {}
    vix_raw = aux.get("^VIX", pd.Series(dtype=float))
    vix_raw.index = vix_raw.index.normalize()

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

        # RSI-14 (shifted 1 for no lookahead)
        rsi_full = _compute_rsi(hist["Close"]).shift(1)
        rsi_w    = rsi_full.values[-(TARGET_DAYS + 1):]

        # MA-50 vs close (shifted 1)
        ma50_full = hist["Close"].rolling(MA_WINDOW).mean().shift(1)
        ma50_w    = ma50_full.values[-(TARGET_DAYS + 1):]
        above_ma50 = c > ma50_w   # bool array; NaN comparisons → False/NaN

        # Sector ETF daily return (not shifted — close-of-day info)
        etf_sym = SECTOR_ETF.get(ticker, "SPY")
        etf_raw = aux.get(etf_sym, pd.Series(dtype=float))
        etf_ret_full = etf_raw.pct_change()
        etf_ret_full.index = etf_ret_full.index.normalize()
        win_dates = window.index.normalize()
        etf_ret_w = etf_ret_full.reindex(win_dates).values

        # VIX level (not shifted — known end-of-day)
        vix_w = vix_raw.reindex(win_dates).values

        # Forward return
        fwd        = np.empty_like(c)
        fwd[:-1]   = c[1:] / c[:-1] - 1
        fwd[-1]    = np.nan

        # Strip warmup row
        features[ticker] = {
            "gap":       gap_pct[1:],
            "mom":       mom_pct[1:],
            "vwap":      vs_vwap[1:],
            "vol_ratio": vol_ratio[1:],
            "rsi":       rsi_w[1:],
            "above_ma50": above_ma50[1:],
            "sector_ret": etf_ret_w[1:],
            "vix":       vix_w[1:],
            "fwd":       fwd[1:],
            "close":     c,
        }
    return features


def eval_params(features, gap_t, mom_t, vwap_t, vol_r, buy_thr, vix_gate):
    sell_thr = -buy_thr
    alphas, n_buy_list = [], []

    for ticker, f in features.items():
        fwd   = f["fwd"]
        valid = ~np.isnan(fwd)

        v_gap  = np.where(f["gap"]  >=  gap_t, 1, np.where(f["gap"]  <= -gap_t,  -1, 0))
        v_mom  = np.where(f["mom"]  >=  mom_t, 1, np.where(f["mom"]  <= -mom_t,  -1, 0))
        v_vwap = np.where(f["vwap"] >= vwap_t, 1, np.where(f["vwap"] <= -vwap_t, -1, 0))
        v_vol  = np.where(~np.isnan(f["vol_ratio"]) & (f["vol_ratio"] >= vol_r), v_mom, 0)

        # RSI
        rsi = f["rsi"]
        v_rsi = np.where(~np.isnan(rsi), np.where(rsi > 50, 1, -1), 0)

        # MA50
        v_ma50 = np.where(f["above_ma50"], 1, -1)

        # Sector ETF
        sr = f["sector_ret"]
        v_sector = np.where(~np.isnan(sr), np.where(sr > 0, 1, np.where(sr < 0, -1, 0)), 0)

        score = v_gap + v_mom + v_vwap + v_vol + v_rsi + v_ma50 + v_sector
        buy_m = (score >= buy_thr) & valid

        # VIX gate: suppress BUY when VIX is elevated
        if vix_gate is not None:
            vix = f["vix"]
            high_fear = ~np.isnan(vix) & (vix >= vix_gate)
            buy_m = buy_m & ~high_fear

        n_buy = int(buy_m.sum())
        if n_buy < MIN_BUY_SIGNALS:
            continue

        strat = np.where(buy_m, fwd, 0.0)
        strat = np.where(np.isnan(strat), 0.0, strat)
        cum_strat = float(np.prod(1 + strat) - 1)

        close = f["close"]
        bh    = float(close[-1] / close[1] - 1)

        alphas.append(cum_strat - bh)
        n_buy_list.append(n_buy)

    if len(alphas) < MIN_STOCKS:
        return None

    return {
        "avg_alpha":    float(np.mean(alphas)),
        "med_alpha":    float(np.median(alphas)),
        "pct_beats_bh": float(np.mean(np.array(alphas) > 0)),
        "avg_n_buy":    float(np.mean(n_buy_list)),
        "n_stocks":     len(alphas),
    }


def main():
    t0 = time.time()

    datasets = download_stocks(TICKERS)
    aux      = download_aux()
    print(f"\nLoaded {len(datasets)} stocks + {len(aux)} aux series in {time.time()-t0:.0f}s\n")

    print("Pre-computing features…")
    features = precompute_features(datasets, aux)

    combos = list(product(
        GAP_THRESHOLDS, MOM_THRESHOLDS, VWAP_THRESHOLDS,
        VOL_RATIOS, BUY_THRESHOLDS, VIX_GATES,
    ))
    print(f"Grid-searching {len(combos)} combinations…\n")

    rows = []
    for i, (gap_t, mom_t, vwap_t, vol_r, buy_thr, vix_gate) in enumerate(combos):
        r = eval_params(features, gap_t, mom_t, vwap_t, vol_r, buy_thr, vix_gate)
        if r:
            rows.append(dict(
                gap_t=gap_t, mom_t=mom_t, vwap_t=vwap_t,
                vol_r=vol_r, buy_thr=buy_thr, sell_thr=-buy_thr,
                vix_gate=str(vix_gate), **r,
            ))
        if (i + 1) % 1000 == 0:
            print(f"  {i+1}/{len(combos)}…")

    if not rows:
        print("No valid parameter sets.")
        return

    df = pd.DataFrame(rows).sort_values("avg_alpha", ascending=False)

    # ── Baseline ──────────────────────────────────────────────────────────────
    baseline = eval_params(features, 0.25, 0.3, 0.2, 1.2, 1, 25.0)
    print("\n" + "="*84)
    print("CURRENT BASELINE  gap=0.25 mom=0.3 vwap=0.2 vol_r=1.2 buy_thr=1 vix_gate=25")
    if baseline:
        for k, v in baseline.items():
            print(f"  {k:<18}: {v:+.4f}" if isinstance(v, float) else f"  {k:<18}: {v}")

    print("\n" + "="*84)
    print("TOP 25 — 8-indicator long-only (sorted by avg alpha vs buy-and-hold)")
    print("="*84)
    cols = ["gap_t","mom_t","vwap_t","vol_r","buy_thr","vix_gate",
            "avg_alpha","med_alpha","pct_beats_bh","avg_n_buy"]
    pd.set_option("display.width", 150)
    pd.set_option("display.float_format", lambda x: f"{x:+.4f}")
    print(df[cols].head(25).to_string(index=False))

    best = df.iloc[0]
    print("\n" + "="*84)
    print("RECOMMENDED PARAMETERS:")
    print(f"  _GAP_PCT_THRESHOLD      = {best['gap_t']}")
    print(f"  _MOMENTUM_PCT_THRESHOLD = {best['mom_t']}")
    print(f"  _VWAP_PCT_THRESHOLD     = {best['vwap_t']}")
    print(f"  _VOL_HIGH_RATIO         = {best['vol_r']}")
    print(f"  _BUY_THRESHOLD          = {int(best['buy_thr'])}")
    print(f"  _SELL_THRESHOLD         = {int(best['sell_thr'])}")
    print(f"  _VIX_GATE               = {best['vix_gate']}")
    print(f"\n  avg alpha vs B&H : {best['avg_alpha']:+.4f}")
    print(f"  pct beats B&H    : {best['pct_beats_bh']:.1%}")
    print(f"  avg BUY days/yr  : {best['avg_n_buy']:.1f}")
    print(f"\nTotal runtime: {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
