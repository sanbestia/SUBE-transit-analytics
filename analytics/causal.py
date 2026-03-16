"""
analytics/causal.py — Interrupted Time Series (ITS) causal impact of fare shocks.

Method: segmented OLS regression with level change (β₂) and slope change (β₃)
at the treatment date, seasonal month-of-year dummies, and a COVID-19 regressor.

  y_t = β₀ + β₁·t + β₂·D_t + β₃·t_post_t + Σγ_m·Month_m + δ·COVID_t + ε_t

  D_t      = 1 for months ≥ treatment date, else 0  (level shift)
  t_post_t = months elapsed since treatment (0 before; 0 at treatment month,
             1, 2, … after)  (slope change)

Treatment: January 2024 — onset of the Milei fare shock.
  Jan 15, 2024: +45% | Feb 6, 2024: +66% | cumulative Jan–Feb: ~141%

Standard errors:
  - COLECTIVO: OLS (Ljung-Box p=0.136 — no autocorrelation detected)
  - TREN, SUBTE: HAC Newey-West, maxlags=12 (autocorrelation detected)

Reference: World Bank developing-country transit elasticity −0.12
  (−1.2% ridership per 10% fare increase)

Limitation: β₂ conflates the fare price effect with the broader Dec 2023
devaluation (+118%) and real-income collapse. The implied elasticity is an
upper bound on the pure price elasticity of demand.
"""

import sys
from pathlib import Path

import pandas as pd
import numpy as np
from loguru import logger

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DASHBOARD_MODES, FARE_HIKES
from analytics.ml import _load_monthly_mode


# ── Constants ──────────────────────────────────────────────────────────────────

TREATMENT_DATE  = pd.Timestamp("2024-01-01")
WB_ELASTICITY   = -0.12   # World Bank developing-country benchmark

# Jan+Feb 2024 combined shock used as the denominator for elasticity
# (the two steps that landed before any subsequent policy adjustment)
SHOCK_WINDOW_END = pd.Timestamp("2024-02-28")

# Modes where residual autocorrelation was detected in diagnostics.py
HAC_MODES    = {"TREN", "SUBTE"}
HAC_MAXLAGS  = 12


# ── Fare helpers ───────────────────────────────────────────────────────────────

def _cumulative_hike_pct(
    from_date: pd.Timestamp,
    to_date:   pd.Timestamp,
    scopes:    set[str] = frozenset({"national", "amba", "amba_local"}),
) -> float:
    """
    Compound cumulative fare increase strictly between from_date and to_date.
    Returns a percentage (e.g. 141.0 for +141%).
    """
    compound = 1.0
    for h in FARE_HIKES:
        d = pd.Timestamp(h["date"])
        if from_date < d <= to_date and h["scope"] in scopes and h["magnitude"] > 0:
            compound *= (1 + h["magnitude"] / 100)
    return (compound - 1) * 100


# ── Feature construction ───────────────────────────────────────────────────────

def _build_its_features(
    df: pd.DataFrame,
    treatment_date: pd.Timestamp,
) -> pd.DataFrame:
    """
    Append ITS regression columns to a (ds, y) DataFrame:
      t        : integer time index from 0
      D        : post-treatment indicator
      t_post   : months elapsed since treatment (0 at treatment month, 1, 2, …)
      covid    : 1 for Mar 2020 – Dec 2021
      m_2..m_12: month-of-year dummies (January = reference)
    """
    df = df.sort_values("ds").reset_index(drop=True)
    df["t"] = np.arange(len(df))

    df["D"] = (df["ds"] >= treatment_date).astype(int)

    T_idx = int(df.loc[df["ds"] >= treatment_date, "t"].iloc[0]) if df["D"].any() else len(df)
    # t_post = 0 at treatment month; 1, 2, … in subsequent months
    df["t_post"] = ((df["t"] - T_idx).clip(lower=0) * df["D"]).astype(int)

    df["covid"] = (
        (df["ds"] >= pd.Timestamp("2020-03-01")) &
        (df["ds"] <= pd.Timestamp("2021-12-01"))
    ).astype(int)

    month_dummies = pd.get_dummies(df["ds"].dt.month, prefix="m").astype(int)
    month_dummies.drop(columns=["m_1"], errors="ignore", inplace=True)

    return pd.concat([df.reset_index(drop=True), month_dummies], axis=1)


# ── Counterfactual ─────────────────────────────────────────────────────────────

def _counterfactual(
    df: pd.DataFrame,
    result,
) -> pd.Series:
    """
    Project the pre-treatment trend into the post-treatment window.
    D=0 and t_post=0 for all rows — gives what ridership would have been
    without the fare shock (holding seasonality and COVID constant).
    """
    import statsmodels.api as sm

    cf = df.copy()
    cf["D"]      = 0
    cf["t_post"] = 0
    feature_cols = (
        ["t", "D", "t_post", "covid"]
        + [c for c in df.columns if c.startswith("m_")]
    )
    X_cf = sm.add_constant(cf[feature_cols].astype(float), has_constant="add")
    return pd.Series(result.predict(X_cf), index=df.index)


# ── Main ITS function ──────────────────────────────────────────────────────────

def its_analysis(
    conn,
    modes: list[str] | None = None,
    treatment_date: pd.Timestamp = TREATMENT_DATE,
) -> pd.DataFrame:
    """
    Run ITS OLS regression per mode. Returns one DataFrame row per mode with:

      mode, n_obs, train_start, n_post,
      beta_level, beta_level_se, pvalue_level, ci_level_lo, ci_level_hi,
      beta_slope, beta_slope_se, pvalue_slope, ci_slope_lo, ci_slope_hi,
      pre_mean, pct_level_change,
      initial_hike_pct, implied_elasticity, wb_elasticity,
      se_type, r2
    """
    try:
        import statsmodels.api as sm
    except ImportError:
        logger.error("statsmodels not installed. Run: uv add statsmodels")
        return pd.DataFrame()

    if modes is None:
        modes = DASHBOARD_MODES

    # Cumulative fare shock (Jan–Feb 2024 window) — same for all modes
    initial_hike_pct = _cumulative_hike_pct(
        from_date=pd.Timestamp("2023-12-31"),
        to_date=SHOCK_WINDOW_END,
    )

    rows = []

    for mode in modes:
        logger.info(f"ITS regression: {mode} …")

        # ── Data ──────────────────────────────────────────────────────────────
        df = _load_monthly_mode(conn, mode)
        train_start = (
            pd.Timestamp("2013-01-01") if mode == "COLECTIVO"
            else pd.Timestamp("2016-01-01")
        )
        df = df[df["ds"] >= train_start].copy()

        n_post = int((df["ds"] >= treatment_date).sum())
        if n_post < 6:
            logger.warning(f"  {mode}: only {n_post} post-treatment months — skipping")
            continue

        # ── Features ──────────────────────────────────────────────────────────
        df = _build_its_features(df, treatment_date)

        feature_cols = (
            ["t", "D", "t_post", "covid"]
            + sorted(c for c in df.columns if c.startswith("m_"))
        )
        X = sm.add_constant(df[feature_cols].astype(float), has_constant="add")
        y = df["y"].astype(float)

        # ── Fit ───────────────────────────────────────────────────────────────
        se_type = "HAC-12" if mode in HAC_MODES else "OLS"

        if mode in HAC_MODES:
            result = sm.OLS(y, X).fit(
                cov_type="HAC",
                cov_kwds={"maxlags": HAC_MAXLAGS, "use_correction": True},
            )
        else:
            result = sm.OLS(y, X).fit()

        base_fit = result  # R² always comes from the same result object

        # ── Coefficients ──────────────────────────────────────────────────────
        beta_level = float(result.params["D"])
        beta_slope = float(result.params["t_post"])
        ci         = result.conf_int()

        # ── Elasticity ────────────────────────────────────────────────────────
        pre_mean         = float(df.loc[df["ds"] < treatment_date, "y"].mean())
        pct_level_change = beta_level / pre_mean * 100 if pre_mean else float("nan")
        implied_elas     = (
            pct_level_change / initial_hike_pct * 10
            if initial_hike_pct else float("nan")
        )

        logger.info(
            f"  {mode}: β₂={beta_level/1e6:+.2f}M  p={result.pvalues['D']:.3f}  "
            f"Δ%={pct_level_change:+.1f}%  hike={initial_hike_pct:.0f}%  "
            f"ε={implied_elas:.3f}  [{se_type}]"
        )

        rows.append({
            "mode":              mode,
            "n_obs":             len(df),
            "train_start":       train_start.strftime("%Y-%m"),
            "n_post":            n_post,
            "beta_level":        beta_level,
            "beta_level_se":     float(result.bse["D"]),
            "pvalue_level":      float(result.pvalues["D"]),
            "ci_level_lo":       float(ci.loc["D", 0]),
            "ci_level_hi":       float(ci.loc["D", 1]),
            "beta_slope":        beta_slope,
            "beta_slope_se":     float(result.bse["t_post"]),
            "pvalue_slope":      float(result.pvalues["t_post"]),
            "ci_slope_lo":       float(ci.loc["t_post", 0]),
            "ci_slope_hi":       float(ci.loc["t_post", 1]),
            "pre_mean":          pre_mean,
            "pct_level_change":  pct_level_change,
            "initial_hike_pct":  initial_hike_pct,
            "implied_elasticity": implied_elas,
            "wb_elasticity":     WB_ELASTICITY,
            "se_type":           se_type,
            "r2":                float(base_fit.rsquared),
            # keep full df and result for counterfactual plotting
            "_df":               df,
            "_result":           result,
        })

    return pd.DataFrame(rows)


def build_counterfactual_df(
    row: pd.Series,
    treatment_date: pd.Timestamp = TREATMENT_DATE,
) -> pd.DataFrame:
    """
    Given one row from its_analysis() output, return a plotting DataFrame:
      ds, actual, fitted, counterfactual, gap
    Only for post-treatment months; gap = actual - counterfactual.
    """
    df     = row["_df"]
    result = row["_result"]

    cf = _counterfactual(df, result)

    out = df[["ds", "y"]].copy()
    out.columns = ["ds", "actual"]
    out["fitted"]         = result.fittedvalues.values
    out["counterfactual"] = cf.values
    out["gap"]            = out["actual"] - out["counterfactual"]
    out["post"]           = df["D"].values

    return out


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import duckdb
    from config import DB_PATH

    conn    = duckdb.connect(str(DB_PATH))
    results = its_analysis(conn)
    conn.close()

    if results.empty:
        print("No results.")
        raise SystemExit(0)

    print("\n" + "=" * 90)
    print(f"ITS ANALYSIS — SUBE FARE SHOCK  |  Treatment: {TREATMENT_DATE.strftime('%b %Y')}")
    print(f"Initial shock (Jan–Feb 2024): {results['initial_hike_pct'].iloc[0]:.0f}%  |  WB benchmark ε: {WB_ELASTICITY}")
    print("=" * 90)
    print(
        f"\n{'Mode':<12} {'N':>5} {'Post':>5} "
        f"{'β_level (M)':>12} {'p_lvl':>7} "
        f"{'β_slope (M)':>12} {'p_slp':>7} "
        f"{'Δ%':>8} {'ε (impl)':>10} {'WB ε':>8} {'R²':>6} {'SE':>8}"
    )
    print("-" * 110)
    for _, r in results.iterrows():
        sig_l = (
            "***" if r["pvalue_level"] < 0.001 else
            "**"  if r["pvalue_level"] < 0.01  else
            "*"   if r["pvalue_level"] < 0.05  else "   "
        )
        sig_s = (
            "***" if r["pvalue_slope"] < 0.001 else
            "**"  if r["pvalue_slope"] < 0.01  else
            "*"   if r["pvalue_slope"] < 0.05  else "   "
        )
        print(
            f"{r['mode']:<12} {r['n_obs']:>5} {r['n_post']:>5} "
            f"{r['beta_level']/1e6:>10.2f}M {r['pvalue_level']:>6.3f}{sig_l} "
            f"{r['beta_slope']/1e6:>10.3f}M {r['pvalue_slope']:>6.3f}{sig_s} "
            f"{r['pct_level_change']:>7.1f}% "
            f"{r['implied_elasticity']:>10.3f} {r['wb_elasticity']:>8.2f} "
            f"{r['r2']:>6.3f} {r['se_type']:>8}"
        )
    print(
        "\nβ_level = immediate step at treatment | β_slope = monthly trend change after | "
        "Δ% = level change as % of pre-treatment mean"
    )
    print(
        "⚠  β₂ conflates fare price effect with Dec 2023 devaluation "
        "(+118%) and real-income collapse.\n   Implied ε is an upper bound on pure price elasticity."
    )
