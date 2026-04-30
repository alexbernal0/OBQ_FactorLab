"""Full OBQ tearsheet metrics — 55+ metrics matching factor_engine.py standard."""
import numpy as np
import scipy.stats as stats
from typing import Optional


def _safe(v):
    if v is None: return None
    if isinstance(v, list): return [_safe(x) for x in v]
    if isinstance(v, dict): return {k: _safe(x) for k, x in v.items()}
    try:
        f = float(v)
        if np.isnan(f) or np.isinf(f): return None
        return f
    except (TypeError, ValueError):
        return v


def compute_all(
    equity: np.ndarray,            # cumulative equity curve (wealth index, starts at 1.0)
    monthly_ret: np.ndarray,       # monthly returns (decimal)
    annual_ret: Optional[np.ndarray] = None,
    bm_equity: Optional[np.ndarray] = None,
    bm_monthly: Optional[np.ndarray] = None,
    ic_series: Optional[np.ndarray] = None,
    q1q5_spread_cagr: Optional[float] = None,
    avg_turnover: Optional[float] = None,
    periods_per_year: int = 12,
    rf_annual: float = 0.04,
    label: str = "",
) -> dict:
    rf = rf_annual / periods_per_year
    r = np.asarray(monthly_ret, dtype=float)
    eq = np.asarray(equity, dtype=float)

    n = len(r)
    years = n / periods_per_year

    # Core return metrics
    total_ret = float(eq[-1] / eq[0] - 1) if len(eq) >= 2 else 0.0
    cagr = float((eq[-1] / eq[0]) ** (1 / years) - 1) if years > 0 else 0.0
    ann_vol = float(r.std() * np.sqrt(periods_per_year))
    exp_mo = float(r.mean())

    # Risk-adjusted
    excess = r - rf
    sharpe = float(excess.mean() / excess.std() * np.sqrt(periods_per_year)) if excess.std() > 0 else 0.0

    # Smart Sharpe (accounts for autocorrelation via sqrt Sharpe)
    sharpe_sq = sharpe ** 2
    smart_sr = float(sharpe / np.sqrt(1 + (sharpe_sq / (4 * n)))) if n > 0 else 0.0

    # Sortino
    down = r[r < rf] - rf
    down_std = float(np.sqrt((down**2).mean())) if len(down) > 0 else 1e-9
    sortino = float((r.mean() - rf) / down_std * np.sqrt(periods_per_year)) if down_std > 0 else 0.0

    # Drawdown
    roll_max = np.maximum.accumulate(eq)
    dd = eq / roll_max - 1
    max_dd = float(dd.min())
    avg_dd = float(dd[dd < 0].mean()) if (dd < 0).any() else 0.0

    # Calmar, Omega
    calmar = float(cagr / abs(max_dd)) if max_dd != 0 else 0.0
    thresh = rf
    gains = r[r >= thresh] - thresh
    losses = thresh - r[r < thresh]
    omega = float(gains.sum() / losses.sum()) if losses.sum() > 0 else float("inf")

    # VaR / CVaR
    var95 = float(np.percentile(r, 5))
    var99 = float(np.percentile(r, 1))
    cvar95 = float(r[r <= var95].mean()) if (r <= var95).any() else var95
    cvar99 = float(r[r <= var99].mean()) if (r <= var99).any() else var99

    # Distribution stats
    skew = float(stats.skew(r))
    kurt = float(stats.kurtosis(r))
    jb_s, jb_p = stats.jarque_bera(r)

    # Win rates
    wr_mo = float((r > 0).mean())
    ann_r = annual_ret if annual_ret is not None else np.array([])
    wr_yr = float((ann_r > 0).mean()) if len(ann_r) > 0 else None

    # Pain / Ulcer
    ulcer = float(np.sqrt((dd**2).mean()))
    pain = float(abs(dd).mean())
    pain_ratio = float(cagr / pain) if pain > 0 else 0.0

    # Recovery factor
    recov_f = float(total_ret / abs(max_dd)) if max_dd != 0 else 0.0

    # Lake ratio (% time underwater)
    lake = float((dd < 0).mean())

    # Tail ratio
    tail_r = float(abs(np.percentile(r, 95)) / abs(np.percentile(r, 5))) if np.percentile(r, 5) != 0 else 0.0

    # Best/worst month/year
    best_mo = float(r.max()); worst_mo = float(r.min())
    best_yr = float(ann_r.max()) if len(ann_r) > 0 else None
    worst_yr = float(ann_r.min()) if len(ann_r) > 0 else None

    avg_up = float(r[r > 0].mean()) if (r > 0).any() else 0.0
    avg_dn = float(r[r < 0].mean()) if (r < 0).any() else 0.0
    payoff = float(abs(avg_up / avg_dn)) if avg_dn != 0 else 0.0
    pf_wins = r[r > 0].sum(); pf_loss = abs(r[r < 0].sum())
    pf = float(pf_wins / pf_loss) if pf_loss > 0 else float("inf")

    # CSR, CPC, K-ratio
    common_sense = float(tail_r * pf) if np.isfinite(pf) else tail_r
    cpc = float(wr_mo * payoff * pf) if np.isfinite(pf) else 0.0

    # K-ratio (linear regression on log equity)
    if len(eq) > 2:
        t = np.arange(len(eq))
        log_eq = np.log(eq)
        slope, intercept, r_val, p_val, se = stats.linregress(t, log_eq)
        k_ratio = float(slope / se * np.sqrt(periods_per_year)) if se > 0 else 0.0
    else:
        k_ratio = 0.0

    # Probabilistic Sharpe Ratio
    sr_std = float(np.sqrt((1 + 0.5 * sharpe**2 - skew * sharpe + (kurt/4) * sharpe**2) / (n - 1)))
    psr_val = float(stats.norm.cdf((sharpe - 0) / sr_std)) if sr_std > 0 else 0.5
    sr_tstat = float(sharpe / sr_std * np.sqrt(n)) if sr_std > 0 else 0.0
    sr_ci_lo = float(sharpe - 1.96 * sr_std); sr_ci_hi = float(sharpe + 1.96 * sr_std)
    haircut_sharpe = float(sharpe * (1 - 1.96 * sr_std / (sharpe + 1e-9))) if sharpe != 0 else 0.0

    # Serenity ratio
    depths = [float(dd[i]) for i in range(len(dd)) if dd[i] < 0]
    bd = float(np.sqrt(sum(d**2 for d in depths))) if depths else 1.0
    serenity = float(cagr / (ulcer * ann_vol)) if ulcer > 0 and ann_vol > 0 else 0.0

    # Benchmark metrics
    bm_m = {}
    if bm_monthly is not None and bm_equity is not None:
        bm = np.asarray(bm_monthly, dtype=float)
        bm_eq = np.asarray(bm_equity, dtype=float)
        bm_cagr = float((bm_eq[-1]/bm_eq[0])**(1/years)-1) if years > 0 else 0.0
        bm_vol = float(bm.std() * np.sqrt(periods_per_year))
        te = float((r - bm[:len(r)]).std() * np.sqrt(periods_per_year))
        corr = float(np.corrcoef(r, bm[:len(r)])[0, 1]) if len(bm) >= len(r) else 0.0
        beta = float(np.cov(r, bm[:len(r)])[0,1] / np.var(bm[:len(r)])) if np.var(bm[:len(r)]) > 0 else 1.0
        alpha = float(cagr - rf_annual - beta * (bm_cagr - rf_annual))
        ir = float((cagr - bm_cagr) / te) if te > 0 else 0.0
        up_p = r > 0; dn_p = r < 0
        uc = float(r[up_p].mean()/bm[:len(r)][up_p].mean()) if up_p.any() and bm[:len(r)][up_p].mean() != 0 else 1.0
        dc = float(r[dn_p].mean()/bm[:len(r)][dn_p].mean()) if dn_p.any() and bm[:len(r)][dn_p].mean() != 0 else 1.0
        treynor = float((cagr - rf_annual) / beta) if beta != 0 else 0.0
        m2 = float(sharpe * bm_vol + rf_annual)
        burke = float(cagr / bd) if bd > 0 else 0.0
        ann_mdd = np.array([dd[i*12:(i+1)*12].min() for i in range(int(years))]) if years >= 1 else np.array([max_dd])
        sterling = float(cagr / (abs(ann_mdd.mean()) + 0.10)) if ann_mdd.mean() != 0 else 0.0
        bm_m = dict(
            bm_cagr=bm_cagr, bm_vol=bm_vol,
            alpha=alpha, beta=beta, r_squared=float(corr**2),
            tracking_error=te, info_ratio=ir,
            up_capture=uc, down_capture=dc,
            treynor_ratio=treynor, m_squared=m2,
            burke_ratio=burke, sterling_ratio=sterling,
        )

    # IC metrics
    ic_m = {}
    if ic_series is not None:
        ics = np.asarray(ic_series)
        ics = ics[~np.isnan(ics)]
        if len(ics) >= 3:
            ic_mean = float(ics.mean()); ic_std = float(ics.std())
            ic_m = dict(
                ic_mean=ic_mean,
                icir=float(ic_mean / ic_std) if ic_std > 0 else 0.0,
                ic_tstat=float(ic_mean / (ic_std / np.sqrt(len(ics)))) if ic_std > 0 else 0.0,
                ic_hit_rate=float((ics > 0).mean()),
                ic_skew=float(stats.skew(ics)),
                ic_kurt=float(stats.kurtosis(ics)),
            )
    if q1q5_spread_cagr is not None:
        ic_m["q1q5_spread_cagr"] = q1q5_spread_cagr
    if avg_turnover is not None:
        ic_m["portfolio_turnover_pct"] = avg_turnover

    result = dict(
        label=label,
        cagr=cagr, total_return=total_ret, expected_monthly=exp_mo,
        ann_vol=ann_vol, sharpe=sharpe, smart_sharpe=smart_sr,
        sortino=sortino, calmar=calmar, omega=omega, serenity=serenity,
        pain_ratio=pain_ratio, recovery_factor=recov_f,
        max_dd=max_dd, avg_dd=avg_dd, ulcer_index=ulcer,
        pain_index=pain, lake_ratio=lake,
        var_95=var95, var_99=var99, cvar_95=cvar95, cvar_99=cvar99,
        skewness=skew, kurtosis=kurt,
        jarque_bera_stat=float(jb_s), jarque_bera_p=float(jb_p),
        tail_ratio=tail_r,
        best_month=best_mo, worst_month=worst_mo,
        best_year=best_yr, worst_year=worst_yr,
        win_rate_monthly=wr_mo, win_rate_yearly=wr_yr,
        avg_up_month=avg_up, avg_down_month=avg_dn, payoff_ratio=payoff,
        profit_factor=pf, common_sense_ratio=common_sense, cpc_index=cpc,
        k_ratio=k_ratio, psr=psr_val, sharpe_tstat=sr_tstat,
        sharpe_ci_95=[sr_ci_lo, sr_ci_hi],
        haircut_sharpe=haircut_sharpe, n_periods=n,
        **bm_m, **ic_m,
    )
    return {k: _safe(v) for k, v in result.items()}
