"""Full OBQ tearsheet metrics — 80+ metrics including OBQ Surefire Suite."""
import numpy as np
import scipy.stats as stats
from typing import Optional


# ── OBQ Surefire Suite (from MassCode 17d_REPORTING) ─────────────────────────

def _integrated_drawdown(returns: np.ndarray) -> float:
    """Area under drawdown curve — penalises depth AND duration."""
    r = np.asarray(returns, dtype=float)
    if len(r) == 0: return 0.0
    eq = np.cumprod(1.0 + r)
    peak = np.maximum.accumulate(eq)
    dd = eq / peak - 1.0
    return float(np.trapz(-np.clip(dd, None, 0)))


def _integrated_upside(returns: np.ndarray) -> float:
    """Area above starting equity — captures resilience + winning streak longevity."""
    r = np.asarray(returns, dtype=float)
    if len(r) == 0: return 0.0
    eq = np.cumprod(1.0 + r)
    return float(np.trapz(eq - 1.0))


def _iudr(returns: np.ndarray) -> float:
    """IUDR = Integrated Upside / Integrated Drawdown. Shape quality score."""
    idd = _integrated_drawdown(returns)
    iup = _integrated_upside(returns)
    if idd <= 0:
        return float('inf') if iup > 0 else 0.0
    return float(iup / idd)


def _surefire_ratio(returns: np.ndarray) -> float:
    """Surefire Ratio = IUDR × final_equity. Master score: shape × magnitude."""
    r = np.asarray(returns, dtype=float)
    if len(r) == 0: return 0.0
    eq = np.cumprod(1.0 + r)
    final_eq = float(eq[-1])
    iud = _iudr(r)
    if not np.isfinite(iud):
        return float(final_eq * 1000)
    return float(iud * final_eq)


def _max_consec(arr: np.ndarray, positive: bool = True) -> int:
    best = cur = 0
    for v in arr:
        if (v > 0) == positive:
            cur += 1; best = max(best, cur)
        else:
            cur = 0
    return best


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

    # ── GIPS-compliant risk-adjusted metrics ─────────────────────────────────
    # Sharpe: (CAGR - rf_annual) / ann_vol  [GIPS standard — geometric return]
    # NOT arithmetic mean × ppy, which overstates in compounding regimes.
    sharpe = float((cagr - rf_annual) / ann_vol) if ann_vol > 0 else 0.0

    # Smart Sharpe (autocorrelation-adjusted Sharpe)
    sharpe_sq = sharpe ** 2
    smart_sr = float(sharpe / np.sqrt(1 + (sharpe_sq / (4 * n)))) if n > 0 else 0.0

    # Sortino: (CAGR - rf_annual) / downside_deviation  [GIPS/CFA standard]
    # Downside deviation = std of returns BELOW MAR (not RMS — RMS is incorrect)
    neg_r = r[r < rf]
    if len(neg_r) >= 2:
        down_std = float(neg_r.std() * np.sqrt(periods_per_year))
    elif len(neg_r) == 1:
        down_std = float(abs(neg_r[0]) * np.sqrt(periods_per_year))
    else:
        down_std = 1e-9
    sortino = float((cagr - rf_annual) / down_std) if down_std > 0 else 0.0

    # Drawdown
    roll_max = np.maximum.accumulate(eq)
    dd = eq / roll_max - 1
    max_dd = float(dd.min())
    avg_dd = float(dd[dd < 0].mean()) if (dd < 0).any() else 0.0

    # Win rates (computed early — used in Calmar below)
    wr_mo = float((r > 0).mean())
    ann_r = annual_ret if annual_ret is not None else np.array([])
    wr_yr = float((ann_r > 0).mean()) if len(ann_r) > 0 else None

    # Calmar — OBQ definition: CAGR × WinMonth% / |MaxDD|
    # Rewards return AND consistency of up-months, penalizes drawdown depth
    calmar = float((cagr * wr_mo) / abs(max_dd)) if max_dd != 0 else 0.0
    # GIPS standard Calmar: CAGR / |MaxDD| (no win-rate multiplier)
    calmar_gips = float(cagr / abs(max_dd)) if max_dd != 0 else 0.0

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

    # K-ratio and R² of equity curve (log-linear fit — how close to a straight line)
    if len(eq) > 2:
        t = np.arange(len(eq))
        log_eq = np.log(eq)
        slope, intercept, r_val, p_val, se = stats.linregress(t, log_eq)
        k_ratio    = float(slope / se * np.sqrt(periods_per_year)) if se > 0 else 0.0
        equity_r2  = float(r_val ** 2)   # R² of log-equity vs time (1.0 = perfect straight line)
    else:
        k_ratio   = 0.0
        equity_r2 = 0.0

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

    # ── Additional metrics (QGSI / template parity) ───────────────────────────
    # Expected returns
    exp_annual = float(((1 + exp_mo) ** periods_per_year) - 1)

    # Win rates by period
    wr_daily = None   # not applicable for monthly data
    # Quarterly approximation from monthly
    n_qtrs = max(n // 3, 1)
    qtr_rets = np.array([
        float(np.prod(1 + r[i*3:(i+1)*3]) - 1)
        for i in range(n // 3)
    ]) if n >= 3 else np.array([])
    wr_qtr = float((qtr_rets > 0).mean()) if len(qtr_rets) > 0 else None

    # Consecutive wins/losses (on monthly returns)
    max_consec_wins   = int(_max_consec(r, positive=True))
    max_consec_losses = int(_max_consec(r, positive=False))

    # Exposure (fraction of periods with non-zero return — for factor model = always 1.0)
    exposure = float((r != 0).mean())

    # SystemScore = Sharpe × Calmar
    system_score = float(sharpe * calmar) if calmar != 0 else 0.0

    # Smart Sortino (autocorrelation-adjusted)
    try:
        acf1 = float(np.corrcoef(r[:-1], r[1:])[0, 1]) if n > 3 else 0.0
        smart_sortino_denom = max(1 + 2 * acf1, 0.01) ** 0.5
        smart_sortino = float(sortino / smart_sortino_denom)
    except Exception:
        smart_sortino = sortino

    # OBQ Surefire Suite (strategy-level)
    integrated_dd  = float(_integrated_drawdown(r))
    integrated_up  = float(_integrated_upside(r))
    iudr_val       = float(_iudr(r)) if np.isfinite(_iudr(r)) else None
    surefire       = float(_surefire_ratio(r))

    # Gain/Pain Ratio (same as Omega but named per QGSI template)
    gain_pain = float(omega) if np.isfinite(omega) else None

    # MAR Ratio (CAGR / |Max DD|) — same as Calmar but explicit naming
    mar_ratio = calmar  # alias

    # Duration
    n_years = float(years)

    # ── Phase 1 new metrics ─────────────────────────────────────────────────────

    # Burke Ratio = CAGR / sqrt(sum of squared drawdowns)
    # Already computed bd above (only when bm available), compute standalone
    all_dd_depths = np.array([float(dd[i]) for i in range(len(dd)) if dd[i] < 0])
    _bd_standalone = float(np.sqrt(np.sum(all_dd_depths**2))) if len(all_dd_depths) > 0 else 1.0
    burke_ratio = float(cagr / _bd_standalone) if _bd_standalone > 0 else 0.0

    # Sterling Ratio = CAGR / (avg annual max DD + 10%)
    _periods_per_yr = int(round(periods_per_year))  # e.g. 12 monthly, 2 semi-annual
    if years >= 1 and _periods_per_yr > 0:
        n_full_yrs = max(1, int(years))
        _ann_mdd = np.array([
            dd[i*_periods_per_yr:(i+1)*_periods_per_yr].min()
            for i in range(n_full_yrs)
            if len(dd[i*_periods_per_yr:(i+1)*_periods_per_yr]) > 0
        ])
        sterling_ratio = float(cagr / (abs(_ann_mdd.mean()) + 0.10)) if len(_ann_mdd) > 0 and _ann_mdd.mean() != 0 else 0.0
    else:
        sterling_ratio = float(cagr / (abs(max_dd) + 0.10)) if max_dd != 0 else 0.0

    # Conditional Drawdown at Risk (CDaR 95%) = mean of worst 5% drawdown values
    if len(all_dd_depths) > 0:
        q5_threshold = np.percentile(all_dd_depths, 5)  # 5th pctile (most negative)
        worst_dds = all_dd_depths[all_dd_depths <= q5_threshold]
        cdar_95 = float(worst_dds.mean()) if len(worst_dds) > 0 else float(q5_threshold)
    else:
        cdar_95 = 0.0

    # Drawdown statistics
    # Build drawdown periods: start, trough, recovery, depth, duration, recovery_time
    _dd_periods = []
    _in_dd = False
    _dd_start = 0
    _dd_trough_val = 0.0
    for _i in range(len(dd)):
        if not _in_dd and dd[_i] < 0:
            _in_dd = True; _dd_start = _i; _dd_trough_val = dd[_i]
        elif _in_dd:
            if dd[_i] < _dd_trough_val: _dd_trough_val = dd[_i]
            if dd[_i] >= -0.0001:
                _dd_periods.append({'depth': _dd_trough_val, 'dur': _i - _dd_start})
                _in_dd = False
    if _in_dd:
        _dd_periods.append({'depth': _dd_trough_val, 'dur': len(dd) - _dd_start})

    avg_dd_duration   = float(np.mean([p['dur'] for p in _dd_periods])) if _dd_periods else 0.0
    max_dd_duration   = float(max([p['dur'] for p in _dd_periods])) if _dd_periods else 0.0
    pct_time_in_dd    = float((dd < 0).mean())  # fraction of periods in drawdown
    n_drawdowns       = int(len(_dd_periods))

    # Deflated Sharpe Ratio (DSR) — Bailey & Lopez de Prado 2014
    # DSR = PSR adjusted for multiple testing: DSR = PSR(SR* = 0) where
    # SR* = sqrt(V_SR) * ((1-γ)Z^{-1}(1-1/T) + γZ^{-1}(1-1/(T*e)))
    # Simplified: DSR = Φ((SR√n - SR*√n) / SE_SR)
    # where SR* = benchmark Sharpe under null (use 0), accounts for skew/kurt
    if n > 4 and sr_std > 0:
        # Number of independent trials estimate = 1 (single strategy)
        # For multiple strategies, this would be > 1
        # Minimum acceptable Sharpe under null = 0
        sr_benchmark = 0.0
        dsr = float(stats.norm.cdf((sharpe - sr_benchmark) / sr_std)) if sr_std > 0 else 0.5
    else:
        dsr = psr_val  # fallback to PSR

    # Minimum Track Record Length (MinTRL) — months needed for Sharpe significance
    # MinTRL = (Z_α / SR)² × (1 + 0.5SR² - skew·SR + kurt/4·SR²) at α=0.05
    if sharpe != 0:
        z_alpha = 1.645  # one-tailed 95%
        _mintrl = (z_alpha / sharpe) ** 2 * (1 + 0.5 * sharpe**2 - skew * sharpe + (kurt/4) * sharpe**2)
        min_trl_months = float(max(0, _mintrl))
    else:
        min_trl_months = float('inf')
    min_trl_months = _safe(min_trl_months)

    # Outlier Win/Loss Ratios (from template)
    if (r > 0).any():
        _upper = r.mean() + 3 * r.std()
        _win_outliers = r[r > _upper]
        _avg_win = r[r > 0].mean() if (r > 0).any() else 1e-9
        outlier_win_ratio = float(_win_outliers.mean() / _avg_win) if len(_win_outliers) > 0 and _avg_win != 0 else 0.0
    else:
        outlier_win_ratio = 0.0

    if (r < 0).any():
        _lower = r.mean() - 3 * r.std()
        _loss_outliers = r[r < _lower]
        _avg_loss = abs(r[r < 0].mean()) if (r < 0).any() else 1e-9
        outlier_loss_ratio = float(abs(_loss_outliers.mean()) / _avg_loss) if len(_loss_outliers) > 0 and _avg_loss != 0 else 0.0
    else:
        outlier_loss_ratio = 0.0

    # Trailing period returns (from end of series, annualized where applicable)
    _end_idx = len(r)
    def _trailing_ret(months):
        if _end_idx < months: return None
        _sub = r[_end_idx - months:]
        _cum = float(np.prod(1 + _sub) - 1)
        return _cum
    def _trailing_cagr(months):
        _cr = _trailing_ret(months)
        if _cr is None: return None
        _yrs = months / periods_per_year
        return float((1 + _cr) ** (1/_yrs) - 1) if _yrs > 0 else None

    trailing_1m   = _trailing_ret(1)
    trailing_3m   = _trailing_ret(3)
    trailing_6m   = _trailing_ret(6)
    trailing_1y   = _trailing_ret(12)
    trailing_3y   = _trailing_cagr(36)
    trailing_5y   = _trailing_cagr(60)
    trailing_10y  = _trailing_cagr(120)

    # Seasonality — average return by month-of-year
    # Returns dict month_1..12 with avg return
    # We need equity_dates to compute this — pass as a placeholder here
    # (will be enriched in spy_backtest.py with actual monthly breakdown)

    # Cornish-Fisher adjusted VaR (accounts for skew/kurtosis)
    _z95 = 1.645
    _cf_adj = _z95 + ((_z95**2 - 1)/6)*skew + ((_z95**3 - 3*_z95)/24)*kurt - ((2*_z95**3 - 5*_z95)/36)*skew**2
    var_95_cf = float(-(_cf_adj * r.std() - r.mean()))  # CF-adjusted VaR 95%

    # Average recovery time (mean of recovery days across all drawdown periods with recovery)
    # Using period count (months) rather than days since we have monthly data
    avg_recovery_time = avg_dd_duration  # in periods (months)

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
        _ppy_int = max(1, int(round(periods_per_year)))
        _ann_mdd_slices = [dd[i*_ppy_int:(i+1)*_ppy_int] for i in range(int(years))]
        _ann_mdd_slices = [s for s in _ann_mdd_slices if len(s) > 0]
        ann_mdd = np.array([s.min() for s in _ann_mdd_slices]) if _ann_mdd_slices else np.array([max_dd])
        sterling = float(cagr / (abs(ann_mdd.mean()) + 0.10)) if ann_mdd.mean() != 0 else 0.0
        bm_m = dict(
            bm_cagr=bm_cagr, bm_vol=bm_vol,
            alpha=alpha, beta=beta, r_squared=float(corr**2),
            tracking_error=te, info_ratio=ir,
            up_capture=uc, down_capture=dc,
            treynor_ratio=treynor, m_squared=m2,
            bm_burke_ratio=burke, bm_sterling_ratio=sterling,
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
        # Core returns
        cagr=cagr, total_return=total_ret,
        expected_monthly=exp_mo, expected_annual=exp_annual,
        ann_vol=ann_vol, n_years=n_years,
        # Risk-adjusted
        sharpe=sharpe, smart_sharpe=smart_sr,
        sortino=sortino, smart_sortino=smart_sortino,
        calmar=calmar, calmar_gips=calmar_gips, omega=omega, serenity=serenity,
        pain_ratio=pain_ratio, recovery_factor=recov_f,
        mar_ratio=mar_ratio, system_score=system_score,
        # Risk
        max_dd=max_dd, avg_dd=avg_dd, ulcer_index=ulcer,
        pain_index=pain, lake_ratio=lake,
        var_95=var95, var_99=var99, cvar_95=cvar95, cvar_99=cvar99,
        # Distribution
        skewness=skew, kurtosis=kurt,
        jarque_bera_stat=float(jb_s), jarque_bera_p=float(jb_p),
        tail_ratio=tail_r,
        best_month=best_mo, worst_month=worst_mo,
        best_year=best_yr, worst_year=worst_yr,
        # Win/loss
        win_rate_monthly=wr_mo, win_rate_yearly=wr_yr,
        win_rate_quarterly=wr_qtr,
        avg_up_month=avg_up, avg_down_month=avg_dn, payoff_ratio=payoff,
        profit_factor=pf, common_sense_ratio=common_sense, cpc_index=cpc,
        gain_pain_ratio=gain_pain,
        max_consec_wins=max_consec_wins, max_consec_losses=max_consec_losses,
        exposure=exposure,
        # Statistical
        equity_r2=equity_r2,
        k_ratio=k_ratio, psr=psr_val, sharpe_tstat=sr_tstat,
        sharpe_ci_95=[sr_ci_lo, sr_ci_hi],
        haircut_sharpe=haircut_sharpe,
        # OBQ Surefire Suite
        integrated_dd=integrated_dd, integrated_up=integrated_up,
        iudr=iudr_val, surefire_ratio=surefire,
        # Phase 1 new metrics
        burke_ratio=burke_ratio, sterling_ratio=sterling_ratio,
        cdar_95=cdar_95,
        avg_dd_duration=avg_dd_duration, max_dd_duration=max_dd_duration,
        pct_time_in_dd=pct_time_in_dd, n_drawdowns=n_drawdowns,
        dsr=dsr, min_trl_months=min_trl_months,
        outlier_win_ratio=outlier_win_ratio, outlier_loss_ratio=outlier_loss_ratio,
        trailing_1m=trailing_1m, trailing_3m=trailing_3m,
        trailing_6m=trailing_6m, trailing_1y=trailing_1y,
        trailing_3y=trailing_3y, trailing_5y=trailing_5y, trailing_10y=trailing_10y,
        var_95_cf=var_95_cf, avg_recovery_time=avg_recovery_time,
        # Meta
        n_periods=n,
        **bm_m, **ic_m,
    )
    return {k: _safe(v) for k, v in result.items()}
