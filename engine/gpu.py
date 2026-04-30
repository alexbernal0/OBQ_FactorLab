"""GPU acceleration layer. Transparent fallback to numpy when cupy unavailable."""
import numpy as np

try:
    import cupy as cp
    _GPU = True
except ImportError:
    cp = np
    _GPU = False


def available() -> bool:
    return _GPU


def to_gpu(arr):
    return cp.array(arr, dtype=cp.float32) if _GPU else arr.astype(np.float32)


def to_cpu(arr):
    return cp.asnumpy(arr) if _GPU and hasattr(arr, "get") else np.asarray(arr)


def percentile_rank_matrix(mat):
    """
    mat: (N_months, N_symbols) float32
    Returns same-shape matrix where each row is percentile ranks 0-100.
    Vectorized across ALL months simultaneously on GPU.
    """
    xp = cp if _GPU else np
    mat = xp.array(mat, dtype=xp.float32)
    n = mat.shape[1]
    order = xp.argsort(xp.argsort(mat, axis=1), axis=1)
    return (order / max(n - 1, 1)) * 100.0


def ic_series(score_mat, return_mat, mask_mat=None):
    """
    score_mat:  (T, N) float32
    return_mat: (T, N) float32
    mask_mat:   (T, N) bool  -- 1 = include
    Returns (T,) IC series
    """
    xp = cp if _GPU else np
    T, N = score_mat.shape
    ic = xp.zeros(T, dtype=xp.float32)
    for t in range(T):
        s = score_mat[t]
        r = return_mat[t]
        if mask_mat is not None:
            m = mask_mat[t]
            s = s[m]
            r = r[m]
        valid = ~(xp.isnan(s) | xp.isnan(r))
        s, r = s[valid], r[valid]
        if len(s) < 10:
            ic[t] = xp.nan
            continue
        # Pearson correlation via vectorized formulas
        sm = s - s.mean(); rm = r - r.mean()
        denom = xp.sqrt((sm**2).sum() * (rm**2).sum())
        ic[t] = float((sm * rm).sum() / denom) if denom > 0 else xp.nan
    return to_cpu(ic)


def rolling_cagr(equity_curve, window):
    """Vectorized rolling CAGR over window periods."""
    xp = cp if _GPU else np
    eq = xp.array(equity_curve, dtype=xp.float32)
    T = len(eq)
    out = xp.full(T, xp.nan)
    for i in range(window - 1, T):
        base = eq[i - window + 1]
        if base > 0:
            out[i] = (eq[i] / base) ** (12.0 / window) - 1
    return to_cpu(out)


def rolling_sharpe(monthly_ret, window, rf_monthly=0.0):
    xp = cp if _GPU else np
    r = xp.array(monthly_ret, dtype=xp.float32)
    T = len(r)
    out = xp.full(T, xp.nan)
    for i in range(window - 1, T):
        w = r[i - window + 1: i + 1] - rf_monthly
        mu = w.mean(); sd = w.std()
        out[i] = float(mu / sd * xp.sqrt(12)) if sd > 1e-9 else xp.nan
    return to_cpu(out)
