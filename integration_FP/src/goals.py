import numpy as np
import pandas as pd
from scipy.optimize import brentq
from src.tsr import compute_tsr

def find_equal_p(
    df: pd.DataFrame,
    base: dict,
    years: float,
    tsr_probs: list[float],
    tol: float = 1e-6
) -> pd.DataFrame:
    rev = df["Revenue"]
    marg = df["EBITDA Margin"]
    mult = df["EV/EBITDA"]
    tsr_s = df["TSR"]
    D1 = base["net_debt_2026"]
    S1 = base["shares_2026"]
    Y1 = base["div_yield_2026"]

    def tsr_at(p_input):
        # Clamp p_input strictly within (0, 1)
        p_input = np.clip(p_input, tol, 1 - tol)
        row = pd.DataFrame({
            "Revenue": [rev.quantile(1 - p_input)],
            "EBITDA Margin": [marg.quantile(1 - p_input)],
            "EV/EBITDA": [mult.quantile(1 - p_input)]
        })
        result = compute_tsr(row, base, years)["TSR"].iloc[0]
        # Handle NaN or inf values explicitly
        if np.isnan(result) or np.isinf(result):
            return np.nan
        return result

    out = []
    for p in tsr_probs:
        target = tsr_s.quantile(1 - p)

        a, b = tol, 1 - tol
        try:
            fa, fb = tsr_at(a) - target, tsr_at(b) - target

            # Check for valid bracket
            if np.isnan(fa) or np.isnan(fb) or fa * fb > 0:
                p_in = np.nan  # No valid solution
            else:
                p_in = brentq(lambda x: tsr_at(x) - target, a, b, xtol=tol)
        except ValueError:
            p_in = np.nan

        if np.isnan(p_in):
            # If solution isn't valid, set threshold as NaN
            thr_rev = thr_marg = thr_mult = market_cap = share_price = np.nan
        else:
            thr_rev = rev.quantile(1 - p_in)
            thr_marg = marg.quantile(1 - p_in)
            thr_mult = mult.quantile(1 - p_in)
            market_cap = thr_mult * thr_rev * thr_marg - D1
            share_price = market_cap / S1

        out.append({
            "p_tsr": p,
            "Revenue": thr_rev,
            "p_revenue": p_in,
            "EBITDA Margin": thr_marg,
            "p_margin": p_in,
            "EV/EBITDA": thr_mult,
            "p_multiple": p_in,
            "Market Cap": market_cap,
            "Share price": share_price,
            "TSR": target,
            "Probability": p_in
        })

    return pd.DataFrame(out).set_index("p_tsr")
