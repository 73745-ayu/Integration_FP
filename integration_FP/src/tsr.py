import numpy as np
import pandas as pd


def compute_tsr(
    df: pd.DataFrame,
    base: dict,
    years: float
) -> pd.DataFrame:
    """
    Append CAGR lines and TSR to df.  
    Expects columns ['Revenue','EBITDA Margin','EV/EBITDA'].
    """
    # unpack
    R0, M0, E0 = base["revenue_2024"], base["ebitda_margin_2024"], base["ev_ebitda_2024"]
    EV0, D0, S0 = base["ev_2024"], base["net_debt_2024"], base["shares_2024"]
    Y1, D1, S1 = base["div_yield_2026"], base["net_debt_2026"], base["shares_2026"]

    # compute CAGRs
    R1 = df["Revenue"]
    M1 = df["EBITDA Margin"]
    E1 = df["EV/EBITDA"]

    df["cagr_revenue"]       = (R1 / R0)**(1/years) - 1
    df["cagr_ebitda_margin"] = (M1 / M0)**(1/years) - 1
    df["cagr_ev_ebitda"]     = (E1 / E0)**(1/years) - 1

    # market-cap/EV CAGR
    EV1 = E1 * R1 * M1
    cap0 = EV0 - D0
    cap1 = EV1 - D1
    df["cagr_mktcap_ev"] = ((cap1/EV1)/(cap0/EV0))**(1/years) - 1

    df["cagr_shares"]     = (S1 / S0)**(1/years) - 1
    df["dividend_return"] = (Y1 * (cap1/S1)) / (cap0/S0)

    # TSR
    df["TSR"] = (
          (1+df["cagr_revenue"])
        * (1+df["cagr_ebitda_margin"])
        * (1+df["cagr_ev_ebitda"])
        * (1+df["cagr_mktcap_ev"])
        * (1+df["cagr_shares"])
    ) - 1 + df["dividend_return"]

    return df
