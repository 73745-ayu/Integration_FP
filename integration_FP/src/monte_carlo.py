import numpy as np
import pandas as pd

def simulate(company_data: dict, n: int) -> pd.DataFrame:
    """Generate triangular Monte Carlo draws for Rev, Margin, EV/EBITDA."""
    rev = np.random.triangular(
        company_data["Revenue"]["0th"],
        company_data["Revenue"]["median"],
        company_data["Revenue"]["100th"],
        n
    )
    marg = np.random.triangular(
        company_data["EBITDA_Margin"]["0th"],
        company_data["EBITDA_Margin"]["median"],
        company_data["EBITDA_Margin"]["100th"],
        n
    )
    mult = np.random.triangular(
        company_data["EV_EBITDA"]["0th"],
        company_data["EV_EBITDA"]["median"],
        company_data["EV_EBITDA"]["100th"],
        n
    )
    return pd.DataFrame({
        "Revenue": rev,
        "EBITDA Margin": marg,
        "EV/EBITDA": mult
    })
