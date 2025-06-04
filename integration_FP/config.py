from read_summary import read_summary_from_excel

# Clearly specify your forecast year
poa_input = "CY2026"
excel_file_path = "Combined_Forecast_Summary_With_Linking.xlsx"
ticker = "CRDA.L"

# Dynamically read statistics from Excel based on poa_input
stats = read_summary_from_excel(excel_file_path, ticker, poa_input)

companies = {
    "Client": {
        "Revenue": {
            "median": stats["Revenue"]["median"],
            "0th": stats["Revenue"]["p10"],
            "100th": stats["Revenue"]["p90"]
        },
        "EBITDA_Margin": {
            "median": stats["EBITDA_Margin"]["median"],
            "0th": stats["EBITDA_Margin"]["p10"],
            "100th": stats["EBITDA_Margin"]["p90"]
        },
        "EV_EBITDA": {
            "median": stats["EV_EBITDA"]["median"],
            "0th": stats["EV_EBITDA"]["p10"],
            "100th": stats["EV_EBITDA"]["p90"]
        },
    },
}

base = {
    "Client": {
        # Historical Values (hardcoded as these typically remain fixed)
        "revenue_2024": 1630.0,
        "ebitda_margin_2024": 0.23,
        "ev_ebitda_2024": 16.45,
        "ev_2024": 6164.0,
        "net_debt_2024": 508.0,
        "shares_2024": 140.0,
        "div_yield_2024": 0.02,
        "net_debt_2026": 370.0,
        "shares_2026": 139.5833,
        "div_yield_2026": 0.00,
        "years": 2.0,
    },
}

n_simulations = 10_000
