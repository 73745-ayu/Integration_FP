# run_analysis.py
from config import companies, base, n_simulations
from src.monte_carlo import simulate
from src.tsr import compute_tsr
from src.goals import find_equal_p

def main():
    df = simulate(companies["Client"], n_simulations)
    df = compute_tsr(df, base["Client"], base["Client"]["years"])
    table = find_equal_p(df, base["Client"], base["Client"]["years"], tsr_probs=[0.8, 0.5, 0.2])

    print(table.round(6))
    

    table.to_csv("multi_goalseek_output.csv")

if __name__ == "__main__":
    main()
