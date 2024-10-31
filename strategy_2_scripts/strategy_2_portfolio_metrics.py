import pandas as pd
import os
from utils import (
    calculate_information_ratio,
    calculate_sharpe_ratio,
    calculate_drawdown,
    calculate_annualized_return,
    calculate_margin,
    calculate_turnover,
)

class PortfolioMetrics:
    def __init__(self, backtest_results_path, output_metrics_path):
        self.backtest_results_path = backtest_results_path
        self.output_metrics_path = output_metrics_path
        self.metrics = {}

    def load_data(self):
        self.df = pd.read_csv(self.backtest_results_path)
        self.df['Date'] = pd.to_datetime(self.df['Date'])
        self.df = self.df.sort_values(by='Date').reset_index(drop=True)
        self.df['Daily_PnL'] = (
            self.df['RGL'] - self.df['Transaction_Costs'] -
            self.df['Overnight_Cost'] - self.df['Slippage_Cost']
        )

    def calculate_metrics(self):
        trading_days_per_year = 252
        total_position_size_sum = self.df['Total_Position_Size'].sum()
        avg_position_size = self.df['Total_Position_Size'].mean()
        daily_pnl_sum = self.df['Daily_PnL'].sum()
        num_days = len(self.df)

        # Calculations
        self.metrics["Information_Ratio"] = calculate_information_ratio(self.df['Daily_PnL'])
        self.metrics["Sharpe_Ratio"] = calculate_sharpe_ratio(self.metrics["Information_Ratio"], trading_days_per_year)
        self.metrics["Max_Drawdown"] = calculate_drawdown(self.df['Daily_PnL'], avg_position_size)
        self.metrics["Annualized_Return"] = calculate_annualized_return(daily_pnl_sum, total_position_size_sum, num_days, trading_days_per_year)
        self.metrics["Margin"] = calculate_margin(daily_pnl_sum, total_position_size_sum)
        self.metrics["Turnover"] = calculate_turnover(avg_position_size, total_position_size_sum)

    def save_metrics(self):
        metrics_df = pd.DataFrame([self.metrics])
        metrics_df.to_csv(self.output_metrics_path, index=False)
        print(f"Portfolio metrics saved to {self.output_metrics_path}")

# Usage Example: 
if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    metric_configs = [
        {"backtest_results_path": os.path.join(base_dir, "strategy_2", "strat_2_SP400_CA2D_backtest_results.csv"), "output_metrics_path": os.path.join(base_dir, "strategy_2", "strat_2_SP400_CA2D_metrics.csv")},
        {"backtest_results_path": os.path.join(base_dir, "strategy_2", "strat_2_SP400_IR7D_backtest_results.csv"), "output_metrics_path": os.path.join(base_dir, "strategy_2", "strat_2_SP400_IR7D_metrics.csv")},
        {"backtest_results_path": os.path.join(base_dir, "strategy_2", "strat_2_SP600_IR2D_backtest_results.csv"), "output_metrics_path": os.path.join(base_dir, "strategy_2", "strat_2_SP600_IR2D_metrics.csv")}
    ]
    for config in metric_configs:
        print(f"Calculating metrics for {config['backtest_results_path']}...")
        metrics_calculator = PortfolioMetrics(config["backtest_results_path"], config["output_metrics_path"])
        metrics_calculator.load_data()
        metrics_calculator.calculate_metrics()
        metrics_calculator.save_metrics()
        print(f"Metrics completed.\n")