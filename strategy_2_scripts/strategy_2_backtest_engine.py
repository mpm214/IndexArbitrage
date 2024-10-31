import os
import pandas as pd
import numpy as np
from utils import calculate_slippage_cost, get_sofr_rate, get_sofr_rates

class BacktestEngine:
    def __init__(self, trade_log_file_path, sofr_file_path, output_file_path, strategy_type, portfolio_cap=5000000, alpha=0.2, beta=0.7):
        self.trade_log_file_path = trade_log_file_path
        self.sofr_file_path = sofr_file_path
        self.output_file_path = output_file_path
        self.strategy_type = strategy_type
        self.portfolio_cap = portfolio_cap
        self.alpha = alpha
        self.beta = beta
        self.cumulative_net_pnl = 0
        self.portfolio_values = []
        self.trade_log_df = pd.read_csv(self.trade_log_file_path)
        self.sofr_df = pd.read_csv(self.sofr_file_path)
        self.sofr_df['DATE'] = pd.to_datetime(self.sofr_df['DATE']).sort_values().reset_index(drop=True)

    def run_backtest(self):
        unique_dates = sorted(self.trade_log_df['Date'].unique())
        for date in unique_dates:
            daily_trades = self.trade_log_df[self.trade_log_df['Date'] == date].copy()
            daily_trades = self.process_daily_trades(daily_trades) 
            if self.strategy_type == "long":
                sofr_rate = get_sofr_rate(date, self.sofr_df)
                overnight_cost = self.calculate_long_overnight_cost(sofr_rate, daily_trades['Position_Size'].sum())
            elif self.strategy_type == "short":
                sofr_rates = get_sofr_rates(date, self.sofr_df, days_back=6)
                overnight_cost = self.calculate_short_overnight_cost(sofr_rates, daily_trades['Position_Size'].sum())
            daily_net_pnl = daily_trades['RGL'].sum() - daily_trades['Transaction_Costs'].sum() - \
                            daily_trades['Slippage_Cost'].sum() - overnight_cost
            self.cumulative_net_pnl += daily_net_pnl
            self.portfolio_values.append({
                "Date": date,
                "Total_Position_Size": daily_trades['Position_Size'].sum(),
                "Total_Sale_Proceeds": daily_trades['Sale_Proceeds'].sum(),
                "RGL": daily_trades['RGL'].sum(),
                "Transaction_Costs": daily_trades['Transaction_Costs'].sum(),
                "Slippage_Cost": daily_trades['Slippage_Cost'].sum(),
                "Overnight_Cost": overnight_cost,
                "Cumulative_Net_PnL": self.cumulative_net_pnl
            })

    def process_daily_trades(self, daily_trades):
        daily_trades['Size_Limit'] = daily_trades['ADV20'] * 0.01
        daily_trades['Trade_Limit'] = daily_trades[['Size_Limit', 'Volume']].min(axis=1)
        daily_trades['Position_Size'] = daily_trades['Trade_Limit'] * (daily_trades['Previous_Close'] if self.strategy_type == "long" else daily_trades['Previous_Close_7D'])
        
        total_position_size = daily_trades['Position_Size'].sum()
        if total_position_size > self.portfolio_cap:
            scale_factor = self.portfolio_cap / total_position_size
            daily_trades['Position_Size'] *= scale_factor
            daily_trades['Trade_Limit'] *= scale_factor

        daily_trades['Sale_Proceeds'] = daily_trades['Trade_Limit'] * daily_trades['Close']
        if self.strategy_type == "short":
            daily_trades['RGL'] = daily_trades['Position_Size'] - daily_trades['Sale_Proceeds']
        else:
            daily_trades['RGL'] = daily_trades['Sale_Proceeds'] - daily_trades['Position_Size']

        daily_trades['Transaction_Costs'] = 2 * daily_trades['Trade_Limit'] * 0.01
        daily_trades['Slippage_Cost'] = calculate_slippage_cost(daily_trades, self.alpha, self.beta)
        return daily_trades

    def calculate_long_overnight_cost(self, sofr_rate, total_position_size):
        if sofr_rate is not None:
            long_overnight_rate = (sofr_rate / 100 + 0.015) * (1 / 365)
            return long_overnight_rate * total_position_size
        else:
            print("No SOFR rate found.")
            return 0

    def calculate_short_overnight_cost(self, sofr_rates, total_position_size):
        if sofr_rates:
            return sofr_rates * total_position_size
        else:
            print("No SOFR rates found for the past 7 days.")
            return 0

    def save_results(self):
        portfolio_values_df = pd.DataFrame(self.portfolio_values)
        portfolio_values_df.to_csv(self.output_file_path, index=False)
        print(f"Portfolio backtest results saved to {self.output_file_path}")

# Usage Example
if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    backtest_configs = [
        {"trade_log_file_path": os.path.join(base_dir, "strategy_2", "strat_2_SP400_IR7D_trade_log.csv"), 
         "sofr_file_path": os.path.join(base_dir, "Overnight_Costs", "Cleaned_SOFR.csv"), 
         "output_file_path": os.path.join(base_dir, "strategy_2", "strat_2_SP400_IR7D_backtest_results.csv"),
         "strategy_type": "short"},
        
        {"trade_log_file_path": os.path.join(base_dir, "strategy_2", "strat_2_SP400_CA2D_trade_log.csv"), 
         "sofr_file_path": os.path.join(base_dir, "Overnight_Costs", "Cleaned_SOFR.csv"), 
         "output_file_path": os.path.join(base_dir, "strategy_2", "strat_2_SP400_CA2D_backtest_results.csv"),
         "strategy_type": "long"},
        
        {"trade_log_file_path": os.path.join(base_dir, "strategy_2", "strat_2_SP600_IR2D_trade_log.csv"), 
         "sofr_file_path": os.path.join(base_dir, "Overnight_Costs", "Cleaned_SOFR.csv"), 
         "output_file_path": os.path.join(base_dir, "strategy_2", "strat_2_SP600_IR2D_backtest_results.csv"),
         "strategy_type": "long"}
    ]
    for config in backtest_configs:
        print(f"Running backtest for {config['output_file_path']}...")
        backtest_engine = BacktestEngine(
            config["trade_log_file_path"], 
            config["sofr_file_path"], 
            config["output_file_path"], 
            config["strategy_type"]
        )
        backtest_engine.run_backtest()
        backtest_engine.save_results()
        print(f"Completed backtest for {config['output_file_path']}")
