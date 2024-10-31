import pandas as pd
import os

class TradeLogCreator:
    def __init__(self, input_file_path, output_file_path, exclude_indices):
        self.input_file_path = input_file_path
        self.output_file_path = output_file_path
        self.exclude_indices = exclude_indices

    def create_trade_log(self):
        df = pd.read_csv(self.input_file_path)
        df_filtered = df[~df['Index_Name'].isin(self.exclude_indices)]
        trade_log = df_filtered[(df_filtered['strategy_1_n'] == 1) & (df_filtered['Event_Type'] == "Corporate Action")]
        trade_log.to_csv(self.output_file_path, index=False)
        print(f"Trade log created and saved to {self.output_file_path}")

# Usage Example:
#if __name__ == "__main__":
#    base_dir = os.path.dirname(os.path.abspath(__file__))
#    output_file_path = os.path.join(base_dir, "strategy_1", "strategy_1_returns.csv")
#    trade_log_configs = [
#        {"output_file_path": os.path.join(base_dir, "strategy_1", "strat_1_CA1D_trade_log.csv"), "exclude_indices": ["DJIA", "DJTA", "S&P 100"]},
#        {"output_file_path": os.path.join(base_dir, "strategy_1", "strat_1_SP500_CA1D_trade_log.csv"), "exclude_indices": ["DJIA", "DJTA", "S&P 100", "S&P SmallCap 600", "S&P MidCap 400"]},
#        {"output_file_path": os.path.join(base_dir, "strategy_1", "strat_1_SP600_1D_trade_log.csv"), "exclude_indices": ["DJIA", "DJTA", "S&P 100", "S&P 500", "S&P MidCap 400"]}
#    ]
#    for config in trade_log_configs:
#        trade_log_creator = TradeLogCreator(output_file_path, config["output_file_path"], config["exclude_indices"])
#        trade_log_creator.create_trade_log()
