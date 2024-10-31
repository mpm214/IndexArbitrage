import os
import pandas as pd

class TradeLogCreator:
    def __init__(self, input_file_path, output_file_path, exclude_indices, strategy_n, event_type):
        self.input_file_path = input_file_path
        self.output_file_path = output_file_path
        self.exclude_indices = exclude_indices
        self.strategy_n = strategy_n
        self.event_type = event_type

    def create_trade_log(self):
        df = pd.read_csv(self.input_file_path)
        df_filtered = df[~df['Index_Name'].isin(self.exclude_indices)]
        trade_log = df_filtered[
            (df_filtered['strategy_2_n'] == self.strategy_n) & 
            (df_filtered['Event_Type'] == self.event_type)
        ]
        trade_log.to_csv(self.output_file_path, index=False)
        print(f"Trade log created and saved to {self.output_file_path}")

# Example Usage
if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    output_file_path = os.path.join(base_dir, "strategy_2", "strategy_2_returns.csv")
    trade_log_configs = [
        {
            "output_file_path": os.path.join(base_dir, "strategy_2", "strat_2_SP400_IR7D_trade_log.csv"),
            "exclude_indices": ["DJIA", "DJTA", "S&P 100", "S&P 500", "S&P SmallCap 600"],
            "strategy_n": 7,
            "event_type": "Index Review"
        },
        {
            "output_file_path": os.path.join(base_dir, "strategy_2", "strat_2_SP400_CA2D_trade_log.csv"),
            "exclude_indices": ["DJIA", "DJTA", "S&P 100", "S&P 500", "S&P SmallCap 600"],
            "strategy_n": 2,
            "event_type": "Corporate Action"
        },
        {
            "output_file_path": os.path.join(base_dir, "strategy_2", "strat_2_SP600_IR2D_trade_log.csv"),
            "exclude_indices": ["DJIA", "DJTA", "S&P 100", "S&P 500", "S&P MidCap 400"],
            "strategy_n": 2,
            "event_type": "Index Review"
        }
    ]

    for config in trade_log_configs:
        creator = TradeLogCreator(
            input_file_path=output_file_path,
            output_file_path=config["output_file_path"],
            exclude_indices=config["exclude_indices"],
            strategy_n=config["strategy_n"],
            event_type=config["event_type"]
        )
        creator.create_trade_log()
