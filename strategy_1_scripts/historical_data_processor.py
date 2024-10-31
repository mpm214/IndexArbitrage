import os
import pandas as pd
from utils import calculate_adv20, calculate_returns, calculate_strategy_returns, calculate_volatility

class HistoricalDataProcessor:
    def __init__(self, historical_data_folder):
        self.historical_data_folder = historical_data_folder

    def process_all_files(self):
        for file_name in os.listdir(self.historical_data_folder):
            if file_name.endswith("_Price_Data.csv"):
                file_path = os.path.join(self.historical_data_folder, file_name)
                df = pd.read_csv(file_path)
                if 'Volume' in df.columns:
                    df = calculate_adv20(df)
                if 'Close' in df.columns and 'Open' in df.columns:
                    df = calculate_returns(df)
                if 'Return' in df.columns:
                    df = calculate_volatility(df)
                if 'Date' in df.columns:
                    df = calculate_strategy_returns(df, file_name)
                df.to_csv(file_path, index=False)
                print(f"Processed all metrics and saved to {file_path}")

# Usage Example:
#if __name__ == "__main__":
#    base_dir = os.path.dirname(os.path.abspath(__file__))
#    historical_data_folder = os.path.join(base_dir, "price_data")
#    processor = HistoricalDataProcessor(historical_data_folder)
#    processor.process_all_files()
#    print("Processing completed for all files.")
