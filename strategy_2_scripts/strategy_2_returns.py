import os
import pandas as pd
from utils import load_and_filter_dataframes, save_dataframe

class DataAggregator:
    def __init__(self, historical_data_folder, output_file_path):
        self.historical_data_folder = historical_data_folder
        self.output_file_path = output_file_path
        self.required_columns = ["strategy_2_n", "strategy_2_md", "strategy_2_md_etf", "strategy_2_net"]
        output_dir = os.path.dirname(self.output_file_path)
        os.makedirs(output_dir, exist_ok=True)

    def aggregate_columns_for_selected_rows(self):
        all_data = load_and_filter_dataframes(self.historical_data_folder, self.required_columns)
        if all_data:
            aggregated_df = pd.concat(all_data, ignore_index=True)
            save_dataframe(aggregated_df, self.output_file_path)
        else:
            print("No data to aggregate.")

# Usage Example:
if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    historical_data_folder = os.path.join(base_dir, "price_data")
    output_file_path = os.path.join(base_dir, "strategy_2", "strategy_2_returns.csv")
    aggregator = DataAggregator(historical_data_folder, output_file_path)
    aggregator.aggregate_columns_for_selected_rows()
