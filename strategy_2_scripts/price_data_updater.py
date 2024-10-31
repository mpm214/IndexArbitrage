import os
import pandas as pd
from utils import load_press_release_data, parse_filename, update_price_data_file

class PriceDataUpdater:
    def __init__(self, historical_data_folder, press_release_file_path, log_file_path):
        self.historical_data_folder = historical_data_folder
        self.press_release_file_path = press_release_file_path
        self.log_file_path = log_file_path
        self.press_release_data = load_press_release_data(press_release_file_path)

    def update_files(self):
        with open(self.log_file_path, 'w') as log_file:
            for file_name in os.listdir(self.historical_data_folder):
                if file_name.endswith("_Price_Data.csv"):
                    file_path = os.path.join(self.historical_data_folder, file_name)
                    file_info = parse_filename(file_name)
                    if file_info:
                        ticker, announced_date, effective_date = file_info
                        match = self.find_matching_row(ticker, announced_date, effective_date)
                        if not match.empty:
                            update_price_data_file(file_path, match, ticker)
                            print(f"Updated file: {file_name}")
                            self.merge_etf_data(file_path)
                        else:
                            self.log_unmatched_file(log_file, file_name, ticker, announced_date, effective_date)
                    else:
                        log_file.write(f"Error parsing dates from filename: {file_name}\n")
        print(f"Process completed. Log of unmatched files saved to {self.log_file_path}")

    def find_matching_row(self, ticker, announced_date, effective_date):
        return self.press_release_data[
            (self.press_release_data['Ticker'] == ticker) &
            (self.press_release_data['Announced_date'] == announced_date) &
            (self.press_release_data['Effective_date'] == effective_date)
        ]

    def log_unmatched_file(self, log_file, file_name, ticker, announced_date, effective_date):
        log_file.write(f"No match found for file: {file_name}\n")
        log_file.write(f"  Ticker: {ticker}, Announced Date: {announced_date}, Effective Date: {effective_date}\n\n")

    def merge_etf_data(self, file_path):
        price_data = pd.read_csv(file_path)
        if len(price_data) == 0:
            print(f"Skipping file with no data rows: {os.path.basename(file_path)}")
            return
        index_name = price_data.get("Index_Name", pd.Series(["Missing Index Name"])).iloc[0]
        etf_info = etf_data_paths.get(index_name, {"ticker": "Skip", "path": "Skip"})
        if etf_info['path'] == "Skip":
            print(f"Skipping file {os.path.basename(file_path)} as Index_Name is set to Skip.")
            return
        etf_data = pd.read_csv(etf_info['path'])
        etf_data['Date'] = pd.to_datetime(etf_data['Date'], utc=True).dt.strftime('%Y-%m-%d')
        merged_data = pd.merge(price_data, etf_data[['Date', 'Open', 'Close', 'Volume']],
                               on='Date', how='left', suffixes=('', f'_{etf_info["ticker"]}'))
        merged_data.to_csv(file_path, index=False)
        print(f"Merged ETF data for {index_name} into file: {os.path.basename(file_path)}")

# Usage example
if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    historical_data_folder = os.path.join(script_dir, "price_data")
    press_release_file_path = os.path.join(script_dir, "press_release_data.csv")
    log_file_path = os.path.join(script_dir, "no_match_log.txt")
    etf_data_paths = {
    "S&P MidCap 400": {"ticker": "IJR", "path": os.path.join(historical_data_folder, "IJR_Price_Data.csv")},
    "S&P SmallCap 600": {"ticker": "IJH", "path": os.path.join(historical_data_folder, "IJH_Price_Data.csv")},
    "S&P 500": {"ticker": "SPY", "path": os.path.join(historical_data_folder, "SPY_Price_Data.csv")},
    "S&P 100": {"ticker": "Skip", "path": "Skip"},
    "DJIA": {"ticker": "Skip", "path": "Skip"},
    "DJTA": {"ticker": "Skip", "path": "Skip"}
}
    updater = PriceDataUpdater(historical_data_folder, press_release_file_path, log_file_path)
    updater.update_files()
