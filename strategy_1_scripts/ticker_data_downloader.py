import pandas as pd
import os
import logging
from datetime import timedelta
from tqdm import tqdm
from utils import fetch_ticker_data, save_to_csv

class TickerDataDownloader:
    def __init__(self, cleaned_data_filename):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.cleaned_data_path = os.path.join(script_dir, cleaned_data_filename)
        self.output_dir = os.path.join(script_dir, 'price_data')
        self.log_file = os.path.join(self.output_dir, 'download_log.txt')
        os.makedirs(self.output_dir, exist_ok=True)
        if not os.path.exists(self.log_file):
            open(self.log_file, 'a').close()
        logging.basicConfig(
            filename=self.log_file,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filemode='a'
        )
    
    def get_addition_tickers(self):
        cleaned_data = pd.read_csv(self.cleaned_data_path)
        return cleaned_data[cleaned_data['Action'] == 'Addition']

    def download_all_ticker_data(self):
        addition_rows = self.get_addition_tickers()
        for _, row in tqdm(addition_rows.iterrows(), total=addition_rows.shape[0], desc="Downloading Ticker Data"):
            ticker = row['Ticker']
            announced_date = pd.to_datetime(row['Announced']).strftime('%Y-%m-%d')
            effective_date = pd.to_datetime(row['Effective_Date']).strftime('%Y-%m-%d')
            start_date = (pd.to_datetime(announced_date) - pd.DateOffset(months=1)).strftime('%Y-%m-%d')
            end_date = min(pd.to_datetime('today') - timedelta(days=1), pd.to_datetime(effective_date) + pd.DateOffset(months=1)).strftime('%Y-%m-%d')
            ticker_data = fetch_ticker_data(ticker, start_date, end_date)
            if ticker_data is not None:
                save_to_csv(self.output_dir, ticker, announced_date, effective_date, ticker_data)
            else:
                logging.warning(f"Data for {ticker} could not be fetched.")

# Usage Example
#if __name__ == "__main__":
    #cleaned_data_filename = 'press_release_data.csv'
    #downloader = TickerDataDownloader(cleaned_data_filename)
    #downloader.download_all_ticker_data()
