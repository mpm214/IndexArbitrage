import os
import logging
from utils import fetch_index_data, save_index_data_to_csv

class IndexDataDownloader:
    def __init__(self, tickers, start_date, end_date):
        self.tickers = tickers
        self.start_date = start_date
        self.end_date = end_date
        self.data_dict = {}
        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.output_dir = os.path.join(script_dir, 'price_data')
        os.makedirs(self.output_dir, exist_ok=True)
        logging.basicConfig(
            filename=os.path.join(self.output_dir, 'download_log.txt'),
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def download_data(self):
        for index_name, ticker_list in self.tickers.items():
            self.data_dict[index_name] = {}
            for ticker in ticker_list:
                print(f"Fetching data for {ticker} ({index_name})...")
                data = fetch_index_data(ticker, self.start_date, self.end_date)
                if data is not None:
                    self.data_dict[index_name][ticker] = data
                    save_index_data_to_csv(self.output_dir, ticker, data)
                    logging.info(f"Successfully fetched and saved data for {ticker}")
                else:
                    logging.warning(f"Data for {ticker} not available.")

#tickers = {
#    "S&P 500": ["SPY"],
#    "S&P 400": ["IJH"],
#    "S&P 600": ["IJR"]
#}
#start_date = "2024-01-01"
#end_date = "2024-10-25"
#index_downloader = IndexDataDownloader(tickers, start_date, end_date)
#index_downloader.download_data()