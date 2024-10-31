import pandas as pd
from datetime import datetime
from utils import search_press_website, extract_table_from_url

class SPGlobalScraper:
    def __init__(self, start_date: str, end_date: str):
        self.start_date = datetime.strptime(start_date, '%Y-%m-%d')
        self.end_date = datetime.strptime(end_date, '%Y-%m-%d')
        self.years = self._get_years_in_period()
        
    def _get_years_in_period(self):
        return list(range(self.start_date.year, self.end_date.year + 1))
    
    def extract_tables_from_all_years(self):
        combined_df = pd.DataFrame() 
        for year in self.years:
            print(f"Processing year: {year}")
            urls = search_press_website(year) 
            for index, url in enumerate(urls):
                print(f"Processing URL {index + 1}/{len(urls)} for {year}: {url}")
                df = extract_table_from_url(url) 
                if df is not None:
                    combined_df = pd.concat([combined_df, df], ignore_index=True)
        filename = f"press_releases_{self.start_date.year}_{self.end_date.year}.csv"
        combined_df.to_csv(filename, index=False)
        print(f"Combined table content saved to {filename}.")

#if __name__ == "__main__":
    #scraper = SPGlobalScraper(start_date="2021-01-01", end_date="2021-12-31")
    #scraper.extract_tables_from_all_years()
