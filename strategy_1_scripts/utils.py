import requests
from bs4 import BeautifulSoup
import pandas as pd
import yfinance as yf
import os
import logging
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.backends.backend_pdf import PdfPages
import numpy as np

# Class: sp_global_scraper

def search_press_website(year):
    search_url = f"https://press.spglobal.com/index.php?s=2429&l=100&year={year}&keywords=%22Set%2Bto%2BJoin%22"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
    response = requests.get(search_url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')
    result_list = soup.find("ul", class_="wd_layout-simple wd_item_list")
    if not result_list:
        print(f"No results found for year {year}.")
        return []
    urls = [link.get("href") for link in result_list.find_all("a", href=True)]
    return urls

def extract_table_from_url(url):
    printable_url = url + "?printable=1"
    response = requests.get(printable_url)
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find("table")
    if not table:
        print(f"No table found in the page: {url}")
        return None
    headers = ["Announced"]
    data = []
    for th in table.find("tr").find_all("td"):
        header_text = th.get_text(strip=True)
        if header_text in ["GICSSector", "GICS Sub-Industry"]:
            header_text = "GICS Sector"
        elif header_text in ["EffectiveDate"]:
            header_text = "Effective Date"
        headers.append(header_text)
    headers.append("Event_Type")
    announcement_date = soup.find("span", class_="xn-chron").get_text(strip=True) if soup.find("span", class_="xn-chron") else "Unknown Date"
    event_type = "Index Review" if "quarterly rebalance" in soup.get_text().lower() else "Corporate Action"
    for row in table.find_all("tr")[1:]:  # Skip header row
        cols = row.find_all("td")
        row_data = [announcement_date] + [col.get_text(strip=True) for col in cols] + [event_type]
        if len(row_data) == len(headers):
            data.append(row_data)
        else:
            print(f"Skipping row due to column mismatch: {row_data}")
    df = pd.DataFrame(data, columns=headers) if data else None
    if df is not None:
        df["Effective Date"] = df["Effective Date"].replace("", pd.NA).ffill()
        df["Index Name"] = df["Index Name"].replace("", pd.NA).ffill()
    return df

# Class: ticker_data_dowloader

def fetch_earliest_available_date(ticker):
    try:
        ticker_obj = yf.Ticker(ticker)
        historical_data = ticker_obj.history(period="max", interval="1d")
        if not historical_data.empty:
            return historical_data.index.min().strftime('%Y-%m-%d')
        return None
    except Exception as e:
        logging.error(f"Error fetching earliest date for {ticker}: {e}")
        return None

def fetch_ticker_data(ticker, start_date, end_date):
    try:
        ticker_data = yf.Ticker(ticker).history(start=start_date, end=end_date, interval="1d")
        if ticker_data.empty:
            earliest_date = fetch_earliest_available_date(ticker)
            if earliest_date:
                start_date = max(pd.to_datetime(start_date), pd.to_datetime(earliest_date)).strftime('%Y-%m-%d')
                ticker_data = yf.Ticker(ticker).history(start=start_date, end=end_date, interval="1d")
        return ticker_data.reset_index() if not ticker_data.empty else None
    except Exception as e:
        logging.error(f"Error fetching data for {ticker}: {e}")
        return None

def save_to_csv(output_dir, ticker, announced_date, effective_date, data):
    filename = f"{ticker}_{announced_date.replace('-', '')}_{effective_date.replace('-', '')}_Price_Data.csv"
    output_path = os.path.join(output_dir, filename)
    data.to_csv(output_path, index=False)
    print(f"Data for {ticker} saved to {output_path}")


# Class: index_data_downloader

def fetch_index_data(ticker, start_date, end_date):
    try:
        data = yf.Ticker(ticker).history(start=start_date, end=end_date, interval="1d")
        return data if not data.empty else None
    except Exception as e:
        logging.error(f"Error fetching data for {ticker}: {e}")
        return None

def save_index_data_to_csv(output_dir, ticker, data):
    filename = f"{ticker}_Price_Data.csv"
    output_path = os.path.join(output_dir, filename)
    data.to_csv(output_path)
    print(f"Data for {ticker} saved to {output_path}")

# Class: price_data_updater

def load_press_release_data(press_release_file_path):
    press_release_data = pd.read_csv(press_release_file_path)
    addition_data = press_release_data[press_release_data['Action'] == "Addition"].copy()
    addition_data['Announced_date'] = pd.to_datetime(addition_data['Announced'], format='%m/%d/%Y').dt.date
    addition_data['Effective_date'] = pd.to_datetime(addition_data['Effective_Date'], format='%m/%d/%Y').dt.date
    return addition_data

def parse_filename(file_name):
    base_name = file_name.replace("_Price_Data.csv", "")
    parts = base_name.split('_')
    if len(parts) != 3:
        print(f"Error: Expected 3 parts in filename, got {len(parts)} - {file_name}")
        return None
    try:
        ticker = parts[0]
        announced_date = pd.to_datetime(parts[1], format='%Y%m%d').date()
        effective_date = pd.to_datetime(parts[2], format='%Y%m%d').date()
        return ticker, announced_date, effective_date
    except (IndexError, ValueError) as e:
        print(f"Error parsing dates in filename {file_name}: {e}")
        return None

def update_price_data_file(file_path, match_row, ticker):
    index_name = match_row.iloc[0]['Index_Name']
    gics_sector = match_row.iloc[0]['GICS_Sector']
    event_type = match_row.iloc[0]['Event_Type']
    price_data = pd.read_csv(file_path)
    price_data['Ticker'] = ticker
    price_data['Index_Name'] = index_name
    price_data['GICS_Sector'] = gics_sector
    price_data['Event_Type'] = event_type
    price_data.to_csv(file_path, index=False)

# Class: historical_data_processor

def calculate_adv20(df):
    df['ADV20'] = df['Volume'].rolling(window=20, min_periods=1).mean()
    return df

def calculate_returns(df):
    df['Return'] = df['Close'].pct_change()
    if not df.empty:
        df.loc[0, 'Return'] = (df.loc[0, 'Close'] - df.loc[0, 'Open']) / df.loc[0, 'Open']
    return df

def calculate_volatility(df):
    df['Volatility'] = df['Return'].rolling(window=20, min_periods=1).std()
    return df

def calculate_strategy_returns(df, file_name):
    dates = parse_filename(file_name)
    if not dates:
        print(f"Error parsing dates for {file_name}")
        return df
    ticker, announced_date, effective_date = dates
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce', utc=True).dt.tz_convert(None)
    df['Date'] = df['Date'].dt.date
    df.sort_values(by='Date', inplace=True)
    period_df = df[(df['Date'] > announced_date) & (df['Date'] <= effective_date)].copy()
    if period_df.empty:
        print(f"No data found in date range for {file_name}")
        return df
    start_price = period_df.iloc[0]['Open']
    period_df['strategy_1_n'] = range(1, len(period_df) + 1)
    period_df['strategy_1'] = period_df['Close'].pct_change().fillna(0)
    strategy_1_md_values = []
    for idx, row in period_df.iterrows():
        days_n = row['strategy_1_n']
        end_price = row['Close']
        total_return = (end_price / start_price) - 1
        arithmetic_return = total_return / days_n
        strategy_1_md_values.append(arithmetic_return)
    period_df['strategy_1_md'] = strategy_1_md_values
    df = df.merge(period_df[['Date', 'strategy_1_n', 'strategy_1', 'strategy_1_md']], on='Date', how='left')
    return df

# Class: strategy_1_returns

def load_and_filter_dataframes(folder_path, required_columns):
    all_data = []
    for file_name in os.listdir(folder_path):
        if file_name.endswith("_Price_Data.csv"):
            file_path = os.path.join(folder_path, file_name)
            df = pd.read_csv(file_path)
            if all(column in df.columns for column in required_columns):
                df = df.dropna(subset=required_columns)
                all_data.append(df)
            else:
                print(f"Skipping file {file_name} as it does not contain all required columns.")
    return all_data

def save_dataframe(df, output_path):
    df.to_csv(output_path, index=False)
    print(f"Aggregated data saved to {output_path}")

# Class: strategy_1_analysis

def calculate_group_stats(data):
    stats_summary = []
    grouped_data = data.groupby(['strategy_1_n', 'Index_Name', 'Event_Type'])
    for (n_value, index_name, event_type), subset in grouped_data:
        stats = {
            "strategy_1_n": n_value,
            "Index_Name": index_name,
            "Event_Type": event_type,
            "count": subset['strategy_1_md'].count(),
            "mean": subset['strategy_1_md'].mean(),
            "median": subset['strategy_1_md'].median(),
            "std_dev": subset['strategy_1_md'].std(),
            "min": subset['strategy_1_md'].min(),
            "max": subset['strategy_1_md'].max(),
            "5PCT": np.percentile(subset['strategy_1_md'], 5),
            "25PCT": np.percentile(subset['strategy_1_md'], 25),
            "50PCT": np.percentile(subset['strategy_1_md'], 50),
            "75PCT": np.percentile(subset['strategy_1_md'], 75),
            "95PCT": np.percentile(subset['strategy_1_md'], 95),
        }
        stats_summary.append(stats)
    return pd.DataFrame(stats_summary)

def save_pdf_plots(data, output_pdf_path):
    grouped_data = data.groupby(['strategy_1_n', 'Index_Name', 'Event_Type'])
    with PdfPages(output_pdf_path) as pdf:
        for (n_value, index_name, event_type), subset in grouped_data:
            if subset['strategy_1_md'].count() == 1:
                print(f"Skipped PDF plot for strategy_1_n = {n_value}, Index_Name = {index_name}, Event_Type = {event_type} due to zero variance")
                continue
            plt.figure(figsize=(8, 6))
            sns.kdeplot(subset['strategy_1_md'], fill=True)
            plt.title(f"PDF: {event_type}, {index_name} - Holding period = {n_value} days")
            plt.xlabel("Avg. Daily Return")
            plt.ylabel("Density")
            pdf.savefig()
            plt.close()
            print(f"Added PDF plot for strategy_1_n = {n_value}, Index_Name = {index_name}, Event_Type = {event_type}")

def save_statistics_summary(stats_df, output_stats_path):
    stats_df.to_csv(output_stats_path, index=False)
    print(f"Summary statistics saved to {output_stats_path}")

# Class: strategy_1_backtest_engine

def get_sofr_rate(trade_date, sofr_df):
    valid_sofr_dates = sofr_df[sofr_df['DATE'] <= pd.to_datetime(trade_date)]
    return valid_sofr_dates.iloc[-1]['SOFR'] if not valid_sofr_dates.empty else None

def calculate_slippage_cost(data, alpha, beta):
    data['Slippage_Cost'] = (
        alpha * ((data['Trade_Limit'] / data['ADV20']) ** beta) * data['Open'] * data['Volatility'] +
        alpha * ((data['Trade_Limit'] / data['ADV20']) ** beta) * data['Close'] * data['Volatility']
    )
    data = data.dropna(subset=['ADV20', 'Volatility', 'Slippage_Cost'])
    return data['Slippage_Cost'].sum()

# Class: strategy_1_portfolio_metrics

def calculate_information_ratio(daily_pnl):
    mean_pnl = daily_pnl.mean()
    std_dev_pnl = daily_pnl.std()
    return mean_pnl / std_dev_pnl if std_dev_pnl != 0 else np.nan

def calculate_sharpe_ratio(information_ratio, trading_days_per_year=252):
    return np.sqrt(trading_days_per_year) * information_ratio

def calculate_drawdown(daily_pnl, avg_position_size):
    return daily_pnl.min() / avg_position_size

def calculate_annualized_return(daily_pnl_sum, avg_position_size, num_days, trading_days_per_year=252):
    return (daily_pnl_sum * (trading_days_per_year / num_days)) / avg_position_size

def calculate_margin(daily_pnl_sum, total_position_size):
    return daily_pnl_sum / total_position_size

def calculate_turnover(avg_position_size, total_position_size):
    return avg_position_size / total_position_size
