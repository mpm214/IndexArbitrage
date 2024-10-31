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
    _, _, effective_date = dates
    open_etf_col = next((col for col in df.columns if col.startswith('Open_')), None)
    close_etf_col = next((col for col in df.columns if col.startswith('Close_')), None)
    if not open_etf_col or not close_etf_col:
        print(f"ETF-specific columns not found in file: {file_name}")
        return df
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce', utc=True).dt.tz_convert(None).dt.date
    df = df.sort_values(by=['Ticker', 'Index_Name', 'GICS_Sector', 'Event_Type', 'Date'])
    df['Previous_Close_7D'] = df.groupby(['Ticker', 'Index_Name', 'GICS_Sector', 'Event_Type'])['Close'].shift(6)
    df['Previous_Close'] = df.groupby(['Ticker', 'Index_Name', 'GICS_Sector', 'Event_Type'])['Close'].shift(1)
    period_df = df[df['Date'] >= effective_date].copy()
    if period_df.empty:
        print(f"No data found from the effective date onward for {file_name}")
        return df
    period_df['strategy_2_n'] = range(1, len(period_df) + 1)
    period_df['Previous_Close'] = period_df['Close'].shift(1)
    period_df['strategy_2'] = period_df.apply(
        lambda row: (row['Close'] - row['Open']) / row['Open'] if row['strategy_2_n'] == 1 
        else (row['Close'] - row['Previous_Close']) / row['Previous_Close'], axis=1
    )
    period_df['strategy_2_md'] = period_df['strategy_2'].expanding(min_periods=1).mean()
    period_df['strategy_2_n_etf'] = range(1, len(period_df) + 1)
    period_df['Previous_Close_etf'] = period_df[close_etf_col].shift(1)
    period_df['strategy_2_etf'] = period_df.apply(
        lambda row: (row[close_etf_col] - row[open_etf_col]) / row[open_etf_col] if row['strategy_2_n_etf'] == 1 
        else (row[close_etf_col] - row['Previous_Close_etf']) / row['Previous_Close_etf'], axis=1
    )
    period_df['strategy_2_md_etf'] = period_df['strategy_2_etf'].expanding(min_periods=1).mean()
    df = df.merge(
        period_df[['Date', 'strategy_2_n', 'strategy_2', 'strategy_2_md', 
                   'strategy_2_n_etf', 'strategy_2_etf', 'strategy_2_md_etf']],
        on='Date', how='left'
    )
    if 'strategy_2_md_etf' in df.columns and 'strategy_2_md' in df.columns:
        df['strategy_2_net'] = df['strategy_2_md'] - df['strategy_2_md_etf']
    else:
        print(f"Skipping strategy_2_net calculation for {file_name} due to missing columns.")
    return df


# Class: strategy_2_returns

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

# Class: strategy_2_analysis

def calculate_group_stats(data):
    stats_summary = []
    grouped_data = data.groupby(['strategy_2_n', 'Index_Name', 'Event_Type'])
    for (n_value, index_name, event_type), subset in grouped_data:
        stats = {
            "strategy_2_n": n_value,
            "Index_Name": index_name,
            "Event_Type": event_type,
            "count": subset['strategy_2_md'].count(),
            "mean": subset['strategy_2_md'].mean(),
            "median": subset['strategy_2_md'].median(),
            "std_dev": subset['strategy_2_md'].std(),
            "min": subset['strategy_2_md'].min(),
            "max": subset['strategy_2_md'].max(),
            "5PCT": np.percentile(subset['strategy_2_md'], 5),
            "25PCT": np.percentile(subset['strategy_2_md'], 25),
            "50PCT": np.percentile(subset['strategy_2_md'], 50),
            "75PCT": np.percentile(subset['strategy_2_md'], 75),
            "95PCT": np.percentile(subset['strategy_2_md'], 95),
            "Net_count": subset['strategy_2_net'].count(),
            "Net_mean": subset['strategy_2_net'].mean(),
            "Net_median": subset['strategy_2_net'].median(),
            "Net_std_dev": subset['strategy_2_net'].std(),
            "Net_min": subset['strategy_2_net'].min(),
            "Net_max": subset['strategy_2_net'].max(),
            "Net_5PCT": np.percentile(subset['strategy_2_net'], 5),
            "Net_25PCT": np.percentile(subset['strategy_2_net'], 25),
            "Net_50PCT": np.percentile(subset['strategy_2_net'], 50),
            "Net_75PCT": np.percentile(subset['strategy_2_net'], 75),
            "Net_95PCT": np.percentile(subset['strategy_2_net'], 95),
        }
        stats_summary.append(stats)
    return pd.DataFrame(stats_summary)

def save_pdf_plots(data, output_pdf_path):
    grouped_data = data.groupby(['strategy_2_n', 'Index_Name', 'Event_Type'])
    with PdfPages(output_pdf_path) as pdf:
        for (n_value, index_name, event_type), subset in grouped_data:
            if subset['strategy_2_md'].count() == 1:
                print(f"Skipped PDF plot for strategy_2_n = {n_value}, Index_Name = {index_name}, Event_Type = {event_type} due to zero variance")
                continue
            plt.figure(figsize=(8, 6))
            sns.kdeplot(subset['strategy_2_md'], fill=True)
            plt.title(f"PDF: {event_type}, {index_name} - Holding period = {n_value} days")
            plt.xlabel("Stock's Avg. Daily Return")
            plt.ylabel("Density")
            pdf.savefig()
            plt.close()

            plt.figure(figsize=(8, 6))
            sns.kdeplot(subset['strategy_2_net'], fill=True)
            plt.title(f"Stock Less ETF Avg. Daily Returns - {event_type}, {index_name}, Hold = {n_value} days")
            plt.xlabel("ETF Less Stock Avg. Daily Returns")
            plt.ylabel("Density")
            pdf.savefig()
            plt.close()

            print(f"Added PDF plot for strategy_2_n = {n_value}, Index_Name = {index_name}, Event_Type = {event_type}")

def save_statistics_summary(stats_df, output_stats_path):
    stats_df.to_csv(output_stats_path, index=False)
    print(f"Summary statistics saved to {output_stats_path}")

# Class: strategy_2_selection

def load_data(file_path):
    return pd.read_csv(file_path)

def filter_candidates(data, strategy_n, net_mean_sign):
    if net_mean_sign == 'negative':
        return data[(data['strategy_2_n'] == strategy_n) & (data['Net_mean'] < 0)]
    elif net_mean_sign == 'positive':
        return data[(data['strategy_2_n'] == strategy_n) & (data['Net_mean'] > 0)]
    else:
        raise ValueError("net_mean_sign should be either 'positive' or 'negative'")

# Class: strategy_2_backtest_engine

def get_sofr_rate(trade_date, sofr_df):
    valid_sofr_dates = sofr_df[sofr_df['DATE'] <= pd.to_datetime(trade_date)]
    return valid_sofr_dates.iloc[-1]['SOFR'] if not valid_sofr_dates.empty else None

def calculate_slippage_cost(data, alpha, beta):
    data['Slippage_Cost'] = (
        alpha * ((data['Trade_Limit'] / data['ADV20']) ** beta) * data['Previous_Close'] * data['Volatility'] +
        alpha * ((data['Trade_Limit'] / data['ADV20']) ** beta) * data['Close'] * data['Volatility']
    )
    data = data.dropna(subset=['ADV20', 'Volatility', 'Slippage_Cost'])
    return data['Slippage_Cost'].sum()

def get_sofr_rates(trade_date, sofr_df, days_back=7):
    trade_date = pd.to_datetime(trade_date)
    valid_sofr_dates = sofr_df[sofr_df['DATE'] <= trade_date]
    sofr_rates = valid_sofr_dates.iloc[-days_back:]['SOFR'].values
    if len(sofr_rates) < days_back:
        sofr_rates = np.pad(sofr_rates, (days_back - len(sofr_rates), 0), mode='edge')
    short_overnight_rates = [(rate / 100 + 0.01) * (1 / 365) for rate in sofr_rates]
    return sum(short_overnight_rates)

# Class: strategy_2_portfolio_metrics

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
