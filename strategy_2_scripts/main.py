import os
import pandas as pd
from sp_global_scraper import SPGlobalScraper
from ticker_data_downloader import TickerDataDownloader
from index_data_downloader import IndexDataDownloader
from price_data_updater import PriceDataUpdater
from historical_data_processor import HistoricalDataProcessor
from strategy_2_returns import DataAggregator
from strategy_2_analysis import StrategyAnalysis
from strategy_2_trade_log_creator import TradeLogCreator
from strategy_2_backtest_engine import BacktestEngine
from strategy_2_equity_curve_plot import EquityCurvePlotter
from strategy_2_portfolio_metrics import PortfolioMetrics
from strategy_2_selection import MeanReversionStrategy

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    historical_data_folder = os.path.join(base_dir, "price_data")
    press_release_file_path = os.path.join(base_dir, "press_release_data.csv")
    log_file_path = os.path.join(base_dir, "no_match_log.txt")
    output_file_path = os.path.join(base_dir, "strategy_2", "strategy_2_returns.csv")
    output_pdf_path = os.path.join(base_dir, "strategy_2", "strategy_2_KDE.pdf")
    output_stats_path = os.path.join(base_dir, "strategy_2", "strategy_2_stats.csv")
    long_output_path = os.path.join(base_dir, "strategy_2", "long_reversion_confirmed.csv")
    short_output_path = os.path.join(base_dir, "strategy_2", "short_reversion_confirmed.csv")

    tickers = {"S&P 500": ["SPY"], "S&P 400": ["IJH"], "S&P 600": ["IJR"]}
    index_start_date = "2020-01-01"
    index_end_date = "2024-10-25"

    # Step 1: Scrape press releases
    print("Step 1: Scraping press releases...")
    scraper = SPGlobalScraper(start_date="2020-01-01", end_date="2024-12-31")
    scraper.extract_tables_from_all_years()

    # Step 2: Process the scraped press release data
    print("Step 2: Processing scraped press release data...")
    df = pd.read_csv('press_releases_2020_2024.csv')
    df['Effective_Date'] = pd.to_datetime(df.get('Effective_Date'), errors='coerce')
    df['Announced'] = pd.to_datetime(df.get('Announced'), errors='coerce')
    df['N_days'] = (df['Effective_Date'] - df['Announced']).dt.days + 1
    df.to_csv(press_release_file_path, index=False)
    print(f"Processed press release data saved to {press_release_file_path}")

    # Step 3: Download ticker data based on press release additions
    print("Step 3: Downloading ticker data based on press release additions...")
    downloader = TickerDataDownloader(press_release_file_path)
    downloader.download_all_ticker_data()

    # Step 4: Download index data for specified tickers
    print("Step 4: Downloading index data for specified tickers...")
    index_downloader = IndexDataDownloader(tickers, index_start_date, index_end_date)
    index_downloader.download_data()

    # Step 5: Update price data files with press release metadata
    print("Step 5: Updating price data files with press release data...")
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

    # Step 6: Process additional metrics (ADV20, Returns, Volatility, Strategy Returns)
    print("Step 6: Calculating ADV20, Returns, Volatility, and Strategy Returns for each file...")
    processor = HistoricalDataProcessor(historical_data_folder)
    processor.process_all_files()

    # Step 7: Aggregate columns for selected rows into a single output
    print("Step 7: Aggregating columns for selected rows across all files...")
    aggregator = DataAggregator(historical_data_folder, output_file_path)
    aggregator.aggregate_columns_for_selected_rows()

    # Step 8: Generate PDF plots and calculate statistics, then identify best subsets of data based on return profile
    print("Step 8: Generating PDFs and calculating statistics by sector and event type...")
    analysis = StrategyAnalysis(output_file_path, output_pdf_path, output_stats_path)
    analysis.load_data()
    analysis.analyze_and_save()
    strategy = MeanReversionStrategy(output_stats_path)
    long_candidates, short_candidates = strategy.identify_reversion_candidates()
    strategy.save_results(long_candidates, short_candidates, long_output_path, short_output_path)

    # Step 9: Create trade logs with different configurations
    print("Step 9: Creating trade logs with different index exclusion configurations...")
    trade_log_configs = [
        {"output_file_path": os.path.join(base_dir, "strategy_2", "strat_2_SP400_IR7D_trade_log.csv"), "exclude_indices": ["DJIA", "DJTA", "S&P 100", "S&P 500", "S&P SmallCap 600"], 
         "strategy_n": 7, "event_type": "Index Review"},
        {"output_file_path": os.path.join(base_dir, "strategy_2", "strat_2_SP400_CA2D_trade_log.csv"), "exclude_indices": ["DJIA", "DJTA", "S&P 100", "S&P 500", "S&P SmallCap 600"],
         "strategy_n": 2, "event_type": "Corporate Action"},
        {"output_file_path": os.path.join(base_dir, "strategy_2", "strat_2_SP600_IR2D_trade_log.csv"), "exclude_indices": ["DJIA", "DJTA", "S&P 100", "S&P 500", "S&P MidCap 400"], 
        "strategy_n": 2, "event_type": "Index Review"}
    ]
    for config in trade_log_configs:
        creator = TradeLogCreator(input_file_path=output_file_path, output_file_path=config["output_file_path"], exclude_indices=config["exclude_indices"], strategy_n=config["strategy_n"], event_type=config["event_type"])
        creator.create_trade_log()

    # Step 10: Run backtests for each trade log configuration
    print("Step 10: Running backtests for each trade log configuration...")
    backtest_configs = [
        {"trade_log_file_path": os.path.join(base_dir, "strategy_2", "strat_2_SP400_IR7D_trade_log.csv"), 
         "sofr_file_path": os.path.join(base_dir, "Overnight_Costs", "Cleaned_SOFR.csv"), 
         "output_file_path": os.path.join(base_dir, "strategy_2", "strat_2_SP400_IR7D_backtest_results.csv"),
         "strategy_type": "short"},
        
        {"trade_log_file_path": os.path.join(base_dir, "strategy_2", "strat_2_SP400_CA2D_trade_log.csv"), 
         "sofr_file_path": os.path.join(base_dir, "Overnight_Costs", "Cleaned_SOFR.csv"), 
         "output_file_path": os.path.join(base_dir, "strategy_2", "strat_2_SP400_CA2D_backtest_results.csv"),
         "strategy_type": "long"},
        
        {"trade_log_file_path": os.path.join(base_dir, "strategy_2", "strat_2_SP600_IR2D_trade_log.csv"), 
         "sofr_file_path": os.path.join(base_dir, "Overnight_Costs", "Cleaned_SOFR.csv"), 
         "output_file_path": os.path.join(base_dir, "strategy_2", "strat_2_SP600_IR2D_backtest_results.csv"),
         "strategy_type": "long"}
    ]
    for config in backtest_configs:
        print(f"Running backtest for {config['output_file_path']}...")
        backtest_engine = BacktestEngine(config["trade_log_file_path"], config["sofr_file_path"], config["output_file_path"], config["strategy_type"])
        backtest_engine.run_backtest()
        backtest_engine.save_results()

    # Step 11: Generate equity curves for each backtest result
    print("Step 11: Generating equity curves for each backtest result...")
    equity_curve_configs = [
        {"backtest_results_path": os.path.join(base_dir, "strategy_2", "strat_2_SP400_CA2D_backtest_results.csv"), "output_image_path": os.path.join(base_dir, "strategy_2", "strat_2_SP400_CA2D_equity_curve.png"), "title": "Strategy 2 - Long S&P 400 Corporate Action: 1D Holding Period"},
        {"backtest_results_path": os.path.join(base_dir, "strategy_2", "strat_2_SP400_IR7D_backtest_results.csv"), "output_image_path": os.path.join(base_dir, "strategy_2", "strat_2_SP400_IR7D_equity_curve.png"), "title": "Strategy 2 - Short S&P 400 Index Review: 6D Holding Period"},
        {"backtest_results_path": os.path.join(base_dir, "strategy_2", "strat_2_SP600_IR2D_backtest_results.csv"), "output_image_path": os.path.join(base_dir, "strategy_2", "strat_2_SP600_IR2D_equity_curve.png"), "title": "Strategy 2 - Long S&P 600 Index Review: 1D Holding Period"}
    ]
    for config in equity_curve_configs:
        print(f"Generating equity curve for {config['title']}...")
        plotter = EquityCurvePlotter(config["backtest_results_path"], config["output_image_path"], config["title"])
        plotter.load_data()
        plotter.plot_equity_curve()

    # Step 12: Calculate portfolio metrics for each backtest
    print("Step 12: Calculating portfolio metrics for each backtest...")
    metric_configs = [
        {"backtest_results_path": os.path.join(base_dir, "strategy_2", "strat_2_SP400_CA2D_backtest_results.csv"), "output_metrics_path": os.path.join(base_dir, "strategy_2", "strat_2_SP400_CA2D_metrics.csv")},
        {"backtest_results_path": os.path.join(base_dir, "strategy_2", "strat_2_SP400_IR7D_backtest_results.csv"), "output_metrics_path": os.path.join(base_dir, "strategy_2", "strat_2_SP400_IR7D_metrics.csv")},
        {"backtest_results_path": os.path.join(base_dir, "strategy_2", "strat_2_SP600_IR2D_backtest_results.csv"), "output_metrics_path": os.path.join(base_dir, "strategy_2", "strat_2_SP600_IR2D_metrics.csv")}
    ]
    for config in metric_configs:
        print(f"Calculating metrics for {config['backtest_results_path']}...")
        metrics_calculator = PortfolioMetrics(config["backtest_results_path"], config["output_metrics_path"])
        metrics_calculator.load_data()
        metrics_calculator.calculate_metrics()
        metrics_calculator.save_metrics()
    print("Process completed: All data scraped, processed, analyzed, trade logs generated, backtests run, equity curves created, and metrics calculated.")
