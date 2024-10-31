import pandas as pd
import matplotlib.pyplot as plt
import os

class EquityCurvePlotter:
    def __init__(self, backtest_results_path, output_image_path, title="Equity Curve"):
        self.backtest_results_path = backtest_results_path
        self.output_image_path = output_image_path
        self.title = title

    def load_data(self):
        self.df = pd.read_csv(self.backtest_results_path)
        self.df['Date'] = pd.to_datetime(self.df['Date'])
        self.df = self.df.sort_values(by='Date')
        print(f"Data loaded and sorted by date from {self.backtest_results_path}")

    def plot_equity_curve(self):
        plt.figure(figsize=(12, 6))
        plt.plot(self.df['Date'], self.df['Cumulative_Net_PnL'], label='Equity Curve', color='b', linewidth=2)
        plt.title(self.title)
        plt.xlabel("Date")
        plt.ylabel("Cumulative Net PnL")
        plt.legend()
        plt.grid(True)
        plt.savefig(self.output_image_path)
        plt.show()
        print(f"Equity curve saved as {self.output_image_path}")

# Usage Example: 
#if __name__ == "__main__":
#    base_dir = os.path.dirname(os.path.abspath(__file__))
#    equity_curve_configs = [
#        {"backtest_results_path": os.path.join(base_dir, "strategy_1", "strat_1_SP600_1D_backtest_results.csv"), "output_image_path": os.path.join(base_dir, "strategy_1", "strat_1_SP600_1D_equity_curve.png"), "title": "Strategy 1 S&P 600: 1D Holding Period"},
#        {"backtest_results_path": os.path.join(base_dir, "strategy_1", "strat_1_CA1D_backtest_results.csv"), "output_image_path": os.path.join(base_dir, "strategy_1", "strat_1_CA1D_equity_curve.png"), "title": "Strategy 1 Corporate Action: 1D Holding Period"},
#        {"backtest_results_path": os.path.join(base_dir, "strategy_1", "strat_1_SP500_CA1D_backtest_results.csv"), "output_image_path": os.path.join(base_dir, "strategy_1", "strat_1_SP500_CA1D_equity_curve.png"), "title": "Strategy 1 - S&P 500 & Corporate Action: 1D Holding Period"}
#    ]
#    for config in equity_curve_configs:
#        print(f"Generating equity curve for {config['title']}...")
#        plotter = EquityCurvePlotter(config["backtest_results_path"], config["output_image_path"], config["title"])
#        plotter.load_data()
#        plotter.plot_equity_curve()
#        print(f"Equity curve for {config['title']} completed.\n")