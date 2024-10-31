import pandas as pd
import os
from utils import calculate_group_stats, save_pdf_plots, save_statistics_summary

class StrategyAnalysis:
    def __init__(self, data_path, output_pdf_path, output_stats_path):
        self.data_path = data_path
        self.output_pdf_path = output_pdf_path
        self.output_stats_path = output_stats_path

    def load_data(self):
        self.data = pd.read_csv(self.data_path)
    
    def analyze_and_save(self):
        stats_summary = calculate_group_stats(self.data)
        save_pdf_plots(self.data, self.output_pdf_path)
        save_statistics_summary(stats_summary, self.output_stats_path)

# Usage Example:
#if __name__ == "__main__":
#    base_dir = os.path.dirname(os.path.abspath(__file__))
#    file_path = os.path.join(base_dir, "strategy_1", "strategy_1_returns.csv")
#    output_pdf_path = os.path.join(base_dir, "strategy_1", "strategy_1_KDE.pdf")
#    output_stats_path = os.path.join(base_dir, "strategy_1", "strategy_1_stats.csv")
#    analysis = StrategyAnalysis(file_path, output_pdf_path, output_stats_path)
#    analysis.load_data()
#    analysis.analyze_and_save()
#    print("PDFs generated and statistics calculated successfully.")
