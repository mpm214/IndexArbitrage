import os
from utils import load_data, filter_candidates

class MeanReversionStrategy:
    def __init__(self, data_path, min_count_threshold=50):
        self.data = load_data(output_stats_path)
        self.min_count_threshold = min_count_threshold

    def identify_reversion_candidates(self):
        """Identifies both long and short reversion candidates."""
        long_day1_candidates = filter_candidates(self.data, strategy_n=1, net_mean_sign='negative')
        short_day1_candidates = filter_candidates(self.data, strategy_n=1, net_mean_sign='positive')
        long_reversion_confirmed = self._confirm_reversion(long_day1_candidates, self.data, positive=True)
        short_reversion_confirmed = self._confirm_reversion(short_day1_candidates, self.data, positive=False)
        long_reversion_confirmed = long_reversion_confirmed[long_reversion_confirmed['count'] >= self.min_count_threshold]
        short_reversion_confirmed = short_reversion_confirmed[short_reversion_confirmed['count'] >= self.min_count_threshold]
        return long_reversion_confirmed, short_reversion_confirmed

    def _confirm_reversion(self, initial_candidates, data, positive=True):
        """Confirms mean reversion by checking subsequent day performance."""
        condition = data['mean'] > 0 if positive else data['mean'] < 0
        confirmed = data[(data['strategy_2_n'] > 1) & condition]
        return confirmed.merge(initial_candidates[['Index_Name', 'Event_Type']].drop_duplicates(), 
                               on=['Index_Name', 'Event_Type'])

    def save_results(self, long_candidates, short_candidates, long_output_path, short_output_path):
        """Saves confirmed long and short candidates to CSV files."""
        long_candidates.to_csv(long_output_path, index=False)
        short_candidates.to_csv(short_output_path, index=False)
        print(f"Long reversion candidates saved to {long_output_path}")
        print(f"Short reversion candidates saved to {short_output_path}")

# Usage Example: 
if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    output_stats_path = os.path.join(base_dir, "strategy_2", "strategy_2_stats.csv")
    long_output_path = os.path.join(base_dir, "strategy_2", "long_reversion_confirmed.csv")
    short_output_path = os.path.join(base_dir, "strategy_2", "short_reversion_confirmed.csv")
    strategy = MeanReversionStrategy(output_stats_path)
    long_candidates, short_candidates = strategy.identify_reversion_candidates()
    strategy.save_results(long_candidates, short_candidates, long_output_path, short_output_path)
