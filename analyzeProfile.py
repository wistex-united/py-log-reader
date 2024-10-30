import pstats
from pathlib import Path
import re
from collections import defaultdict
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt

class ProfileAnalyzer:
    def __init__(self, profile_dir="./profile_output"):
        self.profile_dir = Path(profile_dir)
        self.window_data = defaultdict(list)  # {function_name: [(window_num, time)]}
        self.trend_data = {}  # {function_name: trend_coefficient}
        
    def _extract_window_number(self, filename):
        """Extract window number from profile filename"""
        match = re.search(r'window_(\d+)', filename.name)
        return int(match.group(1)) if match else None
        
    def _is_increasing(self, times, threshold=0.8):
        """
        Check if times are consistently increasing
        Returns correlation coefficient with time
        """
        if len(times) < 3:  # Need at least 3 points for trend
            return 0
            
        x = np.arange(len(times))
        y = np.array(times)
        correlation = np.corrcoef(x, y)[0, 1]
        return correlation

    def load_profiles(self):
        """Load all profile files and extract timing data"""
        profiles = []
        for prof_file in self.profile_dir.glob("*.prof"):
            window_num = self._extract_window_number(prof_file)
            if window_num is not None:
                profiles.append((window_num, prof_file))
        
        # Sort by window number
        for window_num, prof_file in sorted(profiles):
            try:
                stats = pstats.Stats(str(prof_file))
                
                # Process each function's stats
                for func, (cc, nc, tt, ct, callers) in stats.stats.items():
                    # cc: cumulative calls
                    # nc: native calls
                    # tt: total time
                    # ct: cumulative time
                    
                    func_name = f"{func[0]}:{func[1]}:{func[2]}"
                    self.window_data[func_name].append((window_num, ct))  # Use cumulative time
                    
            except Exception as e:
                print(f"Error processing {prof_file}: {e}")

    def analyze_trends(self, min_windows=3, correlation_threshold=0.8):
        """Analyze time trends for each function"""
        increasing_funcs = []
        
        for func_name, data in self.window_data.items():
            if len(data) < min_windows:
                continue
                
            # Sort by window number
            sorted_data = sorted(data)
            times = [t for _, t in sorted_data]
            
            # Calculate trend
            correlation = self._is_increasing(times)
            self.trend_data[func_name] = correlation
            
            if correlation > correlation_threshold:
                increasing_funcs.append((func_name, correlation, times))
        
        return increasing_funcs

    def plot_trends(self, top_n=10):
        """Plot time trends for top N most increasing functions"""
        # Sort functions by trend coefficient
        sorted_funcs = sorted(self.trend_data.items(), key=lambda x: abs(x[1]), reverse=True)
        top_funcs = sorted_funcs[:top_n]
        
        plt.figure(figsize=(15, 8))
        
        for func_name, correlation in top_funcs:
            data = self.window_data[func_name]
            windows, times = zip(*sorted(data))
            plt.plot(windows, times, marker='o', label=f'{func_name[:50]}... (r={correlation:.2f})')
        
        plt.xlabel('Window Number')
        plt.ylabel('Cumulative Time (seconds)')
        plt.title('Function Time Trends')
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.grid(True)
        plt.tight_layout()
        
        # Save plot
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        plt.savefig(f'profile_trends_{timestamp}.png', bbox_inches='tight')
        plt.close()

    def print_report(self, increasing_funcs):
        """Print detailed report of increasing functions"""
        print("\nFunctions with Increasing Time Consumption:")
        print("=" * 80)
        
        for func_name, correlation, times in sorted(increasing_funcs, key=lambda x: x[1], reverse=True):
            print(f"\nFunction: {func_name}")
            print(f"Correlation coefficient: {correlation:.3f}")
            print("Time progression:")
            for window_num, time in enumerate(times):
                change = 0 if window_num == 0 else (time - times[window_num-1]) / times[window_num-1] * 100
                print(f"  Window {window_num}: {time:.3f}s ({change:+.1f}% change)")

def main():
    analyzer = ProfileAnalyzer("./profiles")
    
    print("Loading profiles...")
    analyzer.load_profiles()
    
    print("\nAnalyzing trends...")
    increasing_funcs = analyzer.analyze_trends(min_windows=3, correlation_threshold=0.8)
    
    print("\nGenerating plots...")
    analyzer.plot_trends(top_n=10)
    
    print("\nGenerating report...")
    analyzer.print_report(increasing_funcs)
    
    print("\nAnalysis complete! Check profile_trends_*.png for visualization.")

if __name__ == "__main__":
    main()