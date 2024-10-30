import cProfile
import pstats
from pstats import SortKey
import time
from pathlib import Path
import os

class WindowedProfiler:
    def __init__(self, window_size=60, output_dir="profiles"):
        self.window_size = window_size
        self.output_dir = Path(output_dir)
        # Create output directory if it doesn't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def _safe_dump_stats(self, profiler, window_count):
        """Safely dump profiler stats to file with error handling"""
        try:
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            filename = self.output_dir / f"profile_window_{window_count}_{timestamp}.prof"
            
            # Make sure the directory exists
            filename.parent.mkdir(parents=True, exist_ok=True)
            
            # Convert Path to string for pstats
            profiler.dump_stats(str(filename))
            print(f"Profile saved to: {filename}")
            
            # Also print stats to console
            stats = pstats.Stats(profiler)
            stats.sort_stats(SortKey.CUMULATIVE).print_stats(20)
            
            return True
        except Exception as e:
            print(f"Error saving profile: {e}")
            return False
    
    def profile_with_windows(self, func):
        def wrapper(*args, **kwargs):
            window_count = 0
            
            while True:
                print(f"\nStarting profiling window {window_count}")
                
                # Create new profiler for this window
                profiler = cProfile.Profile()
                window_start = time.time()
                
                # Run and profile the function
                profiler.enable()
                try:
                    result = func(*args, **kwargs)
                    profiler.disable()
                    
                    # Try to dump stats
                    self._safe_dump_stats(profiler, window_count)
                    
                    return result
                    
                except Exception as e:
                    print(f"Error in window {window_count}: {e}")
                    # Try to dump stats even if function failed
                    self._safe_dump_stats(profiler, window_count)
                    raise
                finally:
                    window_count += 1
                    
                # Check if we should start a new window
                if time.time() - window_start < self.window_size:
                    break
                    
        return wrapper