import cProfile
import pstats
from pstats import SortKey
import time
from pathlib import Path
import threading
import multiprocessing
import tqdm
from typing import List

class WindowedProfiler:
    def __init__(self, window_size=60, output_dir="profiles"):
        self.window_size = window_size
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.is_profiling = False
        self.profile_lock = threading.Lock()
        
    def _safe_dump_stats(self, profiler, window_count):
        """Safely dump profiler stats to file with error handling"""
        try:
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            filename = self.output_dir / f"profile_window_{window_count}_{timestamp}.prof"
            filename.parent.mkdir(parents=True, exist_ok=True)
            
            profiler.dump_stats(str(filename))
            print(f"\nProfile saved to: {filename}")
            
            stats = pstats.Stats(profiler)
            stats.sort_stats(SortKey.CUMULATIVE).print_stats(20)
            
            return True
        except Exception as e:
            print(f"Error saving profile: {e}")
            return False

    def _profile_monitor(self, profiler, window_count):
        """Monitor thread that periodically dumps profile data"""
        last_window_time = time.time()
        
        while self.is_profiling:
            current_time = time.time()
            
            if current_time - last_window_time >= self.window_size:
                with self.profile_lock:
                    # Disable current profiler and dump stats
                    profiler.disable()
                    self._safe_dump_stats(profiler, window_count[0])
                    
                    # Start new profiler for next window
                    profiler.enable()
                    
                    # Update counters
                    window_count[0] += 1
                    last_window_time = current_time
            
            # Sleep briefly to prevent high CPU usage
            time.sleep(1)

    def profile_with_windows(self, func):
        def wrapper(*args, **kwargs):
            window_count = [0]  # Using list to allow modification in thread
            current_profiler = cProfile.Profile()
            self.is_profiling = True
            
            try:
                # Start the monitoring thread
                monitor_thread = threading.Thread(
                    target=self._profile_monitor,
                    args=(current_profiler, window_count)
                )
                monitor_thread.start()
                
                # Start profiling
                current_profiler.enable()
                
                # Run the actual function
                result = func(*args, **kwargs)
                
                # Clean up
                self.is_profiling = False
                monitor_thread.join()
                
                # Ensure we get a final profile dump
                with self.profile_lock:
                    current_profiler.disable()
                    self._safe_dump_stats(current_profiler, window_count[0])
                
                return result
                
            except Exception as e:
                print(f"Error in profiled function: {e}")
                self.is_profiling = False
                monitor_thread.join()
                
                if current_profiler:
                    with self.profile_lock:
                        current_profiler.disable()
                        self._safe_dump_stats(current_profiler, window_count[0])
                raise
            
        return wrapper

def process_chunk(chunk_indices: List[int], worker_id: int):
    """Process a chunk of data"""
    pbar = tqdm.tqdm(
        total=len(chunk_indices),
        desc=f'Worker {worker_id}',
        position=worker_id,
        leave=True
    )
    
    # Simulate longer processing
    for _ in chunk_indices:
        time.sleep(0.2)  # Longer sleep to demonstrate multiple profile windows
        pbar.update(1)
    
    pbar.close()

def run_parallel_processing(num_workers: int = 8):
    """Run parallel processing with multiple workers"""
    # Create sample data - larger dataset for longer processing
    total_items = 2000
    chunks = [list(range(i, total_items, num_workers)) for i in range(num_workers)]
    
    # Print empty lines for progress bars
    print('\n' * (num_workers - 1))
    
    # Create and start processes
    processes = []
    for worker_id, chunk_indices in enumerate(chunks):
        p = multiprocessing.Process(
            target=process_chunk,
            args=(chunk_indices, worker_id)
        )
        processes.append(p)
        p.start()
    
    # Wait for all processes to complete
    for p in processes:
        p.join()
    
    print('\n')

# Create profiler instance
profiler = WindowedProfiler(window_size=60)  # Profile dump every 60 seconds

@profiler.profile_with_windows
def main():
    run_parallel_processing(num_workers=8)

if __name__ == '__main__':
    multiprocessing.set_start_method('fork')
    main()