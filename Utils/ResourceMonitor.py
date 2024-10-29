import psutil
import time
import datetime
import threading
import csv
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from typing import Optional

class ResourceMonitor:
    def __init__(self, pid: Optional[int] = None, interval: float = 1.0, output_dir: str = "monitoring"):
        """
        Initialize resource monitor
        
        Args:
            pid: Process ID to monitor (None means current process)
            interval: Sampling interval in seconds
            output_dir: Directory to save monitoring data
        """
        self.pid = pid or psutil.Process().pid
        self.process = psutil.Process(self.pid)
        self.interval = interval
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.monitoring = False
        self.data = []
        self.start_time = None
        
        # Prepare CSV file
        self.csv_path = self.output_dir / f"resource_usage_{self.pid}_{int(time.time())}.csv"
        self._init_csv()
    
    def _init_csv(self):
        """Initialize CSV file with headers"""
        headers = [
            'timestamp',
            'cpu_percent',
            'memory_percent',
            'memory_mb',
            'read_mb_sec',
            'write_mb_sec',
            'read_count_sec',
            'write_count_sec',
            'num_threads',
            'num_fds',
            'system_cpu_percent',
            'system_memory_percent',
            'elapsed_seconds'
        ]
        
        with open(self.csv_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
    
    def _get_io_stats(self, last_io=None, last_time=None):
        """Calculate IO rates between measurements"""
        current_io = self.process.io_counters()
        current_time = time.time()
        
        if last_io is None or last_time is None:
            return 0, 0, 0, 0
        
        time_delta = current_time - last_time
        
        read_bytes = (current_io.read_bytes - last_io.read_bytes) / time_delta / 1024 / 1024  # MB/s
        write_bytes = (current_io.write_bytes - last_io.write_bytes) / time_delta / 1024 / 1024  # MB/s
        read_count = (current_io.read_count - last_io.read_count) / time_delta  # ops/s
        write_count = (current_io.write_count - last_io.write_count) / time_delta  # ops/s
        
        return read_bytes, write_bytes, read_count, write_count
    
    def _monitor_loop(self):
        """Main monitoring loop"""
        last_io = None
        last_time = None
        
        while self.monitoring:
            try:
                timestamp = datetime.datetime.now()
                
                # Get process stats
                cpu_percent = self.process.cpu_percent()
                memory_percent = self.process.memory_percent()
                memory_mb = self.process.memory_info().rss / 1024 / 1024
                num_threads = self.process.num_threads()
                num_fds = self.process.num_fds()
                
                # Get IO stats
                current_io = self.process.io_counters()
                current_time = time.time()
                read_mb_sec, write_mb_sec, read_count_sec, write_count_sec = self._get_io_stats(last_io, last_time)
                last_io = current_io
                last_time = current_time
                
                # Get system stats
                system_cpu_percent = psutil.cpu_percent()
                system_memory_percent = psutil.virtual_memory().percent
                
                # Calculate elapsed time
                elapsed = (datetime.datetime.now() - self.start_time).total_seconds()
                
                # Record data
                row = [
                    timestamp,
                    cpu_percent,
                    memory_percent,
                    memory_mb,
                    read_mb_sec,
                    write_mb_sec,
                    read_count_sec,
                    write_count_sec,
                    num_threads,
                    num_fds,
                    system_cpu_percent,
                    system_memory_percent,
                    elapsed
                ]
                
                # Save to CSV
                with open(self.csv_path, 'a', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(row)
                
                time.sleep(self.interval)
                
            except psutil.NoSuchProcess:
                print(f"Process {self.pid} no longer exists. Stopping monitoring.")
                break
            except Exception as e:
                print(f"Error in monitoring loop: {str(e)}")
                continue
    
    def start(self):
        """Start monitoring"""
        self.monitoring = True
        self.start_time = datetime.datetime.now()
        self.monitor_thread = threading.Thread(target=self._monitor_loop)
        self.monitor_thread.start()
        print(f"Started monitoring process {self.pid}")
    
    def stop(self):
        """Stop monitoring and generate report"""
        self.monitoring = False
        self.monitor_thread.join()
        print(f"Stopped monitoring process {self.pid}")
        self.generate_report()
    
    def generate_report(self):
        """Generate plots from collected data"""
        df = pd.read_csv(self.csv_path)
        
        # Create plots directory
        plots_dir = self.output_dir / 'plots'
        plots_dir.mkdir(exist_ok=True)
        
        # Plot resources over time
        fig, axes = plt.subplots(3, 1, figsize=(12, 15))
        
        # CPU Usage
        axes[0].plot(df['elapsed_seconds'], df['cpu_percent'], label='Process CPU')
        axes[0].plot(df['elapsed_seconds'], df['system_cpu_percent'], label='System CPU')
        axes[0].set_title('CPU Usage Over Time')
        axes[0].set_ylabel('CPU %')
        axes[0].grid(True)
        axes[0].legend()
        
        # Memory Usage
        axes[1].plot(df['elapsed_seconds'], df['memory_mb'], label='Memory (MB)')
        axes[1].set_title('Memory Usage Over Time')
        axes[1].set_ylabel('Memory (MB)')
        axes[1].grid(True)
        
        # Disk I/O
        axes[2].plot(df['elapsed_seconds'], df['read_mb_sec'], label='Read MB/s')
        axes[2].plot(df['elapsed_seconds'], df['write_mb_sec'], label='Write MB/s')
        axes[2].set_title('Disk I/O Over Time')
        axes[2].set_ylabel('MB/s')
        axes[2].grid(True)
        axes[2].legend()
        
        # Common x-axis label
        plt.xlabel('Elapsed Time (seconds)')
        
        # Save plot
        plt.tight_layout()
        plt.savefig(plots_dir / f'resource_usage_{self.pid}.png')
        plt.close()
        
        # Generate summary statistics
        summary = {
            'cpu_avg': df['cpu_percent'].mean(),
            'cpu_max': df['cpu_percent'].max(),
            'memory_avg_mb': df['memory_mb'].mean(),
            'memory_max_mb': df['memory_mb'].max(),
            'read_avg_mbs': df['read_mb_sec'].mean(),
            'write_avg_mbs': df['write_mb_sec'].mean(),
            'duration_seconds': df['elapsed_seconds'].max()
        }
        
        # Save summary
        with open(self.output_dir / f'summary_{self.pid}.txt', 'w') as f:
            for key, value in summary.items():
                f.write(f"{key}: {value:.2f}\n")

        # Print summary
        print(f"Write Summary successfully to {self.output_dir}.")

# # Example usage
# def main():
#     # To monitor a specific process
#     # monitor = ResourceMonitor(pid=1234)
    
#     # To monitor the current process
#     monitor = ResourceMonitor(interval=1.0)
    
#     try:
#         # Start monitoring
#         monitor.start()
        
#         # Your program here
#         your_main_program()
        
#     finally:
#         # Stop monitoring and generate report
#         monitor.stop()
