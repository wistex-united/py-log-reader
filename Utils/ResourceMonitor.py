import psutil
import time
import datetime
import threading
import csv
from pathlib import Path
from torch.utils.tensorboard import SummaryWriter
import numpy as np


class ResourceMonitor:
    def __init__(
        self,
        pid: int = None,
        interval: float = 1.0,
        output_dir: str = "monitoring",
        tensorboard_dir: str = "runs/resource_monitoring",
    ):
        self.pid = pid or psutil.Process().pid
        self.process = psutil.Process(self.pid)
        self.interval = interval
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Initialize TensorBoard writer
        run_name = (
            f"process_{self.pid}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )
        self.writer = SummaryWriter(f"{tensorboard_dir}/{run_name}")

        self.monitoring = False
        self.data = []
        self.start_time = None
        self.csv_path = (
            self.output_dir / f"resource_usage_{self.pid}_{int(time.time())}.csv"
        )
        self._init_csv()

        # For smoothing graphs
        self.smoothing_window = 10
        self.cpu_history = []
        self.memory_history = []

        # For tracking disk stats
        self.disk_name = self._get_process_disk()

    def _smooth_value(self, value, history):
        """Apply smoothing to metrics"""
        history.append(value)
        if len(history) > self.smoothing_window:
            history.pop(0)
        return np.mean(history)

    def _init_csv(self):
        """Initialize CSV file with headers"""
        headers = [
            "timestamp",
            "cpu_percent",
            "memory_percent",
            "memory_mb",
            "read_mb_sec",
            "write_mb_sec",
            "read_count_sec",
            "write_count_sec",
            "num_threads",
            "num_fds",
            "system_cpu_percent",
            "system_memory_percent",
            "io_queue_depth",  # New field
            "io_wait_percent",  # New field
            "elapsed_seconds",
        ]

        with open(self.csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(headers)

    def _get_process_disk(self):
        """Get the disk name where the process is writing"""
        try:
            process_cwd = self.process.cwd()
            disk_partitions = psutil.disk_partitions()
            for partition in disk_partitions:
                if process_cwd.startswith(partition.mountpoint):
                    return partition.device.replace("/dev/", "").replace("\\", "")
            return None
        except:
            return None

    def _get_disk_stats(self):
        """Get disk I/O wait time and queue depth"""
        try:
            # Get disk I/O stats
            disk_io = psutil.disk_io_counters(perdisk=True)
            if self.disk_name and self.disk_name in disk_io:
                disk_stats = disk_io[self.disk_name]

                # Calculate I/O queue depth (busy_time is in milliseconds)
                # Queue depth = busy_time / (interval * 1000)
                queue_depth = disk_stats.busy_time / (self.interval * 1000)

                # Calculate I/O wait time percentage
                # busy_time is since boot, so we need to calculate delta
                current_time = time.time()
                if hasattr(self, "_last_disk_stats"):
                    time_delta = current_time - self._last_disk_stats["time"]
                    busy_delta = (
                        disk_stats.busy_time - self._last_disk_stats["busy_time"]
                    )
                    io_wait_percent = (busy_delta / (time_delta * 1000)) * 100
                else:
                    io_wait_percent = 0

                self._last_disk_stats = {
                    "time": current_time,
                    "busy_time": disk_stats.busy_time,
                }

                return queue_depth, io_wait_percent
            return 0, 0
        except:
            return 0, 0

    def _get_detailed_io_stats(self):
        """Get detailed I/O statistics including true queue depth and wait time"""
        try:
            disk_io = psutil.disk_io_counters(perdisk=True)
            if not self.disk_name or self.disk_name not in disk_io:
                return 0, 0, {}
                
            disk_stats = disk_io[self.disk_name]
            current_time = time.time()
            
            # Get current values
            current_values = {
                'busy_time': disk_stats.busy_time,  # milliseconds
                'read_time': getattr(disk_stats, 'read_time', 0),  # milliseconds
                'write_time': getattr(disk_stats, 'write_time', 0),  # milliseconds
                'read_count': disk_stats.read_count,
                'write_count': disk_stats.write_count,
                'time': current_time
            }
            
            if hasattr(self, '_last_io_stats'):
                last_stats = self._last_io_stats
                time_delta = current_time - last_stats['time']
                
                if time_delta > 0:
                    # Calculate deltas
                    busy_delta = current_values['busy_time'] - last_stats['busy_time']
                    reads_delta = current_values['read_count'] - last_stats['read_count']
                    writes_delta = current_values['write_count'] - last_stats['write_count']
                    
                    # Calculate metrics
                    io_wait_percent = (busy_delta / (time_delta * 1000)) * 100  # Convert to percentage
                    total_ios = reads_delta + writes_delta
                    
                    # Calculate true queue depth (Little's Law: queue_depth = throughput * latency)
                    if total_ios > 0:
                        avg_io_time_ms = busy_delta / total_ios
                        throughput = total_ios / time_delta
                        queue_depth = throughput * (avg_io_time_ms / 1000)
                    else:
                        queue_depth = 0
                    
                    detailed_stats = {
                        'avg_io_time_ms': avg_io_time_ms if total_ios > 0 else 0,
                        'iops': total_ios / time_delta,
                        'read_iops': reads_delta / time_delta,
                        'write_iops': writes_delta / time_delta,
                    }
                else:
                    queue_depth = 0
                    io_wait_percent = 0
                    detailed_stats = {}
            else:
                queue_depth = 0
                io_wait_percent = 0
                detailed_stats = {}
            
            self._last_io_stats = current_values
            return queue_depth, io_wait_percent, detailed_stats
            
        except Exception as e:
            print(f"Error getting detailed I/O stats: {e}")
            return 0, 0, {}

    def _log_to_tensorboard(self, metrics, step):
        """Log metrics to TensorBoard with better organization"""
        # CPU Usage Dashboard
        self.writer.add_scalars('1. CPU/Usage', {
            'Process_CPU': metrics['cpu_percent'],
            'System_CPU': metrics['system_cpu_percent']
        }, step)
        
        # Memory Dashboard
        self.writer.add_scalars('2. Memory/Percentage', {
            'Process_Memory': metrics['memory_percent'],
            'System_Memory': metrics['system_memory_percent']
        }, step)
        
        self.writer.add_scalar('2. Memory/Process_MB', metrics['memory_mb'], step)
        
        # I/O Throughput Dashboard
        self.writer.add_scalars('3. IO/Throughput', {
            'Read_MB_Sec': metrics['read_mb_sec'],
            'Write_MB_Sec': metrics['write_mb_sec']
        }, step)
        
        # I/O Operations Dashboard
        self.writer.add_scalars('3. IO/Operations', {
            'Read_Ops_Sec': metrics['read_count_sec'],
            'Write_Ops_Sec': metrics['write_count_sec']
        }, step)
        
        self.writer.add_scalars('3. IO/Performance', {
            'Avg_IO_Time_ms': metrics['avg_io_time_ms'],
            'Total_IOPS': metrics['iops'],
            'Read_IOPS': metrics['read_iops'],
            'Write_IOPS': metrics['write_iops']
        }, step)

        
        # Process Stats Dashboard
        self.writer.add_scalars('4. Process/Stats', {
            'Threads': metrics['num_threads'],
            'File_Descriptors': metrics['num_fds']
        }, step)
        
        # Smoothed Metrics Dashboard
        smoothed_cpu = self._smooth_value(metrics['cpu_percent'], self.cpu_history)
        smoothed_memory = self._smooth_value(metrics['memory_mb'], self.memory_history)
        
        self.writer.add_scalars('5. Smoothed/Metrics', {
            'CPU_Percent': smoothed_cpu,
            'Memory_MB': smoothed_memory
        }, step)
        
        # Add individual scalar values for better heatmap visualization
        for key, value in metrics.items():
            self.writer.add_scalar(f'6. Raw_Values/{key}', value, step)

    def _monitor_loop(self):
        """Main monitoring loop"""
        last_io = None
        last_time = None
        step = 0

        while self.monitoring:
            try:
                timestamp = datetime.datetime.now()

                # Get process stats
                with self.process.oneshot():
                    cpu_percent = self.process.cpu_percent()
                    memory_percent = self.process.memory_percent()
                    memory_mb = self.process.memory_info().rss / 1024 / 1024
                    num_threads = self.process.num_threads()
                    try:
                        num_fds = self.process.num_fds()
                    except:
                        num_fds = 0

                # Get IO stats
                try:
                    current_io = self.process.io_counters()
                    current_time = time.time()
                    read_mb_sec, write_mb_sec, read_count_sec, write_count_sec = (
                        self._get_io_stats(last_io, last_time)
                    )
                    last_io = current_io
                    last_time = current_time
                except:
                    read_mb_sec, write_mb_sec, read_count_sec, write_count_sec = (
                        0,
                        0,
                        0,
                        0,
                    )

                # Get system stats
                system_cpu_percent = psutil.cpu_percent()
                system_memory_percent = psutil.virtual_memory().percent

                # Calculate elapsed time
                elapsed = (datetime.datetime.now() - self.start_time).total_seconds()

                # Get detailed I/O stats
                queue_depth, io_wait_percent, io_details = self._get_detailed_io_stats()
                
                # Create metrics dictionary
                metrics = {
                    "cpu_percent": cpu_percent,
                    "memory_percent": memory_percent,
                    "memory_mb": memory_mb,
                    "read_mb_sec": read_mb_sec,
                    "write_mb_sec": write_mb_sec,
                    "read_count_sec": read_count_sec,
                    "write_count_sec": write_count_sec,
                    "num_threads": num_threads,
                    "num_fds": num_fds,
                    "system_cpu_percent": system_cpu_percent,
                    "system_memory_percent": system_memory_percent,
                    'io_queue_depth': queue_depth,
                    'io_wait_percent': io_wait_percent,
                    'avg_io_time_ms': io_details.get('avg_io_time_ms', 0),
                    'iops': io_details.get('iops', 0),
                    'read_iops': io_details.get('read_iops', 0),
                    'write_iops': io_details.get('write_iops', 0),
                    "elapsed_seconds": elapsed,
                }

                # Log to TensorBoard
                self._log_to_tensorboard(metrics, step)

                # Record data to CSV
                row = [timestamp] + list(metrics.values())
                with open(self.csv_path, "a", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow(row)

                step += 1
                time.sleep(self.interval)

            except Exception as e:
                print(f"\nError in monitoring loop: {str(e)}")
                continue

    def start(self):
        """Start monitoring"""
        self.monitoring = True
        self.start_time = datetime.datetime.now()
        self.monitor_thread = threading.Thread(target=self._monitor_loop)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        print(f"Started monitoring process {self.pid}")
        print(f"View metrics with: tensorboard --logdir={self.writer.log_dir}")

    def stop(self):
        """Stop monitoring"""
        self.monitoring = False
        if hasattr(self, "monitor_thread"):
            self.monitor_thread.join()
        self.writer.close()
        print(f"\nStopped monitoring process {self.pid}")
        print(f"TensorBoard logs saved in: {self.writer.log_dir}")


# def monitor_program(target_program):
#     """Wrapper function to monitor a program"""
#     monitor = ResourceMonitor(interval=1.0)
#     try:
#         monitor.start()
#         # Run the target program
#         target_program()
#     finally:
#         monitor.stop()

# # Example usage
# if __name__ == '__main__':
#     monitor = ResourceMonitor(interval=1.0)
#     try:
#         monitor.start()
#         # Run the target program
#         # target_program()
#     finally:
#         monitor.stop()
