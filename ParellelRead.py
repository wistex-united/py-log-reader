import multiprocessing
import tracemalloc
import tqdm

from LogInterface import Log
from Primitive import *
from StreamUtils import *
from Utils import ResourceMonitor

from Utils import (
    ObservationJosh,
    countLines,
    displayTopMemoryConsumers,
    extractTrajNumbers,
    readLastLine,
    startMemoryTracing,
)

def chunkList(lst, n):
    """Divide list into n chunks"""
    chunk_size = len(lst) // n
    remainder = len(lst) % n
    
    chunks = []
    start = 0
    for i in range(n):
        end = start + chunk_size + (1 if i < remainder else 0)
        chunks.append(lst[start:end])
        start = end
    
    return chunks

def process_chunk(log_file: str, chunk_indices: list, worker_id: int):
    """Process a chunk of frames using direct accessor"""
    # Initialize Log for this worker
    LOG = Log()
    LOG.readLogFile(log_file)
    LOG.eval(isLogFileLarge=True)
    
    # Get the frame accessor for this chunk
    accessor = LOG.getFrameAccessor(chunk_indices)
    
    # Create positioned progress bar
    pbar = tqdm.tqdm(
        total=len(chunk_indices),
        desc=f'Worker {worker_id}',
        position=worker_id,
        leave=True
    )
    
    # Process frames in our chunk
    for frame in accessor:
        frame.saveFrameDict()
        frame.saveImageWithMetaData(slientFail=True)
        pbar.update(1)
    
    pbar.close()

def run_parallel_processing(log_file: str, num_workers: int = 8):
    """Wrapper function to handle the parallel processing setup"""
    # Initialize temporary Log to get indices
    temp_log = Log()
    temp_log.readLogFile(log_file)
    temp_log.eval(isLogFileLarge=True)
    
    # Get all cognition indices
    cogIndexMap = temp_log.UncompressedChunk.thread("Cognition")._indexMap
    chunks = chunkList(list(cogIndexMap), num_workers)
    del temp_log
    
    # Print empty lines to make room for progress bars
    print('\n' * (num_workers - 1))
    
    # Create and start processes
    processes = []
    for worker_id, chunk_indices in enumerate(chunks):
        p = multiprocessing.Process(
            target=process_chunk,
            args=(log_file, chunk_indices, worker_id)
        )
        processes.append(p)
        p.start()
    
    # Wait for all processes to complete
    for p in processes:
        p.join()
    
    print('\n')

def main():
    monitor = ResourceMonitor(interval=1.0)
    startMemoryTracing()
    try:
        # Start monitoring
        monitor.start()
        
        run_parallel_processing("neargoal.log", num_workers=8)
        
    finally:
        # Stop monitoring and generate report
        monitor.stop()
        displayTopMemoryConsumers( tracemalloc.take_snapshot() )
if __name__ == '__main__':
    # Set multiprocessing start method
    multiprocessing.set_start_method('fork')
    
    # Run the parallel processing
    main()