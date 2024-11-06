import argparse
import multiprocessing
from typing import Any, Dict, List, Optional, Tuple

import tqdm

from LogInterface import FrameAccessor, FrameBase, FrameInstance, Frames, Log

VALID_THREADS = ["Upper", "Lower", "Motion", "Audio", "Cognition", "Referee"]

class FrameFilter:
    def __init__(
        self,
        timeRange: Tuple[Optional[str], Optional[str]] = (None, None),
        frameRange: Tuple[Optional[int], Optional[int]] = (None, None),
        threads: Optional[List[str]] = None,
    ):
        self.startTime = self._parseTime(timeRange[0]) if timeRange[0] else None
        self.endTime = self._parseTime(timeRange[1]) if timeRange[1] else None
        self.startFrame = frameRange[0]
        self.endFrame = frameRange[1]

        # Validate threads
        if threads:
            invalidThreads = [t for t in threads if t not in VALID_THREADS]
            if invalidThreads:
                raise ValueError(
                    f"Invalid thread types: {invalidThreads}. "
                    f"Valid options are: {VALID_THREADS}"
                )
        self.threads = threads

    def _parseTime(self, timeStr: str) -> int:
        """Parse time string to milliseconds
        Accepted formats:
        - "HH:MM:SS.mmm"
        - "MM:SS.mmm"
        - "SS.mmm"
        - "SS"
        HH: hours (optional)
        MM: minutes (optional)
        SS: seconds (required)
        mmm: milliseconds (optional)
        """
        if not timeStr:
            raise ValueError("Time string cannot be empty")

        # Split by colon to handle hours and minutes
        parts = timeStr.split(":")
        if len(parts) > 3:
            raise ValueError("Too many time components")

        # Initialize components
        hours = minutes = 0
        seconds_str = ""

        # Parse based on number of parts
        if len(parts) == 3:  # HH:MM:SS
            hours = int(parts[0])
            minutes = int(parts[1])
            seconds_str = parts[2]
        elif len(parts) == 2:  # MM:SS
            minutes = int(parts[0])
            seconds_str = parts[1]
        else:  # SS only
            seconds_str = parts[0]

        # Handle seconds and milliseconds
        seconds_parts = seconds_str.split(".")
        if len(seconds_parts) > 2:
            raise ValueError("Invalid seconds format")

        seconds = int(seconds_parts[0])
        milliseconds = 0
        if len(seconds_parts) == 2:
            # Handle milliseconds, pad with zeros if needed
            ms_str = seconds_parts[1][:3].ljust(3, "0")
            milliseconds = int(ms_str)

        # Validate ranges
        if hours < 0 or minutes < 0 or seconds < 0 or milliseconds < 0:
            raise ValueError("Time components cannot be negative")
        if minutes >= 60:
            raise ValueError("Minutes must be less than 60")
        if seconds >= 60:
            raise ValueError("Seconds must be less than 60")
        if milliseconds >= 1000:
            raise ValueError("Milliseconds must be less than 1000")

        totalMs = (hours * 3600 + minutes * 60 + seconds) * 1000 + milliseconds
        return totalMs + 100000  # Add base timestamp of 100s

    def validate(self):
        if self.startTime is not None and self.endTime is not None:
            if self.startTime >= self.endTime:
                raise ValueError("End time must be greater than start time")

        if self.startFrame is not None and self.endFrame is not None:
            if self.startFrame >= self.endFrame:
                raise ValueError("End frame must be greater than start frame")

class LogReaderCLI:
    def __init__(self):
        self.profiler = None

    def _divideListIntoChunks(self, lst, n):
        if not lst:
            return []

        chunkSize = len(lst) // n
        remainder = len(lst) % n

        chunks = []
        start = 0
        for i in range(n):
            end = start + chunkSize + (1 if i < remainder else 0)
            chunks.append(lst[start:end])
            start = end

        return chunks

    def _processChunk(self, logFile: str, chunkIndices: list, workerId: int):
        # Initialize Log for this worker
        LOG = Log()
        LOG.readLogFile(logFile)
        LOG.eval(isLogFileLarge=True)

        # Get the frame accessor for this chunk
        accessor = LOG.getFrameAccessor(chunkIndices)

        # Create positioned progress bar
        pbar = tqdm.tqdm(
            total=len(chunkIndices),
            desc=f"Worker {workerId}",
            position=workerId,
            leave=True,
        )

        # Process frames in our chunk
        for frame in accessor:
            frame.saveFrameDict()
            frame.saveImageWithMetaData(slientFail=True)
            pbar.update(1)

        pbar.close()

    def _getFilteredIndexMap(
        self, logFile: str, threads: Optional[List[str]], frameFilter: FrameFilter
    ) -> List[int]:
        # Create temporary Log to get indices
        LOG = Log()
        LOG.readLogFile(logFile)
        LOG.eval(isLogFileLarge=True)

        frameIdxes=[]
        # Get initial frame set based on threads
        if threads:
            for thread in threads:
                threadFrameIdxes = LOG.UncompressedChunk.thread(thread)._indexMap
                frameIdxes.extend(threadFrameIdxes)
            frameIdxes.sort()
        else:
            frameIdxes = list(LOG.UncompressedChunk.frames._indexMap)

        if frameFilter.startFrame is not None or frameFilter.endFrame is not None:
            startIdx = 0 if frameFilter.startFrame is None else frameFilter.startFrame
            endIdx = (
                frameIdxes[-1] if frameFilter.endFrame is None else frameFilter.endFrame
            )
            frameIdxes = [f for f in frameIdxes if startIdx <= f <= endIdx]

        # Apply time range filter if specified
        frameAcc = LOG.getFrameAccessor(indexMap=frameIdxes)
        
        if frameFilter.startTime is not None or frameFilter.endTime is not None:
            startTimestamp = (
                frameAcc[0].timestamp
                if frameFilter.startTime is None
                else frameFilter.startTime
            )
            endTimestamp = (
                frameAcc[-1].timestamp
                if frameFilter.endTime is None
                else frameFilter.endTime
            )

            # Binary search
            left, right = 0, len(frameAcc) - 1
            startPos, endPos = 0, len(frameAcc) - 1

            # Find start position
            while left <= right:
                mid = (left + right) // 2
                if frameAcc[mid].timestamp < startTimestamp:
                    startPos = mid + 1
                    left = mid + 1
                else:
                    right = mid - 1

            # Reset
            left, right = 0, len(frameAcc) - 1

            # Find end position
            while left <= right:
                mid = (left + right) // 2
                if frameAcc[mid].timestamp > endTimestamp:
                    endPos = mid - 1
                    right = mid - 1
                else:
                    left = mid + 1

            # Get filtered frame indices
            frameIdxes = frameAcc.indexMap[startPos : endPos + 1]

        return frameIdxes

    def _setupArgParser(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(
            description="Parallel log file processor",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
Examples:
  # Basic usage with defaults
  %(prog)s input.log
  
  # Specify number of workers and threads
  %(prog)s input.log --numworkers 4 --threads Upper Lower
  
  # Process time range
  %(prog)s input.log --start-time 0:00:00.000 --end-time 1:30:00.000
  
  # Process frame range
  %(prog)s input.log --start-frame 1000 --end-frame 2000
  
  # Enable profiling
  %(prog)s input.log --profile
            """,
        )

        parser.add_argument("inputFile", help="Input log file path")

        parser.add_argument(
            "--numworkers",
            type=int,
            default=multiprocessing.cpu_count(),
            help="Number of worker processes (default: CPU count)",
        )

        parser.add_argument(
            "--threads",
            choices=VALID_THREADS,
            nargs="+",
            help="List of threads to process (default: all frames)",
        )

        parser.add_argument(
            "--start-time",
            help="Start time in format hour:min:sec.millisecond, sec is mandatory, others are optional",
        )

        parser.add_argument(
            "--end-time",
            help="End time in format hour:min:sec.millisecond, sec is mandatory, others are optional",
        )

        parser.add_argument("--start-frame", type=int, help="Start frame number")

        parser.add_argument("--end-frame", type=int, help="End frame number")

        parser.add_argument(
            "--profile", action="store_true", help="Enable performance profiling"
        )

        return parser

    def run(self):
        parser = self._setupArgParser()
        args = parser.parse_args()

        try:
            # Create frame filter with all filtering criteria
            frameFilter = FrameFilter(
                timeRange=(args.start_time, args.end_time),
                frameRange=(args.start_frame, args.end_frame),
                threads=args.threads,
            )
            frameFilter.validate()

        except ValueError as e:
            parser.error(str(e))

        # Get filtered index map once
        filteredIndices = self._getFilteredIndexMap(
            args.inputFile, args.threads, frameFilter
        )

        if len(filteredIndices) == 0:
            print("No frames to process")
            return

        chunks = self._divideListIntoChunks(filteredIndices, args.numworkers)

        # Print empty lines to make room for progress bars
        print("\n" * (args.numworkers - 1))

        # Create and start processes
        processes = []
        for workerId, chunkIndices in enumerate(chunks):
            p = multiprocessing.Process(
                target=self._processChunk,
                args=(args.inputFile, chunkIndices, workerId),
            )
            processes.append(p)
            p.start()

        # Wait for all processes to complete
        for p in processes:
            p.join()

        print("\n")


def main():
    processor = LogReaderCLI()
    processor.run()

if __name__ == "__main__":
    multiprocessing.set_start_method("fork")
    main()
