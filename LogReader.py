from Primitive import *
from StreamUtils import *

from LogInterface import Log
import cProfile
import pstats


def main():
    L = Log()
    L.readLogFile("bc1.log")
    L.eval()
    L.parseBytes()
    index = 0
    for frame in L.frames:
        if frame.hasImage:
            frame.saveImageWithMetaData()
            print(f"Frame {frame.index} has image")
        frame.recoverTrajectory()
        frame.saveFrameDict()
        if len(frame.dummyMessages)!=0:
            print(1)


profiler = cProfile.Profile()
# Start profiling
profiler.enable()

# Code you want to profile
main()

# Stop profiling
profiler.disable()
# Create a Stats object
stats = pstats.Stats(profiler).sort_stats("cumulative")
# Print the stats
# stats.print_stats()

# Optionally, you can save the stats to a file
stats.dump_stats("profile_stats.prof")
# Load the stats
# loaded_stats = pstats.Stats("profile_stats.prof")

# Print the stats
# loaded_stats.strip_dirs().sort_stats("cumulative").print_stats()
