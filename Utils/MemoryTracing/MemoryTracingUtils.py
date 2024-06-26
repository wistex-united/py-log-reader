import linecache
import tracemalloc


def startMemoryTracing():
    tracemalloc.start()


def displayTopMemoryConsumers(snapshot, keyType="lineno", limit=10):
    snapshot = snapshot.filter_traces(
        (
            tracemalloc.Filter(False, "<frozen importlib._bootstrap>"),
            tracemalloc.Filter(False, "<unknown>"),
        )
    )
    topStats = snapshot.statistics(keyType)

    print(f"Top {limit} lines")
    for index, stat in enumerate(topStats[:limit], 1):
        frame = stat.traceback[0]
        print(f"#{index}: {frame.filename}:{frame.lineno} - {stat.size / 1024:.1f} KiB")
        line = linecache.getline(frame.filename, frame.lineno).strip()
        if line:
            print(f"    {line}")

    other = topStats[limit:]
    if other:
        size = sum(stat.size for stat in other)
        print(f"{len(other)} other: {size / 1024:.1f} KiB")
    total = sum(stat.size for stat in topStats)
    print(f"Total allocated size: {total / 1024:.1f} KiB")


def stopMemoryTracing():
    snapshot = tracemalloc.take_snapshot()
    displayTopMemoryConsumers(snapshot)
