# BHuman Log Processing Repository

This repository is designed to process BHuman logs within Python. Generally, the log file is a human unreadable byte file in a hierarchical structure. For detailed information on the log file format, refer to the [BHuman Logging Documentation](https://docs.b-human.de/coderelease2023/architecture/logging/#log-file-format).

The log file records the `MessageQueue` data structure in BHuman's code. However, the BHuman LogReader is deeply coupled with SimRobot's other infrastructure, making it difficult to work on the top of it. This repository provides a tree-structure interface to manipulate images/representations.

## Simple Usage

### Small Log File

For small log files, it is recommended to parse all bytes at once:

1. `readLogFile(filePath)`
2. `eval()`
3. `parseBytes()`
4. Perform operations on the parsed data

### Large Log File

For large log files (if log file size $\times$ 15 > your memory size):

1. `readLogFile(filePath)`
2. `eval(isLogFileLarge=True)`
3. Perform operations on the parsed data

## More In-Depth

The difference between the two modes above is the memory strategy:

- **Small Log File**: Use the `LogInterfaceInstanceClass`, which stores everything in the instance itself. It is generally faster when you need to access all frame's all information.
- **Large Log File**: Use the `LogInterfaceAccessorClass`, which functions like an iterator. All information is cached in the `Log` class, allowing better control of total memory consumption (e.g., setting an upper bound for the cache dictionary).

For large files or when you only need to access part of the frames (e.g., logs from the Cognition thread where Neural Control is running), use `eval(isLogFileLarge=True)` and get an accessor class by `LOG.UncompressedChunk.threads["Cognition"]`. This accessor is an iterator that iterates through all frames in the thread.

TODO: Poor performance. Due to python's less compact classes and my programming skill limit. Currently the performance is relatively poor