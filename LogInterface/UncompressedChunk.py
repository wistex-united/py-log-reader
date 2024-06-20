import asyncio
import csv
import io
import os
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import Dict, List

import numpy as np
from tqdm import tqdm

from Primitive.PrimitiveDefinitions import UChar
from StreamUtils import *

from .Chunk import Chunk, ChunkEnum
from .DataClasses import DataClass, Stopwatch, Timer
from .Frame import FrameAccessor, FrameBase, FrameInstance, Frames
from .Message import MessageAccessor, MessageBase, MessageInstance, Messages

SutilCursor = int
AbsoluteByteIndex = int


class UncompressedChunk(Chunk):
    def __init__(self, parent):
        super().__init__(parent)
        self._threads: Dict[str, Frames] = {}
        self._timers: Dict[str, Timer] = {}

        # cached index of messages and data objects
        self._messages_cached: Messages
        self._reprs_cached: List[DataClass]

    @property
    def frames(self) -> Frames:
        return self._children  # type: ignore

    @frames.setter
    def frames(self, value: Frames):
        self._children = value

    def clearIndexFiles(self):
        messageIdxFilePath: Path = (
            self.log.cacheDir / MessageAccessor.messageIdxFileName
        )
        frameIdxFilePath: Path = self.log.cacheDir / FrameAccessor.frameIdxFileName
        if messageIdxFilePath.exists():
            messageIdxFilePath.unlink()
        if frameIdxFilePath.exists():
            frameIdxFilePath.unlink()

    def verifyIndexFiles(self):

        messageIdxFilePath: Path = (
            self.log.cacheDir / MessageAccessor.messageIdxFileName
        )
        frameIdxFilePath: Path = self.log.cacheDir / FrameAccessor.frameIdxFileName

        findContinuePos = False
        if frameIdxFilePath.exists():
            tempFrameIndexFileHolder = open(frameIdxFilePath, "rb+")
            frameIdxFileSize = frameIdxFilePath.stat().st_size
            remainder = frameIdxFileSize % FrameAccessor.frameIdxByteLength
            validSize = frameIdxFileSize - remainder
            # FrameAccessor
            if validSize < FrameAccessor.frameIdxByteLength:
                # raise Exception("Strange frameIndexFile size")
                self.clearIndexFiles()
                return 0, 0

            tempFrameIndexFileHolder.seek(-32 - remainder, io.SEEK_END)
            last32Bytes = tempFrameIndexFileHolder.read(32)
            if remainder != 0:
                tempFrameIndexFileHolder.truncate(frameIdxFileSize - remainder)

            lastFrameIndex = FrameAccessor.decodeIndexBytes(last32Bytes)

            if lastFrameIndex[0] != validSize // 32 - 1:
                raise Exception("frameIndexFile is not valid")

            if messageIdxFilePath.exists():
                tempMessageIndexFileHolder = open(messageIdxFilePath, "rb+")

                messageIdxFileSize = messageIdxFilePath.stat().st_size
                remainder = messageIdxFileSize % 32
                validSize = messageIdxFileSize - remainder
                if validSize < 32:
                    raise Exception("Strange messageIndexFile size")

                tempMessageIndexFileHolder.seek(-32 - remainder, io.SEEK_END)
                last32Bytes = tempMessageIndexFileHolder.read(32)
                if remainder != 0:
                    tempMessageIndexFileHolder.truncate(messageIdxFileSize - remainder)

                lastMessageIndex = np.frombuffer(last32Bytes, dtype=np.uint64)

                if lastMessageIndex[0] != validSize // 32 - 1:
                    raise Exception("messageIndexFile is not valid")
                if (
                    lastMessageIndex[1] < lastFrameIndex[0]
                    or lastMessageIndex[0] < lastFrameIndex[3] - 1
                ):
                    raise Exception(
                        "frameIndexFile and messageIndexFile are not consistent"
                    )
                else:
                    if (
                        lastMessageIndex[1] > lastFrameIndex[0]
                        and lastMessageIndex[0] > lastFrameIndex[3]
                    ):  # If the are more messages after the last recorded frames
                        tempMessageIndexFileHolder.truncate(
                            int(lastFrameIndex[3]) * 32
                        )  # Cut message file to the end of last frame's last message
                        tempMessageIndexFileHolder.seek(-32, io.SEEK_END)
                        lastMessageIndex = np.frombuffer(
                            tempMessageIndexFileHolder.read(32), dtype=np.uint64
                        )
                    frameCnt = lastFrameIndex[0] + 1
                    messageCnt = lastMessageIndex[0] + 1
                    byteIndex = int(lastMessageIndex[3] - np.uint64(messageStartByte))
                    sutil.seek(int(lastMessageIndex[3]) - offset + startPos)
                    findContinuePos = True
                if not findContinuePos:
                    tempMessageIndexFileHolder.seek(0)
                    tempMessageIndexFileHolder.truncate(0)
                tempMessageIndexFileHolder.close()
            if not findContinuePos:
                tempFrameIndexFileHolder.seek(0)
                tempFrameIndexFileHolder.truncate(0)
            tempFrameIndexFileHolder.close()

    def evalLarge(self, sutil: StreamUtil, offset: int = 0):

        startPos: SutilCursor = sutil.tell()
        chunkMagicBit: UChar = sutil.readUChar()
        if chunkMagicBit != ChunkEnum.UncompressedChunk.value:
            raise Exception(
                f"Expect magic number {ChunkEnum.UncompressedChunk.value}, but get:{chunkMagicBit}"
            )

        header = sutil.readQueueHeader()

        headerSize = sutil.tell() - 1 - startPos
        usedSize = int(header[0]) << 32 | int(header[2])
        logSize = os.path.getsize(self.parent.logFilePath)
        remainingSize = logSize - offset
        hasIndex = header[1] != 0x0FFFFFFF and usedSize != (logSize - offset)

        messageStartByte = offset + (sutil.tell() - startPos)
        byteIndex = 0
        frameCnt = 0
        messageCnt = 0
        self.log.cacheDir.mkdir(parents=True, exist_ok=True)

        messageIdxFilePath: Path = (
            self.log.cacheDir / MessageAccessor.messageIdxFileName
        )
        frameIdxFilePath: Path = self.log.cacheDir / FrameAccessor.frameIdxFileName

        # startPosition = sutil.tell()
        findContinuePos = False
        if frameIdxFilePath.exists():
            tempFrameIndexFileHolder = open(frameIdxFilePath, "rb+")
            frameIdxFileSize = frameIdxFilePath.stat().st_size
            remainder = frameIdxFileSize % 32
            validSize = frameIdxFileSize - remainder
            if validSize < 32:
                raise Exception("Strange frameIndexFile size")

            tempFrameIndexFileHolder.seek(-32 - remainder, io.SEEK_END)
            last32Bytes = tempFrameIndexFileHolder.read(32)
            FA = FrameAccessor(self.log)
            if remainder != 0:
                tempFrameIndexFileHolder.truncate(frameIdxFileSize - remainder)

            lastFrameIndex = (
                np.frombuffer(last32Bytes[0:4], np.uint32)[0],
                last32Bytes[4:16].decode("ascii").rstrip("\0"),
                np.frombuffer(last32Bytes[16:24], np.uint64)[0],
                np.frombuffer(last32Bytes[24:32], np.uint64)[0],
            )

            if lastFrameIndex[0] != validSize // 32 - 1:
                raise Exception("frameIndexFile is not valid")

            if messageIdxFilePath.exists():
                tempMessageIndexFileHolder = open(messageIdxFilePath, "rb+")

                messageIdxFileSize = messageIdxFilePath.stat().st_size
                remainder = messageIdxFileSize % 32
                validSize = messageIdxFileSize - remainder
                if validSize < 32:
                    raise Exception("Strange messageIndexFile size")

                tempMessageIndexFileHolder.seek(-32 - remainder, io.SEEK_END)
                last32Bytes = tempMessageIndexFileHolder.read(32)
                if remainder != 0:
                    tempMessageIndexFileHolder.truncate(messageIdxFileSize - remainder)

                lastMessageIndex = np.frombuffer(last32Bytes, dtype=np.uint64)

                if lastMessageIndex[0] != validSize // 32 - 1:
                    raise Exception("messageIndexFile is not valid")
                if (
                    lastMessageIndex[1] < lastFrameIndex[0]
                    or lastMessageIndex[0] < lastFrameIndex[3] - 1
                ):
                    raise Exception(
                        "frameIndexFile and messageIndexFile are not consistent"
                    )
                else:
                    if (
                        lastMessageIndex[1] > lastFrameIndex[0]
                        and lastMessageIndex[0] > lastFrameIndex[3]
                    ):  # If the are more messages after the last recorded frames
                        tempMessageIndexFileHolder.truncate(
                            int(lastFrameIndex[3]) * 32
                        )  # Cut message file to the end of last frame's last message
                        tempMessageIndexFileHolder.seek(-32, io.SEEK_END)
                        lastMessageIndex = np.frombuffer(
                            tempMessageIndexFileHolder.read(32), dtype=np.uint64
                        )
                    frameCnt = lastFrameIndex[0] + 1
                    messageCnt = lastMessageIndex[0] + 1
                    byteIndex = int(lastMessageIndex[3] - np.uint64(messageStartByte))
                    sutil.seek(int(lastMessageIndex[3]) - offset + startPos)
                    findContinuePos = True
                if not findContinuePos:
                    tempMessageIndexFileHolder.seek(0)
                    tempMessageIndexFileHolder.truncate(0)
                tempMessageIndexFileHolder.close()
            if not findContinuePos:
                tempFrameIndexFileHolder.seek(0)
                tempFrameIndexFileHolder.truncate(0)
            tempFrameIndexFileHolder.close()

        messageIdxFile = open(messageIdxFilePath, "ab")
        frameIdxFile = open(frameIdxFilePath, "ab")

        while byteIndex < min(usedSize, remainingSize):
            frame = FrameInstance(self)
            try:
                frame.eval(sutil, byteIndex + messageStartByte)
            except EOFError:
                break  # TODO: check this, should not be EOFError in UncompressedChunk

            frameMessageIndexStart = messageCnt
            for message in frame.messages:
                messageIndexBytes = np.array(
                    [messageCnt, frameCnt, message.startByte, message.endByte],
                    dtype=np.uint64,
                ).tobytes()
                messageIdxFile.write(messageIndexBytes)

                messageCnt += 1
            frameMessageIndexEnd = messageCnt

            frameIndexBytes = (
                np.uint32(frameCnt).tobytes()
                + frame.threadName.encode("ascii").ljust(12, b"\0")
                + np.uint64(frameMessageIndexStart).tobytes()
                + np.uint64(frameMessageIndexEnd).tobytes()
            )

            frameIdxFile.write(frameIndexBytes)

            byteIndex += frame.size
            frameCnt += 1

        self._startByte = offset
        self._endByte = sutil.tell() - startPos + offset

    def eval(self, sutil: StreamUtil, offset: int = 0):
        startPos = sutil.tell()
        chunkMagicBit = sutil.readUChar()
        if chunkMagicBit != ChunkEnum.UncompressedChunk.value:
            raise Exception(
                f"Expect magic number {ChunkEnum.UncompressedChunk.value}, but get:{chunkMagicBit}"
            )

        self.frames = []

        header = sutil.readQueueHeader()

        headerSize = sutil.tell() - 1 - startPos
        usedSize = int(header[0]) << 32 | int(header[2])
        logSize = os.path.getsize(self.parent.logFilePath)
        remainingSize = logSize - offset
        hasIndex = header[1] != 0x0FFFFFFF and usedSize != (logSize - offset)

        messageStartByte = offset + (sutil.tell() - startPos)
        byteIndex = 0
        frameIndex = []
        while byteIndex < min(usedSize, remainingSize):
            frame = FrameInstance(self)
            try:
                frame.eval(sutil, byteIndex + messageStartByte)
            except EOFError:
                break  # TODO: check this, should not be EOFError in UncompressedChunk
            self.frames.append(frame)

            if frame.threadName not in self._threads:
                self._threads[frame.threadName] = []
                self._timers[frame.threadName] = Timer()

            self._threads[frame.threadName].append(frame)  # type: ignore

            frameIndex.append(frame.startByte - messageStartByte)
            byteIndex += frame.size

        for threadName, threadFrames in self._threads.items():
            self._timers[threadName].initStorage(
                [frame.index for frame in threadFrames]
            )

        self._startByte = offset
        self._endByte = sutil.tell() - startPos + offset

    def parseBytes(self, showProgress: bool = True, cacheRepr: bool = True):
        Wrapper = partial(MessageBase.parseBytesWrapper, logFilePath=self.logFilePath)
        cached = []
        parsed = []
        unparsed = []

        # currently parsing everything is faster TODO: check what cause this strange phenomena

        for message in tqdm(
            self.messages, desc="Checking Message Parsed", disable=not showProgress
        ):
            if message.isParsed:
                parsed.append(message)
                continue
            elif message.hasPickledRepr():
                cached.append(message)
            else:
                unparsed.append(message)

        # failed = asyncio.get_event_loop().run_until_complete(self.loadReprs(cached))
        # unparsed.extend(failed)
        # for message in failed:
        #     print(f"Failed to parse message {message.index}")

        # unparsed = self.messages
        if len(unparsed) == 0:
            print("All messages are parsed")
            return
        with Pool(cpu_count()) as p:
            results = list(
                tqdm(
                    p.imap(
                        Wrapper,
                        [
                            (
                                message.startByte + 4,
                                message.endByte,
                                message.classType.read,
                            )
                            for message in unparsed
                        ],
                    ),
                    total=len(unparsed),
                    desc="Parsing All Messages",
                )
            )
        for idx, result in tqdm(
            enumerate(results),
            total=len(results),
            desc="Distributing All Messages",
        ):
            unparsed[idx].reprObj = result
            if isinstance(result, Stopwatch):
                # if not hasattr(self.messages[idx].frame, "timer"):
                frameTmp: FrameBase = unparsed[idx].frame
                frameTmp.timer.parseStopwatch(result, frameTmp.index)
        if cacheRepr:
            asyncio.get_event_loop().run_until_complete(
                self.dumpReprs(results, unparsed)
            )

    async def loadReprs(self, unparsed: Messages) -> List[DataClass]:
        loop = asyncio.get_running_loop()
        failed = []

        # Create a ThreadPoolExecutor
        with ThreadPoolExecutor() as executor:
            # Schedule the synchronous tasks in the executor
            futures = [
                loop.run_in_executor(
                    executor,
                    MessageBase.loadReprWrapper,
                    unparsed[idx].reprPicklePath,
                    idx,
                )
                for idx in tqdm(
                    range(len(unparsed)),
                    total=len(unparsed),
                    desc="Queuing All Repr to Load",
                )
            ]

            # Show progress bar for loading
            for future in tqdm(
                asyncio.as_completed(futures),
                total=len(futures),
                desc="Loading All Representations",
            ):
                result, index = await future
                if result is not None:
                    unparsed[index].reprObj = result
                failed.append(unparsed[index])

        return failed

    async def dumpReprs(self, results: List[DataClass], unparsed: Messages):
        loop = asyncio.get_running_loop()

        # Create a ThreadPoolExecutor
        with ThreadPoolExecutor() as executor:
            # Schedule the synchronous tasks in the executor
            futures = [
                loop.run_in_executor(
                    executor,
                    MessageBase.dumpReprWrapper,
                    unparsed[idx].reprPicklePath,
                    results[idx],
                )
                for idx in tqdm(
                    range(len(results)),
                    total=len(results),
                    desc="Queuing All Repr to Dump",
                )
            ]

            # Show progress bar for queuing
            for future in tqdm(
                asyncio.as_completed(futures),
                total=len(futures),
                desc="Dumping All Representations",
            ):
                await future

    def numFrames(self):
        return len(self.frames)

    def asDict(self):
        return {
            "numFrames": self.numFrames(),
            "frames": [frame.asDict() for frame in self.frames],
        }

    def writeMessageIndexCsv(self, filePath):
        with open(filePath, "w") as f:
            writer = csv.writer(f)
            for message in self.messages:
                writer.writerow(
                    [
                        message.index,
                        message.frame.index,
                        message.logId,
                        message.startByte,
                        message.endByte,
                    ]
                )

    def readMessageIndexCsv(self, indexFilePath, logFilePath):
        # MessageID = self.log.MessageID
        self._children = []
        self._threads = {}
        self._timers = {}
        with open(indexFilePath, "r") as f:
            reader = csv.reader(f)
            # next(reader)
            frame: FrameInstance = None  # type: ignore
            for row in reader:
                if int(row[2]) == 1:  # idFrameBegin
                    frame = FrameInstance(self)
                    frame._children = []
                    frame.dummyMessages = []

                message = MessageInstance(frame)
                message._index_cached = int(row[0])
                message._logId = UChar(row[2])
                message._startByte = int(row[3])
                message._endByte = int(row[4])

                if len(frame.children) != message.index:
                    raise ValueError
                frame._children.append(message)  # type: ignore
                if int(row[2]) == 2:  # idFrameEnd
                    frame._index_cached = int(row[1])
                    if len(self.frames) != frame.index:
                        raise ValueError
                    frame._startByte = frame.messages[0].startByte
                    frame._endByte = frame.messages[-1].endByte

                    self._children.append(frame)
                    with open(logFilePath, "rb") as logFile:
                        nameStartByte = frame.messages[-1].startByte + 4 + 4
                        nameEndByte = frame.messages[-1].endByte
                        logFile.seek(nameStartByte)
                        threadName = logFile.read(nameEndByte - nameStartByte).decode()
                    if threadName not in self._threads:
                        self._threads[threadName] = []
                        self._timers[threadName] = Timer()
                    self._threads[threadName].append(frame)  # type: ignore

        for threadName, threadFrames in self._threads.items():
            self._timers[threadName].initStorage(
                [frame.index for frame in threadFrames]
            )
        if len(self.frames) == 0:
            raise ValueError

        self._startByte = self.frames[0].startByte
        self._endByte = self.frames[-1].endByte

    def __getstate__(self):
        filePath = f"{self.log.cacheDir}/messageIndex.csv"
        self.writeMessageIndexCsv(filePath)
        return (filePath, self.logFilePath)

    def __setstate__(self, state):
        if isinstance(state, tuple):
            self.readMessageIndexCsv(*state)
        else:
            super().__setstate__(state)

    @property
    def providedAttributes(self) -> List[str]:
        return ["frames"]

    @property
    def children(self) -> Frames:
        return self.frames

    @property
    def messages(self) -> Messages:
        if hasattr(self, "_messages_cached"):
            return self._messages_cached
        self._messages_cached = []
        for frame in self.frames:
            self._messages_cached.extend(frame.messages)  # type: ignore
        return self._messages_cached

    @property
    def reprs(self) -> List[DataClass]:
        if hasattr(self, "_reprs_cached"):
            return self._reprs_cached
        self._reprs_cached = []
        for frame in self.frames:
            for message in frame.messages:
                self._reprs_cached.append(message.reprObj)
        return self._reprs_cached

    def thread(self, name: str) -> Frames:
        return self._threads[name]

    @property
    def threads(self) -> Dict[str, Frames]:
        return self._threads

    @property
    def threadNames(self) -> List[str]:
        return list(self._threads.keys())
