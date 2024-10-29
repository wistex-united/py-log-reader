import asyncio
import csv
import functools
import os
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from multiprocessing import Pool, cpu_count
from pathlib import Path
from threading import Lock, Thread
from typing import Dict, List, Optional

import numpy as np
from numpy.typing import NDArray
from tqdm import tqdm

from Primitive.PrimitiveDefinitions import UChar
from StreamUtils import AbsoluteByteIndex, StreamUtil, SutilCursor
from Utils import MemoryMappedFile

from .Chunk import Chunk, ChunkEnum
from .DataClasses import DataClass, Stopwatch, Timer
from .Frame import FrameAccessor, FrameBase, FrameInstance, Frames
from .LogInterfaceBase import IndexMap, LogInterfaceAccessorClass
from .Message import MessageAccessor, MessageBase, MessageInstance, Messages


class UncompressedChunk(Chunk):
    """
    This chunk stores all the messages in the log file
    It contains list of Frames
    """

    def __init__(self, parent):
        super().__init__(parent)
        self._threads: Dict[str, Frames] = {}
        self._timers: Dict[str, Timer] = {}

        # cached index of messages and data objects
        self._messages_cached: Messages
        self._threadIndexMaps_cahced: Dict[str:NDArray]
        self._executor: ThreadPoolExecutor
        self._lock: Lock
        self._postProcessorThread: Thread

        # self._initExecutor()

    def _initExecutor(self):
        self._executor = ThreadPoolExecutor(max_workers=cpu_count())
        self._futures = defaultdict(list)
        self._lock = Lock()
        self._postProcessorThread = Thread(target=self.fetchParsedMessages)
        self._postProcessorThread.daemon = True
        self._postProcessorThread.start()

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

    def evalFrameAccessor(self, sutil: StreamUtil, offset: int = 0):
        """
        This is the new eval function to support large log files
        The main difference is that it will write index files and instantiate UncompressedChunk.frames to Accessor class instead of Instance class
        The Accessor class parse log bytes on need and cache it in Log class
        The Instance class needed to be parsed before accessing its attributes and cache in its own class
        """

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

        if not UncompressedChunk.ensureIndexFilesValid(self.log):
            self.clearIndexFiles()

        try:
            messageAccessor = MessageAccessor(self.log)
            lastMessage = messageAccessor[-1]
            frameAccessor = FrameAccessor(self.log)
            lastFrame = frameAccessor[-1]
            # Since this frame accessor's parent is not fully initialized, parent related field should not be used
            frameCnt = lastFrame.absIndex + 1
            messageCnt = lastMessage.absIndex + 1
            byteIndex = lastMessage.endByte - messageStartByte
            sutil.seek(lastMessage.endByte - offset + startPos)
        except OSError:
            pass

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
                messageIdxFile.write(
                    MessageAccessor.encodeIndexBytes(
                        (messageCnt, frameCnt, message.startByte, message.endByte)
                    )
                )

                messageCnt += 1

            frameMessageIndexEnd = messageCnt

            frameIdxFile.write(
                FrameAccessor.encodeIndexBytes(
                    (
                        frameCnt,
                        frame.threadName,
                        frameMessageIndexStart,
                        frameMessageIndexEnd,
                    )
                )
            )

            byteIndex += frame.size
            frameCnt += 1
        self.frames = self.log.getFrameAccessor()

        for threadName, indexes in self.threadIndexMaps.items():
            self._threads[threadName] = FrameAccessor(self.log, indexes)
        self._startByte = offset
        self._endByte = sutil.tell() - startPos + offset

    def evalFrameAndMessageInstances(self, sutil: StreamUtil, offset: int = 0):
        """
        Norma eval function
        It will instantiate and create a list of frame & message instance class with their bytes index in logfile provided (not parsed yet)
        """
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

        self.frames = []

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

    def eval(self, sutil: StreamUtil, offset: int = 0, evalAccessor: bool = False):
        """
        Consistent interface for eval
        """
        if evalAccessor:
            self.evalFrameAccessor(sutil, offset)
        else:
            self.evalFrameAndMessageInstances(sutil, offset)

    def parseBytes(self, showProgress: bool = True, cacheReprs: bool = False):
        """
        DEPENDENCY: eval()
        Warning: Consume lots of memory (2GB logfile will consume 30GB memory)
        Parse the whole log file in to representation objects in messages class (Bhuman)
        It need instance classes to be already create by eval()
        It can also cache all the representation objects into pickle files and will be automatically loaded (No need to be parsed next time)
        """
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
        # for message in unparsed:
        #     Wrapper((message.startByte + 4, message.endByte, message.classType.read))
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
        if cacheReprs:
            asyncio.get_event_loop().run_until_complete(
                self.dumpReprs(results, unparsed)
            )

    # background parsing
    @property
    @functools.lru_cache(maxsize=1)
    def parseBytesWrapper(self):
        return partial(MessageBase.parseBytesWrapper, logFilePath=self.logFilePath)

    def submitJob(self, message: MessageAccessor):
        future = self._executor.submit(
            self.parseBytesWrapper,
            message.startByte + 4,
            message.endByte,
            message.classType.read,
        )
        with self.lock:
            self._futures[message.absIndex] = future

    def fetchParsedMessages(self):
        while True:
            with self._lock:
                finishedFutures = []
                for messageAbsIdx, future in self._futures.items():
                    if future.done():
                        result = future.result()
                        # message.reprObj = result
                        # if isinstance(result, Stopwatch):
                        #     frameTmp = message.frame
                        #     frameTmp.timer.parseStopwatch(result, frameTmp.index)
                        self.log.writeCacheInfo(
                            "Message", "reprObj", messageAbsIdx, result
                        )
                        finishedFutures.append(messageAbsIdx)
                for messageAbsIdx in finishedFutures:
                    del self._futures[messageAbsIdx]
            # time.sleep(1)  # Adding a small delay to prevent high CPU usage

    @property
    def threadIndexMaps(self):
        if (
            hasattr(self, "_threadIndexMaps_cahced")
            and len(self._threadIndexMaps_cahced) > 0
        ):
            pass
        else:
            threadIndexMaps = {}
            for index in range(len(self.frames)):
                frame = self.frames[index]
                if frame.threadName not in threadIndexMaps:
                    threadIndexMaps[frame.threadName] = [index]
                else:
                    threadIndexMaps[frame.threadName].append(index)
            for threadName, indexes in threadIndexMaps.items():
                threadIndexMaps[threadName] = np.array(indexes)
            self._threadIndexMaps_cahced = threadIndexMaps
        return self._threadIndexMaps_cahced

    # Index file Validation
    @classmethod
    def ensureIndexFilesValid(
        cls,
        log,
        checkFrameRange: Optional[IndexMap] = None,
        detailedCheck: bool = False,
    ):
        """
        Check if the index file is valid, if not, try to fix it
        If cannot fix, return False, else return True
        """

        indexFrameFilePath = log.cacheDir / FrameAccessor.idxFileName()
        indexMessageFilePath = log.cacheDir / MessageAccessor.idxFileName()
        if not indexFrameFilePath.exists() or not indexMessageFilePath.exists():
            return False
        try:
            tempFrameIdxFile = MemoryMappedFile(indexFrameFilePath)
            tempMessageIdxFile = MemoryMappedFile(indexMessageFilePath)
        except OSError as e:
            return False

        if detailedCheck:
            checkThroughResult = True
            checkThroughResult = (
                cls.checkThroughMessageIndex(log, checkMessageRange=None)
                and checkThroughResult
            )
            checkThroughResult = (
                cls.checkThroughFrameIndex(log, checkFrameRange=None)
                and checkThroughResult
            )
            print(f"checkThroughResult: {checkThroughResult}")

        frameIndexFileSize = tempFrameIdxFile.getSize()
        messageIndexFileSize = tempMessageIdxFile.getSize()

        lastFrameIndex = frameIndexFileSize // FrameAccessor.frameIdxByteLength - 1

        frameTruncatePos = frameIndexFileSize // FrameAccessor.frameIdxByteLength
        messageTruncatePos = (
            messageIndexFileSize // MessageAccessor.messageIdxByteLength
        )

        if isinstance(checkFrameRange, range):
            checkFrameRange = list(checkFrameRange)

        if checkFrameRange is None:
            checkFrameRange = [lastFrameIndex]
        else:
            checkFrameRange += [lastFrameIndex]  # always check the last frame
        # for i in indexMap:

        i = 0
        while True:
            if i >= len(checkFrameRange):
                break
            frameIdx = checkFrameRange[i]
            if frameIdx < 0:
                return False  # Didn't find any valid frame

            if frameIdx > lastFrameIndex:
                continue  # Skip this invalid frame

            startByte = frameIdx * FrameAccessor.frameIdxByteLength
            endByte = startByte + FrameAccessor.frameIdxByteLength
            frameIndex = FrameAccessor.decodeIndexBytes(
                tempFrameIdxFile.getData()[startByte:endByte]
            )

            if (
                frameIndex[0] != frameIdx
            ):  # This is a big problem, the whole file might be wrong
                i = 0
                checkFrameRange = [frameIdx - 1]
                lastFrameIndex = frameIdx - 1
                continue
            for msgAbsIdx in range(frameIndex[2], frameIndex[3]):
                if not MessageAccessor.validate(
                    tempMessageIdxFile, msgAbsIdx, frameIndex[0]
                ):  # This is a small problem, usually caused by writing interrupted by keyboard
                    frameTruncatePos = frameIdx
                    messageTruncatePos = frameIndex[2]
                    checkFrameRange.append(frameIdx - 1)  # Check the position before it
                    lastFrameIndex = frameIdx - 1
                    break
            # Till here we make sure the last frame is valid
            if frameIndex[0] == lastFrameIndex:
                lastMessageIndex = int(frameIndex[3])

                frameTruncatePos = (
                    int(frameIndex[0] + 1)
                    if int(frameIndex[0] + 1) < frameTruncatePos
                    else frameTruncatePos
                )
                messageTruncatePos = (
                    lastMessageIndex
                    if lastMessageIndex < messageTruncatePos
                    else messageTruncatePos
                )
                # updateTruncatePos(frameIndex[0], lastMessageIndex)
            i += 1
        with open(indexFrameFilePath, "r+b") as f:
            f.truncate(frameTruncatePos * FrameAccessor.frameIdxByteLength)
        with open(indexMessageFilePath, "r+b") as f:
            f.truncate(messageTruncatePos * MessageAccessor.messageIdxByteLength)
        return True

    @classmethod
    def checkThroughFrameIndex(
        cls,
        log,
        checkFrameRange: Optional[IndexMap] = None,
    ):
        """Check all the frames in frame index file to validate the correctness of the index file"""
        indexFrameFilePath = log.cacheDir / FrameAccessor.idxFileName()
        try:
            tempFrameIdxFile = MemoryMappedFile(indexFrameFilePath)
        except OSError as e:
            return False

        frameIndexFileSize = tempFrameIdxFile.getSize()
        lastFrameIndex = frameIndexFileSize // FrameAccessor.frameIdxByteLength - 1

        if isinstance(checkFrameRange, range):
            checkFrameRange = list(checkFrameRange)

        if checkFrameRange is None:
            checkFrameRange = range(0, lastFrameIndex + 1)
        else:
            checkFrameRange += [lastFrameIndex]

        for frameIdx in tqdm(checkFrameRange, desc="Checking Frame Index"):
            startByte = frameIdx * FrameAccessor.frameIdxByteLength
            endByte = startByte + FrameAccessor.frameIdxByteLength
            frameIndexBytes = tempFrameIdxFile.getData()[startByte:endByte]
            frameIndex = FrameAccessor.decodeIndexBytes(frameIndexBytes)

            if frameIndex[0] != frameIdx:
                return False
            if frameIdx != 0:
                prevFrameIndex = FrameAccessor.decodeIndexBytes(
                    tempFrameIdxFile.getData()[
                        startByte
                        - FrameAccessor.frameIdxByteLength : endByte
                        - FrameAccessor.frameIdxByteLength
                    ]
                )
                if frameIndex[2] != prevFrameIndex[3]:
                    return False
        return True

    @classmethod
    def checkThroughMessageIndex(
        cls,
        log,
        checkMessageRange: Optional[IndexMap] = None,
    ):
        """Check messages in message index file to validate the correctness of the index file"""

        indexMessageFilePath = log.cacheDir / MessageAccessor.idxFileName()
        try:
            tempMessageIdxFile = MemoryMappedFile(indexMessageFilePath)
        except OSError as e:
            return False

        messageIndexFileSize = tempMessageIdxFile.getSize()
        lastMessageIndex = (
            messageIndexFileSize // MessageAccessor.messageIdxByteLength - 1
        )

        if isinstance(checkMessageRange, range):
            checkMessageRange = list(checkMessageRange)

        if checkMessageRange is None:
            checkMessageRange = range(0, lastMessageIndex + 1)
        else:
            checkMessageRange += [lastMessageIndex]

        for messageIdx in tqdm(checkMessageRange, desc="Checking Message Index"):
            startByte = messageIdx * MessageAccessor.messageIdxByteLength
            endByte = startByte + MessageAccessor.messageIdxByteLength
            messageIndexBytes = tempMessageIdxFile.getData()[startByte:endByte]
            messageIndex = MessageAccessor.decodeIndexBytes(messageIndexBytes)

            if messageIndex[0] != messageIdx:
                return False
            if messageIdx != 0:
                prevMessageIndex = MessageAccessor.decodeIndexBytes(
                    tempMessageIdxFile.getData()[
                        startByte
                        - MessageAccessor.messageIdxByteLength : endByte
                        - MessageAccessor.messageIdxByteLength
                    ]
                )
                if messageIndex[2] != prevMessageIndex[3]:
                    return False
        return True

    # Repr batch IO
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

    def __setstate__(self, state):
        super().__setstate__(state)
        # The Acceesors in theads need to be assigned manually
        if hasattr(self, "_threads"):
            for threadName, thread in self._threads.items():
                if isinstance(thread, LogInterfaceAccessorClass):
                    thread.log = self
                else:
                    pass  # Probably list of Frame Instance, they are in _children list and parent alredy set
        # self._initExecutor()

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
            if isinstance(frame.children, LogInterfaceAccessorClass):
                for message in frame.children:
                    self._messages_cached.append(message.copy().freeze())
            else:  # Instance class
                self._messages_cached.extend(frame.messages)
        return self._messages_cached

    def thread(self, name: str) -> Frames:
        return self._threads[name]

    @property
    def threads(self) -> Dict[str, Frames]:
        return self._threads

    @property
    def threadNames(self) -> List[str]:
        return list(self._threads.keys())
