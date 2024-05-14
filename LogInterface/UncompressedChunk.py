from asyncio import as_completed
import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from multiprocessing.managers import SharedMemoryManager
import os
from pathlib import Path
from re import M, T
from typing import Dict, List, Set

from multiprocessing import Pool, Process, cpu_count, shared_memory

from tqdm import tqdm

from .DataClasses import DataClass
from LogInterface import Message, Stopwatch, Timer
from .Chunk import Chunk, ChunkEnum
from StreamUtils import *
from .Frame import Frame


class UncompressedChunk(Chunk):
    def __init__(self, parent):
        super().__init__(parent)
        self._threads: Dict[str, List[Frame]] = {}
        self._timers: Dict[str, Timer] = {}

        # cached index of messages and data objects
        self._messages_cached: List[Message]
        self._reprs_cached: List[DataClass]

    @property
    def frames(self) -> List[Frame]:
        return self._children

    @frames.setter
    def frames(self, value: List[Frame]):
        self._children = value

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
            frame = Frame(self)
            try:
                frame.eval(sutil, byteIndex + messageStartByte)
            except EOFError:
                break  # TODO: check this, should not be EOFError in UncompressedChunk
            self.frames.append(frame)

            if frame.threadName not in self._threads:
                self._threads[frame.threadName] = []
                self._timers[frame.threadName] = Timer()

            self._threads[frame.threadName].append(frame)

            frameIndex.append(frame.startByte - messageStartByte)
            byteIndex += frame.size

        for threadName, threadFrames in self._threads.items():
            self._timers[threadName].initStorage(
                [frame.index for frame in threadFrames]
            )

        self._threads

        self._startByte = offset
        self._endByte = sutil.tell() - startPos + offset

    def parseBytes(self, showProgress: bool = True, cacheRepr: bool = True):
        Wrapper = partial(Message.parseBytesWrapper, logFilePath=self.logFilePath)
        unparsed = [message for message in self.messages if not message.hasCachedRepr()]
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
                frameTmp: Frame = unparsed[idx].frame
                frameTmp.timer.parseStopwatch(result, frameTmp.index)
        if cacheRepr:
            asyncio.get_event_loop().run_until_complete(self.dumpReprs(results, unparsed))

    async def dumpReprs(self, results, unparsed):
        loop = asyncio.get_event_loop()

        with ThreadPoolExecutor() as executor:
            futures = []
            futures = list(
                tqdm(
                    (
                        loop.run_in_executor(
                            executor,
                            Message.asyncDumpReprWrapper,
                            unparsed[idx].reprPicklePath,
                            results[idx],
                        )
                        for idx in range(len(results))
                    ),
                    total=len(results),
                    desc="Queuing All Representations",
                )
            )

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

    @property
    def providedAttributes(self) -> List[str]:
        return ["frames"]

    @property
    def children(self) -> List[Frame]:
        return self.frames

    @property
    def messages(self) -> List[Message]:
        if hasattr(self, "_messages_cached"):
            return self._messages_cached
        self._messages_cached = []
        for frame in self.frames:
            self._messages_cached.extend(frame.messages)
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

    def thread(self, name: str) -> List[Frame]:
        return self._threads[name]

    @property
    def threads(self) -> Dict[str, List[Frame]]:
        return self._threads

    @property
    def threadNames(self) -> List[str]:
        return list(self._threads.keys())