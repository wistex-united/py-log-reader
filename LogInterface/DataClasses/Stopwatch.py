from copy import copy
from multiprocessing import shared_memory
from multiprocessing.managers import DictProxy
from numpy.typing import NDArray
from typing import Deque, Dict, List, Optional, Tuple
import numpy as np
from Primitive import *
from StreamUtils import StreamUtil
from Utils import findClosestValidValue
from .DataClass import DataClass

from collections import deque

EMPTY_INDICATOR = UInt(-1)

class Stopwatch(DataClass):
    def __init__(self):
        super().__init__()
        self.names: Dict[int, str]
        self.infos: Dict[int, UInt]
        self.threadStartTime: UInt
        self.frameNo: UInt

    @classmethod
    def read(
        cls,
        sutil: StreamUtil,
        justReadNames=False,
    ) -> "Stopwatch":
        instance = Stopwatch()
        instance.names = {}
        instance.infos = {}

        nameCount = sutil.readUShort()

        for i in range(nameCount):
            watchId = int(sutil.readUShort())
            watchName = sutil.readStr()
            j = instance.names.get(watchId, None)
            if j is None or j != watchName:  # new or different name
                instance.names[watchId] = watchName
                instance.infos[watchId] = EMPTY_INDICATOR

        dataCount = sutil.readUShort()
        noDataIds = set(instance.names.keys())
        for i in range(dataCount):
            watchId = int(sutil.readUShort())
            noDataIds.discard(watchId)
            time = sutil.readUInt()
            instance.infos[watchId] = time

        if not justReadNames:
            for watchId in noDataIds:
                instance.infos[watchId] = EMPTY_INDICATOR

        if justReadNames:
            return instance

        instance.threadStartTime = sutil.readUInt()
        instance.frameNo = sutil.readUInt()

        return instance

    def asDict(self):
        return {
            "names": self.names,
            "infos": {
                (self.names[watchId] if watchId in self.names else watchId): (
                    info if info != EMPTY_INDICATOR else None
                )
                for watchId, info in sorted(self.infos.items())
            },
            "threadStartTime": self.threadStartTime,
            "frameNo": self.frameNo,
        }

    readOrder = ["names", "infos", "threadStartTime", "frameNo"]
    attributeCtype = {
        "names": "unsigned int*",
        "infos": "unsigned int*",
        "threadStartTime": "unsigned int",
        "frameNo": "unsigned int",
    }


class Timer:
    def __init__(self):
        super().__init__()
        self.names: Dict[int, str] = {}

        # These two are different view for the same block of memory
        self.storage: NDArray[UInt]
        self.sharedMemory: shared_memory.SharedMemory
        self.frameIdxMap: Dict[int, int]
        # cache
        self._interpolatedInfos_cached: NDArray[UInt]

    @property
    def shape(self):
        if hasattr(self, "storage"):
            return self.storage.shape
        else:
            return (0, 0)

    @property
    def validIndexs(self) -> List[int]:
        return sorted(list(self.names.keys()))

    @property
    def validInfos(self) -> Dict[int, NDArray[UInt]]:
        return {idx: self.storage[:][idx] for idx in self.validIndexs}

    def info(self, watchId):
        return self.storage[:][watchId]

    def __getitem__(self, frameIdx):
        return self.getStopwatch(frameIdx)

    def __getstate__(self):
        # Store enough information to recreate the shared memory and storage
        state = {}
        for key in self.__dict__:
            if key not in ["sharedMemory", "storage"]:
                state[key] = self.__dict__[key]
        state["dataStorage"] = self.storage
        state["sharedMemoryName"] = self.sharedMemory.name
        state["shape"] = self.shape
        return state

    def __setstate__(self, state):
        # Reconstruct the shared memory from the name
        sharedMemoryName = state.pop("sharedMemoryName")
        dataStorage = state.pop("dataStorage")
        shape = state.pop("shape")
        try:
            # Try to create a new shared memory segment
            self.sharedMemory = shared_memory.SharedMemory(
                name=sharedMemoryName, create=True, size=shape[0] * shape[1] * 4
            )
        except FileExistsError:
            # If it already exists, open it without creating
            self.sharedMemory = shared_memory.SharedMemory(
                name=sharedMemoryName, create=False, size=shape[0] * shape[1] * 4
            )
        self.storage = np.ndarray(
            shape=shape,
            dtype=UInt,
            buffer=self.sharedMemory.buf,
        )

        np.copyto(self.storage, dataStorage)

        self.__dict__.update(state)

    def initStorage(self, frameIndxes, infoLength=100):
        frameNumbers = len(frameIndxes)
        self.sharedMemory = shared_memory.SharedMemory(
            create=True, size=frameNumbers * infoLength * 4
        )
        # Create a NumPy array on this shared memory
        self.storage = np.ndarray(
            (frameNumbers, infoLength), dtype=UInt, buffer=self.sharedMemory.buf
        )
        self.storage.fill(EMPTY_INDICATOR)
        self.frameIdxMap = {}
        for i in range(frameNumbers):
            self.frameIdxMap[frameIndxes[i]] = i

    def parseStopwatch(
        self,
        stopwatch: Stopwatch,
        frameIdx: int,
        justReadNames=False,
    ):
        index = self.frameIdxMap[frameIdx]
        self.names.update(stopwatch.names)
        if justReadNames:
            return
        # Update infos dictionary
        for watchId, info in stopwatch.infos.items():
            if (
                watchId < self.shape[1] - 2
            ):  # The last two index are reserved for frameNo and threadStartTime
                self.storage[index][watchId] = info
            else:
                raise IndexError(f"Index {watchId} out of range")

        # self.interpolateTimeCost(EMPTY_INDICATOR) interpolate it directly?

    def interpolatedInfos(self) -> NDArray[UInt]:
        if hasattr(self, "_interpolatedInfos_cached"):
            return self._interpolatedInfos_cached

        self._interpolatedInfos_cached = np.zeros_like(self.storage)
        for watchId, info in self.validInfos.items():
            validMap = np.where(info != EMPTY_INDICATOR)
            if len(validMap) == 0:
                continue
            firstValidIdx = validMap[0]
            lastValidIdx = validMap[-1]

            self._interpolatedInfos_cached[:firstValidIdx][watchId] = self.storage[
                firstValidIdx
            ][watchId]
            self._interpolatedInfos_cached[lastValidIdx + 1 :][watchId] = self.storage[
                lastValidIdx
            ][watchId]

            for i in range(1, len(validMap)):
                prev = validMap[i - 1]
                next = validMap[i]
                if next - prev == 1:
                    continue
                prevTime = self.storage[next][-1]
                nextTime = self.storage[prev][-1]
                timeDiff = nextTime - prevTime
                for j in range(prev, next):
                    threadStartTime = self.storage[prev][-1]
                    self._interpolatedInfos_cached[j][watchId] = (
                        self.storage[prev][watchId] * (threadStartTime - prevTime)
                        + self.storage[next][watchId] * (nextTime - threadStartTime)
                    ) / (timeDiff)

        return self._interpolatedInfos_cached

    @staticmethod
    def parseStopwatchStatic(
        sharedMemoryName: str,
        storageShape: Tuple[int, int],
        nameDict: DictProxy,
        stopwatch: Stopwatch,
        frameIdx: int,
        frameIdxMap: Dict[int, int],
        justReadNames=False,
    ):
        index = frameIdxMap[frameIdx]

        sharedMemory = shared_memory.SharedMemory(sharedMemoryName)
        storage = np.ndarray(storageShape, dtype=np.uint32, buffer=sharedMemory.buf)

        nameDict.update(stopwatch.names)

        if not justReadNames:
            # Update infos dictionary
            for watchId, info in stopwatch.infos.items():
                if (
                    watchId < storageShape[1] - 2
                ):  # The last two index are reserved for frameNo and threadStartTime
                    storage[index][watchId] = info
                else:
                    raise IndexError(f"Index {watchId} out of range")

            storage[index][-1] = stopwatch.frameNo
            storage[index][-2] = stopwatch.threadStartTime

    # def threadDelta(self, frameIdx, consideredFrames=100):
    #     cnt = consideredFrames

    #     diff = instance.frameNo - self.lastFrameNo
    #     # TODO: I'm not using threadDeltas, I don't know why it was here
    #     if self.threadDeltas.maxlen and diff < self.threadDeltas.maxlen:
    #         # sometimes we do not get data every frame. Compensate by assuming that the missing frames have the same timing as the last one
    #         for i in range(diff):
    #             average = float((instance.threadStartTime - self.lastStartTime) / diff)
    #             self.threadDeltas.appendleft(average)

    def asDict(self):
        return {
            "names": self.names,
            "infos": self.validInfos,
        }

    def getStopwatch(self, frameIdx):
        index = self.frameIdxMap[frameIdx]
        instance = Stopwatch()
        instance.names = self.names
        instance.infos = {
            watchIndx: self.storage[index][watchIndx] for watchIndx in self.validIndexs
        }
        instance.frameNo = self.storage[index][-2]
        instance.threadStartTime = self.storage[index][-1]
        return instance

    def clear(self):
        self.names.clear()
        self.storage[:][:] = 0

    def getStatistics(self, timeInput) -> Tuple[float, float, float]:
        info = copy(timeInput)
        for index in range(len(info)):  # interpolate
            info[index] = findClosestValidValue(info, index)
        avgTime = sum(info) / len(info) / 1000.0
        minTime = min(info) / 1000.0
        maxTime = max(info) / 1000.0
        return avgTime, minTime, maxTime

    # def getThreadStatistics(self):
    #     outAvgFreq = (
    #         1000.0 / (sum(self.threadDeltas) / len(self.threadDeltas))
    #         if sum(self.threadDeltas) != 0.0
    #         else 0.0
    #     )
    #     outMin = min(self.threadDeltas)
    #     outMax = max(self.threadDeltas)
    #     return outAvgFreq, outMin, outMax

    def getName(self, watchId):
        if watchId in self.names:
            return self.names[watchId]
        else:
            return "unknown"

    def getTimeCost(self, frameIdx) -> Dict[str, int]:
        """Get the time cost of the given frame number with the closest time cost that is not EMPTY_INDICATOR"""
        return self.getStopwatch(frameIdx).asDict()
