from enum import Enum
from importlib import import_module
from typing import Any, List, Optional, Tuple, Type, Union

import numpy as np

from StreamUtils import StreamUtil
from Utils import MemoryMappedFile

from ..LogInterfaceBase import (
    IndexMap,
    LogInterfaceAccessorClass,
    LogInterfaceBaseClass,
    LogInterfaceInstanceClass,
)
from ..Message import MessageAccessor, MessageBase, Messages
from .FrameBase import FrameBase
from .FrameInstance import FrameInstance

Frames = Union[List[FrameInstance], "FrameAccessor"]


class FrameAccessor(FrameBase, LogInterfaceAccessorClass):
    @staticmethod
    def decodeIndexBytes(bytes: bytes) -> Tuple[int, str, int, int]:
        if len(bytes) != FrameAccessor.frameIdxByteLength:
            raise ValueError(f"Invalid index bytes length: {len(bytes)}")
        return (
            np.frombuffer(bytes[0:4], np.uint32)[0],
            bytes[4:16].decode("ascii").rstrip("\0"),
            np.frombuffer(bytes[16:24], np.uint64)[0],
            np.frombuffer(bytes[24:32], np.uint64)[0],
        )

    @staticmethod
    def encodeIndexBytes(info: Tuple[int, str, int, int]) -> bytes:
        absFrameIndex, threadName, frameMessageIndexStart, frameMessageIndexEnd = info
        return (
            np.uint32(absFrameIndex).tobytes()
            + threadName.encode("ascii").ljust(12, b"\0")
            + np.uint64(frameMessageIndexStart).tobytes()
            + np.uint64(frameMessageIndexEnd).tobytes()
        )

    def __init__(self, log: Any, indexMap: Optional[IndexMap] = None):
        LogInterfaceAccessorClass.__init__(self, log, indexMap)

    def __getitem__(
        self, key: Union[int, str, Enum]
    ) -> Union["FrameAccessor", MessageBase]:
        if isinstance(key, int):
            self.indexCursor = key
            return self
        else:
            result = self.log.getCachedInfo(self, key)
            if result is not None:
                return result
            result = super().__getitem__(key).copy()
            self.log.cacheInfo(self, key, result.copy().freeze())
            return result

    def __contains__(self, key: Union[str, Enum, "FrameAccessor"]) -> bool:
        if isinstance(key, FrameAccessor):
            return LogInterfaceAccessorClass.__contains__(self, key)
        else:
            return FrameBase.__contains__(self, key)

    # Core
    @property
    def startByte(self) -> int:
        return self.messages[0].startByte

    @property
    def endByte(self) -> int:
        return self.messages[-1].endByte

    # Index file related
    @staticmethod
    def idxFileName() -> str:
        return FrameAccessor.frameIdxFileName

    @property
    def frameByteIndex(self) -> Tuple[int, str, int, int]:
        """
        [frameIndex, threadName, startMessageIndex, endByteMessageIndex]
        """
        return self.decodeIndexBytes(self.indexFileBytes)

    @property
    def indexFileBytes(self) -> bytes:
        """The bytes of current index in frameIndexFile, which store the start and end message index of the frame"""
        byteIndex = self.absIndex * self.frameIdxByteLength
        return self.getBytesFromMmap(
            self.idxFile.getData(), byteIndex, byteIndex + self.frameIdxByteLength
        )

    @property
    def threadName(self) -> str:
        """The thread that generates this log frame"""
        return self.frameByteIndex[1]

    @property
    def absMessageIndexStart(self) -> int:
        return self.frameByteIndex[2]

    @property
    def absMessageIndexEnd(self) -> int:
        return self.frameByteIndex[3]

    def verifyMessages(self):
        for i in range(len(self)):
            self.children[i].verify()

    # Children
    @property
    def children(self) -> Messages:
        if not hasattr(self, "_children") or self._children.frameIndex != self.absIndex:
            self._children = self.getMessageAccessor()
        return self._children

    @children.setter
    def children(self, value: Messages):
        self._children = value

    def getMessageAccessor(self, index: int = 0) -> MessageAccessor:
        messageAccessor = self.log.getMessageAccessor(
            range(self.absMessageIndexStart, self.absMessageIndexEnd)
        )
        messageAccessor.parent = self  # Actually not needed, just ensure it works fine
        messageAccessor.index = index
        return messageAccessor

    # Parent
    @property
    def parent(self) -> LogInterfaceInstanceClass:
        return self.log.getContentChunk()

    @parent.setter
    def parent(self, value: LogInterfaceBaseClass):
        valueClassName = value.__class__.__name__
        if valueClassName == "UncompressedChunk" or valueClassName == "CompressedChunk":
            return
        else:
            raise ValueError(f"Invalid parent type: {valueClassName}")

    def eval(self, sutil: StreamUtil, offset: int = 0):
        """Accessor is only used to access messages already evaluated, it cannot eval to instance"""
        raise NotImplementedError(
            "Accessor is only used to access messages already evaluated, it cannot eval to instance"
        )

    def evalNext(self, sutil: StreamUtil, offset: int = 0):
        """
        This function will try to continue eval the next frame, start from the last index of indexMap
        if the target index <= mmap index file length, it will write to index file
        if the target index > mmap index file length, it will open the index file and append the new index

        offset is the start byte of stuil (so sutil can start from any position of the log file)
        stuil is a stream of logfile

        this would also update the indexMap (append the new index)
            from range, update, the range, for List, add new index to list
        also the indexCursor after the indexMap is updated
        """
        raise NotImplementedError("Not Implemented yet")

    # Derived properties
    @property
    def classNames(self) -> List[str]:
        result = self.log.getCachedInfo(self, "classNames")
        if result is not None:
            pass
        else:
            result = []
            for message in self.messages:
                result.append(message.className)
            self.log.cacheInfo(self, "classNames", result)
        return result

    @staticmethod
    def getInstanceClass() -> Type["FrameInstance"]:
        return import_module(
            "LogInterface.Frame"
        ).FrameInstance  # Static import would cause loop import

    # Tools
    def getInstance(self) -> "FrameInstance":
        raise NotImplementedError(
            "TODO: Not Implemented yet, directly get the instance from log with abs index"
        )
        # reuslt: FrameInstance = LogInterfaceAccessorClass.getInstance(self)  # type: ignore
        # reuslt.dummyMessages = []
        # if isinstance(self.children, LogInterfaceAccessorClass):
        #     reuslt.children = [child.getInstance() for child in self.children]  # type: ignore
        # elif isinstance(self.children, list):
        #     reuslt.children = self.children  # type: ignore
        # reuslt.messages = self.messages
        # return reuslt
