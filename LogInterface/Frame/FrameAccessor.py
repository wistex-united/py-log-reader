from typing import Any, List, Optional, Tuple, Type, Union

import numpy as np

from StreamUtils import StreamUtil

from ..LogInterfaceBase import (IndexMap, LogInterfaceAccessorClass,
                                LogInterfaceBase)
from ..Message import MessageAccessor, Messages
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

    def __init__(self, log: Any, indexMap: Optional[IndexMap] = None):
        LogInterfaceAccessorClass.__init__(self, log, indexMap)

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
            self._idxFile.getData(), byteIndex, byteIndex + self.frameIdxByteLength
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

    # Children
    @property
    def children(self) -> Messages:
        if not hasattr(self, "_children"):
            self._children = self.getMessageAccessor()
        return self._children

    @children.setter
    def children(self, value: Messages):
        self._children = value

    def getMessageAccessor(self, index: int = 0) -> MessageAccessor:
        messageAccessor = MessageAccessor(
            self.log, range(self.absMessageIndexStart, self.absMessageIndexEnd)
        )
        messageAccessor.assignParent(self)
        messageAccessor.index = index
        return messageAccessor

    # Parent
    @property
    def parent(self) -> LogInterfaceBase:
        if not self.parentIsAssigend:
            self._parent = self.log.getContentChunk()
        return self._parent

    def eval(self, sutil: StreamUtil, offset: int = 0):
        """Accessor is only used to access messages already evaluated, it cannot eval"""
        raise NotImplementedError(
            "Accessor is only used to access messages already evaluated, it cannot eval"
        )

    @staticmethod
    def getInstanceClass() -> Type["FrameInstance"]:
        return FrameInstance

    # Tools
    def getInstance(self) -> "FrameInstance":
        reuslt: FrameInstance = LogInterfaceAccessorClass.getInstance(self)  # type: ignore
        reuslt.dummyMessages = []
        if isinstance(self.children, LogInterfaceAccessorClass):
            reuslt.children = [child.getInstance() for child in self.children]  # type: ignore
        elif isinstance(self.children, list):
            reuslt.children = self.children  # type: ignore
        reuslt.messages = self.messages
        return reuslt
