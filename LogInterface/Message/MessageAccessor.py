from importlib import import_module
import os
from typing import Any, Dict, List, Optional, Tuple

from Primitive import *
from StreamUtils import StreamUtil
from Utils import MemoryMappedFile
from Utils.GeneralUtils import canBeRange

from ..DataClasses import Annotation, DataClass, Stopwatch
from ..LogInterfaceBase import (
    IndexMap,
    LogInterfaceBaseClass,
    LogInterfaceAccessorClass,
    LogInterfaceInstanceClass,
)
from .MessageBase import MessageBase
from .MessageInstance import MessageInstance

Messages = Union[List[MessageInstance], "MessageAccessor"]


class MessageAccessor(MessageBase, LogInterfaceAccessorClass):
    messageIdxFileName: str = "messageIndexFile.cache"
    maxCachedReprObj: int = 200

    @staticmethod
    def decodeIndexBytes(bytes: bytes) -> Tuple[int, int, int, int]:
        if len(bytes) != MessageAccessor.messageIdxByteLength:
            raise ValueError(f"Invalid index bytes length: {len(bytes)}")
        parsedBytes = np.frombuffer(
            bytes,
            dtype=np.uint64,
        )
        return (
            int(parsedBytes[0]),
            int(parsedBytes[1]),
            int(parsedBytes[2]),
            int(parsedBytes[3]),
        )

    @staticmethod
    def encodeIndexBytes(info: Tuple[int, int, int, int]) -> bytes:
        absMessageIndex, absFrameIndex, messageStartByte, messageEndByte = info
        return np.array(
            [absMessageIndex, absFrameIndex, messageStartByte, messageEndByte],
            dtype=np.uint64,
        ).tobytes()

    def __init__(self, log: Any, indexMap: Optional[IndexMap] = None):
        LogInterfaceAccessorClass.__init__(self, log, indexMap)
        # cache
        self._reprObj_cached: Dict[int, DataClass] = {}
        self._reprDict_cached: Dict[int, Dict[str, Any]] = {}

    def __getitem__(
        self, indexOrKey: Union[int, str]
    ) -> Union["MessageAccessor", DataClass, Any]:
        """Two mode, int index can change the Accessor's index; while str index can fetch an attribute from current message repr object"""
        if isinstance(indexOrKey, str):
            result = MessageBase.__getitem__(self, indexOrKey)
        elif isinstance(indexOrKey, int):
            result = LogInterfaceAccessorClass.__getitem__(self, indexOrKey)
        else:
            raise KeyError("Invalid key type")

        return result

    # Core Properties
    @property
    def logId(self) -> UChar:
        return UChar(int.from_bytes(self.headerBytes[0:1], "little"))

    @property
    def reprObj(self) -> DataClass:
        if not self.isParsed():
            self.parseBytes()
        return self._reprObj_cached[self.absIndex]

    @reprObj.setter
    def reprObj(self, value: DataClass):
        """Set the representation object"""
        if not isinstance(value, self.classType):
            raise ValueError("Invalid representation object")
        if isinstance(value, Annotation):
            value.frame = self.frame.threadName
        self._reprObj_cached[self.absIndex] = value
        if len(self._reprObj_cached) > self.maxCachedReprObj:
            self._reprObj_cached.pop(next(iter(self._reprObj_cached)))

    # Index file related
    @staticmethod
    def idxFileName() -> str:
        return MessageAccessor.messageIdxFileName

    @property
    def indexFileBytes(self) -> bytes:
        """The bytes of current index in messageIndexFile, which store the location of the message in the log file"""
        byteIndex = self.absIndex * self.messageIdxByteLength
        return self.getBytesFromMmap(
            self._idxFile.getData(), byteIndex, byteIndex + self.messageIdxByteLength
        )

    @property
    def messageByteIndex(self) -> Tuple[int, int, int, int]:
        """
        [messageIndex, parentFrameIndex, startByte, endByte]
        """
        return self.decodeIndexBytes(self.indexFileBytes)

    @property
    def frameIndex(self) -> int:
        return self.messageByteIndex[1]

    @property
    def startByte(self) -> int:
        return self.messageByteIndex[2]

    @property
    def endByte(self) -> int:
        return self.messageByteIndex[3]

    @classmethod
    def validate(cls, idxFile: MemoryMappedFile, absIndex: int, frameIndex: int):
        size = idxFile.getSize()
        if size == 0:
            return False
        lastIndex = size // cls.messageIdxByteLength - 1
        if absIndex > lastIndex:
            return False
        startByte = absIndex * cls.messageIdxByteLength
        endByte = startByte + cls.messageIdxByteLength
        messageIndex = cls.decodeIndexBytes(idxFile.getData()[startByte:endByte])
        if messageIndex[0] != absIndex:
            return False
        if frameIndex != messageIndex[1]:
            return False
        return True

    # Parent
    @property
    def parent(self) -> Union[LogInterfaceAccessorClass, LogInterfaceInstanceClass]:
        if not self.parentIsAssigend:  # Fake a parent
            self._parent = (
                self.log.getFrameAccessor()
            )  # instantiate a FrameAccessor without any constraints
            self._parent.absIndex = self.frameIndex

        if self._parent.isAccessorClass:
            if not self.parentIsAssigend:
                self._parent.absIndex = self.frameIndex
            return self._parent
        elif self._parent.isInstanceClass:  # Must be assigned
            return self._parent
        else:
            raise Exception("Invalid parent type")

    @parent.setter
    def parent(
        self, parent: Union[LogInterfaceAccessorClass, LogInterfaceInstanceClass]
    ) -> None:
        LogInterfaceAccessorClass.parent.fset(self, parent)
        if not hasattr(parent, "_children") or parent._children is None:
            # if parent.absIndex != parent._children.frameIndex:
            #     raise Exception("Parent and children frame index mismatch")
            # This is a complicated case: it ususally caused by : child find its generated parent mismatch its current frame index
            if parent.isAccessorClass:
                self.indexMap = range(
                    parent.absMessageIndexStart, parent.absMessageIndexEnd
                )
            else:
                raise Exception("What is that?")
        else:
            if parent._children.isAccessorClass:
                self.indexMap = parent._children.indexMap
            elif parent.children.isInstanceClass:
                self.indexMap = [child.absIndex for child in parent.children.children]
                toRange, indexRange = canBeRange(self.indexMap)
                if toRange:
                    self.indexMap = indexRange
            else:
                raise Exception("Invalid parent type")

    # Children
    @property
    def children(self) -> None:
        raise NotImplementedError("Message doesn't have children")

    @children.setter
    def children(self, value):
        raise NotImplementedError("Message doesn't have children")

    def eval(self, sutil: StreamUtil, offset: int = 0):
        raise NotImplementedError(
            "Accessor is only used to access messages already evaluated, it cannot eval"
        )

    def isParsed(self) -> bool:
        return self.absIndex in self._reprObj_cached

    def hasPickledRepr(self) -> bool:
        if os.path.isfile(self.reprPicklePath):  # type: ignore
            return True
        return False

    @property
    def reprDict(self) -> Dict[str, Any]:
        if self.absIndex not in self._reprDict_cached:
            if isinstance(self.reprObj, Stopwatch):
                # We don't want to replace our orignal Stopwatch object
                self._reprDict_cached[self.absIndex] = self.frame.timer.getStopwatch(
                    self.frameIndex
                ).asDict()
            else:
                self._reprDict_cached[self.absIndex] = self.reprObj.asDict()
        return self._reprDict_cached[self.absIndex]

    @staticmethod
    def getInstanceClass() -> Type["MessageInstance"]:
        return import_module("LogInterface.Message").MessageInstance

    def getInstance(self) -> MessageInstance:
        result: MessageInstance = LogInterfaceAccessorClass.getInstance(self)  # type: ignore
        result._parent = self.parent
        result._startByte = self.startByte
        result._endByte = self.endByte
        result._logId = UChar(self.logId)
        if self.isParsed():
            result.reprObj = self.reprObj
        result._absIndex_cached = self.absIndex
        return result
