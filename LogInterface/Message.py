from abc import abstractmethod
import asyncio
import bisect
import functools
import io
import os
import pickle
from mmap import ACCESS_READ, mmap
from pathlib import Path
import re
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Tuple
from numpy.typing import NDArray
import aiofiles
from PIL import PngImagePlugin

from ImageUtils import CameraImage, JPEGImage
from Primitive import *
from StreamUtils import StreamUtil
from Utils import dumpJson, MemoryMappedFile

from .DataClasses import Annotation, DataClass, Stopwatch
from .LogInterfaceBase import (
    LogInterfaceAccessorClass,
    LogInterfaceBase,
    LogInterfaceInstanceClass,
    IndexMap,
)


class MessageBase(LogInterfaceBase):
    # Core Properties
    @property
    @abstractmethod
    def logId(self) -> UChar:
        pass

    @property
    @abstractmethod
    def reprObj(self) -> DataClass:
        pass

    # Parse bytes
    def parseBytes(self):
        """@Override: Parse the message body bytes into a representation object, which hold the information of the message"""
        sutil = StreamUtil(MemoryMappedFile(self.logFilePath).getData())
        sutil.seek(self.startByte + 4, io.SEEK_SET)
        self._reprObject = self.classType.read(sutil)

    @staticmethod
    def parseBytesWrapper(
        args: Tuple[int, int, Callable], logFilePath: str
    ) -> DataClass:
        """This is the wrapper function for parsing bytes with multiprocessing"""
        start, end, read = args
        with open(logFilePath, "rb") as logFile, mmap(
            logFile.fileno(), 0, access=ACCESS_READ
        ) as buf:
            buf.seek(start, io.SEEK_SET)
            reprObject = read(StreamUtil(buf))
        return reprObject

    # Derived Properties
    @property
    @abstractmethod
    def isParsed(self) -> bool:
        """Whether the message has been parsed into a representation object"""
        pass

    @property
    @abstractmethod
    def reprDict(self) -> Dict[str, Any]:
        pass

    @property
    def infoDict(self) -> Dict:
        return {
            "logId": self.logId,
            "id": self.id,
            "idName": self.idName,
            "className": self.className,
            "isImage": self.isImage,
            "frameIndex": self.frame.absIndex,
            "relativeIndexInFrame": self.index,
            "bytesSize": self.size,
            "byteStartPos": self.startByte,
            "byteEndPos": self.endByte,
        }

    def asDict(self) -> Dict[str, Any]:
        """Almost everything you need to know about this message"""
        return {"Info": self.infoDict, "Repr": self.reprDict}

    @property
    def timestamp(self):
        """The time stamp of this message"""
        return self.frame.timestamp

    @property
    def headerBytes(self) -> bytes:
        """
        Header bytes that contains the id and size of the message
        1 byte for id; 3 byte for size
        """
        return self.logBytes[self.startByte : self.startByte + 4]

    @property
    def bodyBytes(self) -> bytes:
        """
        Body bytes contains actual information of the message, can be parsed into a representation object by parseBytes()
        """
        return self.logBytes[self.startByte + 4 : self.endByte]

    @property
    def id(self) -> UChar:
        """Corresponding id in MessageID, most commonly used"""
        return self.log.MessageIDChunk.mapLogToID[self.logId]

    @property
    def idName(self) -> str:
        """
        Name of the id, the only identifier of the type of the message
        """
        return self.log.MessageIDChunk.logIDNames[self.logId]

    @property
    def className(self) -> str:
        """The representation object's class name"""
        result = self.idName
        result = result[2:] if result.startswith("id") else result
        return result

    @property
    def classType(self) -> Type[DataClass]:
        """Type class of the representation object"""
        return self.log.TypeInfoChunk.dataClasses[self.className]

    def __str__(self) -> str:
        """Convert the current object to string"""
        result = dumpJson(self.reprDict, self.strIndent)
        return result

    def __getitem__(self, key: str) -> Any:
        """Allow to use [<field name>] to access fields in the representation object"""
        if isinstance(key, str):
            return self.reprObj[key]
        else:
            raise KeyError("Invalid key type")

    # Dump: pickle IO
    @property
    def picklePath(self) -> Path:
        return self.log.cacheDir / f"Message_{self.absIndex}.pkl"  # type: ignore

    @property
    def reprPicklePath(self) -> Path:
        return self.log.cacheDir / f"Message_{self.absIndex}_repr.pkl"  # type: ignore

    @property
    @functools.lru_cache(maxsize=1)
    def reprPicklePathPattern(self) -> re.Pattern:
        return re.compile(r"^Message_(\d+)_repr\.pkl$")

    def dumpRepr(self):
        os.makedirs(self.picklePath.parent, exist_ok=True)
        pickle.dump(self.reprObj, open(self.reprPicklePath, "wb"))

    @staticmethod
    def dumpReprWrapper(picklePath: Path, reprObject: DataClass):
        with open(picklePath, "wb") as f:
            pickle.dump(reprObject, f)

    def loadRepr(self) -> bool:
        if self.hasPickledRepr():
            try:
                self._reprObject = pickle.load(open(self.reprPicklePath, "rb"))
            except EOFError:
                return False
            return True
        return False

    @abstractmethod
    def hasPickledRepr(self) -> bool:
        pass

    @staticmethod
    def loadReprWrapper(
        picklePath: Path, index: int
    ) -> Tuple[Optional[DataClass], int]:
        try:
            reprObject = pickle.load(open(picklePath, "rb"))
            return reprObject, index
        except EOFError:
            return None, index

    # Parent
    @property
    def frame(self):
        """
        The parent of this Message is always a Frame
        TODO: Give a type hint here
        """
        return self.parent

    # Children
    @property
    def children(self):
        """
        @Override: Root of the hierarchical tree, no children
        Just used to satisfy the requirement of the interface
        """
        return []

    # Image Related
    @property
    def isImage(self) -> bool:
        """Whether the message is an image"""
        MessageID: Any = self.log.MessageID  # type: ignore MessageID is actually Type[Enum]
        return self.id in [MessageID.idCameraImage.value, MessageID.idJPEGImage.value]

    def draw(self, slientFail=False):
        """Draw the image if current message is a CameraImage or JPEGImage"""
        if self.isImage and (
            isinstance(self.reprObj, CameraImage) or isinstance(self.reprObj, JPEGImage)
        ):
            self.reprObj.draw()
        else:
            if slientFail:
                return
            else:
                raise ValueError("Message is not an image")

    def saveImage(
        self,
        dir: Path,
        imgName: str,
        metadata=PngImagePlugin.PngInfo | None,
        slientFail: bool = False,
    ):
        if self.isImage:
            os.makedirs(dir, exist_ok=True)

            if isinstance(self.reprObj, CameraImage):
                self.reprObj.saveImage(os.path.join(dir, imgName), metadata)
            elif isinstance(self.reprObj, JPEGImage):
                self.reprObj.saveImage(os.path.join(dir, imgName), metadata)
            else:
                raise Exception("Not valid image type")
        else:
            if slientFail:
                return None
            else:
                raise Exception("Save image failed, message is not an image")


class MessageInstance(MessageBase, LogInterfaceInstanceClass):
    def __init__(self, frame):
        LogInterfaceInstanceClass.__init__(self, frame)

        self._reprObject: DataClass
        self._logId: UChar

        # cache
        self._reprDict_cached: Dict[str, Any]
        self._absIndex_cached: int

    def hasPickledRepr(self) -> bool:
        if (
            not hasattr(self.log, "_messageCachedReprList_cached")
            or self.log._messageCachedReprList_cached is None
        ):
            repr_list = os.listdir(self.log.cacheDir)
            pattern = self.reprPicklePathPattern
            self.log._messageCachedReprList_cached = np.zeros(
                len(self.log.messages), dtype=np.bool_
            )
            for filename in repr_list:
                match = pattern.match(filename)
                if match:
                    abs_message_index = int(match.group(1))
                    self.log._messageCachedReprList_cached[abs_message_index] = True
                    # print(f"Message {abs_message_index} has cached repr")
        return self.log._messageCachedReprList_cached[self.absIndex]
        # if os.path.isfile(self.reprPicklePath):  # type: ignore
        #     return True
        # return False

    def eval(self, sutil: StreamUtil, offset: int = 0):
        """
        Evaluate a message' size, calculate the start and end position in log file
        Don't care whether it is a dummy message or has invalid id, upper level eval (Frame.eval) will handle that
        """
        startPos = sutil.tell()
        try:
            id, size = sutil.readMessageHeader()
            sutil.seek(size, io.SEEK_CUR)
        except EOFError:
            sutil.seek(
                startPos, io.SEEK_SET
            )  # Set the stream back to avoid misreporting startByte and endByte
            raise EOFError("Not enough data to eval message")

        self._logId = id

        self._startByte = offset
        self._endByte = offset + 4 + size

    @property
    def logId(self) -> UChar:
        """
        Id shows up in the log file, not necessarily the same as corresponding id in MessageID
        Usually used for debugging
        """
        return self._logId

    @property
    def reprObj(self) -> DataClass:
        """The representation object"""
        if not hasattr(self, "_reprObject"):
            if self.hasPickledRepr():
                if self.loadRepr():
                    return self._reprObject
                else:
                    raise ValueError(
                        "Message has cached representation object, but failed to load it"
                    )
            else:
                raise ValueError(
                    "Message has no representation object, please parse the message first"
                )
        return self._reprObject

    @reprObj.setter
    def reprObj(self, value: DataClass):
        """Set the representation object"""
        if not isinstance(value, self.classType):
            raise ValueError("Invalid representation object")
        if isinstance(value, Annotation):
            value.frame = self.frame.threadName
        self._reprObject = value

    def freeMem(self):
        """@Override: Free the message's repr object memory"""
        del self._reprObject
        del self._reprDict_cached

    @property
    def isParsed(self) -> bool:
        return hasattr(self, "_reprObject") and self._reprObject is not None

    @property
    def reprDict(self) -> Dict[str, Any]:
        """Representation object as dict"""
        if hasattr(self, "_reprDict_cached") and self._reprDict_cached:
            return self._reprDict_cached
        else:
            if isinstance(self.reprObj, Stopwatch):
                self._reprDict_cached = self.frame.timer.getStopwatch(
                    self.frameIndex
                ).asDict()
            else:
                self._reprDict_cached = self.reprObj.asDict()
            return self._reprDict_cached

    @property
    def frameIndex(self) -> int:
        """The index of the frame in the chunk"""
        return self.frame.index

    @property
    def index(self) -> int:
        """The index of the message in the frame"""
        if hasattr(self, "_index_cached"):
            return self._index_cached
        else:
            # All the parent's children are Instance Classes, no need to worry
            result = -1
            for i, c in enumerate(self.parent.children):
                if c is self:
                    result = i
                else:
                    c._index_cached = i
            if result == -1:
                raise ValueError(f"{self} not found in {self.parent}'s children")
            return result

    @index.setter
    def index(self, value: int):
        raise ValueError("Message Instance's index cannot be set")

    @property
    def absIndex(self) -> int:
        """Absolute message index in the whole file (after removing the dummy messages)"""
        if hasattr(self, "_absIndex_cached"):
            return self._absIndex_cached
        if isinstance(self.frame, LogInterfaceInstanceClass):
            cnt = self.frame.absMessageIndexStart  # type: ignore
            for dummy in self.frame.dummyMessages:  # type: ignore
                dummy._absIndex_cached = cnt
                cnt += 1
            for message in self.frame.messages:  # type: ignore
                message._absIndex_cached = cnt
                cnt += 1
        elif isinstance(self.frame, LogInterfaceAccessorClass):
            self._absIndex_cached = self.frame.absMessageIndexStart + self.index  # type: ignore
        else:
            raise ValueError(f"Unsupported frame type: {type(self.frame)}")

        return self._absIndex_cached

    @absIndex.setter
    def absIndex(self, value: int):
        raise ValueError("Message Instance's absIndex cannot be set")


class MessageAccessor(MessageBase, LogInterfaceAccessorClass):
    messageidxFileName: str = "messageIndexFile.cache"

    @staticmethod
    def decodeIndexBytes(bytes: bytes) -> Tuple[int, int, int, int]:
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

    def __init__(self, log: Any, indexMap: Optional[IndexMap]):
        LogInterfaceAccessorClass.__init__(self, log, indexMap)
        # cache
        self._reprDict_cached: Dict[str, Any]
        self._parent: LogInterfaceBase
        self._parentIsAssigend: bool = False

    def __getitem__(self, indexOrKey: Union[int, str]) -> Any:
        """Two mode, int index can change the Accessor's index; while str index can fetch an attribute from current message repr object"""
        if isinstance(indexOrKey, str):
            result = MessageBase.__getitem__(self, indexOrKey)
        elif isinstance(indexOrKey, int):
            result = LogInterfaceAccessorClass.__getitem__(self, indexOrKey)
        else:
            raise KeyError("Invalid key type")

        return result

    # Core Properties

    # Index file related
    @staticmethod
    def idxFileName() -> str:
        return MessageAccessor.messageidxFileName

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
    def frameAbsIndex(self) -> int:
        return self.messageByteIndex[1]

    @property
    def startByte(self) -> int:
        return self.messageByteIndex[2]

    @property
    def endByte(self) -> int:
        return self.messageByteIndex[3]

    # index & absIndex
    @property
    def index(self) -> int:
        """
        The index of current item in the parent's children (another LogInterfaceAccessorClass)
        """
        return self.clacRelativeIndex(self.absIndex, self.parent.children.indexMap)  # type: ignore

    @index.setter
    def index(self, value: int):
        self.absIndex = self.parent.children.indexMap[value]  # type: ignore

    @property
    def absIndex(self) -> int:
        """The location of current index in the messageIndexFile"""
        return self.indexMap[self.indexCursor]

    @absIndex.setter
    def absIndex(self, value: int):
        self.indexCursor = self.clacRelativeIndex(
            value, self.indexMap
        )  # Will raise ValueError if not in indexMap

    # parent
    def assignParent(self, parent: Any):
        self._parent = parent
        # TODO: update self.indexMap based on parent's absIndex
        self._parentIsAssigend = True

    @property
    def parentIsAssigend(self) -> bool:
        return self._parentIsAssigend

    @property
    def parent(self) -> Union[LogInterfaceAccessorClass, LogInterfaceInstanceClass]:
        if not self.parentIsAssigend:  # Fake a parent
            self._parent = (
                self.log.getFrameAccessor()
            )  # instantiate a FrameAccessor without any constraints
            self._parent.absIndex = self.frameAbsIndex

        if isinstance(self._parent, LogInterfaceAccessorClass):
            if not self.parentIsAssigend:
                self._parent.absIndex = self.frameAbsIndex
            return self._parent
        elif isinstance(self._parent, LogInterfaceInstanceClass):  # Must be assigned
            return self._parent
        else:
            raise Exception("Invalid parent type")

    def eval(self, sutil: StreamUtil, offset: int = 0):
        raise NotImplementedError(
            "Accessor is only used to access messages already evaluated, it cannot eval"
        )

    def hasPickledRepr(self) -> bool:
        if os.path.isfile(self.reprPicklePath):  # type: ignore
            return True
        return False
