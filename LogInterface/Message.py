import asyncio
import io
import os
import pickle
from mmap import ACCESS_READ, mmap
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple

import aiofiles
from PIL import PngImagePlugin

from ImageUtils import CameraImage, JPEGImage
from Primitive import *
from StreamUtils import StreamUtil
from Utils import dumpObj

from .DataClasses import Annotation, DataClass, Stopwatch
from .LogInterfaceBase import LogInterfaceBase


class Message(LogInterfaceBase):
    def __init__(self, frame):
        super().__init__(frame)

        self._reprObject: DataClass
        self._logId: UChar

        # cache
        self._reprDict_cached: Dict[str, Any]
        self._absMessageIndex_cached: int

    def __str__(self) -> str:
        """Convert the current object to string"""
        result = dumpObj(self.reprDict, self.strIndent)
        return result

    def __getitem__(self, key: str) -> Any:
        """Allow to use [<field name>] to access fields in the representation object"""
        if isinstance(key, str):
            return self.reprObj[key]
        else:
            raise KeyError("Invalid key type")

    # def __getstate__(self):
    #     state = super().__getstate__()
    #     if state.get("_reprObject", None):
    #         del state["_reprObject"]
    #     return state

    def dumpRepr(self):
        os.makedirs(self.picklePath.parent, exist_ok=True)
        pickle.dump(self._reprObject, open(self.reprPicklePath, "wb"))

    @staticmethod
    def dumpReprWrapper(picklePath: Path, reprObject: DataClass):
        with open(picklePath, "wb") as f:
            pickle.dump(reprObject, f)

    def loadRepr(self) -> bool:
        if self.hasCachedRepr():
            try:
                self._reprObject = pickle.load(open(self.reprPicklePath, "rb"))
            except EOFError:
                return False
            return True
        return False

    @staticmethod
    def loadReprWrapper(picklePath: Path) -> Optional[DataClass]:
        try:
            reprObject = pickle.load(open(picklePath, "rb"))
            return reprObject
        except EOFError:
            return None

    def hasCachedRepr(self) -> bool:
        if os.path.isfile(self.reprPicklePath):  # type: ignore
            return True
        return False

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

    def parseBytes(self):
        """@Override: Parse the message body bytes into a representation object, which hold the information of the message"""
        sutil = StreamUtil(self.bodyBytes)
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
            dataSlice = buf[start:end]
            reprObject = read(StreamUtil(dataSlice))
        return reprObject

    def freeMem(self):
        """@Override: Free the message's repr object memory"""
        del self._reprObject
        del self._reprDict_cached

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
    def reprObj(self) -> DataClass:
        """The representation object"""
        if not hasattr(self, "_reprObject"):
            if self.hasCachedRepr():
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

    @property
    def isParsed(self) -> bool:
        """Whether the message has been parsed into a representation object"""
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
    def className(self) -> str:
        """The representation object's class name"""
        result = self.log.MessageIDChunk.logIDNames[self.logId]  # type: ignore
        result = result[2:] if result.startswith("id") else result
        return result

    @property
    def classType(self) -> Type[DataClass]:
        """Type class of the representation object"""
        return self.log.TypeInfoChunk.dataClasses[self.className]  # type: ignore

    @property
    def logId(self) -> UChar:
        """
        Id shows up in the log file, not necessarily the same as corresponding id in MessageID
        Usually used for debugging
        """
        return self._logId

    @property
    def id(self) -> UChar:
        """Corresponding id in MessageID, most commonly used"""
        return self.log.MessageIDChunk.mapLogToID[self.logId]  # type: ignore

    @property
    def idName(self) -> str:
        """
        Name of the id, the only identifier of the type of the message
        """
        return self.log.MessageIDChunk.logIDNames[self.logId]  # type: ignore

    @property
    def picklePath(self) -> Path:
        return self.log.cacheDir / f"Message_{self.absMessageIndex}.pkl"  # type: ignore

    @property
    def reprPicklePath(self) -> Path:
        return self.log.cacheDir / f"Message_{self.absMessageIndex}_repr.pkl"  # type: ignore

    # Parent / Grandparent
    @property
    def children(self):
        """
        @Override: Root of the hierarchical tree, no children
        Just used to satisfy the requirement of the interface
        """
        return []

    @property
    def frame(self):
        """
        The parent of this Message is always a Frame
        TODO: Give a type hint here
        """
        return self.parent

    @property
    def chunk(self):
        """
        The chunk of this Message is always a Uncompressed/Compressed Chunk
        Just a shortcut here
        """
        return self.frame.parent

    @property
    def messageTuple(self) -> Tuple[UChar, int, int]:
        """
        (id, startByte, endByte)
        Provide the most basic information of the message and its location in the log file
        """
        return (self.id, self.startByte, self.endByte)

    @property
    def isImage(self) -> bool:
        """Whether the message is an image"""
        MessageID: Any = self.log.MessageID  # type: ignore MessageID is actually Type[Enum]
        return self.id in [MessageID.idCameraImage.value, MessageID.idJPEGImage.value]

    @property
    def frameIndex(self) -> int:
        """The index of the frame in the chunk"""
        return self.frame.index

    @property
    def absMessageIndex(self) -> int:
        """Absolute message index in the whole file (after removing the dummy messages)"""
        if hasattr(self, "_absMessageIndex_cached"):
            return self._absMessageIndex_cached
        cnt = self.frame.absMessageOffset
        for dummy in self.frame.dummyMessages:
            dummy._absMessageIndex_cached = cnt
            cnt += 1
        for message in self.frame.messages:
            message._absMessageIndex_cached = cnt
            cnt += 1
        return self._absMessageIndex_cached

    @property
    def infoDict(self) -> Dict:
        return {
            "logId": self.logId,
            "id": self.id,
            "idName": self.idName,
            "className": self.className,
            "isImage": self.isImage,
            "frameIndex": self.frameIndex,
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
