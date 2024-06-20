import functools
import io
import os
import pickle
import re
from abc import abstractmethod
from mmap import ACCESS_READ, mmap
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple

from PIL import PngImagePlugin

from ImageUtils import CameraImage, JPEGImage
from Primitive import *
from StreamUtils import StreamUtil
from Utils import dumpJson

from ..DataClasses import DataClass
from ..LogInterfaceBase import LogInterfaceBaseClass


class MessageBase(LogInterfaceBaseClass):
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
    def parseBytes(self) -> DataClass:
        """@Override: Parse the message body bytes into a representation object, which hold the information of the message"""
        if self.loadRepr():
            pass
        else:
            sutil = StreamUtil(self.logBytes)
            sutil.seek(self.startByte + 4, io.SEEK_SET)
            self.reprObject = self.classType.read(sutil)
        return self.reprObject

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

    @property
    @abstractmethod
    def reprDict(self) -> Dict[str, Any]:
        """Representation object as dict"""

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
        """Load the representation object from the pickle file, returns whether it has been loaded successfully"""
        if self.hasPickledRepr():
            try:
                self.reprObject = pickle.load(open(self.reprPicklePath, "rb"))
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
            self.reprObj.draw()  # type: ignore
        else:
            if slientFail:
                return
            else:
                raise ValueError("Message is not an image")

    def saveImage(
        self,
        dir: Path,
        imgName: str,
        metadata=Optional[PngImagePlugin.PngInfo],
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

