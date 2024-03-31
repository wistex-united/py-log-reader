import json
from math import e
from mmap import mmap
from operator import index
import os
from pathlib import Path
import re
import time
from typing import Dict, Iterable, List, Tuple, Union
from unittest import result
from PIL import PngImagePlugin
from PIL import Image as PILImage

from numpy import size
from CameraImage import CameraImage

from messageID import MessageID
from Angle import Angle


class Log:
    def __init__(
        self,
        logFilePath: str,
        logBuffer: bytes,
        settings: Dict,
        logIDNames: Dict,
        mapNameToID: Dict,
        mapLogToID: Dict,
        typeInfo: Dict,
        frameIndex: List,
        framesHaveImage: List,
        anyFrameHasImage: bool,
        statsPerThread: Dict[str, Dict[int, Tuple[int, int]]],
        annotationsPerThread: Dict[str, List[Tuple[int, int, str, str]]],
        sizeWhenIndexWasComputed: int,
        enum_types: Dict,
        class_types: Dict,
        messageSlices: List,
        parsedMessages: Dict,
    ):
        # Log file related
        self.logFilePath: str = logFilePath
        self.logBuffer: bytes = logBuffer
        # Settings chunk
        self.settings: Dict = settings
        # Message IDs chunk
        self.logIDNames: Dict[int, str] = logIDNames
        self.mapNameToID: Dict[str, int] = mapNameToID
        self.mapLogToID: Dict[int, int] = mapLogToID
        # Type info chunk
        self.typeInfo: Dict = typeInfo
        # Indices chunk
        self.frameIndex: List[int] = frameIndex
        self.framesHaveImage: List = framesHaveImage
        self.anyFrameHasImage: bool = anyFrameHasImage
        self.statsPerThread: Dict[str, Dict[int, Tuple[int, int]]] = statsPerThread
        self.annotationsPerThread: Dict[
            str, List[Tuple[int, int, str, str]]
        ] = annotationsPerThread
        self.sizeWhenIndexWasComputed: int = sizeWhenIndexWasComputed

        # Python classes - Derived from typeInfo
        self.enum_types: Dict = enum_types
        self.class_types: Dict = class_types

        # Messages indices
        self.messageSlices: List = messageSlices
        self.parsedMessages: Dict = parsedMessages

    def __iter__(self):
        frameIterator = self.frameAt(0)
        return frameIterator

    @property
    def frameSize(self) -> int:
        return len(self.frameIndex)

    @property
    def messageSize(self) -> int:
        return len(self.messageSlices)

    @property
    def bytesSize(self) -> int:
        return len(self.logBuffer)

    def messageIdx2FrameIdx(self, index: int) -> int:
        if index < 0 or index >= self.messageSize:
            raise IndexError(f"Message index out of range: {index}")
        for idx, messageStartPoint in enumerate(self.frameIndex):
            if index < messageStartPoint:
                return idx - 1
        return self.frameSize - 1

    def frameAt(self, index: int) -> "FrameIterator":
        return FrameIterator(self, index)

    def messageAt(self, index: int) -> "MessageIterator":
        return MessageIterator(self, index)

    def recursive_vars(self, obj, seen=None):
        if seen is None:
            seen = set()

        if id(obj) in seen:
            return "circular reference detected"
        seen.add(id(obj))

        if isinstance(obj, (int, float, str, bool, type(None))):
            # Return basic types as is
            return obj
        elif isinstance(obj, dict):
            # Recursively call on each value in the dictionary
            return {k: self.recursive_vars(v, seen) for k, v in obj.items()}
        elif isinstance(obj, bytes):
            return f"Bytes[{len(obj)}]: {obj[:4] if len(obj) >= 4 else b''} ..."
        elif isinstance(obj, list):
            # Recursively call on each item in the list
            return [self.recursive_vars(item, seen) for item in obj]
        elif hasattr(obj, "__dict__") and (
            obj.__class__.__name__ in self.class_types or isinstance(obj, Angle)
        ):
            # Recursively call on each attribute of the object
            return {k: self.recursive_vars(v, seen) for k, v in vars(obj).items()}
        elif obj.__class__.__name__ in self.enum_types:
            return {"name": obj.name, "value": obj.value}
        else:
            return str(obj)

    @property
    def dict(self):
        result = {}
        for frame in self:
            result[f"Frame{str(frame.index)}<{frame.threadName}>"] = frame.dict
        return result

    @property
    def outline(self):
        return json.dumps(self.dict, indent=4)

    def dumpOutline(self, filePath: str) -> None:
        if not filePath.endswith(".json"):
            filePath = filePath + ".json"
        with open(filePath, "w") as f:
            f.write(self.outline)


class FrameIterator:
    def __init__(self, log: Log, index):
        if index < 0 or index >= log.frameSize:
            raise IndexError(f"Frame Index out of range: {index}")
        self.log: Log = log
        self._index: int = index

        self.lower_limit = 0
        self.upper_limit = log.frameSize

    @property
    def index(self) -> int:
        return self._index

    @index.setter
    def index(self, value: int):
        # Here we allow index==self.log.frameSize as a special case for end() iterator
        if value < 0 or value > self.log.frameSize:
            raise IndexError(f"Frame index out of range: {value}")
        self._index = value

    def __getattribute__(self, name: str):
        if name in ["lower_limit", "upper_limit", "index", "_index"]:
            return super().__getattribute__(name)
        elif self.index < self.lower_limit or self.index >= self.upper_limit:
            raise IndexError(
                f"Current object is an end iterator, doesn't have any attribute: {name}"
            )
        return super().__getattribute__(name)

    def __getitem__(self, key: Union[int, str, MessageID]):
        if isinstance(key, int):
            return self.messageAtRelativeIndex(key)
        elif isinstance(key, str) or isinstance(key, MessageID):
            result = None
            for message in self:
                check = (
                    message.className == key
                    if isinstance(key, str)
                    else message.id == key.value
                )
                if check:
                    result = message
                    break
            if result is None:
                raise KeyError(f"Message with className {key} not found")
            else:
                return result
        else:
            raise KeyError("Invalid key type")

    def __iter__(self):
        messageIterator = self.messageAtRelativeIndex(0)
        messageIterator.lower_limit = self.messageStartPos
        messageIterator.upper_limit = self.messageEndPos
        return messageIterator

    def __next__(self):
        if self.index < self.upper_limit:
            frameForUse = FrameIterator(self.log, self.index)
            self.index += 1
            return frameForUse
        else:
            raise StopIteration

    def __str__(self) -> str:
        return json.dumps(self.dict, indent=4)

    @property
    def threadName(self) -> str:
        return self[0].reprDict.get("threadName", "Unknown")

    @property
    def hasImage(self) -> str:
        return self.log.framesHaveImage[self.index]

    @property
    def messageStartPos(self) -> int:  # type: ignore
        return self.log.frameIndex[self.index]

    @property
    def messageEndPos(self) -> int:  # type: ignore
        if self.index == self.log.frameSize - 1:
            return self.log.messageSize
        return self.log.frameIndex[self.index + 1]

    @property
    def byteStartPos(self) -> int:
        return self.log.messageSlices[self.messageStartPos][1]

    @property
    def byteEndPos(self) -> int:
        return self.log.messageSlices[self.messageEndPos - 1][2]

    @property
    def messageSize(self) -> int:
        return self.messageEndPos - self.messageStartPos

    @property
    def bytesSize(self) -> int:
        return self.byteEndPos - self.byteStartPos

    def messageAtRelativeIndex(self, relativeIndex: int) -> "MessageIterator":
        return MessageIterator(self.log, self.messageStartPos + relativeIndex)

    def messageAtAbsoluteIndex(self, absoluteIndex: int) -> "MessageIterator":
        return MessageIterator(self.log, absoluteIndex)

    @property
    def infoDict(self) -> Dict:
        return {
            "threadName": self.threadName,
            "frameIndex": self.index,
            "messageSize": self.messageSize,
            "messageStartPos": self.messageStartPos,
            "messageEndPos": self.messageEndPos,
            "bytesSize": self.bytesSize,
            "byteStartPos": self.byteStartPos,
            "byteEndPos": self.byteEndPos,
        }

    @property
    def reprDict(self) -> Dict:
        result = {}
        for message in self:
            result[message.className] = message.reprDict
        return result

    @property
    def reprObjs(self) -> Dict[str, object]:
        result = {}
        for message in self:
            result[message.className] = message.reprObj
        return result

    @property
    def dict(self) -> Dict:
        return {"Info": self.infoDict, "Reprs": self.reprDict}

    def getReprObjFromRelativeIndex(self, messageIndex: int) -> object:
        return self.log.parsedMessages.get(self.messageStartPos + messageIndex, None)

    def getReprObjFromId(self, id: int, default=None) -> object:
        result = None
        for message in self:
            if message.id == id:
                result = message.reprObj
                break
        if result is None:
            return default
        else:
            return result

    def getReprObjFromName(self, className: str, default=None) -> object:
        id = self.log.mapNameToID[className]
        return self.getReprObjFromId(id, default)

    def saveImageWithMetaData(self, dir=None, imgName=None, slientFail=False):
        if self.hasImage:
            CameraImage = self["CameraImage"]
            CameraImage.saveImageWithMetaData(dir, imgName, slientFail)
        else:
            if slientFail:
                return
            else:
                raise ValueError("This frame does not have an image")


class MessageIterator:
    def __init__(
        self,
        log: Log,
        index: int,
    ):
        if index < 0 or index >= log.messageSize:
            raise IndexError(f"Message index out of range: {index}")
        self._messageIndex: int = index
        self.log: Log = log

        self.lower_limit = 0
        self.upper_limit = log.messageSize

    @property
    def messageIndex(self) -> int:
        return self._messageIndex

    @messageIndex.setter
    def messageIndex(self, value: int):
        # Here we allow messageSize==self.log.messageSize as a special case for end() iterator
        if value < 0 or value > self.log.messageSize:
            raise IndexError(f"Message index out of range: {value}")
        else:
            self._messageIndex = value

    def __getitem__(self, key: int):
        return self.byteAtRelativeIndex(key)

    def __getattribute__(self, name: str):
        if name in ["lower_limit", "upper_limit", "messageIndex", "_messageIndex"]:
            return super().__getattribute__(name)
        if (
            self.messageIndex < self.lower_limit
            or self.messageIndex >= self.upper_limit
        ):
            raise IndexError(
                f"Current object is an end iterator, doesn't have any attribute: {name}"
            )
        return super().__getattribute__(name)

    def __next__(self):
        if self.messageIndex < self.upper_limit:
            messageForUse = MessageIterator(self.log, self.messageIndex)
            self.messageIndex += 1
            return messageForUse
        else:
            raise StopIteration

    def __str__(self) -> str:
        return json.dumps(self.dict, indent=4)

    @property
    def messageRaw(self) -> Tuple[int, int, int]:  # type: ignore
        return self.log.messageSlices[self.messageIndex]

    @property
    def logId(self) -> int:
        return self.messageRaw[0]

    @property
    def id(self) -> int:
        return self.log.mapLogToID[self.logId]

    @property
    def idName(self) -> str:
        return self.log.logIDNames[self.logId]

    @property
    def className(self) -> str:
        return self.reprObj.__class__.__name__

    @property
    def isImage(self) -> bool:
        return self.id in [MessageID.idCameraImage.value, MessageID.idJPEGImage.value]

    @property
    def byteStartPos(self):
        return self.messageRaw[1]

    @property
    def byteEndPos(self):
        return self.messageRaw[2]

    @property
    def bytesSize(self) -> int:
        return self.byteEndPos - self.byteStartPos

    @property
    def bytes(self) -> bytes:
        return self.log.logBuffer[self.byteStartPos : self.byteEndPos]

    @property
    def frameIndex(self) -> int:
        return self.log.messageIdx2FrameIdx(self.messageIndex)

    @property
    def frame(self) -> FrameIterator:
        return self.log.frameAt(self.frameIndex)

    @property
    def frameRelativeIndex(self) -> int:
        return self.messageIndex - self.frame.messageStartPos

    @property
    def infoDict(self) -> Dict:
        return {
            "logId": self.logId,
            "id": self.id,
            "idName": self.idName,
            "className": self.className,
            "isImage": self.isImage,
            "messageIndex": self.messageIndex,
            "frameIndex": self.frameIndex,
            "frameRelativeIndex": self.frameRelativeIndex,
            "bytesSize": self.bytesSize,
            "byteStartPos": self.byteStartPos,
            "byteEndPos": self.byteEndPos,
        }

    @property
    def reprObj(self) -> object:
        return self.log.parsedMessages.get(self.messageIndex, None)

    @property
    def reprDict(self) -> Dict:
        result = self.log.recursive_vars(self.reprObj)
        if isinstance(result, dict):
            return result
        else:
            raise ValueError("Message is not a dict")

    @property
    def dict(self) -> Dict:
        return {"Info": self.infoDict, "Repr": self.reprDict}

    def draw(self, sleep=0, slientFail=False):
        if self.isImage and isinstance(self.reprObj, CameraImage):
            self.reprObj.PILImage.show()
            if sleep != 0:
                time.sleep(sleep)
        else:
            if slientFail:
                return
            else:
                raise ValueError("Message is not an image")

    def saveImageWithMetaData(self, dir=None, imgName=None, slientFail=False):
        if self.isImage:
            CameraInfo = self.frame["CameraInfo"]
            CameraMatrix = self.frame["CameraMatrix"]
            ImageCoordinateSystem = self.frame["ImageCoordinateSystem"]

            metadata = PngImagePlugin.PngInfo()
            # Add metadata (using the tEXt chunk for simplicity)
            metadata.add_text("CameraInfo", json.dumps(CameraInfo.dict))
            metadata.add_text("CameraMatrix", json.dumps(CameraMatrix.dict))
            metadata.add_text(
                "ImageCoordinateSystem", json.dumps(ImageCoordinateSystem.dict)
            )

            img: PILImage = self.reprObj.PILImage  # type: ignore
            if imgName is None:
                imgName = (
                    Path(self.log.logFilePath).stem
                    + f"_F{self.frameIndex}_M{self.messageIndex}_Bf{self.byteStartPos}_Bt{self.byteEndPos}.png"
                )
            if dir is None:
                dir = os.path.join(
                    Path(self.log.logFilePath).parent,
                    f"{Path(self.log.logFilePath).stem}_images",
                )
            os.makedirs(dir, exist_ok=True)
            img.save(  # type: ignore
                os.path.join(dir, imgName),
                pnginfo=metadata,
            )
        else:
            if slientFail:
                return None
            else:
                raise ValueError("This frame does not have an image")

    def byteAtRelativeIndex(self, relativeIndex: int) -> int:
        return self.log.logBuffer[self.byteStartPos + relativeIndex]
