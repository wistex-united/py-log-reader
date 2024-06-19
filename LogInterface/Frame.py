import ast
import os
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Tuple, Union

from PIL import PngImagePlugin

from StreamUtils import StreamUtil
from Utils import dumpJson

from .Chunk import Chunk
from .DataClasses import Timer
from .LogInterfaceBase import LogInterfaceBase
from .Message import Message

class FrameIterator:
    def __init__(self, frame: Frame):
class Frame(LogInterfaceBase):
    _threadWithTimestamp: List[str] = [
        "Upper",
        "Lower",
        "Motion",
        "Audio",
        "Cognition",
    ]
    """These threads has the FrameInfo module that reports the time stamp of the frame, Referee do not have such module"""

    def __init__(self, chunk: Chunk):
        super().__init__(chunk)

        self._children: List[Message]
        self.dummyMessages: List[Message]

        # cache
        self._threadIndex_cached: int
        self._timestamp_cached: int
        self._timer_cached: Timer
        self._absMessageIndexStart_cached: int

    @property
    def messages(self) -> List[Message]:
        return self._children

    @messages.setter
    def messages(self, value: List[Message]):
        self._children = value

    def eval(self, sutil: StreamUtil, offset: int = 0):
        """
        Try to locate the start the end bytes of a frame
        Start at the FrameBegin message and end at the FrameEnd message
        FrameBegin and FrameEnd should have the same threadName
        It will keep evaluating messages util it finds a corresponding FrameEnd message
        """
        startPos = sutil.tell()
        self.messages = []
        self.dummyMessages = []

        dummyEnd = 0

        byteIndex = startPos

        MessageID: Any = self.log.MessageID  # type: ignore MessageID is actually Type[Enum]
        while True:
            message = Message(self)
            message.eval(sutil, byteIndex - startPos + offset)

            byteIndex = (
                sutil.tell()
            )  # No matter this message is valid or not, update the byteIndex
            if message.logId == 255:  # 255 means it is a message without message id
                self.dummyMessages.append(message)
                raise Exception(
                    "Found Message without MessageID, probably because a representation is included in logger.cfg but not assigned a id in MessageIDs.h"
                )
            if message.logId > len(MessageID):
                raise Exception(f"Current id not valid:{id} > {len(MessageID)}")

            self.messages.append(message)

            if message.id == MessageID.idFrameFinished.value:
                if (
                    len(self.messages) > 0
                    and self.messages[0].id == MessageID.idFrameBegin.value
                    and self.messages[0].bodyBytes[4:] == message.bodyBytes[4:]
                ):
                    break
                else:
                    raise Exception(
                        f"Frame end without frame begin at {byteIndex-startPos+offset}"
                    )
            elif message.id == MessageID.idFrameBegin.value:
                # Here's a strange behavior, if we met with double begin, we simply take the second one and consider all the messages before it as dummy messages
                if len(self.messages) != 1:
                    self.dummyMessages.extend(self.messages[:-1])
                    self.messages = [self.messages[-1]]
                    dummyEnd = byteIndex
        # TODO: Think twice whether I should use offset+dummyEnd instead, since that's the real start of valid messages
        self._startByte = offset
        self._endByte = byteIndex - startPos + offset

    @property
    def children(self) -> List[Message]:
        """Children of a Frame are messages"""
        return self.messages

    def messageAt(self, index) -> Message:
        """
        Return the message at relative index within this frame, indes starts from 0 and ends at frame.numMessages - 1
        """
        return self.messages[index]

    def messageAtAbs(self, index) -> Message:
        """
        Return the message at absolute index in the whole log file
        If the message is not in the frame, raise IndexError
        TODO: Do I really need this function?
        """
        if (
            index < self.messages[0].absIndex
            or index > self.messages[-1].absIndex
        ):
            raise IndexError(f"Index {index} out of range")
        return self.messages[index - self.messages[0].absIndex]

    # Message Related Information
    @property
    def hasImage(self) -> bool:
        """Check if this frame contains at least one Image message"""
        for message in self.messages:
            if message.isImage:
                return True
        return False

    @property
    def classNames(self) -> List[str]:
        """The representation class's name of the messages in this frame"""
        return [message.className for message in self.messages]

    @property
    def threadName(self) -> str:
        """The thread that generates this log frame"""
        return self.messages[-1].bodyBytes[4:].decode()

    @property
    def threadIndex(self) -> int:
        """The index of this frame in its thread"""
        if hasattr(self, "_threadIndex_cached"):
            return self._threadIndex_cached
        for i, c in enumerate(self.log["UncompressedChunk"].thread(self.threadName)):
            if c is self:
                self._threadIndex_cached = i
                break
        return self._threadIndex_cached

    @property
    def absIndexRange(self) -> Tuple[int, int]:
        """Absolute message index range of this frame"""
        return (self.messages[0].absIndex, self.messages[-1].absIndex)

    @property
    def numMessages(self) -> int:
        """The number of messages in this frame"""
        return len(self.messages)

    # Dict Representation of the object
    @property
    def infoDict(self) -> Dict:
        """Information about this frame"""
        return {
            "threadName": self.threadName,
            "timestamp": self.timestamp,
            "threadTimeInterval": self.threadTimeInterval,
            "frameIndex": self.index,
            "frameIndexInThread": self.threadIndex,
            "hasImage": self.hasImage,
            "numMessages": self.numMessages,
            "classNames": self.classNames,
            "bytesSize": self.size,
            "byteStartPos": self.startByte,
            "byteEndPos": self.endByte,
        }

    @property
    def timestamp(self) -> int:
        """
        The time stamp of this frame, if it doesn't have a timestamp, use the timestamp of closest frame that has one
        """
        if hasattr(self, "_timestamp_cached"):
            return self._timestamp_cached
        if "FrameInfo" in self.classNames and "time" in self["FrameInfo"].reprDict:
            self._timestamp_cached = self["FrameInfo"]["time"]
        else:
            # Fake a reasonable timestamp
            sign = -1
            distance = 0
            found = False
            rangeOfIndex = range(len(self.parent.children))
            while (
                self.index + distance in rangeOfIndex
                or self.index - distance in rangeOfIndex
            ):
                if self.index + sign * distance in rangeOfIndex:
                    cand = self.parent.children[self.index + sign * distance]
                    if (
                        "FrameInfo" in cand.classNames
                        and "time" in cand["FrameInfo"].reprDict
                    ):
                        for i in range(distance):
                            self.parent.children[
                                self.index + sign * i
                            ]._timestamp_cached = cand.timestamp - sign * (distance - i)
                        found = True
                        break
                if sign == 1:
                    distance += 1
                else:
                    sign = -sign
            if not found:
                for i in rangeOfIndex:
                    self.parent.children[i]._timestamp_cached = i
        return self._timestamp_cached

    @property
    def thread(self) -> List["Frame"]:
        """The thread list that contains this frame"""
        return self.parent.threads[self.threadName]

    @property
    def threadTimeInterval(self) -> int:
        """The time elapse between this log frame and the last log frame of the thread"""
        if self.threadIndex == 0:
            return 0
        return self.timestamp - self.thread[self.threadIndex - 1].timestamp

    @property
    def reprsDict(self) -> Dict[str, Dict]:
        """Dict of ClassName: Representation object for all messages in this frame"""
        result = {}
        for message in self.messages:
            result[message.className] = message.reprDict
        return result

    @property
    def timer(self) -> Timer:
        if hasattr(self, "_timer_cached"):
            return self._timer_cached
        self._timer_cached = self.parent._timers[self.threadName]
        return self._timer_cached

    @property
    def absMessageIndexStart(self) -> int:
        """Absolute message index in the whole file (after removing the dummy messages)"""
        if hasattr(self, "_absMessageIndexStart_cached"):
            return self._absMessageIndexStart_cached
        cnt = 0
        for frame in self.parent.children:
            frame._absMessageIndexStart_cached = cnt
            cnt += len(frame.messages) + len(frame.dummyMessages)
        return self._absMessageIndexStart_cached

    @property
    def absMessageIndexEnd(self) -> int:
        return self.absMessageIndexStart+len(self.dummyMessages) + len(self.messages)
    
    def asDict(self) -> Dict:
        """Almost everything you need to know about this frame"""
        return {"Info": self.infoDict, "ReprsDict": self.reprsDict}

    def __str__(self) -> str:
        """Convert the frame object to string"""
        return dumpJson(self.asDict(), indent=self.strIndent)

    def __getitem__(self, key: Union[int, str, Enum]) -> Message:
        """
        Allow to use [<message idx>/<message name>/<message id enum>] to access a message in the frame
        Special case for "Annotation": There might be multiple Annotations in a frame, so please use frame["Annotations"] or frame.Annotations to get them
        """
        if isinstance(key, int):
            return self.messageAt(key)
        elif key == "Annotation" or key == self.log.MessageID["idAnnotation"]:  # type: ignore
            raise Exception(
                "There might be multiple Annotations in a frame, please use frame.Annotations to get them"
            )
        elif isinstance(key, str) or isinstance(key, Enum):
            result = None
            for message in self.messages:
                if (
                    message.className == key
                    if isinstance(key, str)
                    else message.id == key.value
                ):
                    result = message
                    break
            if result is None:
                raise KeyError(f"Message with key: {key} not found")
            else:
                return result
        else:
            raise KeyError("Invalid key type")

    @property
    def Annotations(self) -> List["Message"]:
        """Get all the Annotation messages in this frame"""
        return [
            message for message in self.messages if message.className == "Annotation"
        ]

    @property
    def picklePath(self) -> Path:
        return self.log.cacheDir / f"Frame_{self.index}.pkl"  # type: ignore

    @property
    def imageMessage(self, slientFail=True):
        """Return the message that contains the image, if not found, return None"""
        MessageID: Any = self.log.MessageID  # type: ignore MessageID is actually Type[Enum]
        if self.hasImage:
            if "CameraImage" in self.classNames:
                CameraImage = self[MessageID.idCameraImage]
                return CameraImage
            elif "JPEGImage" in self.classNames:
                JPEGImage = self[MessageID.idJPEGImage]
                return JPEGImage
            else:
                raise ValueError(
                    "This frame does not have an image, but hasImage field is True"
                )
        else:
            if slientFail:
                return None
            else:
                raise ValueError("This frame does not have an image")

    def saveImageWithMetaData(self, dir=None, imgName=None, slientFail=False):
        """Try to find some meta-data in this frame and write it along with the image in this frame into a PNG file"""
        if self.hasImage is False:
            if slientFail:
                return
            else:
                raise ValueError("This frame does not have an image")

        if imgName is None:
            imgName = self.imageName
        if dir is None:
            dir = self.imageDir

        metadata = None
        try:
            # Try to get metadata in the parent frame
            CameraInfo = self["CameraInfo"]
            CameraMatrix = self["CameraMatrix"]
            ImageCoordinateSystem = self["ImageCoordinateSystem"]

            metadata = PngImagePlugin.PngInfo()
            # Add metadata (using the tEXt chunk)
            # TODO: here I directly write the json string, maybe there is a more compressed way
            metadata.add_text("CameraInfo", str(CameraInfo))
            metadata.add_text("CameraMatrix", str(CameraMatrix))
            metadata.add_text("ImageCoordinateSystem", str(ImageCoordinateSystem))
        except KeyError:
            pass

        if self.imageMessage is not None:
            self.imageMessage.saveImage(dir, imgName, metadata, slientFail=slientFail)

    def recoverTrajectory(self):
        if self.threadName == "Cognition":
            requiredRepresentations = [
                "RobotPose",
                "FieldBall",
                "GlobalTeammatesModel",
                "GlobalOpponentsModel",
            ]
            try:
                agentLoc = [
                    self["RobotPose"]["translation"].x,
                    self["RobotPose"]["translation"].y,
                ]
                ball_loc = [
                    self["FieldBall"]["positionOnField"].x,
                    self["FieldBall"]["positionOnField"].y,
                ]
                Action = []
                for message in self.Annotations:
                    if message["name"] == "NeuralControlAction":
                        Action = ast.literal_eval(message["annotation"])
                print(agentLoc, ball_loc, Action)
            except:
                return None
        return None

    @property
    def outputDir(self):
        return Path(self.logFilePath).parent / Path(self.logFilePath).stem

    @property
    def imageDir(self):
        if self.hasImage:
            return self.outputDir / f"{Path(self.logFilePath).stem}_images"
        else:
            raise ValueError("This frame does not have an image")

    @property
    def frameDir(self):
        return self.outputDir / f"{Path(self.logFilePath).stem}_frames"

    @property
    def imageName(self):
        if self.hasImage and self.imageMessage is not None:
            return (
                Path(self.logFilePath).stem
                + f"_R{self.log['SettingsChunk'].playerNumber}_T{self.timestamp}_{self.threadName}_{self.threadIndex}_M{self.imageMessage.absIndex}_Bf{self.startByte}_Bt{self.endByte}.png"
            )
        else:
            raise ValueError("This frame does not have an image")

    @property
    def jsonName(self):
        """LogFileName_RobotNumber_Timestamp_ThreadName_FrameIndexInThread_StartByte_EndByte.json"""
        return (
            Path(self.logFilePath).stem
            + f"_R{self.log['SettingsChunk'].playerNumber}_T{self.timestamp}_{self.threadName}_{self.threadIndex}_Bf{self.startByte}_Bt{self.endByte}.json"
        )

    def saveFrameDict(self, dir=None):
        """Save the frame as a json file"""

        fileName = self.jsonName
        if dir is None:
            dir = self.frameDir
        os.makedirs(dir, exist_ok=True)
        with open(os.path.join(dir, fileName), "w") as f:
            f.write(str(self))
