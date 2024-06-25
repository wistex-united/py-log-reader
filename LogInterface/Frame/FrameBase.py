import ast
import csv
import os
from abc import abstractmethod
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Tuple, Union
import numpy as np
from numpy.typing import NDArray
from PIL import PngImagePlugin

from StreamUtils import StreamUtil
from Utils import dumpJson
from Utils.GeneralUtils import countLines

from ..Chunk import Chunk
from ..DataClasses import Timer
from ..LogInterfaceBase import LogInterfaceAccessorClass, LogInterfaceBaseClass
from ..Message import MessageAccessor, MessageInstance, Messages


class FrameBase(LogInterfaceBaseClass):
    _threadWithTimestamp: List[str] = [
        "Upper",
        "Lower",
        "Motion",
        "Audio",
        "Cognition",
    ]
    """These threads has the FrameInfo module that reports the time stamp of the frame, Referee do not have such module"""

    _timestamps_cache: List[int]
    # _timestamps_cache: NDArray[np.uint32]

    def __init__(self, chunk: Chunk):
        super().__init__()
        self._children: Messages

        # cache
        self._threadIndex_cached: int
        self._timestamp_cached: int
        self._timer_cached: Timer

    # Magic functions
    def __str__(self) -> str:
        """Convert the frame object to string"""
        return dumpJson(self.asDict(), indent=self.strIndent)

    def __contains__(self, key: Union[str, Enum]) -> bool:
        for message in self.messages:
            if (
                message.className == key
                if isinstance(key, str)
                else message.id == key.value
            ):
                return True

    @abstractmethod  # Children class need to implement the int case seperately
    def __getitem__(self, key: Union[str, Enum]) -> "FrameBase":
        """
        Allow to use [<message idx>/<message name>/<message id enum>] to access a message in the frame
        Special case for "Annotation": There might be multiple Annotations in a frame, so please use frame["Annotations"] or frame.Annotations to get them
        """
        result = self.log.getCachedInfo(self, key)
        if result is not None:
            return result

        if key == "Annotation" or key == self.log.MessageID["idAnnotation"]:
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
                self.log.cacheInfo(self, key, result)
                return result
        else:
            raise KeyError("Invalid key type")

    # Common properties
    @property
    def agentLoc(self) -> Tuple[float, float, float]:
        """[x, y, rotation]"""
        try:
            agentLoc = (
                float(self["RobotPose"]["translation"].x),
                float(self["RobotPose"]["translation"].y),
                float(self["RobotPose"]["rotation"].value),
            )
        except:
            return None
        return agentLoc

    @property
    def ballLoc(self) -> Tuple[float, float]:
        """[x, y]"""
        try:
            ballLoc = (
                float(self["FieldBall"]["positionOnField"].x),
                float(self["FieldBall"]["positionOnField"].y),
            )
        except:
            return None
        return ballLoc

    @property
    def teammateLoc(self) -> List[Tuple[float, float]]:
        """[[x, y],...]"""
        try:
            teammateLoc = []
            for teammate in self["GlobalTeammatesModel"]["teammates"]:
                teammateLoc.append(
                    (
                        float(teammate.pose.translation.x),
                        float(teammate.pose.translation.y),
                    )
                )
        except:
            return None
        return teammateLoc

    @property
    def opponentLoc(self) -> List[Tuple[float, float]]:
        """[[x, y],...]"""
        try:
            opponentLoc = []
            for opponent in self["GlobalOpponentsModel"]["opponents"]:
                # Have to convert to global coordinates
                opponentLoc.append(
                    (
                        float(opponent.position.x + self["RobotPose"]["translation"].x),
                        float(opponent.position.y + self["RobotPose"]["translation"].y),
                    )
                )
        except:
            return None
        return opponentLoc

    @property
    def motionBasics(self) -> Tuple[float, float, float]:
        """[speed_x, speed_y, rotation]"""
        try:
            motionBasics = (
                float(self["MotionInfo"]["speed"]["translation"].x),
                float(self["MotionInfo"]["speed"]["translation"].y),
                float(self["MotionInfo"]["speed"]["rotation"].value),
            )
        except:
            return None
        return motionBasics

    @property
    def kickBasics(self) -> Tuple[float, float, float]:
        try:
            alignPreciselyModified = 0
            if self["MotionRequest"]["alignPrecisely"].value == 0:
                alignPreciselyModified = 1
            elif self["MotionRequest"]["alignPrecisely"].value == 1:
                alignPreciselyModified = 0
            elif self["MotionRequest"]["alignPrecisely"].value == 2:
                alignPreciselyModified = 0.5
            kickBasics = (
                self["MotionRequest"]["kickType"].name,
                int(self["MotionRequest"]["kickLength"]),
                alignPreciselyModified,
            )
        except:
            return None
        return kickBasics

    @property
    def rollOutResult(self) -> str:
        try:
            rollOutResult = self["GameControllerData"]["rollOutResult"]
        except:
            return None
        return rollOutResult

    # Core
    @property
    @abstractmethod
    def threadName(self) -> str:
        """The thread that generates this log frame"""
        pass

    @property
    @abstractmethod
    def absMessageIndexStart(self) -> int:
        """The index of the first message in this frame"""
        pass

    @property
    @abstractmethod
    def absMessageIndexEnd(self) -> int:
        """The index of the last message in this frame"""
        pass

    # Childeren
    @property
    @abstractmethod
    def children(self) -> Messages:
        """Children of a Frame are messages"""
        pass

    @children.setter
    @abstractmethod
    def children(self, value: Messages):
        pass

    @property
    def messages(self) -> Messages:
        return self.children

    @messages.setter
    def messages(self, value: Messages):
        self.children = value

    @property
    def Annotations(self) -> Messages:
        """Get all the Annotation messages in this frame"""
        if isinstance(self.children, LogInterfaceAccessorClass):
            annotationMap: list[int] = []
            for message in self.messages:
                print(message.indexCursor)
                print(message._iterStart_cache)
                if message.className == "Annotation":
                    annotationMap.append(message.absIndex)
            result: MessageAccessor = self.children.copy()  # type: ignore
            if len(annotationMap) == 0:
                return []
            result.indexMap = annotationMap  # type: ignore
            return result
        elif isinstance(self.children, list):
            result: list[MessageInstance] = []
            for message in self.messages:
                if message.className == "Annotation":
                    result.append(message)  # type:ignore
            return result
        else:
            raise Exception("Invalid children type")

    # Parent
    @property
    @abstractmethod
    def parent(self) -> Any:
        pass

    @abstractmethod
    def eval(self, sutil: StreamUtil, offset: int = 0):
        pass

    # Derived Properties
    @property
    def classNames(self) -> List[str]:
        """The representation class's name of the messages in this frame"""
        result = self.log.getCachedInfo(self, "classNames")
        if result is not None:
            pass
        else:
            result = []
            for message in self.messages:
                result.append(message.className)
            self.log.cacheInfo(self, "classNames", result)
        return result

    @property
    def numMessages(self) -> int:
        """The number of messages in this frame"""
        return len(self.messages)

    @property
    def timer(self) -> Timer:
        if hasattr(self, "_timer_cached"):
            return self._timer_cached
        self._timer_cached = self.parent._timers[self.threadName]
        return self._timer_cached

    # Thread related
    @property
    def thread(self) -> List["FrameBase"]:
        """The thread list that contains this frame"""
        return self.parent.threads[self.threadName]

    @property
    def threadIndex(self) -> int:
        """The index of this frame in its thread"""
        if hasattr(self, "_threadIndex_cached"):
            return self._threadIndex_cached
        thread = self.log.getContentChunk().thread(self.threadName)
        if isinstance(thread, LogInterfaceAccessorClass):
            if thread is self:
                return self.indexCursor
            for frame in thread:
                if frame.absIndex == self.absIndex:
                    return frame.indexCursor
        elif isinstance(thread, list):
            for i, c in enumerate(thread):
                c._threadIndex_cached = i
        return self._threadIndex_cached

    @property
    def threadTimeInterval(self) -> int:
        """The time elapse between this log frame and the last log frame of the thread"""
        if self.threadIndex == 0:
            return 0
        return self.timestamp - self.thread[self.threadIndex - 1].timestamp

    # Dict Representation of the object
    @property
    def reprsDict(self) -> Dict[str, Dict]:
        """Dict of ClassName: Representation object for all messages in this frame"""
        result = {}
        for message in self.messages:
            result[message.className] = message.reprDict
        return result

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

    def asDict(self) -> Dict:
        """Almost everything you need to know about this frame"""
        return {"Info": self.infoDict, "ReprsDict": self.reprsDict}

    @property
    def timestamp(self) -> int:
        """
        The time stamp of this frame, if it doesn't have a timestamp, use the timestamp of closest frame that has one
        """

        if (
            not hasattr(FrameBase, "_timestamps_cache")
            or FrameBase._timestamps_cache is None
        ):
            setattr(FrameBase, "_timestamps_cache", [0] * len(self.parent.children))
        elif len(self._timestamps_cache) != len(self.parent.children):
            self._timestamps_cache.extend(
                [0] * (len(self.parent.children) - len(self._timestamps_cache))
            )

        if self._timestamps_cache[self.absIndex] != 0:
            return self._timestamps_cache[self.absIndex]

        if "FrameInfo" in self and "time" in self["FrameInfo"]:
            self._timestamps_cache[self.absIndex] = self["FrameInfo"]["time"]
            return self._timestamps_cache[self.absIndex]
        else:
            # Fake a reasonable timestamp
            sign = -1
            distance = 0
            found = False
            rangeOfIndex = range(len(self.parent.children))
            while (
                self.absIndex + distance in rangeOfIndex
                or self.absIndex - distance in rangeOfIndex
            ):
                if self.absIndex + sign * distance in rangeOfIndex:
                    cand = self.parent.children[self.absIndex + sign * distance]
                    if "FrameInfo" in cand and "time" in cand["FrameInfo"]:
                        for i in range(distance):
                            self._timestamps_cache[self.absIndex + sign * i] = (
                                cand.timestamp - sign * (distance - i)
                            )
                        found = True
                        break
                if sign == 1:
                    distance += 1
                else:
                    sign = -sign
            if not found:
                for i in rangeOfIndex:
                    self._timestamps_cache = range(len(self.parent.children))
        return self._timestamps_cache[self.absIndex]

    def interpolateAllTimestamps(self):
        # Fake a reasonable timestamp
        lastValidTimestamp = -1
        lastFrameWithoutTimestamp = (
            -2
        )  # -1 means no frame without timestamp; -2 no valid timestamp from [0]
        for i in range(len(self.parent.children)):
            frame = self.parent.children[i]

            currentFrameHasTimestamp = False
            if self._timestamps_cache[frame.absIndex] != 0:
                timestamp = self._timestamps_cache[frame.absIndex]
                currentFrameHasTimestamp = True
            elif "FrameInfo" in frame and "time" in frame["FrameInfo"]:
                timestamp = frame["FrameInfo"]["time"]
                currentFrameHasTimestamp = True

            if currentFrameHasTimestamp:
                if lastFrameWithoutTimestamp != -1:
                    pass
                elif lastFrameWithoutTimestamp == -2:
                    for j in range(lastFrameWithoutTimestamp, i):
                        self._timestamps_cache[j] = timestamp - j
                else:
                    for j in range(lastFrameWithoutTimestamp, i):
                        interpolatedTimestamp = int(
                            lastValidTimestamp
                            + (timestamp - lastValidTimestamp)
                            / (i - lastFrameWithoutTimestamp)
                            * (j - lastFrameWithoutTimestamp)
                        )
                        self._timestamps_cache[j] = interpolatedTimestamp
            else:
                if lastFrameWithoutTimestamp == -1:
                    lastFrameWithoutTimestamp = i

            if currentFrameHasTimestamp:
                lastValidTimestamp = timestamp

        if lastFrameWithoutTimestamp == -1:
            pass
        elif lastFrameWithoutTimestamp == -2:
            print("Warning: No frame has valid timestamp, frame index is used instead")
            for i in range(len(self.parent.children)):
                self._timestamps_cache[i] = i
        else:
            for j in range(lastFrameWithoutTimestamp, len(self.parent.children)):
                self._timestamps_cache[j] = (
                    lastValidTimestamp + (j - lastFrameWithoutTimestamp) + 1
                )

    @property
    def picklePath(self) -> Path:
        return self.log.cacheDir / f"Frame_{self.index}.pkl"  # type: ignore

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

    # Image Utils
    @property
    def hasImage(self) -> bool:
        """Check if this frame contains at least one Image message"""
        for message in self.messages.copy():
            if message.isImage:
                return True
        return False

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
            dir = self.log.imageDir

        metadata = None
        try:
            # Try to get metadata in the frame
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
            self.imageMessage.saveImage(dir, imgName, metadata, slientFail=slientFail)  # type: ignore

    # JSON Utils
    @property
    def jsonName(self):
        """LogFileName_RobotNumber_Timestamp_ThreadName_FrameIndexInThread_StartByte_EndByte.json"""
        return (
            Path(self.logFilePath).stem
            + f"_R{self.log.SettingsChunk.playerNumber}_T{self.timestamp}_{self.threadName}_{self.threadIndex}_Bf{self.startByte}_Bt{self.endByte}.json"
        )

    def saveFrameDict(self, dir=None):
        """Save the frame as a json file"""
        fileName = self.jsonName
        if dir is None:
            dir = self.log.frameDir
        os.makedirs(dir, exist_ok=True)
        with open(os.path.join(dir, fileName), "w") as f:
            f.write(str(self))
