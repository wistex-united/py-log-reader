import io
import os
from typing import Any, Dict

from Primitive import *
from StreamUtils import StreamUtil

from ..DataClasses import Annotation, DataClass, Stopwatch
from ..LogInterfaceBase import LogInterfaceAccessorClass, LogInterfaceInstanceClass
from .MessageBase import MessageBase


class MessageInstance(MessageBase, LogInterfaceInstanceClass):
    def __init__(self, frame):
        LogInterfaceInstanceClass.__init__(self, frame)

        self._reprObject: DataClass
        self._logId: UChar

        # cache
        self._reprDict_cached: Dict[str, Any]

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
        # try:
        id, size = sutil.readMessageHeader()
        if size < sutil.remainingSize():
            sutil.seek(size, io.SEEK_CUR)
        else:
            sutil.seek(
                startPos, io.SEEK_SET
            )  # Set the stream back to avoid misreporting startByte and endByte
            raise EOFError("Not enough data to eval message")

        self._logId = id

        self._startByte = offset
        self._endByte = offset + 4 + int(size)

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
            self.parseBytes()
        if isinstance(self._reprObject, Stopwatch):
            try:
                self._reprObject = self.frame.timer.getStopwatch(self.frameIndex)
            except:
                pass
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
        if hasattr(self, "_reprDict_cached") and self._reprDict_cached:
            return self._reprDict_cached
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
        """Absolute message index in the whole file"""
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
