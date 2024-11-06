from enum import Enum
from typing import Any, List, Union

from StreamUtils import StreamUtil
from Utils import isIntAlike

from ..LogInterfaceBase import LogInterfaceInstanceClass
from ..Message import MessageInstance, Messages
from .FrameBase import FrameBase


class FrameInstance(FrameBase, LogInterfaceInstanceClass):
    def __init__(self, chunk):
        LogInterfaceInstanceClass.__init__(self, chunk)

        self.dummyMessages: Messages

        # cache
        self._absMessageIndexStart_cached: int

    def __getitem__(self, key: Union[int, str, Enum]) -> FrameBase:
        if isIntAlike(key):
            return self.messages[key]
        else:
            return super().__getitem__(key)

    # Core
    @property
    def threadName(self) -> str:
        return self.messages[-1].bodyBytes[4:].decode()

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
        return self.absMessageIndexStart + len(self.dummyMessages) + len(self.messages)

    @property
    def absIndex(self) -> int:
        """Absolute frame index in the whole file"""
        if hasattr(self, "_absIndex_cached"):
            return self._absIndex_cached
        else:
            for idx, frame in enumerate(self.parent.children):
                frame._absIndex_cached = idx
        return self._absIndex_cached

    # Children
    @property
    def children(self) -> Messages:
        if not hasattr(self, "_children"):
            self._children = []
        return self._children

    @children.setter
    def children(self, value: Messages) -> None:
        self._children = value

    @property
    def parent(self) -> Any:
        return self._parent

    @parent.setter
    def parent(self, value: Any) -> None:
        self._parent = value

    def eval(self, sutil: StreamUtil, offset: int = 0):
        """
        Try to locate the start the end bytes of a frame
        Start at the FrameBegin message and end at the FrameFinished message
        FrameBegin and FrameFinished should have the same threadName
        It will keep evaluating messages util it finds a corresponding FrameFinished message
        """
        startPos = sutil.tell()
        self.messages = []
        self.dummyMessages = []

        byteIndex = startPos

        MessageID: Any = self.log.MessageID  # type: ignore MessageID is actually Type[Enum]
        while True:
            message = MessageInstance(self)
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

    # Derived Properties
    @property
    def classNames(self) -> List[str]:
        result = []
        for message in self.messages:
            result.append(message.className)
        return result
