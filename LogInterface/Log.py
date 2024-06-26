import csv
import io
import os
from collections import OrderedDict
from enum import Enum, auto
from mmap import mmap
from pathlib import Path
from typing import Any, Dict, List, Optional, Type, Union

from numpy.typing import NDArray

from Primitive.PrimitiveDefinitions import Bool
from StreamUtils import StreamUtil
from Utils import MemoryMappedFile

from .Chunk import Chunk, ChunkEnum
from .DataClasses import DataClass
from .Frame import FrameAccessor, FrameBase, FrameInstance, Frames
from .LogInterfaceBase import (IndexMap, LogInterfaceAccessorClass,
                               LogInterfaceBaseClass,
                               LogInterfaceInstanceClass)
from .Message import MessageAccessor, MessageBase, MessageInstance, Messages
from .MessageIDChunk import MessageIDChunk as MChunk
from .SettingsChunk import SettingsChunk as SChunk
from .TypeInfoChunk import TypeInfoChunk as TChunk
from .UncompressedChunk import UncompressedChunk as UChunk

"""
                                    Log
        //             //            ||               \\              \\
SettingsChunk MessageIDsChunk TypeInfoChunk (Un)CompressedChunk IndicesChunk
                                                        |
                                                    [ Frame ]
                                                        |
                                                    [ Message ]
                                                        |
                                            Representation Object (ReprObj)
"""


class Log(LogInterfaceInstanceClass):
    """
    Root class for the Log interface, it holds all the information of the log file in its logBytes field
    To use it,

    For small log file, it is recommended to parse all bytes at once
    1. readLogFile(filePath)
    2. eval()
    3. parseBytes()
    4. Do something on the parsed data

    For large log file (if log file size * 15 > you memory size)
    1. readLogFile(filePath)
    2. eval(isLogFileLarge=True)
    3. Do something on the parsed data
    """

    class EvalInformationFormat(Enum):
        PICKLE = 0
        CSV = auto()

    # TODO: Move it to a config file
    evalInformationFormat = EvalInformationFormat.CSV

    def __init__(self, parent=None):
        super().__init__(parent)

        self._children: List[Chunk]  # @Override the default type hint

        self.UncompressedChunk: UChunk
        self.MessageIDChunk: MChunk
        self.TypeInfoChunk: TChunk
        self.SettingsChunk: SChunk

        # Root specific fields
        self.file: MemoryMappedFile
        self._logFilePath: str

        # cache
        self._messageCachedReprList_cached: NDArray[Bool]

    def __getitem__(self, key: Union[int, str, ChunkEnum]) -> Chunk:
        """Allow to use [<chunk idx>/<chunk name>/<chunk enum>] to access a chunk"""
        if isinstance(key, int) and key < len(self._children):
            return self._children[key]
        elif isinstance(key, str) or isinstance(key, ChunkEnum):
            result = None
            for chunk in self._children:
                if (
                    chunk.ChunkEnum.name == key
                    if isinstance(key, str)
                    else chunk.ChunkEnum == key
                ):
                    result = chunk
                    break
            if result is None:
                raise KeyError(f"Chunk with key: {key} not found")
            else:
                return result
        else:
            raise KeyError("Invalid key type")

    @property
    def logBytes(self) -> mmap:
        return self.file.getData()

    @property
    def logFilePath(self) -> str:
        return self._logFilePath

    def __getstate__(self):
        state = super().__getstate__()
        if state.get("file", None):
            del state["file"]
        return state

    def __setstate__(self, state: "Log") -> None:
        super().__setstate__(state)
        if (
            hasattr(self, "_logFilePath") and self._logFilePath != ""
        ):  # If the _logFilePath is set, read the file
            self.readLogFile()  # recover the "file" attribute

    def eval(
        self,
        sutil: StreamUtil = None,  # type: ignore TODO: usually we use the Log's file stream
        offset: int = 0,
        isLogFileLarge: bool = False,
        forceReEval: bool = False,
    ):
        """
        This function evaluate the the start and end position of messages, read settings and write the LogClasses
        The first time you run eval on a log file, it will dump an indexes file, and use the file afterwards
        """
        if not forceReEval and os.path.isfile(self.picklePath):
            try:
                self.pickleLoad()
                return
            except EOFError as e:  # Something wrong with the indexes file, remove it
                os.remove(self.picklePath)

        self._children = []

        if sutil is None:
            sutil = StreamUtil(
                self.logBytes, showProgress=True, desc="Evaluating Message Positions"
            )
        startPos = sutil.tell()

        readed = [False] * len(ChunkEnum)
        shouldTrucate = False
        while not sutil.atEnd():
            chunkMagicBit = sutil.readUChar()
            sutil.seek(-1, io.SEEK_CUR)

            if chunkMagicBit not in range(len(readed)) or readed[chunkMagicBit] == True:
                shouldTrucate = True
                break
            else:
                readed[chunkMagicBit] = True
            offset = self._children[-1].endByte if self._children else 0
            match chunkMagicBit:
                case ChunkEnum.UncompressedChunk.value:
                    self.UncompressedChunk = UChunk(self)
                    self.UncompressedChunk.eval(sutil, offset, isLogFileLarge)
                    self._children.append(self.UncompressedChunk)
                case ChunkEnum.CompressedChunk.value:
                    raise NotImplementedError("Compressed chunk not implemented")
                case ChunkEnum.MessageIDsChunk.value:
                    self.MessageIDChunk = MChunk(self)
                    self.MessageIDChunk.eval(sutil, offset)
                    self._children.append(self.MessageIDChunk)
                case ChunkEnum.TypeInfoChunk.value:
                    self.TypeInfoChunk = TChunk(self)
                    self.TypeInfoChunk.eval(sutil, offset)
                    self._children.append(self.TypeInfoChunk)
                case ChunkEnum.SettingsChunk.value:
                    self.SettingsChunk = SChunk(self)
                    self.SettingsChunk.eval(sutil, offset)
                    self._children.append(self.SettingsChunk)
                case ChunkEnum.IndicesChunk.value:
                    break
                case _:
                    break  # TODO: For debug Only
                    # raise Exception(f"Unknown chunk magic number: {chunkMagicBit}")
                    # raise NotImplementedError("Indices chunk not implemented")

        self._startByte = offset
        self._endByte = sutil.tell() - startPos + offset
        self.pickleDump()

    def parseBytes(self):
        for i in self.children:
            i.parseBytes()
        self.pickleDump()

    @property
    def numMessages(self) -> int:
        """The number of messages in this frame"""
        return len(self.messages)

    @property
    def picklePath(self) -> Path:
        return self.cacheDir / f"Log_{Path(self._logFilePath).stem}.pkl"

    @property
    def cacheDir(self) -> Path:
        return Path("cache") / Path(self._logFilePath).stem

    def readLogFile(self, filePath: str = ""):
        if filePath == "":
            if hasattr(self, "_logFilePath"):
                self.file = MemoryMappedFile(self._logFilePath)
                return
            else:
                raise Exception("No log file path provided")
        self.file = MemoryMappedFile(filePath)
        self._logFilePath = filePath

    @property
    def MessageID(self) -> Type[Enum]:
        return self.MessageIDChunk.MessageID

    @property
    def frames(self) -> Frames:
        return self.UncompressedChunk.frames

    @property
    def messages(self) -> Messages:
        return self.UncompressedChunk.messages

    @property
    def children(self) -> List[Chunk]:
        return self._children

    @property
    def outputDir(self):
        return (
            Path(self.logFilePath).parent
            / "LogReaderOutputs"
            / Path(self.logFilePath).stem
        )

    @property
    def imageDir(self):
        return self.outputDir / f"{Path(self.logFilePath).stem}_images"

    @property
    def frameDir(self):
        return self.outputDir / f"{Path(self.logFilePath).stem}_frames"

    def writeCacheInfo(self, type, name: str, absIndex: int, value):
        if not hasattr(self, "_Info_cached") or self._Info_cached is None:
            self._Info_cached = {}
        if type not in self._Info_cached:
            self._Info_cached[type] = {}
        if name not in self._Info_cached[type]:
            self._Info_cached[type][name] = OrderedDict({absIndex: value})
        else:
            if absIndex not in self._Info_cached[type][name]:
                self._Info_cached[type][name][absIndex] = value
            else:
                self._Info_cached[type][name].pop(absIndex)  # remove the old one
                self._Info_cached[type][name][absIndex] = [value]
        if len(self._Info_cached[type][name]) > 100:
            self._Info_cached[type][name].popitem(last=False)

    def cacheInfo(self, obj, name: str, value):
        if isinstance(obj, LogInterfaceAccessorClass):
            if isinstance(obj, FrameBase):
                type = "Frame"
            elif isinstance(obj, MessageBase):
                type = "Message"
            else:
                raise ValueError
        else:
            raise ValueError
        absIndex = obj.absIndex

        self.writeCacheInfo(type, name, absIndex, value)

    def getCachedInfo(self, obj, name: str):
        if isinstance(obj, LogInterfaceAccessorClass):
            if isinstance(obj, FrameBase):
                type = "Frame"
            elif isinstance(obj, MessageBase):
                type = "Message"
            else:
                raise ValueError
        else:
            raise ValueError(f"Unsupported type {obj.__class__.__name__}")
        absIndex = obj.absIndex

        if not hasattr(self, "_Info_cached") or self._Info_cached is None:
            self._Info_cached = {}
        if type not in self._Info_cached:
            return None
        if name not in self._Info_cached[type]:
            return None
        if absIndex not in self._Info_cached[type][name]:
            return None
        return self._Info_cached[type][name][absIndex]

    def getMessageAccessor(
        self, indexMap: Optional[IndexMap] = None
    ) -> MessageAccessor:
        return MessageAccessor(self, indexMap)

    def getFrameAccessor(self, indexMap: Optional[IndexMap] = None) -> FrameAccessor:
        return FrameAccessor(self, indexMap)

    def getAccessorCopyOf(
        self, source: LogInterfaceBaseClass
    ) -> LogInterfaceAccessorClass:
        if isinstance(source, FrameBase):
            if isinstance(source, FrameAccessor):
                result = self.getFrameAccessor(source.indexMap)
            elif isinstance(source, FrameInstance):
                result = self.getFrameAccessor()
            else:
                raise NotImplementedError
            result.absIndex = source.absIndex
            return result
        elif isinstance(source, MessageBase):
            if isinstance(source, MessageAccessor):
                result = self.getMessageAccessor(source.indexMap)
            elif isinstance(source, MessageInstance):
                frameHelper = self.getFrameAccessor()
                frameHelper.absIndex = source.frameIndex
                result = frameHelper.getMessageAccessor()
            else:
                raise NotImplementedError
            result.absIndex = source.absIndex
            return result
        else:
            raise NotImplementedError

    def getContentChunk(self) -> UChunk:
        # TODO: after implementing CompressedChunk, check this to return the true content Chunk
        return self.UncompressedChunk

    @property
    def absIndex(self) -> int:
        raise NotImplementedError("Absolute index of Chunk is meaningless")
