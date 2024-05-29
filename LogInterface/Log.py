import io
import os
from enum import Enum
from mmap import mmap
from pathlib import Path
from typing import List, Type, Union

from StreamUtils import StreamUtil
from Utils import MemoryMappedFile

from .Chunk import Chunk, ChunkEnum
from .DataClasses import DataClass
from .Frame import Frame
from .LogInterfaceBase import LogInterfaceBase
from .Message import Message
from .MessageIDChunk import MessageIDChunk as MChunk
from .SettingsChunk import SettingsChunk as SChunk
from .TypeInfoChunk import TypeInfoChunk as TChunk
from .UncompressedChunk import UncompressedChunk as UChunk

# TODO: separate top/root level specific functions into a separate class

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


class Log(LogInterfaceBase):
    """
    Root class for the Log interface, it holds all the information of the log file in its logBytes field
    To use it,
    1. readLogFile(filePath)
    2. eval()
    3. parseBytes()
    4. Do something on the parsed data
    """

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
    ):
        """
        This function evaluate the the start and end position of messages, read settings and write the LogClasses
        The first time you run eval on a log file, it will dump an indexes file, and use the file afterwards
        """
        if os.path.isfile(self.picklePath):
            try:
                self.pickleLoad()
                return
            except EOFError:  # Something wrong with the indexes file, remove it
                os.remove(self.picklePath)
                pass

        self._children = []

        if sutil is None:
            sutil = StreamUtil(
                self.logBytes, showProgress=True, desc="Evaluating Message Positions"
            )
        startPos = sutil.tell()

        while not sutil.atEnd():
            chunkMagicBit = sutil.readUChar()
            sutil.seek(-1, io.SEEK_CUR)

            offset = self._children[-1].endByte if self._children else 0
            match chunkMagicBit:
                case ChunkEnum.UncompressedChunk.value:
                    self.UncompressedChunk = UChunk(self)
                    self.UncompressedChunk.eval(sutil, offset)
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
        if os.path.isfile(self.picklePath):
            try:
                self.pickleLoad()
            except EOFError:  # Something wrong with the indexes file, remove it
                os.remove(self.picklePath)
        for i in self.children:
            i.parseBytes()
        self.pickleDump()

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
    def frames(self) -> List[Frame]:
        return self.UncompressedChunk.frames

    @property
    def messages(self) -> List[Message]:
        return self.UncompressedChunk.messages

    @property
    def reprs(self) -> List[DataClass]:
        return self.UncompressedChunk.reprs

    @property
    def children(self) -> List[Chunk]:
        return self._children
