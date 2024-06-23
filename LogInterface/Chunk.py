from abc import abstractmethod
from enum import Enum, auto
from pathlib import Path
from typing import List

from .LogInterfaceBase import LogInterfaceInstanceClass


class Chunk(LogInterfaceInstanceClass):
    def __init__(self, parent):
        super().__init__(parent)

        self._children = []

    @property
    def chunkMagicNumber(self) -> int:
        return self.logBytes[self.startByte]

    @property
    def ChunkEnum(self):
        return ChunkEnum(self.chunkMagicNumber)

    @property
    def children(self) -> List:
        """
        @Override: Different from other LogInterface, usually Chunk does not children
        UncompressedChunk and CompressedChunk will override this property
        """
        return []

    @property
    def absIndex(self) -> int:
        raise NotImplementedError("Absolute index of Chunk is meaningless")

    @property
    def size(self):  # -> Any:
        return self.endByte - self.startByte

    @property
    @abstractmethod
    def providedAttributes(self) -> List[str]:
        pass

    @property
    def picklePath(self) -> Path:
        return self.log.cacheDir / f"Chunk_{self.ChunkEnum.name}.pkl"  # type: ignore


class ChunkEnum(Enum):
    UncompressedChunk = 0
    CompressedChunk = auto()
    MessageIDsChunk = auto()
    TypeInfoChunk = auto()
    SettingsChunk = auto()
    IndicesChunk = auto()
