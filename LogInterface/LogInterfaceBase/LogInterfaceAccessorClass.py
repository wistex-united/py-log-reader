import bisect
import functools
from abc import abstractmethod
from mmap import mmap
from pathlib import Path
from typing import Any, Optional, Type

from Utils import MemoryMappedFile

from .LogInterfaceBase import IndexMap, LogInterfaceBaseClass
from .LogInterfaceInstanceClass import LogInterfaceInstanceClass


class LogInterfaceAccessorClass(LogInterfaceBaseClass):
    @staticmethod
    @functools.lru_cache(maxsize=1000)
    def getBytesFromMmap(idxFile: mmap, indexStart: int, indexEnd: int) -> bytes:
        return idxFile[indexStart:indexEnd]

    def __init__(self, log: Any, indexMap: Optional[IndexMap]):
        """Core invariants: log; idxFileName; indexMap (valid range of the accessor)"""
        """Core variables: indexCursor"""
        super().__init__()
        self._log = log

        indexFilePath = log.cacheDir / self.idxFileName()
        if not indexFilePath.exists():
            raise OSError(f"Accessor depends on index file, not found: {indexFilePath}")
        self._idxFile = MemoryMappedFile(indexFilePath)
        self.indexMap = indexMap

        self._parent: LogInterfaceBaseClass
        self._parentIsAssigend: bool = False

        self._indexCursor = 0

    def __len__(self):
        return len(self._indexMap)

    def __iter__(self):
        return self

    def __next__(self):
        try:
            self.indexCursor += 1  # Will raise IndexError if out of range
            return self
        except IndexError:
            raise StopIteration

    def __getitem__(self, index: int) -> Any:
        """Change the Accessor's index to a new position"""
        self.indexCursor = index
        return self

    # Core
    @property
    def log(self) -> Any:
        return self._log

    @property
    def indexMap(self) -> IndexMap:
        return self._indexMap

    @indexMap.setter
    def indexMap(self, value: Optional[IndexMap]) -> None:
        if value is None:
            self._indexMap = range(self._idxFile.getSize() // self.messageIdxByteLength)
        elif isinstance(value, list):
            self._indexMap = sorted(value.copy())
        elif isinstance(value, range):
            self._indexMap = value
        else:
            raise ValueError("Invalid index range")
        if hasattr(self, "_indexCursor"):
            try:
                self.indexCursor = self._indexCursor
            except:
                self.indexCursor = 0

    @staticmethod
    @abstractmethod
    def idxFileName() -> str:
        pass

    @staticmethod
    @abstractmethod
    def getInstanceClass() -> Type["LogInterfaceInstanceClass"]:
        pass

    @property
    def indexCursor(self) -> int:
        return self._indexCursor

    @indexCursor.setter
    def indexCursor(self, value) -> None:
        self._indexMap[
            value
        ]  # if out of range, either range or List will raise IndexError
        self._indexCursor = value

    # Parent and Children
    def assignParent(self, parent: LogInterfaceBaseClass):
        self._parent = parent
        # TODO: update self.indexMap based on parent's absIndex
        self._parentIsAssigend = True

    @property
    def parentIsAssigend(self) -> bool:
        return self._parentIsAssigend

    @property
    @abstractmethod
    def parent(self) -> "LogInterfaceAccessorClass":
        pass

    @property
    @abstractmethod
    def children(self) -> "LogInterfaceAccessorClass":
        pass

    # Index File related
    @property
    def indexFilePath(self) -> Path:
        return self.log.cacheDir / self.idxFileName()

    @abstractmethod
    # index & absIndex
    @property
    def index(self) -> int:
        """
        The index of current item in the parent's children (another LogInterfaceAccessorClass)
        """
        return self.clacRelativeIndex(self.absIndex, self.parent.children.indexMap)  # type: ignore

    @index.setter
    def index(self, value: int):
        self.absIndex = self.parent.children.indexMap[value]  # type: ignore

    @property
    def absIndex(self) -> int:
        """The location of current index in the messageIndexFile"""
        return self.indexMap[self.indexCursor]

    @absIndex.setter
    def absIndex(self, value: int):
        self.indexCursor = self.clacRelativeIndex(
            value, self.indexMap
        )  # Will raise ValueError if not in indexMap

    # Tools
    def copy(self) -> "LogInterfaceAccessorClass":
        result = self.__class__(self.log, self.indexMap)
        result.indexCursor = self.indexCursor
        result._parent = self.parent
        result._parentIsAssigend = self._parentIsAssigend
        return result

    @abstractmethod
    def getInstance(self) -> "LogInterfaceInstanceClass":
        result = self.getInstanceClass()(self.parent)
        return result

    def clacRelativeIndex(self, absIndex: int, indexMap: Optional[IndexMap]) -> int:
        """If IndexMap is a list, it must be sorted"""

        if indexMap is None:
            indexMap = self.indexMap

        if isinstance(indexMap, list):
            index = bisect.bisect_left(indexMap, absIndex)
            if index != len(indexMap) and indexMap[index] == absIndex:
                return index
            else:
                raise ValueError("absIndex not in indexMap: List")
        elif isinstance(indexMap, range):
            try:
                return indexMap.index(absIndex)
            except ValueError:
                raise ValueError("absIndex not in indexMap: range")
        else:
            raise ValueError("Invalid index range")
