import bisect
import functools
from abc import abstractmethod
from mmap import mmap
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Type, Union

from Utils import MemoryMappedFile

from .LogInterfaceBase import IndexMap, LogInterfaceBaseClass
from .LogInterfaceInstanceClass import LogInterfaceInstanceClass

import pdb

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

        # cache
        self._iterStart_cache = False

    def __len__(self):
        return len(self._indexMap)

    def __iter__(self):
        self.indexCursor = 0
        self._iterStart_cache = True
        return self

    def __next__(self):
        try:
            if self._iterStart_cache:
                self._iterStart_cache = False
                self.indexCursor = 0
            else:
                self.indexCursor += 1  # Will raise IndexError if out of range
            return self
        except IndexError:
            raise StopIteration

    def __getitem__(self, index: int) -> Any:
        """Change the Accessor's index to a new position"""
        self.indexCursor = index
        return self

    def __getstate__(self):
        state = {}
        for key, value in self.__dict__.items():
            if key.endswith("_cached") or key in ["_log", "_parent", "_idxFile"]:
                continue
            state[key] = value
        state["indexFilePath"] = self.indexFilePath
        return state

    def __setstate__(self, state: Dict):
        """
        TODO: This function relies on its parent's __setstate__ to set _parent
        """
        indexFilePath = state.pop("indexFilePath")
        if not indexFilePath.exists():
            raise OSError(f"Accessor depends on index file, not found: {indexFilePath}")
        self._idxFile = MemoryMappedFile(indexFilePath)

        self.__dict__.update(state)

        if hasattr(self, "_children"):
            for idx in range(len(self._children)):
                child = self._children[idx]
                if isinstance(child, LogInterfaceInstanceClass):
                    child.parent = self
                elif child.isAccessorClass:
                    child._log = self
                    if child.parentIsAssigend:
                        child.parent = self
                    else:
                        child._parent = self
                    break  # For an accessor, only need to set once
                else:
                    pass

    # Core
    @property
    def log(self) -> Any:
        """In some cases, we have to delay the log resolve"""
        while hasattr(self._log, "parent") and self._log.parent is not None:
            self._log = self._log.parent
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
    def encodeIndexBytes(info: Tuple) -> bytes:
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
        if value < 0:
            value = len(self._indexMap) + value
        self._indexMap[
            value
        ]  # if out of range, either range or List will raise IndexError

        self._indexCursor = value

    # Parent and Children
    @property
    def parentIsAssigend(self) -> bool:
        return self._parentIsAssigend

    @property
    @abstractmethod
    def parent(self) -> "LogInterfaceBaseClass":
        pass

    @parent.setter
    @abstractmethod
    def parent(self, value: "LogInterfaceBaseClass"):
        self._parent = value
        self._parentIsAssigend = True

    @property
    @abstractmethod
    def children(
        self,
    ) -> Union["LogInterfaceAccessorClass", List["LogInterfaceInstanceClass"]]:
        pass

    @children.setter
    @abstractmethod
    def children(
        self,
        value: Union["LogInterfaceAccessorClass", List["LogInterfaceInstanceClass"]],
    ):
        pass

    # Index File related
    @property
    def indexFilePath(self) -> Path:
        return self.log.cacheDir / self.idxFileName()

    # index & absIndex
    @property
    def index(self) -> int:
        """
        The index of current item in the parent's children (another LogInterfaceAccessorClass)
        """
        if isinstance(self.parent.children, LogInterfaceAccessorClass):
            # if self is self.parent.children:
            #     return self.indexCursor
            # else:
                return self.clacRelativeIndex(self.absIndex, self.parent.children.indexMap)  # type: ignore
        elif isinstance(self.parent.children, list):
            return self.clacRelativeIndex(self.absIndex, [child.absIndex for child in self.parent.children])  # type: ignore
        else:
            raise ValueError("Invalid parent's children type")

    @index.setter
    def index(self, value: int):
        if isinstance(self.parent.children, LogInterfaceAccessorClass):
            self.absIndex = self.parent.children.indexMap[value]  # type: ignore
        elif isinstance(self.parent.children, list):
            self.absIndex = self.parent.children[value].asbIndex
        else:
            raise ValueError("Invalid parent's children type")

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

    def clacRelativeIndex(
        self, absIndex: int, indexMap: Optional[IndexMap] = None
    ) -> int:
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

    @property
    def isInstanceClass(self) -> bool:
        return False

    @property
    def isAccessorClass(self) -> bool:
        return True
