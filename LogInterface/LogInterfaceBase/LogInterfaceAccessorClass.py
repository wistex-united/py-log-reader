import bisect
import functools
from abc import abstractmethod
from mmap import mmap
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Type, Union

from Utils import MemoryMappedFile

from .LogInterfaceBase import IndexMap, LogInterfaceBaseClass
from .LogInterfaceInstanceClass import LogInterfaceInstanceClass


class LogInterfaceAccessorClass(LogInterfaceBaseClass):
    """
    An Iterator Class, mainly used when accessing large log files
    Very light weight, feel free to copy many of it

    All of its functionality is based on log class
    , if you cannot access the log class when instantiate accessor
    , you can also pass in a class that will be in the log
    hierarchy when you start to use the accessor, the
    accessor would delay the resolve of log class
    """

    def __init__(self, log: Any, indexMap: Optional[IndexMap]):
        """Core invariants: log; idxFileName; indexMap (valid range of the accessor)"""
        """Core variables: indexCursor"""
        super().__init__()
        self._log = log

        self._frozen = False

        self.indexMap = indexMap

        self.indexCursor = 0

        # cache
        self._iterStart = True

    def __len__(self):
        return len(self._indexMap)

    def __iter__(self):
        result = self.copy()
        result.indexCursor = 0
        result._iterStart = True
        return result

    def __next__(self):
        try:
            if self._iterStart:
                self._iterStart = False
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
            if key.endswith("_cached") or key in ["_log", "_idxFile"]:
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
                if child.isInstanceClass:
                    raise ValueError(
                        "It's not possible for a Accessor to have Instance child"
                    )
                elif child.isAccessorClass:
                    child.log = self  # deplay setting the log field
                else:
                    pass

    def __contains__(self, key: "LogInterfaceAccessorClass") -> bool:
        if isinstance(key, self.__class__):
            if isinstance(self.indexMap, range):
                return key in self.indexMap
            elif isinstance(self.indexMap, list):
                index = bisect.bisect_left(self.indexMap, key.absIndex)
                return index < len(self.indexMap)
            raise ValueError("Invalid indexMap")
        else:
            return False

    # Core
    @property
    def log(self) -> Any:
        """In some cases, we have to delay the log instance resolve"""
        while hasattr(self._log, "parent") and self._log.parent is not None:
            self._log = self._log.parent
        return self._log

    @property
    def idxFile(self) -> MemoryMappedFile:
        if not hasattr(self, "_idxFile"):
            if not self.indexFilePath.exists():
                raise OSError(
                    f"Accessor depends on index file, not found: {self.indexFilePath}"
                )
            self._idxFile = MemoryMappedFile(self.indexFilePath)
        return self._idxFile

    @log.setter
    def log(self, value: Any) -> None:
        self._log = value

    @property
    def indexMap(self) -> IndexMap:
        return self._indexMap

    @indexMap.setter
    def indexMap(self, value: Optional[IndexMap]) -> None:
        initialSet = True
        if hasattr(self, "_indexMap"):
            prevAbsIndex = self.absIndex
            initialSet = False

        if value is None:
            self._indexMap = range(self.idxFile.getSize() // self.messageIdxByteLength)
        elif len(value) == 0:
            raise ValueError("Empty index map")
        elif isinstance(value, list):
            self._indexMap = sorted(value)
        elif isinstance(value, range):
            self._indexMap = value
        else:
            raise ValueError("Invalid index range")

        # New indexMap might not contain the old abs index position
        # in that case, reset to the first element in new indexMap
        if not initialSet:
            try:
                self.absIndex = prevAbsIndex
            except:
                print(
                    "Warning: failed to set indexCursor to previous absIndex in the new indexMap"
                )
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

    @staticmethod
    @functools.lru_cache(maxsize=1000)
    def getBytesFromMmap(idxFile: mmap, indexStart: int, indexEnd: int) -> bytes:
        return idxFile[indexStart:indexEnd]

    @property
    def indexCursor(self) -> int:
        return self._indexCursor

    @indexCursor.setter
    def indexCursor(self, value) -> None:
        if self._frozen:
            raise RuntimeError("Cannot modify frozen accessor")
        if value < 0:
            value = len(self._indexMap) + value
        self._indexMap[
            value
        ]  # if out of range, either range or List will raise IndexError

        self._indexCursor = value

    @property
    @abstractmethod
    def parent(self) -> "LogInterfaceBaseClass":
        """Should be implement by each instance class to resolve its parent with only its index information"""
        pass

    @parent.setter
    @abstractmethod
    def parent(self, value: "LogInterfaceBaseClass"):
        raise NotImplementedError("Accessor deduce their parent by index information")

    @property
    @abstractmethod
    def children(
        self,
    ) -> "LogInterfaceAccessorClass":
        pass

    @children.setter
    @abstractmethod
    def children(
        self,
        value: "LogInterfaceAccessorClass",
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
        The relative index of current item in its indexMap
        """
        return self.indexCursor

    @index.setter
    def index(self, value: int):
        self.indexCursor = value

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
        """
        Copy and accessor with the same indexCursor
        NOTE: copy() will not copy frozen state
        """
        result = self.log.getAccessorCopyOf(self)
        result.indexCursor = self.indexCursor
        return result

    @abstractmethod
    def getInstance(self) -> "LogInterfaceInstanceClass":
        raise NotImplementedError(
            "TODO: Not Implemented yet, directly get the instance from log with abs index"
        )

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

    def freeze(self) -> "LogInterfaceAccessorClass":
        """
        Freeze the object's absIndex (if it is a iterator, it won't be able to move anymore)
        NOTE: copy() will not copy frozen state
        """
        self._frozen = True
        return self
