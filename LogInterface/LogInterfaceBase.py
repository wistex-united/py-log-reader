import bisect
import functools
import json
import os
import pickle
from abc import ABC, abstractmethod
from mmap import mmap
from pathlib import Path
from typing import Any, List, Optional, Union

from StreamUtils import StreamUtil
from Utils import MemoryMappedFile, SpecialEncoder

IndexMap = Union[range, List[int]]


class LogInterfaceBase(ABC):
    strIndent: int = 2

    frameIdxFileName: str = "frameIndexFile.cache"
    messageIdxByteLength: int = 32
    frameIdxByteLength: int = 32

    def __init__(self):
        super().__init__()

    @property
    @abstractmethod
    def children(self) -> List["LogInterfaceBase"]:
        """Helper property used for __iter__ and __len__, it should be set to child objects that makes sense"""
        pass

    @abstractmethod
    def eval(self, sutil: StreamUtil, offset: int = 0):
        """Only read necessary bytes to calculate the start and end byte of this object, create the hierarchical tree but does not parse bytes into data classes"""
        pass

    def __getitem__(self, key) -> Any:
        """Allow to use [<attribute name>] to access attributes"""
        return self.__getattribute__(key)

    def __iter__(self):
        return self.children.__iter__()

    def __len__(self):
        return self.children.__len__()

    def __str__(self) -> str:
        return json.dumps(self.asDict(), indent=self.strIndent, cls=SpecialEncoder)

    @property
    @abstractmethod
    def startByte(self) -> int:
        pass

    @property
    @abstractmethod
    def endByte(self) -> int:
        pass

    @property
    def size(self) -> int:
        return self.endByte - self.startByte

    @property
    @abstractmethod
    def parent(self) -> "Any":
        pass

    @property
    @abstractmethod
    def log(self) -> "Any":
        pass

    @property
    @functools.lru_cache(maxsize=1)
    def logFilePath(self) -> str:
        return self.log.logFilePath

    @property
    @functools.lru_cache(maxsize=1)
    def logBytes(self) -> mmap:
        """Shortcut reference to root's logBytes"""
        return self.log.logBytes

    @property
    @abstractmethod
    def index(self) -> int:
        pass

    @index.setter
    @abstractmethod
    def index(self, value) -> int:
        pass

    @property
    @abstractmethod
    def absIndex(self) -> int:
        pass

    @absIndex.setter
    @abstractmethod
    def absIndex(self, value) -> int:
        pass

    @property
    @abstractmethod
    def picklePath(self) -> Path:
        pass

    def asDict(
        self,
    ):
        """Base recursive asDict() implementation"""
        result = {}
        for attr in dir(self):
            if not attr.startswith("_"):
                try:
                    obj = getattr(self, attr)
                    if hasattr(obj, "asDict"):
                        result[attr] = obj.asDict()
                except AttributeError:
                    result[attr] = getattr(self, attr)

        return result


class LogInterfaceAccessorClass(LogInterfaceBase):
    @staticmethod
    @functools.lru_cache(maxsize=1000)
    def getBytesFromMmap(idxFile: mmap, indexStart: int, indexEnd: int) -> bytes:
        return idxFile[indexStart:indexEnd]

    def __init__(self, log: Any, indexMap: Optional[IndexMap]):
        """Core invariants: log; idxFileName; indexMap (valid range of the accessor)"""
        """Core variables: indexCursor"""
        super().__init__()
        self._log = log

        self._idxFile = MemoryMappedFile(log.cacheDir / self.idxFileName())

        if indexMap is None:
            self._indexMap = range(self._idxFile.getSize() // self.messageIdxByteLength)
        if isinstance(indexMap, list):
            self._indexMap = sorted(indexMap.copy())
        elif isinstance(indexMap, range):
            self._indexMap = indexMap
        else:
            raise ValueError("Invalid index range")

        self._indexCursor = 0

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

    @staticmethod
    @abstractmethod
    def idxFileName() -> str:
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

    # Deduced properties
    @property
    @abstractmethod
    def parent(self) -> "LogInterfaceAccessorClass":
        pass

    @property
    @abstractmethod
    def children(self) -> "LogInterfaceAccessorClass":
        pass

    # Tools
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


class LogInterfaceInstanceClass(LogInterfaceBase):
    def __init__(self, parent):
        super().__init__()
        self._parent = parent

        # available after eval
        self._startByte: int
        self._endByte: int
        self._children: List

        # cached references
        self._log_cached: Any  # type: ignore TODO: add a forward reference to Log
        self._index_cached: int

    @property
    def parent(self) -> Any:
        return self._parent

    @property
    def startByte(self) -> int:
        return self._startByte

    @property
    def endByte(self) -> int:
        return self._endByte

    @property
    def children(self) -> List:
        return self._children

    @property
    def index(self) -> int:
        """Relative index of current object in its parent's children list"""
        if self.parent is None:
            return 0
        if not hasattr(self, "_index_cached"):
            return self.parent.indexOf(self)
        else:
            return self._index_cached

    # def indexOf(self, child):
    #     """Return the index of a child in its parent's children list"""
    #     result = -1
    #     for i, c in enumerate(self.children):
    #         if c is child:
    #             result = i
    #         else:
    #             c.index = i
    #     if result == -1:
    #         raise ValueError(f"{child} not found in {self.children}'s children")
    #     return result

    @property
    def log(self) -> "Any":
        """Shortcut reference to root interface, result is cached to avoid repeated resolves"""
        if not hasattr(self, "_log_cached") or self._log_cached.file is None:
            ref = self.parent
            if ref is None:
                return self
            while ref.parent is not None:
                ref = ref.parent
            self._log_cached = ref
        return self._log_cached

    # IO using pickle
    def pickleDump(self):
        print(f"Pickling {self.picklePath}")
        os.makedirs(self.picklePath.parent, exist_ok=True)
        pickle.dump(self, open(self.picklePath, "wb"))
        print("finished pickling")

    def pickleLoad(self):
        self.__setstate__(pickle.load(open(self.picklePath, "rb")).__dict__)

    def __getstate__(self):
        state = {}
        for key, value in self.__dict__.items():
            if key.endswith("_cached"):
                continue
            state[key] = value
        return state

    def __setstate__(self, state) -> None:
        self.__dict__.update(state)
        if hasattr(self, "_children"):
            for idx in range(len(self._children)):
                if isinstance(self._children[idx], LogInterfaceBase):
                    self._children[idx]._parent = self

    #
    def parseBytes(self):
        """Parse bytes hierarchically, leaf interface will override this method"""
        for i in self.children:
            i.parseBytes()

    def freeMem(self) -> None:
        """free memory hierarchically, leaf interface will override this method"""
        for i in self.children:
            i.freeMem()
