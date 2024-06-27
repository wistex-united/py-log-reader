import functools
import json
from abc import ABC, abstractmethod
from mmap import mmap
from pathlib import Path
from typing import Any, List, Union

import numpy as np

from StreamUtils import StreamUtil
from Utils import SpecialEncoder

IndexMap = Union[range, List[int], np.ndarray]


class LogInterfaceBaseClass(ABC):
    strIndent: int = 2

    frameIdxFileName: str = "frameIndexFile.cache"
    messageIdxByteLength: int = 32
    frameIdxByteLength: int = 32

    def __init__(self):
        super().__init__()

    @property
    @abstractmethod
    def children(self) -> List["LogInterfaceBaseClass"]:
        """Helper property used for __iter__ and __len__, it should be set to child objects that makes sense"""

    @abstractmethod
    def eval(self, sutil: StreamUtil, offset: int = 0):
        """Only read necessary bytes to calculate the start and end byte of this object, create the hierarchical tree but does not parse bytes into data classes"""

    def __getitem__(self, key) -> Any:
        """Allow to use [<attribute name>] to access attributes"""
        return self.__getattribute__(key)

    def __iter__(self):
        return self.children.__iter__()

    def __len__(self):
        return len(self.children)

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

    # Recursive Methods
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

    def parseBytes(self):
        """Parse bytes hierarchically, leaf interface will override this method"""
        for i in self.children:
            i.parseBytes()

    def freeMem(self) -> None:
        """free memory hierarchically, leaf interface will override this method"""
        for i in self.children:
            i.freeMem()

    @property
    @abstractmethod
    def isInstanceClass(self) -> bool:
        pass

    @property
    @abstractmethod
    def isAccessorClass(self) -> bool:
        pass

    @abstractmethod
    def freeze(self) -> None:
        """Freeze the object's absIndex (if it is a iterator, it won't be able to move anymore)"""
