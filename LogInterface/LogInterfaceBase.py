from abc import ABC, abstractmethod
import copy
import json
from mmap import mmap
import os
from pathlib import Path
import pickle
from typing import Any, Dict, List

from StreamUtils import StreamUtil
from Utils import SpecialEncoder


class LogInterfaceBase(ABC):
    strIndent: int = 2

    def __init__(self, parent):
        super().__init__()
        self._parent = parent

        # available after eval
        self._startByte: int
        self._endByte: int
        self._children: List

        # cached references
        self._log_cached: "Log"  # type: ignore TODO: add a forward reference to Log
        self._index_cached: int

    @property
    @abstractmethod
    def children(self) -> List["LogInterfaceBase"]:
        """Helper property used for __iter__ and __len__, it should be set to child objects that makes sense"""
        pass

    @abstractmethod
    def eval(self, sutil: StreamUtil, offset: int = 0):
        """Only read necessary bytes to calculate the start and end byte of this object, create the hierarchical tree but does not parse bytes into data classes"""
        pass

    def parseBytes(self):
        """Parse bytes hierarchically, leaf interface will override this method"""
        for i in self.children:
            i.parseBytes()

    def freeMem(self) -> None:
        """free memory hierarchically, leaf interface will override this method"""
        for i in self.children:
            i.freeMem()

    def __getitem__(self, key) -> Any:
        """Allow to use [<attribute name>] to access attributes"""
        return self.__getattribute__(key)

    def __iter__(self):
        return self.children.__iter__()

    def __len__(self):
        return self.children.__len__()

    def __str__(self) -> str:
        return json.dumps(self.asDict(), indent=self.strIndent, cls=SpecialEncoder)

    def pickleDump(self):
        os.makedirs(self.picklePath.parent, exist_ok=True)
        pickle.dump(self, open(self.picklePath, "wb"))

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
                if hasattr(self._children[idx], "_parent"):
                    self._children[idx]._parent = self

    @property
    def startByte(self) -> int:
        return self._startByte

    @property
    def endByte(self) -> int:
        return self._endByte

    @property
    def logBytes(self) -> mmap:
        """Shortcut reference to root's logBytes"""
        return self.log.logBytes

    @property
    def parent(self):
        return self._parent

    @property
    def size(self) -> int:
        return self.endByte - self.startByte

    @property
    def log(self) -> "LogInterfaceBase":
        """Shortcut reference to root interface, result is cached to avoid repeated resolves"""
        if not hasattr(self, "_log") or self._log_cached.file is None:
            ref = self.parent
            if ref is None:
                return self
            while ref.parent is not None:
                ref = ref.parent
            self._log_cached = ref
        return self._log_cached

    @property
    def logFilePath(self) -> str:
        return self.log.logFilePath

    @property
    def index(self) -> int:
        """Relative index of current object in its parent's children list"""
        if self.parent is None:
            return 0
        if not hasattr(self, "_index"):
            return self.parent.indexOf(self)
        else:
            return self._index_cached

    @property
    @abstractmethod
    def picklePath(self) -> Path:
        pass

    def indexOf(self, child):
        """Return the index of a child in its parent's children list"""
        for i, c in enumerate(self.children):
            if c is child:
                return i
        raise ValueError(f"{child} not found in {self.children}'s children")

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
