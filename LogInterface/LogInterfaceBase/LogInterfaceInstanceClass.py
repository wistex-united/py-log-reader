import os
import pickle
from typing import Any, List, Union

from .LogInterfaceBase import LogInterfaceBaseClass


class LogInterfaceInstanceClass(LogInterfaceBaseClass):
    def __init__(self, parent):
        super().__init__()
        self._parent = parent

        # available after eval
        self._startByte: int
        self._endByte: int
        self._children: Union[List, "LogInterfaceBaseClass"]

        # cached references
        self._log_cached: Any  # type: ignore TODO: add a forward reference to Log
        self._index_cached: int
        self._absIndex_cached: int

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
    def children(self) -> Union[List, LogInterfaceBaseClass]:
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
                if isinstance(self._children[idx], LogInterfaceBaseClass):
                    self._children[idx]._parent = self
