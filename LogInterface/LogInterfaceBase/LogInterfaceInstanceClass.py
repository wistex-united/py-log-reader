import os
import pickle
import types
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

    @parent.setter
    def parent(self, value: Any):
        self._parent = value

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
            for i, c in enumerate(self.parent.children):
                c._index_cached = i
        return self._index_cached

    @property
    def log(self) -> "Any":
        """Shortcut reference to root interface, result is cached to avoid repeated resolves"""
        if not hasattr(self, "_log_cached") or self._log_cached.file is None:
            ref = self.parent
            if ref is None:
                return self
            while hasattr(ref, "parent") and ref.parent is not None:
                ref = ref.parent
            self._log_cached = ref
        return self._log_cached

    # IO using pickle
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

    # def __setstate__(self, state) -> None:
    #     self.__dict__.update(state)
    #     if hasattr(self, "_children"):
    #         for idx in range(len(self._children)):
    #             if isinstance(self._children[idx], LogInterfaceBaseClass):
    #                 self._children[idx]._parent = self
    def __setstate__(self, state) -> None:
        self.__dict__.update(state)

        if hasattr(self, "_children"):
            for idx in range(len(self._children)):
                child = self._children[idx]
                if child.isInstanceClass:
                    child.parent = self
                elif child.isAccessorClass:
                    self._children.log = self
                    break  # Since there's only one accessor, we just need to set it once
                else:
                    pass  # Some strange child, it might not have parent field and don't need to be set

    @property
    def isInstanceClass(self) -> bool:
        return True

    @property
    def isAccessorClass(self) -> bool:
        return False

    def freeze(self) -> None:
        """Currently the instance class is freezed by default"""
        pass
