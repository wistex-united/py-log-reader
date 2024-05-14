from abc import ABC, ABCMeta, abstractmethod
from enum import EnumMeta
import json
import re
from typing import Any, Dict, List, Tuple
import numpy as np

from StreamUtils import ReadInstruction, StreamUtil


class DataClassEncoder(json.JSONEncoder):
    """Special json encoder for DataClass"""

    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, ABCMeta):
            return f"Class Type: {obj.__name__}"
        elif isinstance(obj, EnumMeta):
            return f"Enum Type: {obj.__name__}"
        else:
            return str(obj)


class DataClass:
    readOrder: List[str]
    attributeCtype: Dict[str, str]
    strIndent = 2

    readInstructions: List[ReadInstruction]
    def __init__(self):
        pass

    @abstractmethod
    def asDict(self) -> Dict[str, Any]:
        pass

    def __str__(self):
        result = json.dumps(self.asDict(), indent=self.strIndent, cls=DataClassEncoder)
        return result
    @classmethod
    @abstractmethod
    def read(cls, sutil: StreamUtil) -> "DataClass":
        pass
    """Deprecated: Instantiation method on dynamicly generated classes"""

    def __getitem__(self, key):
        return getattr(self, key)
    # @classmethod
    # def read(cls, sutil: StreamUtil) -> "DataClass":
    #     Instructions = cls.getReadInstructions()
    #     result = sutil.processReadInstructions(Instructions)
    #     instance = cls.distributeReadResult(result)
    #     return instance

    # @classmethod
    # @abstractmethod
    # def getReadInstructions(cls) -> List[ReadInstruction]:
    #     pass

    # @classmethod
    # @abstractmethod
    # def distributeReadResult(cls, result) -> "DataClass":
    #     pass

