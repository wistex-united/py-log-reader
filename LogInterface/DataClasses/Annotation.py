import shlex

import numpy as np

from StreamUtils import StreamUtil

from .DataClass import DataClass


class Annotation(DataClass):
    readOrder = ["annotationNumber", "frame", "name", "annotation"]
    attributeCtype = {
        "annotationNumber": "unsigned int",
        "frame": "unsigned int",
        "name": "std::string",
        "annotation": "std::string",
    }

    def __init__(self):
        super().__init__()
        self.annotationNumber: int
        self.frame: int
        self.name: str
        self.annotation: str

    @classmethod
    def read(cls, sutil: StreamUtil, end: int):
        instance = cls()
        instance.annotationNumber = sutil.readUInt()
        if not (instance.annotationNumber & 0x80000000):
            instance.frame = sutil.readUInt()
        size = end - sutil.tell()
        inputBytes = sutil.read(size)
        lex = shlex.shlex(inputBytes.decode("ascii"))
        lex.whitespace_split = True
        strings = list(lex)
        instance.name = strings.pop(0)
        instance.annotation = " ".join(strings)
        instance.annotationNumber = np.int32(np.int64(~0x80000000) & np.int64(instance.annotationNumber))
        return instance

    def asDict(self):
        return {
            "annotationNumber": self.annotationNumber,
            "frame": self.frame,
            "name": self.name,
            "annotation": self.annotation,
        }