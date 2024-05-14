import shlex
from Primitive import *
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
    def read(cls, sutil: StreamUtil):
        instance = cls()
        instance.annotationNumber = sutil.readUInt()
        if not (instance.annotationNumber & 0x80000000):
            instance.frame = sutil.readUInt()
        size = sutil.size() - sutil.tell()
        lex = shlex.shlex(sutil.read(size).decode("ascii"))
        lex.whitespace_split = True
        strings = list(lex)
        instance.name = strings.pop(0)
        instance.annotation = " ".join(strings)
        instance.annotationNumber &= ~0x80000000
        return instance

    def asDict(self):
        return {
            "annotationNumber": self.annotationNumber,
            "frame": self.frame,
            "name": self.name,
            "annotation": self.annotation,
        }

    """Deprecated: Instantiation method on dynamicly generated classes"""
    # @classmethod
    # def getReadInstructions(cls):
    #     """
    #     The way annotation is read is not consistent with other dataclasses, so we cannot use the universal way to read it
    #     This is just a method to satisfy Interface Requirement, don't use it directly
    #     """
    #     # The way annotation is read is not consistent with other dataclasses, so we cannot use the universal way to read it
    #     return [
    #         (UInt, 1),  # annotationNumber
    #         (UInt, 1),  # frame
    #     ]

    # @classmethod
    # def distributeReadResult(cls, result) -> "Annotation":
    #     """
    #     The way annotation is read is not consistent with other dataclasses, so we cannot use the universal way to read it
    #     This is just a method to satisfy Interface Requirement, don't use it directly
    #     """
    #     instance = cls()
    #     instance.annotationNumber = result[0]
    #     instance.frame = result[1]
    #     return instance
