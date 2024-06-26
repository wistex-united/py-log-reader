from StreamUtils import StreamAble, StreamUtil

from .DataClass import DataClass


class FrameBegin(DataClass):
    """The message class that always appear at the beginning of a frame"""
    readOrder = ["threadName"]
    attributeCtype = {"threadName": "std::string"}

    def __init__(self):
        super().__init__()
        self.threadName: str

    @classmethod
    def read(cls, sutil: StreamUtil, end: int):
        instance = cls()
        setattr(instance, "threadName", sutil.readPrimitives(str))
        return instance

    def asDict(self):
        return {"threadName": self.threadName}
