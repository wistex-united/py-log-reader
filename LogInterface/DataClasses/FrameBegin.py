from StreamUtils import StreamAble, StreamUtil

from .DataClass import DataClass


class FrameBegin(DataClass):
    readOrder = ["threadName"]
    attributeCtype = {"threadName": "std::string"}

    def __init__(self):
        super().__init__()
        self.threadName: str

    @classmethod
    def read(cls, stream: StreamAble):
        sutil = StreamUtil(stream)
        instance = cls()
        setattr(instance, "threadName", sutil.readPrimitives(str))
        return instance

    def asDict(self):
        return {"threadName": self.threadName}
