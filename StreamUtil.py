import ctypes
import io
import mmap
import re
import struct
from typing import Any, Dict, Optional, Union
from Angle import Angle

SIZE_OF_UINT = ctypes.sizeof(ctypes.c_uint)
SIZE_OF_INT = ctypes.sizeof(ctypes.c_int)
SIZE_OF_FLOAT = ctypes.sizeof(ctypes.c_float)
SIZE_OF_DOUBLE = ctypes.sizeof(ctypes.c_double)
SIZE_OF_SIZE_T = ctypes.sizeof(ctypes.c_size_t)

StreamAble = Union[io.BytesIO, mmap.mmap]


class StreamUtil:
    def __init__(self, stream: Optional[StreamAble] = None):
        self._stream = stream
        self.additionalSupportedClasses = {}
        self.additionalSupportedEnums = {}
        self.numReadedBytes = 0
        self.type_to_method = {
            "unsigned int": self.readUInt,
            "int": self.readInt,
            "unsigned char": self.readUChar,
            "char": self.readChar,
            "std::string": self.readStr,
            "bool": self.readBool,
            "unsigned short": self.readUShort,
            "short": self.readShort,
            "double": self.readDouble,
            "float": self.readFloat,
            "signed char": self.readSChar,
            "Angle": self.readAngle,
            # Add any other type mappings as needed
        }

    def provideAdditionalSupportedClasses(self, types: Dict[str, Any]):
        self.additionalSupportedClasses = types

    def provideAdditionalSupportedEnums(self, types: Dict[str, Any]):
        self.additionalSupportedEnums = types

    @property
    def stream(self) -> StreamAble:
        if self._stream is not None:
            return self._stream
        else:
            raise ValueError("Stream not set")

    def tell(self):
        return self.stream.tell()

    def seek(self, offset, whence=0):
        return self.stream.seek(offset, whence)

    def size(self):
        pos = self.tell()
        self.seek(0, io.SEEK_END)
        size = self.tell()
        self.seek(pos)
        return size

    def getValue(self):
        if isinstance(self.stream, io.BytesIO):
            return self.stream.getvalue()
        elif isinstance(self.stream, mmap.mmap):
            return self.stream[:]
        else:
            raise ValueError("Stream not supported")

    def atEnd(self):
        pos = self.tell()
        self.seek(0, io.SEEK_END)
        if self.tell() == pos:
            return True
        else:
            self.seek(pos)
            return False

    def read(self, numBytes):
        return self.stream.read(numBytes)

    def close(self):
        self.stream.close()

    def readUInt(
        self, stream: Optional[StreamAble] = None, numBytes=SIZE_OF_UINT
    ) -> int:
        if stream is None:
            stream = self.stream
        self.numReadedBytes += numBytes
        return int.from_bytes(stream.read(numBytes), "little", signed=False)

    def readInt(self, stream: Optional[StreamAble] = None, numBytes=SIZE_OF_INT) -> int:
        if stream is None:
            stream = self.stream
        self.numReadedBytes += numBytes
        return int.from_bytes(stream.read(numBytes), "little", signed=True)

    def readUChar(self, stream: Optional[StreamAble] = None) -> int:
        if stream is None:
            stream = self.stream
        self.numReadedBytes += 1
        return self.stream.read(1)[0]

    def readAngle(self, stream: Optional[StreamAble] = None) -> Angle:
        return Angle(self.readFloat(stream=stream))

    def readChar(self, stream: Optional[StreamAble] = None) -> str:
        return chr(self.readUChar(stream=stream))

    def readStr(self, stream: Optional[StreamAble] = None) -> str:
        if stream is None:
            stream = self.stream
        str_len = self.readUInt(stream=stream)
        self.numReadedBytes += str_len
        return stream.read(str_len).decode("utf-8")

    def readBool(self, stream: Optional[StreamAble] = None) -> bool:
        val = self.readUChar(stream=stream)
        if val == 0:
            return False
        elif val == 1:
            return True
        else:
            raise ValueError("Invalid value for bool")

    def readUShort(self, stream: Optional[StreamAble] = None) -> int:
        return self.readUInt(stream=stream, numBytes=2)

    def readShort(self, stream: Optional[StreamAble] = None) -> int:
        return self.readInt(stream=stream, numBytes=2)

    def readDouble(
        self, stream: Optional[StreamAble] = None, numBytes=SIZE_OF_DOUBLE
    ) -> float:
        if stream is None:
            stream = self.stream
        self.numReadedBytes += numBytes
        double_bytes = stream.read(numBytes)
        result = struct.unpack("<d", double_bytes)[0]
        return result

    def readFloat(
        self, stream: Optional[StreamAble] = None, numBytes=SIZE_OF_FLOAT
    ) -> float:
        if stream is None:
            stream = self.stream
        self.numReadedBytes += numBytes
        float_bytes = stream.read(numBytes)
        result = struct.unpack("<f", float_bytes)[0]
        return result

    def readSChar(self, stream: Optional[StreamAble] = None) -> int:
        raise NotImplementedError("readSChar not implemented")
        self.numReadedBytes += numBytes
        result = self.readUChar(stream=stream)
        return result - 0x100 if result > 127 else result

    def readSingle(self, type_str: str, stream: Optional[StreamAble] = None):
        if type_str in self.type_to_method:  # Read primitive
            read_method = self.type_to_method[type_str]
            result = read_method(stream=stream)
        elif type_str in self.additionalSupportedEnums:
            result = self.additionalSupportedEnums[type_str](
                self.readUChar(stream=stream)
            )
        elif type_str in self.additionalSupportedClasses:
            val = self.additionalSupportedClasses[type_str].read(stream=stream)
            result = val
        else:
            raise ValueError(f"Unsupported type: {type_str}")
        return result

    def readArray(self, type_str: str, size: int, stream: Optional[StreamAble] = None):
        result = []
        for i in range(size):
            result.append(self.readSingle(type_str, stream=stream))
        return result

    def readAny(self, type_str: str, stream: Optional[StreamAble] = None) -> object:
        if stream is None:
            stream = self.stream
        # Mapping from type strings to the actual read methods of this class

        result = None

        if re.search(r"\[\d+\]$", type_str) is not None:
            type_str_fixed_size = re.sub(r"\[\d+\]$", "", type_str)
            t_str = type_str.rsplit("[")[0]
            size = int(type_str.rsplit("[")[1][:-1])
            result = self.readArray(type_str=t_str, size=size, stream=stream)
        elif type_str.endswith("*"):
            t_str = type_str[:-1]
            size = self.readUInt()
            result = self.readArray(type_str=t_str, size=size, stream=stream)
        else:
            result = self.readSingle(type_str, stream=stream)

        return result

    def readSizeT(
        self, stream: Optional[StreamAble] = None, numBytes=SIZE_OF_SIZE_T
    ) -> int:
        if stream is None:
            stream = self.stream
        return int.from_bytes(stream.read(numBytes), "little", signed=False)

    def readQueueHeader(self, stream: Optional[StreamAble] = None) -> Dict[str, int]:
        if stream is None:
            stream = self.stream
        all = self.readSizeT(stream=stream)
        low = all & 0xFFFFFFFF
        messages = (all >> (32)) & 0xFFFFFFF
        high = (all >> (32 + 28)) & 0xF
        return {
            "low": low,
            "messages": messages,
            "high": high,
        }
