import functools
import io
from mmap import mmap
from typing import Any, List, Tuple, Union

import tqdm

from Primitive import *

StreamAble = Union[mmap, io.BytesIO, bytes]
"""Type acceptable to init a StreamUtil"""
ReadInstruction = Tuple[Union[PrimitiveTypeHint, List[Tuple]], int]
"""
A single read instruction is a tuple of (type, length);
type: 
    - Single Type Indicator or 
    - Nested Sub-ReadInstruction or
    - List of Sub-ReadInstruction
length: 
    - 1: Read a single value
    - -1: First read a UInt to determine the array length then read the array, else read the array with the given length
    - >1: Read an array with the given length
TODO: It should be an recursive Type, but I don't want to use python __future__.annotation to support it.
"""


class StreamUtil:
    """Suggestion: if you assign a stream to StreamUtil, you should not use it elsewhere"""

    def __init__(self, stream: StreamAble, showProgress=False, desc="Streaming"):
        """
        stream: Can be any type of object that supports read(), seek(), tell(), close(), but it might trigger warnings from StreamAble type check
        """
        self._stream: Union[mmap, io.BytesIO]
        self._pbar: tqdm.tqdm

        self.numReadedBytes: int

        if isinstance(stream, bytes):
            self._stream = io.BytesIO(stream)
        else:  # it's a mmap or io.BytesIO
            self._stream = stream  # type: ignore
        self._pbar = tqdm.tqdm(
            total=self.size(),
            unit_scale=True,
            unit_divisor=1024,
            position=0,
            desc=desc,
            disable=not showProgress,
        )
        self.numReadedBytes = 0

    # Basic Stream Methods
    @property
    def stream(self) -> Union[mmap, io.BytesIO]:
        if self._stream is not None:
            return self._stream
        else:
            raise ValueError("Stream not set")

    def read(self, numBytes):
        if self.remainSize() < numBytes:
            raise EOFError("Not enough data to read")
        result = self.stream.read(numBytes)
        self.numReadedBytes += numBytes
        self._pbar.update(numBytes)
        return result

    def tell(self) -> int:
        """Current position of StreamUtil's stream"""
        return self.stream.tell()

    def seek(self, offset, whence=0) -> None:
        origin=self.tell()
        self.stream.seek(offset, whence)
        if whence == io.SEEK_CUR:
            self._pbar.update(offset)
        elif whence == io.SEEK_SET:
            self._pbar.update(offset - origin)
    def size(self) -> int:
        """Total size of StreamUtil's stream"""
        if isinstance(self.stream, io.BytesIO):
            return len(self.stream.getvalue())
        elif isinstance(self.stream, mmap):
            return self.stream.size()
        else:
            pos = self.tell()
            self.seek(0, io.SEEK_END)
            size = self.tell()
            self.seek(pos)
            return size

    def getValue(self) -> bytes:
        """All bytes of StreamUtil's stream"""
        if isinstance(self.stream, io.BytesIO):
            return self.stream.getvalue()
        elif isinstance(self.stream, mmap):
            return self.stream[:]
        else:
            try:
                currentPos = self.tell()
                self.seek(0)
                result = self.read(self.size())
                self.seek(currentPos)
                return result
            except:
                raise ValueError("Stream not supported")

    def remainSize(self) -> int:
        return self.size() - self.tell()

    def atEnd(self):
        pos = self.tell()
        self.stream.seek(0, io.SEEK_END)
        if self.tell() == pos:
            return True
        else:
            self.stream.seek(pos)
            return False

    def probe(self, numBytes) -> bytes:
        """Read some bytes without moving the cursor"""
        result = self.read(numBytes)
        self.seek(-numBytes, io.SEEK_CUR)
        return result

    def close(self):
        self.stream.close()

    # Read Primitives (Core function)
    def readPrimitives(self, typeIndicator: PrimitiveTypeHint, length: int = 1) -> Any:
        """
        General function to read all primitives
        typeIndicator: String (C format or np format) or Type format of target primitive, e.g. 'unsigned char' = 'UChar' = np.uint8 = UChar, will read an np.uint8 value
        length: Number of primitives to read, ==1 will return single value, ==-1 will first read a UInt to determine the array length then read the array, else read the array with the given length
        """
        type = Indicator2RealType[typeIndicator]
        originalLength = length

        if type is Angle:
            return self.readAngle(originalLength)
        elif type is Str:
            return self.readStr(originalLength)
        else:
            if length == -1:
                length = np.frombuffer(self.read(self.getSize(UInt)), np.uint32)[0]

            numBytes = self.getSize(type) * length
            result = np.frombuffer(self.read(numBytes), type)

        if originalLength == 1:
            result = result[0]
        return result

    def readAngle(self, length=1) -> Any:
        """Special function for reading Angle Non-Numpy Primitive"""
        if length == 1:
            return Angle(self.readPrimitives(Float))
        elif length == -1:
            length = self.readPrimitives(UInt)

        return [Angle(self.readPrimitives(Float)) for _ in range(length)]

    def readStr(self, length=1) -> Any:
        """Special function for reading Str Non-Numpy Primitive"""
        if length == 1:
            return self.read(self.readPrimitives(UInt)).decode("ascii")
        elif length == -1:
            length = self.readPrimitives(UInt)

        return [
            self.read(self.readPrimitives(UInt)).decode("ascii") for _ in range(length)
        ]

    def processReadInstructions(self, Instructions) -> Any:
        """
        Single instruction: (type,length);
        Multiple instructions: [(type,length), ...];
        Nested instructions: [(<sub instruction(s)>,length), ...];
        """
        singleInstruction = False
        if isinstance(Instructions, tuple):
            singleInstruction = True
            Instructions = [Instructions]

        result = []
        for instruction in Instructions:
            length = instruction[1]
            resultIsList = length != 1

            if length == -1:
                length = self.readUInt()
            if self.isHashable(instruction[0]) and instruction[0] in Indicator2RealType:
                result.append(self.readPrimitives(instruction[0], length))
            elif not isinstance(instruction[0], str):
                if resultIsList:
                    val = []
                    for _ in range(length):
                        val.append(
                            self.processReadInstructions(
                                instruction[0]
                            )  #  type: ignore
                        )
                    result.append(val)
                else:
                    result.append(
                        self.processReadInstructions(instruction[0])  #  type: ignore
                    )
        if singleInstruction:
            result = result[0]
        return result

    # Specific read functions, read a specific pattern of bytes
    def readQueueHeader(self) -> tuple[UInt, UInt, UInt]:
        """(high, messages, low)"""
        all = self.readLInt()
        low = UInt(all & 0xFFFFFFFF)
        messages = UInt((all >> (32)) & 0xFFFFFFF)
        high = UInt((all >> (32 + 28)) & 0xF)
        return (high, messages, low)

    def readMessageHeader(self) -> tuple[UChar, UInt]:
        """(messageId, messageSize)"""
        header = self.read(4)
        messageId = int.from_bytes(header[0:1], "little")
        messageSize = int.from_bytes(header[1:4], "little")
        return UChar(messageId), UInt(messageSize)

    # Shortcut functions

    # TODO: the real type should be Union[type|List[type]], but it cause too many warnings, so I set all return types to Any

    def readUInt(self, length=1) -> Any:
        return self.readPrimitives(UInt, length)

    def readInt(self, length=1) -> Any:
        return self.readPrimitives(Int, length)

    def readUChar(self, length=1) -> Any:
        return self.readPrimitives(UChar, length)

    def readChar(self, length=1) -> Any:
        return self.readPrimitives(Char, length)

    def readBool(self, length=1) -> Any:
        return self.readPrimitives(Bool, length)

    def readUShort(self, length=1) -> Any:
        return self.readPrimitives(UShort, length)

    def readShort(self, length=1) -> Any:
        return self.readPrimitives(Short, length)

    def readDouble(self, length=1) -> Any:
        return self.readPrimitives(Double, length)

    def readFloat(self, length=1) -> Any:
        return self.readPrimitives(Float, length)

    def readSChar(self, length=1) -> Any:
        return self.readPrimitives(SChar, length)

    def readLInt(self, length=1) -> Any:
        """
        Special read function for long int (size_t in c++)
        Since long int is not in primitives, this function does not call readPrimitives()
        """
        return np.frombuffer(self.read(length * np.dtype(np.int64).itemsize), np.int64)[
            0
        ]

    # Helper functions
    def isHashable(self, obj) -> bool:
        """Check if an object is hashable"""
        try:
            hash(obj)
            return True
        except TypeError:
            return False

    def getSize(self, type: PrimitiveTypeHint) -> int:
        """Get size of a ReadAble type"""
        if type is Str:
            return 1
        elif type is Angle:
            return np.dtype(Float).itemsize
        elif type in NPPrimitiveTypeList:
            return np.dtype(type).itemsize
        else:
            raise ValueError(f"Unsupported type: {type}")

    def printTQDM(self):
        self._pbar = tqdm.tqdm(total=self.size(), unit_scale=True, unit_divisor=1024)
        self._pbar.reset()
