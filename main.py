from ast import Not
import io
import json
import os
from pathlib import Path
import struct
from typing import Any, Dict, List, Tuple
from enum import Enum, auto
import re
from enum import Enum
from CameraImage import CameraImage

from messageID import MessageID
from MemoryMappedFile import MemoryMappedFile
from StreamUtil import StreamUtil, StreamAble
from Log import Log

unifiedTypeNames = 0x80000000
indexVersion = 2


class LogFileChunk(Enum):
    logFileUncompressed = 0
    logFileCompressed = auto()
    logFileMessageIDs = auto()
    logFileTypeInfo = auto()
    logFileSettings = auto()
    logFileIndices = auto()


class LogFileReader:
    def __init__(self):
        self.settings: Dict = {}  # Holds various settings read from the log file
        self.logIDNames: Dict = {}  # Maps log ID names to their values
        self.log_file_path: str = ""  # Path to the log file being read
        self.mapNameToID: Dict = {}  # Maps names to IDs for messages
        self.mapLogToID: Dict = {}  # Maps log IDs to their corresponding message IDs
        self.mapIDToLog: Dict = {}  # Maps message IDs back to log IDs
        self.typeInfo: Dict = {
            "primitives": set(),
            "classes": {},
            "enums": {},
        }  # Detailed type info storage
        self.frameIndex: List = []  # Indexes of frames in the log file
        self.framesHaveImage: List = []  # Indicates which frames have images
        self.anyFrameHasImage: bool = (
            False  # Flag to indicate if any frame has an image
        )
        self.statsPerThread: Dict[str, Dict[int, Tuple[int, int]]] = {}
        """{threadName: (size_t messageFrequency, size_t storageSize)} How often is each message id present in each thread and how much space is used?"""

        self.annotationsPerThread: Dict[
            str, List[Tuple[int, int, str, str]]
        ] = {}  # (annotationNumber, frame, name, annotation) Annotations per thread
        self.sizeWhenIndexWasComputed: int = 0

        self.buffer: bytes = b""
        self.messageSlices: List = []
        # self.used = 0 # We use self.messageSlices to indicate all valid records, self.used = self.messageSlices[-1][2] if self.messageSlices else 0
        self.parsedMessages: Dict = {}
        self.enum_types: Dict = {}
        self.class_types: Dict = {}

    def open(self, log_file_path):
        self.log_file_path = log_file_path
        self.read_chunk()

    def read_settings(self, stream):
        sutil = StreamUtil(stream)
        version = sutil.readUInt()
        if version == 1:
            self.settings["headName"] = sutil.readStr()
            self.settings["bodyName"] = sutil.readStr()
            self.settings["playerNumber"] = sutil.readInt()
            self.settings["location"] = sutil.readStr()
            self.settings["scenario"] = sutil.readStr()
        print(self.settings)

    def read_message_ids(self, stream):
        sutil = StreamUtil(stream)

        logIDNames_size = sutil.readUChar()
        print(f"logIDNames Size: {logIDNames_size}")
        self.mapNameToID = {mid.name: mid.value for mid in MessageID}
        self.mapNameToID["idProcessBegin"] = MessageID.idFrameBegin.value
        self.mapNameToID["idProcessFinished"] = MessageID.idFrameFinished.value
        self.mapLogToID = {}
        self.mapIDToLog = {}

        for id in range(logIDNames_size):
            self.logIDNames[id] = sutil.readStr()
            if self.logIDNames[id] in self.mapNameToID:
                self.mapLogToID[id] = self.mapNameToID[self.logIDNames[id]]
                self.mapIDToLog[self.mapNameToID[self.logIDNames[id]]] = id
            else:
                self.mapLogToID[id] = MessageID.undefined.value
        print(self.mapLogToID)

    def read_type_info(self, stream):
        sutil = StreamUtil(stream)

        self.typeInfo["primitives"].clear()
        self.typeInfo["classes"].clear()
        self.typeInfo["enums"].clear()

        size = sutil.readUInt()
        needsTypenameUnification = unifiedTypeNames & size == 0
        size = size & ~unifiedTypeNames
        print(f"Primitives num: {size}")
        for _ in range(size, 0, -1):
            type = sutil.readStr()
            type = self.demangle(type) if needsTypenameUnification else type

            self.typeInfo["primitives"].add(type)

        size = sutil.readUInt()
        print(f"Classes num: {size}")
        for _ in range(size, 0, -1):
            type = sutil.readStr()
            type = self.demangle(type) if needsTypenameUnification else type

            size2 = sutil.readUInt()
            self.typeInfo["classes"][type] = []
            attributes = self.typeInfo["classes"][type]
            for _ in range(size2, 0, -1):
                name = sutil.readStr()
                type2 = sutil.readStr()
                type2 = self.demangle(type2) if needsTypenameUnification else type2
                attributes.append((name, type2))
            if len(self.typeInfo["classes"][type]) != size2:
                raise Exception(
                    f"Expected {size2} attributes for class {type}, but got {len(self.typeInfo['classes'][type])}"
                )
        if len(self.typeInfo["classes"]) != size:
            raise Exception(
                f"Expected {size} classes, but got {len(self.typeInfo['classes'])}"
            )

        size = sutil.readUInt()
        print(f"Enums num: {size}")
        for _ in range(size, 0, -1):
            type = sutil.readStr()
            type = self.demangle(type) if needsTypenameUnification else type

            size2 = sutil.readUInt()
            self.typeInfo["enums"][type] = []
            constants = self.typeInfo["enums"][type]
            for _ in range(size2, 0, -1):
                name = sutil.readStr()
                constants.append(name)
            if len(self.typeInfo["enums"][type]) != size2:
                raise Exception(
                    f"Expected {size2} attributes for enum {type}, but got {len(self.typeInfo['enums'][type])}"
                )
        if len(self.typeInfo["enums"]) != size:
            raise Exception(
                f"Expected {size} enums, but got {len(self.typeInfo['enums'])}"
            )
        # for i in range(self.readUInt()):
        print(self.typeInfo)

    def read_uncompressed(self, stream, file):
        sutil = StreamUtil(stream)

        header = sutil.readQueueHeader()
        usedSize = header["low"] | header["high"] << 32
        print("UsedSize size: ", usedSize)
        position = sutil.tell()
        total_Size = MemoryMappedFile(self.log_file_path).getSize()
        remainingSize = total_Size - position
        print("Remaining size: ", remainingSize)
        hasIndex = header["messages"] != 0x0FFFFFFF and usedSize != remainingSize

        if hasIndex:
            sutil.seek(usedSize, 0)
            if not self.readIndices(usedSize, sutil.stream):
                sutil.close()
                with open(self.log_file_path, "rb+") as f:
                    f.truncate(position + usedSize)
                remainingSize = usedSize
                hasIndex = False
                file = MemoryMappedFile(self.log_file_path)
        if not hasIndex:  # No index -> create one
            if header["messages"] == 0x0FFFFFFF:
                usedSize = remainingSize

            self.setBuffer(file.getData()[position:], usedSize)  # type: ignore
            self.updateIndices()
            usedSize = self.messageSlices[-1][2]

            # We just leave the invalid data in the file
            # with open(self.log_file_path, "r+b") as stream_file:
            #     stream_file.seek(position - 8)
            #     # Simulate writing QueueHeader struct - adjust as needed
            #     queue_header = self.create_queue_header(usedSize)
            #     stream_file.write(queue_header)
            #     stream_file.seek(usedSize, os.SEEK_CUR)
            # Removed:self.writeIndices(stream_file);
            # Since we only need to read the log file, we don't write the indices into the file
            file = MemoryMappedFile(self.log_file_path)

        self.setBuffer(file.getData()[position:], usedSize)  # type: ignore

    # def read_compressed(self):
    #     while True:
    #         if self.stream.tell() == self.stream.size():
    #             break

    #         compressed_size = self.readUInt()

    #         # Read the compressed data based on the size
    #         compressed_data = self.stream.read(compressed_size)
    #         if len(compressed_data) != compressed_size:
    #             break  # Incomplete data

    #         # Decompress the data
    #         decompressed_data = snappy.decompress(compressed_data)

    #         # Process the decompressed data
    #         # Assuming process_decompressed_data does what InBinaryMemory >> *this; does in C++
    #         process_decompressed_data(decompressed_data)

    #     # Assuming updateIndices updates indices after all chunks are processed
    #     updateIndices()

    def setBuffer(self, buffer: bytes, valid_size):
        self.buffer = buffer
        self.messageSlices.clear()

        index = 0
        while index < len(self.buffer):
            id, size = self.phraseMessageHeader(self.buffer[index : index + 4])
            if id > len(MessageID):
                raise Exception(f"Current id not valid:{id} > {len(MessageID)}")
            index += 4
            self.messageSlices.append((id, index, index + size))
            if id == 5:
                folder = "images"
                if not os.path.exists(folder):
                    os.makedirs(folder)
                with open(os.path.join(folder, str(index) + ".yuy2"), "wb") as f:
                    f.write(self.buffer[index : index + size])

            index += size

        if self.messageSlices[-1][2] == valid_size:
            print(
                len(self.messageSlices),
                " messages fill the whole buffer size: ",
                valid_size,
            )
        for idx, message in enumerate(self.messageSlices):
            if message[2] > valid_size:
                self.messageSlices = self.messageSlices[
                    :idx
                ]  # remove messages afterwards (include the current one)

    def read_chunk(self):
        try:
            file = MemoryMappedFile(self.log_file_path)
        except OSError as e:
            return False
        stream = file.getData()
        sutil = StreamUtil(stream)

        while True:
            chunk_identifier = sutil.readUChar()

            print(f"Chunk Identifier: {chunk_identifier}")

            match chunk_identifier:
                case LogFileChunk.logFileSettings.value:
                    self.read_settings(stream)
                case LogFileChunk.logFileMessageIDs.value:
                    self.read_message_ids(stream)
                case LogFileChunk.logFileTypeInfo.value:
                    self.read_type_info(stream)
                case LogFileChunk.logFileUncompressed.value:
                    self.read_uncompressed(stream, file)
                    break
                case LogFileChunk.logFileCompressed.value:
                    raise NotImplementedError("logFileCompressed not implemented")
                    # self.read_compressed(stream)
                    break

    def phraseMessageHeader(self, header: bytes):
        # RISK: Here we assume little endian
        if len(header) != 4:
            raise ValueError("MessageHeader must be 4 bytes")
        messageId = int.from_bytes(header[0:1], "little")
        messageSize = int.from_bytes(header[1:4], "little")
        return messageId, messageSize

    def readIndices(self, usedSize, stream) -> bool:
        sutil = StreamUtil(stream)
        chunk = sutil.readUChar()
        version = sutil.readUChar()
        if (chunk != LogFileChunk.logFileIndices) or (version != indexVersion):
            return False
        usedSize = (sutil.readUInt() << 32) | sutil.readUInt()

        size = sutil.readUInt()
        offsets = []
        for i in range(size):
            offsets.append(sutil.readSizeT())

        self.frameIndex = []
        self.framesHaveImage = []
        self.anyFrameHasImage = False
        for offset in offsets:
            self.frameIndex.append(offset & ~(1 << 63))
            isImageFrame = (offset & (1 << 63)) != 0
            self.framesHaveImage.append(isImageFrame)
            self.anyFrameHasImage = self.anyFrameHasImage or isImageFrame

        self.statsPerThread.clear()
        size = sutil.readUInt()
        for _ in range(size):
            threadName = sutil.readStr()
            statsSize = sutil.readUInt()
            self.statsPerThread[threadName] = {}
            for j in range(statsSize):
                self.statsPerThread[threadName][j] = (
                    sutil.readSizeT(),
                    sutil.readSizeT(),
                )

        self.annotationsPerThread.clear()
        size = sutil.readUInt()
        for _ in range(size):
            threadName = sutil.readStr()
            annotationsSize = sutil.readUInt()
            self.annotationsPerThread[threadName] = []
            annotations = self.annotationsPerThread[threadName]
            for _ in range(annotationsSize):
                annotations.append(
                    (
                        sutil.readUInt(),  # annotationNumber
                        sutil.readUInt(),  # frame
                        sutil.readStr(),  # name
                        sutil.readStr(),  # annotation
                    )
                )

        self.sizeWhenIndexWasComputed = usedSize

        return True

    def updateIndices(self):
        self.frameIndex.clear()
        self.framesHaveImage.clear()
        self.statsPerThread.clear()
        self.annotationsPerThread.clear()

        frame = 0
        hasImage = self.anyFrameHasImage = False
        currentThread = ""
        lastFrame = 0

        for idx, (id, start_index, end_index) in enumerate(self.messageSlices):
            message_bytes = self.buffer[start_index:end_index]
            sutil = StreamUtil(io.BytesIO(message_bytes))

            msg_id = self.logIDNames[id]

            if msg_id == "idFrameBegin":
                frame = idx
                currentThread = sutil.readStr()
                hasImage = False
            elif msg_id == "idFrameFinished":
                if (
                    len(self.frameIndex) != 0 and self.frameIndex[-1] == frame
                ):  # in this case, there's two consecutive idFrameFinished
                    raise RuntimeError("FrameFinished without FrameBegin")
                self.frameIndex.append(frame)
                self.framesHaveImage.append(hasImage)
                lastFrame = idx
            elif msg_id in ["idCameraImage", "idJPEGImage"]:
                hasImage = self.anyFrameHasImage = True
            elif msg_id == "idAnnotation":
                stream = io.BytesIO(message_bytes)
                sutil = StreamUtil(stream)
                annotationNumber = sutil.readUInt()
                if not (annotationNumber & 0x80000000):
                    frame = sutil.readUInt()
                name = sutil.readStr()
                annotation = sutil.readStr()
                annotationNumber = annotationNumber & ~0x80000000
                if sutil.tell() != len(message_bytes):
                    raise ValueError("Invalid buffer size")
                frame = len(self.frameIndex)
                self.annotationsPerThread.setdefault(currentThread, []).append(
                    (
                        annotationNumber,
                        frame,
                        name,
                        annotation,
                    )
                )

            self.statsPerThread.setdefault(currentThread, {})
            stats = self.statsPerThread[currentThread]
            stat = stats.setdefault(id, (0, 0))
            newStat = (stat[0] + 1, stat[1] + len(message_bytes) + 4)
            stats[id] = newStat

        totalSize = sum(
            stat[1]
            for _, stats in self.statsPerThread.items()
            for _, stat in stats.items()
        )
        if totalSize != len(self.buffer):
            raise ValueError(
                "statsPerThread doesn't add up to buffer size. statsPerThread: ",
                totalSize,
                " buffer size: ",
                len(self.buffer),
            )

        if len(self.messageSlices) != lastFrame + 1:
            print(
                f"Some incomplete frames detected, messages after {lastFrame} are removed, aka buffer after {self.messageSlices[lastFrame][2]} are removed"
            )
        # Remove all the frames that are incomplete
        if lastFrame == 0:
            self.messageSlices = []
        else:
            self.messageSlices = self.messageSlices[: lastFrame + 1]

        # Remove all the annotations for frames that are incomplete
        for annotations in self.annotationsPerThread.values():
            while annotations and annotations[-1][1] >= len(
                self.frameIndex
            ):  # annotations[-1][1] is the "frame" field
                annotations.pop()

    def create_queue_header(self, used_size) -> bytes:
        # Ensure used_size fits in 5 bytes
        if used_size >= 2 ** (28 + 32):
            raise ValueError("used_size must be less than 2^40")

        # Extract lower 32 bits (4 bytes)
        lower_part = used_size & 0xFFFFFFFF

        # Extract next 8 bits (1 byte)
        higher_part = (used_size >> 32 + 28) & 0xF

        combined = (higher_part << 32) | lower_part

        # Pack into a 64-bit value, ensuring little endian format if needed
        packed_header = struct.pack("<Q", combined)

        return packed_header

    def extract_annotation_data(self, message_bytes):
        stream = io.BytesIO(message_bytes)
        sutil = StreamUtil(stream)
        annotationNumber = sutil.readUInt()
        if not (annotationNumber & 0x80000000):
            frame = sutil.readUInt()
        else:
            frame = 0
        name = sutil.readStr()
        annotation = sutil.readStr()
        annotationNumber = annotationNumber & ~0x80000000
        if sutil.tell() != len(self.buffer):
            raise ValueError("Invalid buffer size")

        return (
            annotationNumber,
            frame,
            name,
            annotation,
        )

    def demangle(self, type_str):
        # Regular expressions
        matchAnonymousNamespace = re.compile(r"::__1\b")
        matchUnsignedLong = re.compile(r"([0-9][0-9]*)ul\b")
        matchComma = re.compile(r", ")
        matchAngularBracket = re.compile(r" >")
        matchBracket = re.compile(r" \[")
        matchAsterisk = re.compile(r" \*\(\*\)")

        # Replacements
        type_str = re.sub(matchAnonymousNamespace, "", type_str)
        type_str = re.sub(matchUnsignedLong, r"\1", type_str)
        type_str = re.sub(matchComma, ",", type_str)
        type_str = re.sub(matchAngularBracket, ">", type_str)
        type_str = re.sub(matchBracket, "[", type_str)
        type_str = re.sub(matchAsterisk, "", type_str)

        return type_str

    def parseMessage(self, message):
        IDName = self.logIDNames[message[0]]
        message_buffer = self.buffer[message[1] : message[2]]
        className = IDName[2:] if IDName.startswith("id") else IDName
        stream = io.BytesIO(message_buffer)
        sutil = StreamUtil(stream)
        instance = self.class_types[className].read(stream)
        if not sutil.atEnd():
            raise ValueError("Buffer not used up")
        return instance

    def regClassesAndEnums(self):
        self.enum_types.clear()
        self.class_types.clear()

        for enum_name, enum_options in self.typeInfo["enums"].items():
            enum_class = Enum(
                enum_name, {option: idx for idx, option in enumerate(enum_options)}
            )
            # Store it in the dictionary
            self.enum_types[enum_name] = enum_class

        for class_name, log_class_attributes in self.typeInfo["classes"].items():
            class_attributes = {}
            read_order = []
            attribute_type = {}
            for attr_name, type_ in log_class_attributes:
                class_attributes[attr_name] = None
                attribute_type[attr_name] = type_
                read_order.append(attr_name)

            def read(cls, stream: StreamAble):
                sutil = StreamUtil(stream)
                sutil.provideAdditionalSupportedClasses(self.class_types)
                sutil.provideAdditionalSupportedEnums(self.enum_types)
                instance = cls()
                for attr_name in cls.read_order:
                    attr_type: str = cls.attribute_type[attr_name]
                    val = sutil.readAny(attr_type)
                    setattr(instance, attr_name, val)

                return instance

            class_attributes["read_order"] = read_order
            class_attributes["attribute_type"] = attribute_type
            # class_attributes["class_types"] = self.class_types
            class_attributes["read"] = classmethod(read)
            class_variable = type(class_name, (object,), class_attributes)
            self.class_types[class_name] = class_variable

        def readThreadName(cls, stream: StreamAble):
            sutil = StreamUtil(stream)
            instance = cls()
            setattr(instance, "threadName", sutil.readStr())
            return instance

        FrameBegin = type(
            MessageID.idFrameBegin.name[2:],
            (object,),
            {
                "threadName": None,
                "read": classmethod(readThreadName),
            },
        )
        FrameFinished = type(
            MessageID.idFrameFinished.name[2:],
            (object,),
            {
                "threadName": None,
                "read": classmethod(readThreadName),
            },
        )
        self.class_types[MessageID.idFrameBegin.name[2:]] = FrameBegin
        self.class_types[MessageID.idFrameFinished.name[2:]] = FrameFinished

    def parseAll(self):
        self.regClassesAndEnums()
        # Start parsing
        message_idx = 0
        for frame_idx, frame_start in enumerate(self.frameIndex):
            frame_end = (
                self.frameIndex[frame_idx + 1]
                if frame_idx + 1 < len(self.frameIndex)
                else len(self.buffer)
            )
            frame = self.messageSlices[frame_start:frame_end]
            for message_idx_in_frame, message in enumerate(frame):
                id = message[0]
                id = self.mapLogToID[id]  # map from log id to the id used in MessageID

                message_bytes = self.buffer[message[1] : message[2]]
                message_bytes_stream = io.BytesIO(message_bytes)
                if message_idx_in_frame == 0:
                    if id != MessageID.idFrameBegin.value:
                        raise Exception("First message must be idFrameBegin")

                elif message_idx_in_frame == len(frame) - 1:
                    if id != MessageID.idFrameFinished.value:
                        raise Exception("Last message must be idFrameFinished")

                match id:
                    case MessageID.idCameraImage.value | MessageID.idJPEGImage.value:
                        image = CameraImage()
                        image.read(message_bytes_stream)
                        # image.draw()
                        self.parsedMessages[message_idx] = image
                    case _:
                        instance = self.parseMessage(message)
                        self.parsedMessages[message_idx] = instance
                message_idx += 1

        for class_name, log_class_attributes in self.typeInfo["classes"].items():
            class_variable = self.class_types[class_name]
            if hasattr(class_variable, "read_order") and hasattr(
                class_variable, "attribute_type"
            ):
                delattr(class_variable, "read_order")
                delattr(class_variable, "attribute_type")

    def create_log_instance(self) -> Log:
        self.parseAll()
        # Assuming self.buffer contains the full log file content
        return Log(
            logFilePath=self.log_file_path,
            logBuffer=self.buffer,
            settings=self.settings,
            logIDNames=self.logIDNames,
            mapNameToID=self.mapNameToID,
            mapLogToID=self.mapLogToID,
            typeInfo=self.typeInfo,
            frameIndex=self.frameIndex,
            framesHaveImage=self.framesHaveImage,
            anyFrameHasImage=self.anyFrameHasImage,
            statsPerThread=self.statsPerThread,
            annotationsPerThread=self.annotationsPerThread,
            sizeWhenIndexWasComputed=self.sizeWhenIndexWasComputed,
            enum_types=self.enum_types,
            class_types=self.class_types,
            messageSlices=self.messageSlices,
            parsedMessages=self.parsedMessages,
        )


if __name__ == "__main__":
    log_reader = LogFileReader()
    log_reader.open("rb6.log")
    log = log_reader.create_log_instance()
    log.dumpOutline(Path(log.logFilePath).stem)
    result = {}
    for frame in log:
        frame.saveImageWithMetaData(slientFail=True)
    