from typing import List
from .Chunk import Chunk, ChunkEnum
from Primitive import *
from StreamUtils import *


class SettingsChunk(Chunk):
    """Information like head & body Name, player number, location, and scenario"""

    def __init__(self, parent):
        super().__init__(parent)

        self.settingVersion: UInt

        self.headName: Str
        self.bodyName: Str
        self.playerNumber: Int
        self.location: Str
        self.scenario: Str

    def eval(self, sutil: StreamUtil, offset: int = 0):
        startPos = sutil.tell()
        chunkMagicBit = sutil.readUChar()
        if chunkMagicBit != ChunkEnum.SettingsChunk.value:
            raise Exception(
                f"Expect magic number {ChunkEnum.SettingsChunk.value}, but get:{chunkMagicBit}"
            )

        self.settingVersion = sutil.readUInt()
        if self.settingVersion != 1:
            raise ValueError("Unknown settings version {}.".format(self.settingVersion))
        result = sutil.processReadInstructions(
            [
                (Str, 1),
                (Str, 1),
                (Int, 1),
                (Str, 1),
                (Str, 1),
            ]
        )
        (
            self.headName,
            self.bodyName,
            self.playerNumber,
            self.location,
            self.scenario,
        ) = result
        self._endByte = sutil.tell() + offset

        self._children = []  # This chunk has no children

        self._startByte = offset
        self._endByte = sutil.tell() - startPos + offset

    def asDict(self):
        return {
            "headName": self.headName,
            "bodyName": self.bodyName,
            "playerNumber": self.playerNumber,
            "location": self.location,
            "scenario": self.scenario,
        }

    @property
    def providedAttributes(self) -> List[str]:
        return [
            "primitives",
            "enumDescriptions",
            "dataClassDescriptions",
            "enumClasses",
            "dataClasses",
        ]
