from Primitive import *
from StreamUtils import *

from .Chunk import Chunk, ChunkEnum


class SettingsChunk(Chunk):
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

        self._children = [
            self.settingVersion,
            self.headName,
            self.bodyName,
            self.playerNumber,
            self.location,
            self.scenario,
        ]

        self._startByte = offset
        self._endByte = sutil.tell() - startPos + offset


    def parseBytes(self):
        pass
# with open("rb5.log", "rb") as f:
#     MyStream = io.BytesIO(f.read())
#     sutil = StreamUtil(MyStream)
#     sutil.read(1)
#     SettingsChunk = SettingsChunk(None)
#     SettingsChunk.eval(sutil)
#     print(vars(SettingsChunk))
