import struct
from typing import List
from Image import Image
from PixelTypes import PixelBase, YUYVPixel, YUVPixel, GrayscaledPixel, pixel_size
from StreamUtil import StreamUtil, StreamAble
from PIL import Image as PILImage


class CameraImage(Image):
    maxResolutionWidth = 1280
    maxResolutionHeight = 960

    def __init__(self):
        super().__init__()
        self.timestamp: int

        self._PILImage = None

    def getPixel(self, x, y) -> YUYVPixel:
        loc = y * self.width * 4 + x * 2
        startLoc = (loc // 4) * 4
        endLoc = (loc // 4 + 1) * 4
        return YUYVPixel(self.image[startLoc:endLoc])

    def getY(self, x, y):
        return self.getPixel(x, y).y(x)

    def getU(self, x, y):
        return self.getPixel(x, y).u

    def getV(self, x, y):
        return self.getPixel(x, y).v

    def getYUV(self, x, y) -> YUVPixel:
        yuyv = self.getPixel(x, y)
        return YUVPixel((0, yuyv.u, yuyv.y(x), yuyv.v))

    def getGrayscaled(self):
        raise NotImplementedError("getGrayscale not implemented")

    def read(self, stream: StreamAble):
        sutil = StreamUtil(stream)
        width = sutil.readUInt()
        height = sutil.readUInt()
        timestamp = sutil.readUInt()

        if timestamp & (1 << 31):
            height *= 2
            timestamp &= ~(1 << 31)

        self.setResolution(width, height)
        self.image = sutil.read(width * height * YUYVPixel.size)
        if not sutil.atEnd():
            raise ValueError("Buffer Size not used up")

    def write(self, stream: StreamAble):
        stream.write(struct.pack("<I", self.width))
        stream.write(struct.pack("<I", self.height))
        stream.write(struct.pack("<I", self.timestamp))
        stream.write(self.image)

    def draw(self):
        self.PILImage.show()

    @property
    def PILImage(self):
        if self._PILImage is not None:
            return self._PILImage

        self._PILImage = PILImage.new("RGB", (self.width * 2, self.height))
        pixels = self._PILImage.load()

        for y in range(self.height):
            for x in range(self.width):
                rgb1, rgb2 = self.getPixel(x * 2, y).rgb()

                pixels[x * 2, y] = rgb1
                pixels[x * 2 + 1, y] = rgb2
        return self._PILImage
