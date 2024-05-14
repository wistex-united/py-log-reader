from io import BytesIO

import cv2
import numpy as np
from PIL.Image import fromarray, open

from LogInterface import DataClass
from Primitive import *
from StreamUtils import StreamUtil

from .Image import Image as ImageBase


class JPEGImage(ImageBase, DataClass):
    maxResolutionWidth = 1280
    maxResolutionHeight = 960
    readOrder = ["width", "height", "timestamp"]
    attributeCtype = {
        "width": "unsigned int",
        "height": "unsigned int",
        "timestamp": "unsigned int",
    }

    def __init__(self):
        super().__init__()
        self.timestamp: int
        self.size: int

    @classmethod
    def read(cls, sutil: StreamUtil) -> "JPEGImage":
        jpegImage = JPEGImage()
        width = sutil.readUInt()
        height = sutil.readUInt()
        timestamp = sutil.readUInt()

        if (timestamp & (1 << 31)) == 0:
            raise ValueError("Invalid timestamp")
        timestamp &= ~(1 << 31)

        jpegImage.size = sutil.readUInt()

        jpegImage.setResolution(width, height * 2)
        jpegImage.timestamp = timestamp

        rawImg = open(
            BytesIO(sutil.read(jpegImage.size)), formats=["JPEG"]
        )  # PIL deduce it is CMYK but it is actually YUYV
        jpegImage.image = 255 - np.array(rawImg).reshape((height * 2, width * 2, 2))

        if not sutil.atEnd():
            raise ValueError("Buffer Size not used up")

        return jpegImage

    def asDict(self):
        return {
            "width": self.width,
            "height": self.height,
            "timestamp": self.timestamp,
            "size": self.size,
        }

    def draw(self):
        cv2.imshow("Image", self.rgbImage)
        cv2.waitKey(0)
        cv2.destroyAllWindows()

    @property
    def rgbImage(self):
        if isinstance(self.image, np.ndarray):
            return cv2.cvtColor(self.image, cv2.COLOR_YUV2RGB_YUYV)
        elif isinstance(self.image, ImageBase):
            return cv2.cvtColor(
                np.array(self.image).reshape((self.height, self.width * 2, 2)),
                cv2.COLOR_YUV2RGB_YUYV,
            )
        else:
            raise ValueError("self.image is not an valid image")

    def saveImage(self, path, metadata=None, slientFail=False):
        try:
            img = fromarray(self.rgbImage)
            img.save(
                path,
                pnginfo=metadata,
            )
        except:
            if slientFail:
                return
            else:
                raise Exception("Failed to save image")

    """Deprecated: Instantiation method on dynamicly generated classes"""
    # @classmethod
    # def getReadInstructions(cls):
    #     """Read Instructions does support dynamic length, so this instruction would only read the width, height and timestamp, leaving the image bytes unread"""
    #     return [
    #         (UInt, 1),  # width
    #         (UInt, 1),  # height
    #         (UInt, 1),  # timestamp
    #         (UInt, 1),  # size
    #     ]

    # @classmethod
    # def distributeReadResult(cls, result) -> "JPEGImage":
    #     instance = cls()
    #     instance.width = result[0]
    #     instance.height = result[1]
    #     instance.timestamp = result[2]
    #     instance.size = result[3]
    #     return instance
