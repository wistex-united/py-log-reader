from abc import ABC, abstractmethod
from typing import Tuple, Union

# BT 601-YUV coefficients
yCoeffR = 0.299
yCoeffG = 0.587
yCoeffB = 0.114

uCoeff = 0.564
vCoeff = 0.713

scaleExponent = 16

# Scaled coefficients
scaledYCoeffR = int(yCoeffR * (1 << scaleExponent))
scaledYCoeffG = int(yCoeffG * (1 << scaleExponent))
scaledYCoeffB = int(yCoeffB * (1 << scaleExponent))

scaledUCoeff = int(uCoeff * (1 << scaleExponent))
scaledVCoeff = int(vCoeff * (1 << scaleExponent))
scaledInvUCoeff = int((1 << scaleExponent) / uCoeff)
scaledInvVCoeff = int((1 << scaleExponent) / vCoeff)

scaledGCoeffU = int((yCoeffB / (yCoeffG * uCoeff)) * (1 << scaleExponent))
scaledGCoeffV = int((yCoeffR / (yCoeffG * vCoeff)) * (1 << scaleExponent))


class PixelBase(ABC):
    """
    This class is modified from BadgerRLSystem's C++ class
    At:
    https://github.com/bhuman/BHumanCodeRelease/blob/master/Src/Libs/ImageProcessing/PixelTypes.h
    """
    @property
    @abstractmethod
    def size(self):
        pass


class RGBPixel(PixelBase):
    """
    This class is modified from BadgerRLSystem's C++ class
    At:
    https://github.com/bhuman/BHumanCodeRelease/blob/master/Src/Libs/ImageProcessing/PixelTypes.h
    """
    size: int = 4

    def __init__(self, raw: bytes):
        if len(raw) != self.size:
            raise ValueError(f"Expected {self.size} bytes, got {len(raw)}")
        self.bytes = raw

    @property
    def r(self) -> int:
        return self.bytes[0]

    @property
    def g(self) -> int:
        return self.bytes[1]

    @property
    def b(self) -> int:
        return self.bytes[2]

    def tuple(self) -> Tuple[int, int, int]:
        return (self.r, self.g, self.b)


class BGRAPixel(PixelBase):
    """
    This class is modified from BadgerRLSystem's C++ class
    At:
    https://github.com/bhuman/BHumanCodeRelease/blob/master/Src/Libs/ImageProcessing/PixelTypes.h
    """
    size: int = 4  # Size of the tuple

    def __init__(self, raw: bytes):
        if len(raw) != self.size:
            raise ValueError(f"Expected {self.size} bytes, got {len(raw)}")
        self.bytes = raw

    @property
    def numPixels(self) -> int:
        return 1

    @property
    def b(self) -> int:
        return self.bytes[0]

    @property
    def g(self) -> int:
        return self.bytes[1]

    @property
    def r(self) -> int:
        return self.bytes[2]

    @property
    def a(self) -> int:
        return self.bytes[3]

    def tuple(self) -> Tuple[int, int, int, int]:
        return (self.b, self.g, self.r, self.a)

    def rgb(self) -> Tuple[int, int, int]:
        return (self.r, self.g, self.b)

    def greyscale(self) -> int:
        return int((self.r + self.g + self.b) / 2)


class YUYVPixel(PixelBase):
    """
    This class is modified from BadgerRLSystem's C++ class
    At:
    https://github.com/bhuman/BHumanCodeRelease/blob/master/Src/Libs/ImageProcessing/PixelTypes.h
    """
    size: int = 4  # Y0, U, Y1, V for two pixels

    def __init__(self, raw: bytes):
        if len(raw) != self.size:
            raise ValueError(f"Expected {self.size} bytes, got {len(raw)}")
        self.bytes = raw

    @property
    def numPixels(self) -> int:
        return 2

    @property
    def y0(self) -> int:
        return self.bytes[0]

    @property
    def u(self) -> int:
        return self.bytes[1]

    @property
    def y1(self) -> int:
        return self.bytes[2]

    @property
    def v(self) -> int:
        return self.bytes[3]

    def tuple(self) -> Tuple[int, int, int, int]:
        return (self.y0, self.u, self.y1, self.v)

    def rgb(self) -> Tuple[Tuple[int, int, int], Tuple[int, int, int]]:
        r0, g0, b0 = YUVPixel.fromYUVToRGB(self.y0, self.u, self.v)
        r1, g1, b1 = YUVPixel.fromYUVToRGB(self.y1, self.u, self.v)
        return (r0, g0, b0), (r1, g1, b1)

    def greyscale(self) -> Tuple[int, int]:
        return (self.y0, self.y1)

    def y(self, x: int) -> int:
        return self.y0 if x % 2 == 0 else self.y1


class YUVPixel(PixelBase):
    """
    This class is modified from BadgerRLSystem's C++ class
    At:
    https://github.com/bhuman/BHumanCodeRelease/blob/master/Src/Libs/ImageProcessing/PixelTypes.h
    """
    size: int = 4  # padding, U, Y, V

    def __init__(self, raw: Union[bytes, Tuple[int, int, int, int]]):
        if len(raw) != self.size:
            raise ValueError(f"Expected {self.size} bytes, got {len(raw)}")
        self.bytes = raw

    @property
    def u(self) -> int:
        return self.bytes[1]

    @property
    def y(self) -> int:
        return self.bytes[2]

    @property
    def v(self) -> int:
        return self.bytes[3]

    def tuple(self) -> Tuple[int, int, int]:
        return (self.u, self.y, self.v)

    @classmethod
    def fromYUVToRGB(cls, Y: int, U: int, V: int) -> Tuple[int, int, int]:
        u = U - 128
        v = V - 128
        b = Y + ((u * scaledInvUCoeff) >> scaleExponent)
        g = Y - ((u * scaledGCoeffU + v * scaledGCoeffV) >> scaleExponent)
        r = Y + ((v * scaledInvVCoeff) >> scaleExponent)

        R = max(0, min(255, int(r)))
        G = max(0, min(255, int(g)))
        B = max(0, min(255, int(b)))
        return R, G, B


class HSIPixel(PixelBase):
    """
    This class is modified from BadgerRLSystem's C++ class
    At:
    https://github.com/bhuman/BHumanCodeRelease/blob/master/Src/Libs/ImageProcessing/PixelTypes.h
    """
    size: int = 4  # padding, H, S, I

    def __init__(self, raw: bytes):
        if len(raw) != self.size:
            raise ValueError(f"Expected {self.size} bytes, got {len(raw)}")
        self.bytes = raw

    @property
    def h(self) -> int:
        return self.bytes[1]

    @property
    def s(self) -> int:
        return self.bytes[2]

    @property
    def i(self) -> int:
        return self.bytes[3]

    def tuple(self) -> Tuple[int, int, int]:
        return (self.h, self.s, self.i)


class GrayscaledPixel(PixelBase):
    """
    This class is modified from BadgerRLSystem's C++ class
    At:
    https://github.com/bhuman/BHumanCodeRelease/blob/master/Src/Libs/ImageProcessing/PixelTypes.h
    """
    size: int = 1

    def __init__(self, hue=0):
        self.value = hue

    @property
    def val(self) -> int:
        return self.value


class HuePixel(PixelBase):
    """
    This class is modified from BadgerRLSystem's C++ class
    At:
    https://github.com/bhuman/BHumanCodeRelease/blob/master/Src/Libs/ImageProcessing/PixelTypes.h
    """
    size: int = 1  # Only one byte for the hue value

    def __init__(self, hue=0):
        self.value = hue

    @property
    def val(self) -> int:
        return self.value

    def __int__(self):
        return self.value

    # arithmetic operations
    def __add__(self, other):
        return HuePixel((self.value + other) % 256)

    def __sub__(self, other):
        return HuePixel((self.value - other) % 256)

    def __mul__(self, other):
        return HuePixel((self.value * other) % 256)

    def __truediv__(self, other):
        if other == 0:
            raise ValueError("Cannot divide by zero")
        return HuePixel(self.value // other)

    def tuple(self):
        return (self.value,)


class Edge2Pixel(PixelBase):
    """
    This class is modified from BadgerRLSystem's C++ class
    At:
    https://github.com/bhuman/BHumanCodeRelease/blob/master/Src/Libs/ImageProcessing/PixelTypes.h
    """
    size: int = 2

    def __init__(self, e1=0, e2=0):
        self.filterX = e1
        self.filterY = e2

    def tuple(self):
        return (self.filterX, self.filterY)


class BinaryPixel(PixelBase):
    """
    This class is modified from BadgerRLSystem's C++ class
    At:
    https://github.com/bhuman/BHumanCodeRelease/blob/master/Src/Libs/ImageProcessing/PixelTypes.h
    """
    size: int = 1

    def __init__(self, val=0):
        if val != 0 and val != 1:
            raise ValueError(f"Expected 0 or 1, got {val}")
        self.value = val

    @property
    def val(self) -> int:
        return self.value


def pixel_size(pixel_type: PixelBase):
    return pixel_type.size
