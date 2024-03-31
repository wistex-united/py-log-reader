from typing import TypeVar, Generic, List
from abc import ABC, abstractmethod

from numpy import byte
from torch import Stream

from PixelTypes import PixelBase
from StreamUtil import StreamAble


class Image:
    def __init__(self, width: int = 0, height: int = 0, padding: int = 0):
        self.width: int
        self.height: int
        self.image: bytes

    def setResolution(self, width, height):
        self.width = width
        self.height = height

    # @abstractmethod
    # def read(self, stream: StreamAble):
    #     pass

    # @abstractmethod
    # def write(self, stream: StreamAble):
    #     pass
