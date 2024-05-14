from numpy import ndarray
from .PixelTypes import *

class Image:
    def __init__(self, width: int = 0, height: int = 0, padding: int = 0):
        self.width: int
        self.height: int
        self.image: ndarray

    def setResolution(self, width, height):
        self.width = width
        self.height = height
