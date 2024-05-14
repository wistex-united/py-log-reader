from typing import Union, Type, Literal
from .Angle import Angle
import numpy as np

# To map the Log's c++ type to python type, we create a series of primitive types here
# Currently I map them to numpy types
UInt = np.uint32
"""Python primitive type for unsigned int"""
Int = np.int32
"""Python primitive type for int"""
UChar = np.uint8
"""Python primitive type for unsigned char"""
Char = np.int8
"""Python primitive type for char"""
Str = str
"""Python primitive type for std::string"""
Bool = np.bool_
"""Python primitive type for bool"""
UShort = np.uint16
"""Python primitive type for unsigned short"""
Short = np.int16
"""Python primitive type for short"""
Double = np.float64
"""Python primitive type for double"""
Float = np.float32
"""Python primitive type for float"""
SChar = np.int8
"""Python primitive type for signed char"""

NPPrimitiveType = Union[
    UInt,
    Int,
    UChar,
    Char,
    Bool,
    UShort,
    Short,
    Double,
    Float,
    SChar,
]

PrimitiveType = Union[
    NPPrimitiveType,
    Str,
    Angle,
]
"""Python types for corresponding c++ types, this is just a collection of Aliases"""


PrimitiveTypeHint = Union[
    Type[UInt],
    Type[Int],
    Type[UChar],
    Type[Char],
    Type[Str],
    Type[Bool],
    Type[UShort],
    Type[Short],
    Type[Double],
    Type[Float],
    Type[SChar],
    Type[Angle],
    str,
    Literal["UInt"],
    Literal["Int"],
    Literal["UChar"],
    Literal["Char"],
    Literal["Str"],
    Literal["Bool"],
    Literal["UShort"],
    Literal["Short"],
    Literal["Double"],
    Literal["Float"],
    Literal["SChar"],
    Literal["Angle"],
    Literal["unsigned int"],
    Literal["int"],
    Literal["unsigned char"],
    Literal["char"],
    Literal["std::string"],
    Literal["bool"],
    Literal["unsigned short"],
    Literal["short"],
    Literal["double"],
    Literal["float"],
    Literal["signed char"],
]
"""
Type Hint/Indicator for PrimitiveType parameters/return values
All values above can indicate a c++ primitive type
You can use them to refer to a c++ primitive type in readPrimitives()
By Indicator2RealType[indicator] you can get the corresponding python type
"""

# Basic byte to numpy type conversion, Str and Angle are handled differently
bytes2UInt = lambda x: np.frombuffer(x, dtype=np.uint32)
bytes2Int = lambda x: np.frombuffer(x, dtype=np.int32)
bytes2UChar = lambda x: np.frombuffer(x, dtype=np.uint8)
bytes2Char = lambda x: np.frombuffer(x, dtype=np.int8)
bytes2Bool = lambda x: np.frombuffer(x, dtype=np.bool_)
bytes2UShort = lambda x: np.frombuffer(x, dtype=np.uint16)
bytes2Short = lambda x: np.frombuffer(x, dtype=np.int16)
bytes2Double = lambda x: np.frombuffer(x, dtype=np.float64)
bytes2Float = lambda x: np.frombuffer(x, dtype=np.float32)
bytes2SChar = lambda x: np.frombuffer(x, dtype=np.int8)

# You can also try to map it to ctypes.

# import ctypes

# cpp_to_ctypes_mapping = {
#     'int': ctypes.c_int,
#     'unsigned int': ctypes.c_uint,
#     'short': ctypes.c_short,
#     'unsigned short': ctypes.c_ushort,
#     'char': ctypes.c_char,
#     'signed char': ctypes.c_byte,
#     'unsigned char': ctypes.c_ubyte,
#     'bool': ctypes.c_bool,
#     'float': ctypes.c_float,
#     'double': ctypes.c_double,
#     'std::string': ctypes.c_char_p,  # For C-strings; std::string handling may require more work.
#     'Angle': None,  # Custom class, representation would depend on its definition.
# }
