from Angle import Angle
import numpy as np

# cpp_to_python_mapping = {
#     "unsigned int": int,  # Use Python int; ensure non-negative values for unsigned types
#     "int": int,
#     "unsigned char": int,  # 0 to 255, or 'bytes' for binary data
#     "Angle": Angle,
#     "char": str,  # Single character or use 'bytes' for binary data
#     "std::string": str,
#     "bool": bool,
#     "unsigned short": int,  # Use Python int; ensure non-negative values
#     "short": int,
#     "double": float,  # Python's float is double precision
#     "float": float,
#     "signed char": int,  # -128 to 127
# }

cpp_to_python_mapping = {
    "unsigned int": np.uint32,  # NumPy's unsigned 32-bit integer
    "int": np.int32,  # NumPy's 32-bit integer
    "unsigned char": np.uint8,  # NumPy's unsigned 8-bit integer (0 to 255)
    "Angle": Angle,  # Custom type, unchanged
    "char": np.int8,  # NumPy's 8-bit integer, can also represent characters with ord/chr
    "std::string": np.unicode_,  # For Unicode strings; np.bytes_ for byte strings
    "bool": np.bool_,  # NumPy's boolean type
    "unsigned short": np.uint16,  # NumPy's unsigned 16-bit integer
    "short": np.int16,  # NumPy's 16-bit integer
    "double": np.float64,  # NumPy's 64-bit floating point number
    "float": np.float32,  # NumPy's 32-bit floating point number
    "signed char": np.int8,  # NumPy's signed 8-bit integer (-128 to 127)
}
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
