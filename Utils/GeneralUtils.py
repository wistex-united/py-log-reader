"""
Tool functions set. All the function will be exposed in Utils package
"""

import cProfile
import json
import mmap
import os
import pstats
import re
import sys
from typing import List, Optional, Tuple, Union

from numpy.typing import NDArray

from Primitive import *

from .JSONEncoder import SpecialEncoder


def findClosestValidValue(list: Union[List, NDArray], index, null_value=-1):
    if list[index] != null_value:
        return list[index]
    distance = 1
    presult = null_value
    teminate = False
    while True:
        if teminate:
            break
        teminate = True
        if index + distance < len(list):
            if list[index + distance] != null_value:
                presult = list[index + distance]
                break
            teminate = False
        if index - distance >= 0:
            if list[index - distance] != null_value:
                presult = list[index - distance]
                break
            teminate = False
        distance += 1
    return presult


def dumpJson(obj, indent=2) -> str:
    return json.dumps(obj, indent=indent, cls=SpecialEncoder)


def bytes2ShortStr(b: bytes):
    return f"Bytes[{len(b)}]: {b[:4] if len(b) >= 4 else b''} ..."

def isIntAlike(value):
    # Check if it's a regular integer
    if isinstance(value, int):
        return True
    
    # Check if it's a numpy integer
    if np.issubdtype(type(value), np.integer):
        return True
    
    return False

class _NestedClassGetter(object):
    """
    When called with the containing class as the first argument,
    and the name of the nested class as the second argument,
    returns an instance of the nested class.
    """

    def __call__(self, containing_class, class_name):
        nested_class = getattr(containing_class, class_name)

        # make an instance of a simple object (this one will do), for which we can change the
        # __class__ later on.
        nested_instance = _NestedClassGetter()

        # set the class of the instance, the __init__ will never be called on the class
        # but the original state will be set later on by pickle.
        nested_instance.__class__ = nested_class
        return nested_instance


def sanitizeCName(c_style_name: str):
    c_style_name = c_style_name.replace("::", "_")
    c_style_name = c_style_name.replace(" ", "_")
    c_style_name = c_style_name.replace("<", "_")
    c_style_name = c_style_name.replace(">", "")
    c_style_name = c_style_name.replace(",", "_")
    c_style_name = c_style_name.replace(".", "_")
    c_style_name = c_style_name.replace("&", "")
    c_style_name = c_style_name.replace("*", "")
    c_style_name = c_style_name.replace("(", "_")
    c_style_name = c_style_name.replace(")", "")
    c_style_name = c_style_name.replace("[", "_")
    c_style_name = c_style_name.replace("]", "")
    if c_style_name in ["pass", "from"]:
        c_style_name = f"_{c_style_name}"
    return c_style_name


def parseCtype2Pytype(ctype: str, withQuotation: bool = False) -> str:
    ctype, length = type2ReadInstruction(ctype)
    if ctype not in CType2PyStr:
        ctype = f'"{ctype}"' if withQuotation else ctype
        Pytype = sanitizeCName(c_style_name=ctype)
    else:
        Pytype = CType2PyStr[ctype]
    if length != 1:
        return f"List[{Pytype}]"
    else:
        return Pytype


def type2ReadInstruction(ctype) -> Tuple[str, int]:
    if re.search(r"\[\d+\]$", ctype) is not None:
        size = int(ctype.rsplit("[")[1][:-1])
        instruction = (ctype.rsplit("[")[0], size)
    elif ctype.endswith("*"):
        instruction = (ctype[:-1], -1)  # -1 means unknown length
    else:
        instruction = (ctype, 1)
    return instruction


def canBeRange(
    lst: List,
) -> Tuple[bool, Optional[range]]:
    if len(lst) < 2:
        return False, None

    step = lst[1] - lst[0]
    for i in range(2, len(lst)):
        if lst[i] - lst[i - 1] != step:
            return False, None

    return True, range(lst[0], lst[-1] + step, step)


def countLines(filename):
    try:
        with open(filename, "r") as file:
            return sum(1 for line in file)
    except FileNotFoundError:
        return 0


def readLastLine(csvFilePath):
    with open(csvFilePath, "r+b") as f:
        # Memory-map the file, size 0 means whole file
        mmappedFile = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)

        # Find the position of the last newline character
        mmappedFile.seek(0, 2)  # Move to the end of the file
        endPos = mmappedFile.tell()

        pos = endPos - 1

        laseLineEndPos = None
        while pos >= 0:
            if mmappedFile[pos] == ord(b"\n"):
                if laseLineEndPos is None:
                    laseLineEndPos = pos
                else:
                    break
            pos -= 1

        # Read the last line
        if pos < 0:
            mmappedFile.seek(0)
        else:
            mmappedFile.seek(pos + 1)

        lastLine = mmappedFile.readline().decode()

        if mmappedFile.tell() != laseLineEndPos + 1:
            mmappedFile.close()
            raise ValueError("Error reading last line")
        else:
            # truncate the file
            mmappedFile.close()
            with open(csvFilePath, "r+b") as f:
                f.seek(laseLineEndPos + 1)
                f.truncate()

    return lastLine.strip()


def extractTrajNumbers(folderPath):
    if not os.path.exists(folderPath):
        return []
    # List all files in the specified folder
    files = os.listdir(folderPath)

    # Regular expression to match 'traj' files and extract the numbers
    pattern = re.compile(r"traj_(\d+)\.npz")

    # Extract numbers from the filenames
    numbers = []
    for file in files:
        match = pattern.match(file)
        if match:
            numbers.append(int(match.group(1)))

    return sorted(numbers)


def profileFunction(func, args=(), kwargs={}, profileFile="profile_result.prof"):
    """
    Profiles the time consumption of the given function and dumps the profile result to a file.

    Args:
        func (callable): The function to profile.
        args (tuple): Positional arguments to pass to the function.
        kwargs (dict): Keyword arguments to pass to the function.
        profileFile (str): The file to dump the profile result.
    """
    profiler = cProfile.Profile()
    profiler.enable()
    try:
        func(*args, **kwargs)
    finally:
        profiler.disable()
        print("Profile Result: ")
        print("Profile Result: ")
        stats = pstats.Stats(profiler, stream=sys.stdout)
        stats.sort_stats(pstats.SortKey.TIME)
        stats.print_stats()
        with open(profileFile, "w") as f:
            stats = pstats.Stats(profiler, stream=f)
            stats.sort_stats(pstats.SortKey.TIME)
            stats.print_stats()
