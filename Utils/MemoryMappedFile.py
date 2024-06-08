import mmap
import os
from typing import Optional


class MemoryMappedFile:
    """
    This class is modified from BadgerRLSystem's C++ class
    At:
    https://github.com/bhuman/BHumanCodeRelease
    Src/Libs/Platform/MemoryMappedFile
    """

    def __init__(self, filename: str):
        self.filename: str = filename
        self.data: Optional[mmap.mmap] = None  # This will be a memory-mapped object
        self.size: int = 0

        if os.path.exists(self.filename):
            self.size = os.path.getsize(self.filename)
            file = open(self.filename, "rb")
            self.mmap = mmap.mmap(
                file.fileno(), length=self.size, access=mmap.ACCESS_READ
            )
            self.data = self.mmap
            file.close()  # Close the file descriptor since mmap keeps its own reference
        else:
            raise OSError(f"File {self.filename} does not exist.")
            exit(-1)

    def __del__(self):
        if hasattr(self, "mmap") and self.mmap:
            self.mmap.close()

    def exists(self) -> bool:
        return self.data is not None

    def getData(self) -> mmap.mmap:
        if self.data is None:
            raise ValueError("MemoryMappedFile does not exist.")
        return self.data

    def getSize(self) -> int:
        return self.size


# Corresponding C++ class: Src/Libs/Platform/MemoryMappedFile
# /**
#  * @file MemoryMappedFile.h
#  *
#  * This file declares a class that represents read-only memory mapped files.
#  *
#  * @author Thomas Röfer
#  */

# #pragma once

# #include <string>

# class MemoryMappedFile
# {
#   char* data = nullptr; /**< The start address of the mapped file in memory. */
#   size_t size = 0; /**< The size of the file and the memory block. */
# #ifdef WINDOWS
#   void* handle = nullptr; /**< Windows also needs a file handle for the mapping. */
# #endif

# public:
#   /**
#    * Open a file and map it to a memory block.
#    * @param filename The name of the file.
#    */
#   MemoryMappedFile(const std::string& filename);

#   /** Destructor. */
#   ~MemoryMappedFile();

#   /**
#    * Does the file exist?
#    * @return Does it exist?
#    */
#   bool exists() const {return data != nullptr;}

#   /**
#    * Returns the begin of the memory block the file is mapped to.
#    * @return The address of the memory block or \c nullptr
#    *         if the file does not exist.
#    */
#   const char* getData() {return data;}

#   /**
#    * Returns the size of the file and the memory block.
#    * @return The size in bytes or 0 if the file does not exist.
#    */
#   size_t getSize() {return size;}
# };
