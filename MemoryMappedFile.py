import mmap
import os


class MemoryMappedFile:
    def __init__(self, filename):
        self.filename = filename
        self.data = None  # This will be a memory-mapped object
        self.size = 0

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

    def __del__(self):
        if self.mmap:
            self.mmap.close()

    def exists(self):
        return self.data is not None

    def getData(self):
        return self.data

    def getSize(self):
        return self.size


# Example usage
if __name__ == "__main__":
    mmf = MemoryMappedFile("example.txt")
    if mmf.exists():
        print(f"MemoryMappedFile exists with size {mmf.getSize()} bytes.")
        # Access data using mmf.getData()
    else:
        print("MemoryMappedFile does not exist.")
