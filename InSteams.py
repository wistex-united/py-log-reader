class StreamReader:
    def skip_data(self, size, stream):
        # Skip data implementation
        pass

    def read_data(self, size, stream):
        # Read data implementation
        pass


class PhysicalInStream(StreamReader):
    def skip_in_stream(self, size):
        # Implementation for skipping data in the stream
        pass

class InText(PhysicalInStream):
    def read_string(self, stream):
        # Implementation for reading strings, handling whitespace and quotes
        pass

    def read_bool(self, stream):
        # Implementation for reading boolean values
        pass


def skip_whitespace(stream):
    # Skip whitespace implementation
    pass

def read_int(stream):
    # Read integer values, handling sign and conversion
    pass


class InBinary(StreamReader):
    def read_angle(self, stream):
        # Specific method to read angle data in binary form
        pass

class InMemory(StreamReader):
    def __init__(self, memory):
        self.memory = memory

    def read_from_stream(self, size):
        # Read directly from memory
        pass
class InFile(StreamReader):
    def __init__(self, filename):
        self.filename = filename

    def open(self):
        # Open file and prepare for reading
        pass
