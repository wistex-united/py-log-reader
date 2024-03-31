import io
from typing import Callable, List, Tuple, Dict

from messageID import MessageID

class MessageHeader:
    def __init__(self, message_id: MessageID, size: int):
        self.id = message_id
        self.size = size

class QueueHeader:
    def __init__(self, size_low: int, messages: int, size_high: int):
        self.size_low = size_low
        self.messages = messages
        self.size_high = size_high

class Message:
    def __init__(self, message_id: MessageID, data: bytes):
        self.id = message_id
        self.data = data

class MessageQueue:
    def __init__(self):
        self.messages: List[Message] = []
        self.capacity = 0
        self.max_capacity = 64 * 1024 * 1024  # 64MB max capacity
        self.protected_capacity = 0
        self.used = 0

    def add_message(self, message_id: MessageID, data: bytes):
        if not self.ensure_capacity(len(data)):
            return False  # Not enough capacity to add message
        self.messages.append(Message(message_id, data))
        self.used += len(data) + self.header_size(message_id, len(data))
        return True

    def ensure_capacity(self, required_capacity: int) -> bool:
        return self.used + required_capacity <= self.max_capacity

    def header_size(self, message_id: MessageID, data_size: int) -> int:
        # Placeholder method to calculate the size of a message header.
        # Adjust as necessary based on actual header structure.
        return 8  # Example fixed header size

    def filter_messages(self, keep: Callable[[Message], bool]):
        self.messages = [msg for msg in self.messages if keep(msg)]
        self.used = sum(len(msg.data) + self.header_size(msg.id, len(msg.data)) for msg in self.messages)

    def clear(self):
        self.messages.clear()
        self.used = 0

    def set_buffer(self, buffer: bytes, size: int):
        # Simulated method for setting an external buffer. This is a simplified approach.
        # In actual usage, careful consideration is needed for memory management and ownership.
        self.messages = []  # Clear current messages
        # Example: Reinterpret the buffer as a list of messages. Adjust based on actual data format.
        stream = io.BytesIO(buffer)
        while stream.tell() < size:
            
            message_id = MessageID()  # Example: Replace with actual method to read message ID
            data_size = 0  # Example: Replace with actual method to read data size
            data = stream.read(data_size)
            self.add_message(message_id, data)

# Example usage
def main():
    mq = MessageQueue()
    # Example: Add messages
    mq.add_message(MessageID(), b"Hello World")
    # Example: Filter messages
    mq.filter_messages(lambda msg: msg.id == MessageID())
    # Example: Clear messages
    mq.clear()
    # Example: Set external buffer (simulated)
    mq.set_buffer(b"External data", len(b"External data"))

if __name__ == "__main__":
    main()
