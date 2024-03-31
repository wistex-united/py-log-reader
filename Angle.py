import math

class Angle:
    def __init__(self, angle=0.0):
        self.value = angle

    def __neg__(self):
        return Angle(-self.value)

    def __iadd__(self, other):
        self.value += other
        return self

    def __isub__(self, other):
        self.value -= other
        return self

    def __imul__(self, other):
        self.value *= other
        return self

    def __itruediv__(self, other):
        self.value /= other
        return self

    def normalize(self):
        self.value = self.normalize_angle(self.value)
        return self

    @staticmethod
    def normalize_angle(data):
        if -math.pi <= data < math.pi:
            return data
        else:
            data -= math.floor(data / (2 * math.pi)) * (2 * math.pi)
            return data - (2 * math.pi) if data >= math.pi else data + (2 * math.pi) if data < -math.pi else data

    def diff_abs(self, other):
        return abs(self.normalize_angle(self.value - other.value))

    @staticmethod
    def from_degrees(degrees):
        return Angle(math.radians(degrees))

    def to_degrees(self):
        return math.degrees(self.value)

# Utility functions to replace user-defined literals
def angle_from_deg(degrees):
    return Angle.from_degrees(degrees)

def angle_from_rad(radians):
    return Angle(radians)

# Example Usage
a = Angle.from_degrees(180)
print(a.value)  # Should show pi radians
a += 0.1  # Increment angle
a.normalize()  # Normalize angle
print(a.to_degrees())  # Convert back to degrees

b = angle_from_deg(45)  # Equivalent to "45_deg" in C++
c = angle_from_rad(math.pi / 2)  # Equivalent to "1.5708_rad" in C++
