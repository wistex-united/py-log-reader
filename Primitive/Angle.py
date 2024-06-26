import math


class Angle:
    """
    This class is modified from BadgerRLSystem's C++ class
    At:
    https://github.com/bhuman/BHumanCodeRelease/blob/master/Src/Libs/MathBase/Angle.h
    """

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

    def __str__(self) -> str:
        return str(self.value) + "rad"

    def __repr__(self) -> str:
        return str(self.value) + "rad"

    def normalize(self):
        self.value = self.normalize_angle(self.value)
        return self

    @staticmethod
    def normalize_angle(data):
        if -math.pi <= data < math.pi:
            return data
        else:
            data -= math.floor(data / (2 * math.pi)) * (2 * math.pi)
            return (
                data - (2 * math.pi)
                if data >= math.pi
                else data + (2 * math.pi)
                if data < -math.pi
                else data
            )

    def diff_abs(self, other):
        return abs(self.normalize_angle(self.value - other.value))

    @staticmethod
    def from_degrees(degrees) -> "Angle":
        return Angle(math.radians(degrees))

    def to_degrees(self) -> float:
        return math.degrees(self.value)


# Corresponding C++ class: Src/Libs/MathBase/Angle
# #pragma once

# #include "MathBase/Constants.h"
# #include <cmath>

# /**
#  * Converts angle from rad to degrees.
#  * @param angle code in rad
#  * @return angle coded in degrees
#  */
# template<typename V>
# constexpr V toDegrees(V angle) { return angle * V(180.f / pi); }

# /**
#  * The Angle class stores the represented angle in radiant.
#  */
# class Angle
# {
# public:
#   constexpr Angle() = default;
#   constexpr Angle(float angle) : value(angle) {}

#   operator float& () { return value; }
#   constexpr operator const float& () const { return value; }

#   constexpr Angle operator-() const { return Angle(-value); }
#   Angle& operator+=(float angle) { value += angle; return *this; }
#   Angle& operator-=(float angle) { value -= angle; return *this; }
#   Angle& operator*=(float angle) { value *= angle; return *this; }
#   Angle& operator/=(float angle) { value /= angle; return *this; }

#   Angle& normalize() { value = normalize(value); return *this; }

#   /**
#    * reduce angle to [-pi..+pi[
#    * @param data angle coded in rad
#    * @return normalized angle coded in rad
#    */
#   template<typename V>
#   static V normalize(V data);

#   Angle diffAbs(Angle b) const { return std::abs(normalize(value - b)); }

#   static constexpr Angle fromDegrees(float degrees) { return Angle((degrees / 180.f) * pi); }
#   static constexpr Angle fromDegrees(int degrees) { return fromDegrees(static_cast<float>(degrees)); }

#   constexpr float toDegrees() const { return (value / pi) * 180.f; }

# private:
#   float value = 0.f;
# };

# inline constexpr Angle operator "" _deg(unsigned long long int angle)
# {
#   return Angle::fromDegrees(static_cast<float>(angle));
# }

# inline constexpr Angle operator "" _deg(long double angle)
# {
#   return Angle::fromDegrees(static_cast<float>(angle));
# }

# inline constexpr Angle operator "" _rad(unsigned long long int angle)
# {
#   return Angle(static_cast<float>(angle));
# }

# inline constexpr Angle operator "" _rad(long double angle)
# {
#   return Angle(static_cast<float>(angle));
# }

# template<typename V>
# V Angle::normalize(V data)
# {
#   if(data >= -V(pi) && data < V(pi))
#     return data;
#   else
#   {
#     data = data - static_cast<float>(static_cast<int>(data / V(pi2))) * V(pi2);
#     return data >= V(pi) ? V(data - V(pi2)) : data < -V(pi) ? V(data + V(pi2)) : data;
#   }
# }

# #ifndef isfinite
# namespace std
# {
#   inline bool isfinite(Angle angle) noexcept
#   {
#     return isfinite(static_cast<float>(angle));
#   }
# }
# #endif
