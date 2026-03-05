package io.nosqlbench.nbdatatools.api.types;

/*
 * Copyright (c) nosqlbench
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/// An immutable IEEE 754 binary16 (half-precision) floating-point value.
///
/// Java has no native half-precision type. This class wraps a {@code short}
/// containing the raw binary16 bit pattern and provides conversion to/from
/// {@code float} and {@code double} using pure-Java IEEE 754 bit manipulation.
///
/// ## Usage
///
/// ```java
/// Half h = Half.of(3.14f);     // round float to half precision
/// float f = h.toFloat();       // widen back to float
/// double d = h.toDouble();     // widen to double
/// short bits = h.toBits();     // raw binary16 bits
/// Half h2 = Half.fromBits((short) 0x3C00); // 1.0 in half precision
/// ```
public final class Half implements Comparable<Half> {

    private final short bits;

    private Half(short bits) {
        this.bits = bits;
    }

    /// Create a Half from a float value, rounding to half precision.
    ///
    /// @param value the float value
    /// @return a new Half
    public static Half of(float value) {
        return new Half(floatToHalf(value));
    }

    /// Create a Half from a double value, rounding to half precision.
    /// The double is first narrowed to float, then to half.
    ///
    /// @param value the double value
    /// @return a new Half
    public static Half of(double value) {
        return of((float) value);
    }

    /// Create a Half from raw binary16 bits.
    ///
    /// @param bits the raw IEEE 754 binary16 bit pattern
    /// @return a new Half
    public static Half fromBits(short bits) {
        return new Half(bits);
    }

    /// Convert to float (lossless widening).
    ///
    /// @return the float value
    public float toFloat() {
        return halfToFloat(bits);
    }

    /// Convert to double (lossless widening via float).
    ///
    /// @return the double value
    public double toDouble() {
        return (double) toFloat();
    }

    /// Return the raw binary16 bit pattern.
    ///
    /// @return the 16-bit IEEE 754 binary16 representation
    public short toBits() {
        return bits;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Half)) return false;
        return bits == ((Half) o).bits;
    }

    @Override
    public int hashCode() {
        return Short.hashCode(bits);
    }

    @Override
    public String toString() {
        return "Half(" + toFloat() + ")";
    }

    @Override
    public int compareTo(Half other) {
        return Float.compare(toFloat(), other.toFloat());
    }

    // ==================== IEEE 754 binary16 conversion ====================

    /// Convert a float to IEEE 754 binary16 (half-precision) bits.
    /// Pure-Java bit manipulation — works on Java 11+.
    private static short floatToHalf(float value) {
        int fbits = Float.floatToIntBits(value);
        int sign = (fbits >>> 16) & 0x8000;
        int exp = ((fbits >>> 23) & 0xFF) - 127;
        int mantissa = fbits & 0x007FFFFF;

        if (exp > 15) {
            if (exp == 128 && mantissa != 0) {
                return (short) (sign | 0x7C00 | (mantissa >>> 13));
            }
            return (short) (sign | 0x7C00);
        }
        if (exp > -15) {
            int roundBit = 1 << 12;
            int raw = sign | ((exp + 15) << 10) | (mantissa >>> 13);
            if ((mantissa & roundBit) != 0) {
                if ((mantissa & (roundBit - 1)) != 0 || (raw & 1) != 0) {
                    raw++;
                }
            }
            return (short) raw;
        }
        if (exp >= -24) {
            mantissa |= 0x00800000;
            int shift = -1 - exp;
            int roundBit = 1 << (shift - 1 + 13);
            int raw = sign | (mantissa >>> (shift + 13));
            if (shift + 13 < 32) {
                int remainder = mantissa & ((1 << (shift + 13)) - 1);
                if ((remainder & roundBit) != 0) {
                    if ((remainder & (roundBit - 1)) != 0 || (raw & 1) != 0) {
                        raw++;
                    }
                }
            }
            return (short) raw;
        }
        return (short) sign;
    }

    /// Convert IEEE 754 binary16 bits to a float.
    /// Pure-Java bit manipulation — works on Java 11+.
    private static float halfToFloat(short bits) {
        int h = bits & 0xFFFF;
        int sign = (h & 0x8000) << 16;
        int exp = (h >>> 10) & 0x1F;
        int mantissa = h & 0x03FF;

        if (exp == 0) {
            if (mantissa == 0) {
                return Float.intBitsToFloat(sign);
            }
            while ((mantissa & 0x0400) == 0) {
                mantissa <<= 1;
                exp--;
            }
            exp++;
            mantissa &= 0x03FF;
            return Float.intBitsToFloat(sign | ((exp + 112) << 23) | (mantissa << 13));
        }
        if (exp == 31) {
            return Float.intBitsToFloat(sign | 0x7F800000 | (mantissa << 13));
        }
        return Float.intBitsToFloat(sign | ((exp + 112) << 23) | (mantissa << 13));
    }
}
