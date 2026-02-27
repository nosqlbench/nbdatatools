package io.nosqlbench.vectordata.spec.metadata;

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
/// {@code float} and {@code double} via {@link Float#float16ToFloat(short)}
/// and {@link Float#floatToFloat16(float)} (Java 20+).
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
///
/// This class requires Java 20+ for the {@code Float.float16ToFloat} and
/// {@code Float.floatToFloat16} methods, and is compiled as part of the
/// multi-release JAR under {@code META-INF/versions/25/}.
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
        return new Half(Float.floatToFloat16(value));
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
        return Float.float16ToFloat(bits);
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
}
