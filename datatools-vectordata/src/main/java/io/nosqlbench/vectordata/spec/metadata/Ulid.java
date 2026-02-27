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

import java.util.Arrays;

/// An immutable ULID (Universally Unique Lexicographically Sortable Identifier).
///
/// A ULID is a 128-bit identifier encoded as 16 bytes. The first 48 bits are a
/// Unix timestamp in milliseconds, and the remaining 80 bits are random. The
/// canonical string representation uses Crockford Base32 (26 characters).
///
/// ## Usage
///
/// ```java
/// Ulid ulid = Ulid.of("01ARZ3NDEKTSV4RRFFQ69G5FAV");
/// long millis = ulid.timestamp();
/// byte[] raw = ulid.toBytes();
/// String text = ulid.toString();
/// ```
///
/// @see <a href="https://github.com/ulid/spec">ULID Specification</a>
public final class Ulid implements Comparable<Ulid> {

    private static final char[] CROCKFORD_ENCODE = "0123456789ABCDEFGHJKMNPQRSTVWXYZ".toCharArray();

    /// Crockford Base32 decode table: maps ASCII code point to 5-bit value.
    /// -1 indicates an invalid character.
    private static final byte[] CROCKFORD_DECODE = new byte[128];
    static {
        Arrays.fill(CROCKFORD_DECODE, (byte) -1);
        for (int i = 0; i < CROCKFORD_ENCODE.length; i++) {
            char c = CROCKFORD_ENCODE[i];
            CROCKFORD_DECODE[c] = (byte) i;
            if (c >= 'A' && c <= 'Z') {
                CROCKFORD_DECODE[c + 32] = (byte) i; // lowercase
            }
        }
        // Crockford aliases: I/i/L/l -> 1, O/o -> 0
        CROCKFORD_DECODE['I'] = 1;
        CROCKFORD_DECODE['i'] = 1;
        CROCKFORD_DECODE['L'] = 1;
        CROCKFORD_DECODE['l'] = 1;
        CROCKFORD_DECODE['O'] = 0;
        CROCKFORD_DECODE['o'] = 0;
    }

    private final byte[] data;

    private Ulid(byte[] data) {
        this.data = data;
    }

    /// Parse a ULID from its Crockford Base32 string representation.
    ///
    /// The string must be exactly 26 characters long and contain only valid
    /// Crockford Base32 characters. The first character must be in the range
    /// 0–7 (the timestamp portion occupies 50 bits mapped to 10 characters,
    /// and 5×10 = 50 bits can represent values up to 2^50, but a ULID only
    /// uses 48 bits of timestamp plus 2 overflow bits that must be ≤ 7).
    ///
    /// @param s the 26-character Crockford Base32 string
    /// @return a new Ulid
    /// @throws IllegalArgumentException if the string is null, wrong length,
    ///         or contains invalid characters
    public static Ulid of(String s) {
        if (s == null || s.length() != 26) {
            throw new IllegalArgumentException(
                "ULID string must be exactly 26 characters, got: " + (s == null ? "null" : s.length())
            );
        }
        // Decode Crockford Base32 to 128 bits (16 bytes)
        byte[] data = new byte[16];
        // Accumulate all 130 bits (26 × 5 = 130) into a long pair,
        // but only 128 bits are significant. Top 2 bits must be zero.
        long hi = 0;
        long lo = 0;

        for (int i = 0; i < 26; i++) {
            char c = s.charAt(i);
            if (c >= 128) {
                throw new IllegalArgumentException("Invalid ULID character: " + c);
            }
            int val = CROCKFORD_DECODE[c];
            if (val < 0) {
                throw new IllegalArgumentException("Invalid ULID character: " + c);
            }
            if (i < 13) {
                hi = (hi << 5) | val;
            } else {
                lo = (lo << 5) | val;
            }
        }

        // 13 chars × 5 bits = 65 bits for hi, 13 chars × 5 bits = 65 bits for lo
        // hi contains bits 127..63, lo contains bits 64..0
        // We need to shift: hi has 65 bits, top bit is bit 128 (overflow for 128-bit value)
        // Actually, let's use a simpler approach: accumulate 130 bits properly

        // Re-decode with proper 128-bit accumulation
        hi = 0;
        lo = 0;
        for (int i = 0; i < 26; i++) {
            int val = CROCKFORD_DECODE[s.charAt(i)];
            // Shift the 128-bit value left by 5, then OR in val
            hi = (hi << 5) | (lo >>> 59);
            lo = (lo << 5) | val;
        }
        // After 26 iterations we have 130 bits. Top 2 bits (bits 129,128) must be zero.
        if ((hi >>> 62) != 0) {
            throw new IllegalArgumentException("ULID overflow: value exceeds 128 bits");
        }
        // Now hi has bits 127..64 in positions 63..0, lo has bits 63..0
        // But we shifted hi by (hi << 5 | lo>>>59) per iteration across 26 iterations,
        // so hi holds the top 64 bits of the 128-bit result, lo holds the bottom 64.
        // However, we accumulated 130 bits total. hi actually has 66 bits of data,
        // but we verified bits 129-128 are zero, so the top 64 significant bits are
        // in hi[63:0] and lo[63:0] has the bottom 64.

        for (int i = 0; i < 8; i++) {
            data[i] = (byte) (hi >>> (56 - i * 8));
        }
        for (int i = 0; i < 8; i++) {
            data[8 + i] = (byte) (lo >>> (56 - i * 8));
        }
        return new Ulid(data);
    }

    /// Create a ULID from raw bytes.
    ///
    /// @param bytes a 16-byte array
    /// @return a new Ulid
    /// @throws IllegalArgumentException if bytes is null or not 16 bytes
    public static Ulid of(byte[] bytes) {
        if (bytes == null || bytes.length != 16) {
            throw new IllegalArgumentException(
                "ULID bytes must be exactly 16 bytes, got: " + (bytes == null ? "null" : bytes.length)
            );
        }
        return new Ulid(bytes.clone());
    }

    /// Return the raw 16-byte representation.
    ///
    /// @return a defensive copy of the underlying bytes
    public byte[] toBytes() {
        return data.clone();
    }

    /// Extract the Unix timestamp in milliseconds from the first 48 bits.
    ///
    /// @return the timestamp in milliseconds since the Unix epoch
    public long timestamp() {
        long ts = 0;
        for (int i = 0; i < 6; i++) {
            ts = (ts << 8) | (data[i] & 0xFF);
        }
        return ts;
    }

    /// Return the Crockford Base32 string representation (26 characters, uppercase).
    @Override
    public String toString() {
        long hi = 0;
        long lo = 0;
        for (int i = 0; i < 8; i++) {
            hi = (hi << 8) | (data[i] & 0xFF);
        }
        for (int i = 0; i < 8; i++) {
            lo = (lo << 8) | (data[8 + i] & 0xFF);
        }

        char[] chars = new char[26];
        // Encode 128 bits as 26 × 5-bit Crockford Base32 chars
        // We work from the least significant bits upward
        for (int i = 25; i >= 13; i--) {
            chars[i] = CROCKFORD_ENCODE[(int) (lo & 0x1F)];
            lo >>>= 5;
        }
        // lo now has its remaining bits; combine with hi
        // After extracting 13 chars from lo (65 bits), lo should be 0
        // but hi has 64 bits. We need bits from hi shifted.
        // At position 13 we extracted 65 bits from lo. lo has -1 bits remaining
        // so some bits from hi spilled into the lo extraction.
        // Let me reconsider: 128 bits / 5 = 25.6, so 26 chars.
        // Bottom 65 bits (13 chars) come from lo. Top 63 bits (remaining 13 chars) from hi+lo spillover.

        // Simpler approach: extract from combined 128 bits
        hi = 0;
        lo = 0;
        for (int i = 0; i < 8; i++) {
            hi = (hi << 8) | (data[i] & 0xFF);
        }
        for (int i = 0; i < 8; i++) {
            lo = (lo << 8) | (data[8 + i] & 0xFF);
        }

        // Extract 5-bit groups from the 128-bit value, MSB first
        // Bit 127 is MSB of hi. We need 26 groups of 5 bits = 130 bits,
        // so the top 2 bits are zero padding.
        for (int i = 25; i >= 0; i--) {
            chars[i] = CROCKFORD_ENCODE[(int) (lo & 0x1F)];
            // Shift 128-bit value right by 5
            lo = (lo >>> 5) | (hi << 59);
            hi >>>= 5;
        }
        return new String(chars);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Ulid)) return false;
        return Arrays.equals(data, ((Ulid) o).data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }

    @Override
    public int compareTo(Ulid other) {
        for (int i = 0; i < 16; i++) {
            int cmp = Integer.compare(data[i] & 0xFF, other.data[i] & 0xFF);
            if (cmp != 0) return cmp;
        }
        return 0;
    }
}
