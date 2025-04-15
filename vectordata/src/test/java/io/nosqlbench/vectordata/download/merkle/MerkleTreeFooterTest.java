package io.nosqlbench.vectordata.download.merkle;

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


import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class MerkleTreeFooterTest {

    @Test
    void testEncodeAndDecode() {
        byte[] fileDigest = new byte[32]; // SHA-256 digest length
        for (int i = 0; i < fileDigest.length; i++) {
            fileDigest[i] = (byte) i;
        }

        MerkleTreeFooter original = new MerkleTreeFooter(
            MerkleTreeFooter.CURRENT_VERSION,
            (byte) 0,
            MerkleTreeFooter.HashAlgorithm.SHA_256.getValue(),
            MerkleTreeFooter.HashAlgorithm.SHA_256.getDigestLength(),
            1024L, // totalDataSize
            16,    // numberOfLeaves
            512,   // leafBoundaryTableOffset
            128,   // leafBoundaryTableLength
            fileDigest,
            (short) MerkleTreeFooter.size()
        );

        ByteBuffer encoded = original.encode();
        encoded.flip();
        MerkleTreeFooter decoded = MerkleTreeFooter.decode(encoded);

        assertAll(
            () -> assertEquals(original.version(), decoded.version()),
            () -> assertEquals(original.flags(), decoded.flags()),
            () -> assertEquals(original.hashAlgorithmId(), decoded.hashAlgorithmId()),
            () -> assertEquals(original.hashDigestLength(), decoded.hashDigestLength()),
            () -> assertEquals(original.totalDataSize(), decoded.totalDataSize()),
            () -> assertEquals(original.numberOfLeaves(), decoded.numberOfLeaves()),
            () -> assertEquals(original.leafBoundaryTableOffset(), decoded.leafBoundaryTableOffset()),
            () -> assertEquals(original.leafBoundaryTableLength(), decoded.leafBoundaryTableLength()),
            () -> assertArrayEquals(original.fileDigest(), decoded.fileDigest()),
            () -> assertEquals(original.footerLength(), decoded.footerLength())
        );
    }

    @Test
    void testFooterSize() {
        int expectedSize = 1 + 1 + 1 + 1 + 8 + 4 + 4 + 4 + 32 + 2; // 58 bytes
        assertEquals(expectedSize, MerkleTreeFooter.size());
    }

    @ParameterizedTest
    @EnumSource(MerkleTreeFooter.HashAlgorithm.class)
    void testHashAlgorithms(MerkleTreeFooter.HashAlgorithm algorithm) {
        byte value = algorithm.getValue();
        assertEquals(algorithm, MerkleTreeFooter.HashAlgorithm.fromValue(value));
        
        switch (algorithm) {
            case SHA_256 -> assertEquals(32, algorithm.getDigestLength());
            case SHA_512 -> assertEquals(64, algorithm.getDigestLength());
            default -> fail("Unexpected hash algorithm: " + algorithm);
        }
    }

    @Test
    void testInvalidHashAlgorithm() {
        assertThrows(IllegalArgumentException.class,
            () -> MerkleTreeFooter.HashAlgorithm.fromValue((byte) 99));
    }

    @Test
    void testFlags() {
        byte flags = (byte) (MerkleTreeFooter.Flags.LITTLE_ENDIAN | MerkleTreeFooter.Flags.COMPRESSED);
        
        assertTrue((flags & MerkleTreeFooter.Flags.LITTLE_ENDIAN) != 0);
        assertTrue((flags & MerkleTreeFooter.Flags.COMPRESSED) != 0);
        
        flags = 0;
        assertFalse((flags & MerkleTreeFooter.Flags.LITTLE_ENDIAN) != 0);
        assertFalse((flags & MerkleTreeFooter.Flags.COMPRESSED) != 0);
    }

    @Test
    void testValidation() {
        byte[] fileDigest = new byte[32];
        
        // Test invalid version
        MerkleTreeFooter invalidVersion = new MerkleTreeFooter(
            (byte) 0x99, // Invalid version
            (byte) 0,
            MerkleTreeFooter.HashAlgorithm.SHA_256.getValue(),
            (byte) 32,
            1024L,
            16,
            512,
            128,
            fileDigest,
            (short) MerkleTreeFooter.size()
        );
        assertThrows(IllegalStateException.class, invalidVersion::validate);

        // Test mismatched digest length
        MerkleTreeFooter invalidDigestLength = new MerkleTreeFooter(
            MerkleTreeFooter.CURRENT_VERSION,
            (byte) 0,
            MerkleTreeFooter.HashAlgorithm.SHA_256.getValue(),
            (byte) 64, // Wrong digest length for SHA-256
            1024L,
            16,
            512,
            128,
            fileDigest,
            (short) MerkleTreeFooter.size()
        );
        assertThrows(IllegalStateException.class, invalidDigestLength::validate);

        // Test invalid file digest length
        byte[] wrongSizeDigest = new byte[16]; // Wrong size
        MerkleTreeFooter invalidFileDigest = new MerkleTreeFooter(
            MerkleTreeFooter.CURRENT_VERSION,
            (byte) 0,
            MerkleTreeFooter.HashAlgorithm.SHA_256.getValue(),
            (byte) 32,
            1024L,
            16,
            512,
            128,
            wrongSizeDigest,
            (short) MerkleTreeFooter.size()
        );
        assertThrows(IllegalStateException.class, invalidFileDigest::validate);
    }

    @Test
    void testEncodingBuffer() {
        byte[] fileDigest = new byte[32];
        MerkleTreeFooter footer = new MerkleTreeFooter(
            MerkleTreeFooter.CURRENT_VERSION,
            (byte) 0,
            MerkleTreeFooter.HashAlgorithm.SHA_256.getValue(),
            (byte) 32,
            1024L,
            16,
            512,
            128,
            fileDigest,
            (short) MerkleTreeFooter.size()
        );

        // Test encode() creates new buffer
        ByteBuffer buffer1 = footer.encode();
        ByteBuffer buffer2 = footer.encode();
        assertNotSame(buffer1, buffer2);
        
        // Test encodeTo() with existing buffer
        ByteBuffer customBuffer = ByteBuffer.allocate(MerkleTreeFooter.size());
        ByteBuffer result = footer.encodeTo(customBuffer);
        assertSame(customBuffer, result);
        
        // Verify content
        customBuffer.flip();
        MerkleTreeFooter decoded = MerkleTreeFooter.decode(customBuffer);
        assertEquals(footer, decoded);
    }

    @Test
    void testEqualsAndHashCode() {
        byte[] digest1 = new byte[32];
        byte[] digest2 = new byte[32];
        Arrays.fill(digest1, (byte) 1);
        Arrays.fill(digest2, (byte) 1);
        
        MerkleTreeFooter footer1 = new MerkleTreeFooter(
            MerkleTreeFooter.CURRENT_VERSION,
            (byte) 0,
            MerkleTreeFooter.HashAlgorithm.SHA_256.getValue(),
            (byte) 32,
            1024L,
            16,
            512,
            128,
            digest1,
            (short) MerkleTreeFooter.size()
        );

        MerkleTreeFooter footer2 = new MerkleTreeFooter(
            MerkleTreeFooter.CURRENT_VERSION,
            (byte) 0,
            MerkleTreeFooter.HashAlgorithm.SHA_256.getValue(),
            (byte) 32,
            1024L,
            16,
            512,
            128,
            digest2,
            (short) MerkleTreeFooter.size()
        );

        // Test equality
        assertEquals(footer1, footer2);
        assertEquals(footer1.hashCode(), footer2.hashCode());

        // Test inequality with different digest
        byte[] differentDigest = new byte[32];
        Arrays.fill(differentDigest, (byte) 2);
        MerkleTreeFooter footer3 = new MerkleTreeFooter(
            MerkleTreeFooter.CURRENT_VERSION,
            (byte) 0,
            MerkleTreeFooter.HashAlgorithm.SHA_256.getValue(),
            (byte) 32,
            1024L,
            16,
            512,
            128,
            differentDigest,
            (short) MerkleTreeFooter.size()
        );
        assertNotEquals(footer1, footer3);
        assertNotEquals(footer1.hashCode(), footer3.hashCode());

        // Test inequality with different field
        MerkleTreeFooter footer4 = new MerkleTreeFooter(
            MerkleTreeFooter.CURRENT_VERSION,
            (byte) 0,
            MerkleTreeFooter.HashAlgorithm.SHA_256.getValue(),
            (byte) 32,
            1024L,
            17, // Different number of leaves
            512,
            128,
            digest1,
            (short) MerkleTreeFooter.size()
        );
        assertNotEquals(footer1, footer4);
        assertNotEquals(footer1.hashCode(), footer4.hashCode());
    }
}
