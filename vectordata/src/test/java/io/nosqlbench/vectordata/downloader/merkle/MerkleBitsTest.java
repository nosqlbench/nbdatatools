package io.nosqlbench.vectordata.downloader.merkle;

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


import io.nosqlbench.vectordata.merkle.MerkleBits;
import org.junit.jupiter.api.Test;

import java.util.BitSet;

import static org.junit.jupiter.api.Assertions.*;

public class MerkleBitsTest {

    @Test
    void testReadOnlyOperations() {
        // Create a BitSet with some bits set
        BitSet originalBits = new BitSet(100);
        originalBits.set(10);
        originalBits.set(20);
        originalBits.set(30);

        // Create a MerkleBits from the BitSet
        MerkleBits merkleBits = new MerkleBits(originalBits);

        // Test basic read operations
        assertTrue(merkleBits.get(10));
        assertTrue(merkleBits.get(20));
        assertTrue(merkleBits.get(30));
        assertFalse(merkleBits.get(15));

        // Test nextSetBit and nextClearBit
        assertEquals(10, merkleBits.nextSetBit(0));
        assertEquals(20, merkleBits.nextSetBit(11));
        assertEquals(11, merkleBits.nextClearBit(10));

        // Test previousSetBit and previousClearBit
        assertEquals(20, merkleBits.previousSetBit(25));
        assertEquals(19, merkleBits.previousClearBit(20));

        // Test cardinality and size
        assertEquals(3, merkleBits.cardinality());
        assertTrue(merkleBits.size() >= 31); // Size is implementation-dependent but must be at least 31

        // Test isEmpty
        assertFalse(merkleBits.isEmpty());
        assertTrue(new MerkleBits(new BitSet()).isEmpty());
    }

    @Test
    void testLogicalOperations() {
        // Create two BitSets
        BitSet bits1 = new BitSet(100);
        bits1.set(10);
        bits1.set(20);
        bits1.set(30);

        BitSet bits2 = new BitSet(100);
        bits2.set(20);
        bits2.set(40);
        bits2.set(50);

        // Create MerkleBits from the BitSets
        MerkleBits merkleBits1 = new MerkleBits(bits1);
        MerkleBits merkleBits2 = new MerkleBits(bits2);

        // Test intersects
        assertTrue(merkleBits1.intersects(merkleBits2));

        // Test AND operation
        MerkleBits andResult = merkleBits1.andResult(bits2);
        assertTrue(andResult.get(20));
        assertFalse(andResult.get(10));
        assertFalse(andResult.get(30));
        assertFalse(andResult.get(40));
        assertFalse(andResult.get(50));

        // Test OR operation
        MerkleBits orResult = merkleBits1.orResult(bits2);
        assertTrue(orResult.get(10));
        assertTrue(orResult.get(20));
        assertTrue(orResult.get(30));
        assertTrue(orResult.get(40));
        assertTrue(orResult.get(50));

        // Test XOR operation
        MerkleBits xorResult = merkleBits1.xorResult(bits2);
        assertTrue(xorResult.get(10));
        assertFalse(xorResult.get(20));
        assertTrue(xorResult.get(30));
        assertTrue(xorResult.get(40));
        assertTrue(xorResult.get(50));

        // Test NOT operation
        MerkleBits notResult = merkleBits1.notResult();
        assertFalse(notResult.get(10));
        assertFalse(notResult.get(20));
        assertFalse(notResult.get(30));
        assertTrue(notResult.get(15));
    }

    @Test
    void testCopyOf() {
        // Create a BitSet with some bits set
        BitSet originalBits = new BitSet(100);
        originalBits.set(10);
        originalBits.set(20);
        originalBits.set(30);

        // Create a MerkleBits using copyOf
        MerkleBits merkleBits = MerkleBits.copyOf(originalBits);

        // Test that the copy has the same bits set
        assertTrue(merkleBits.get(10));
        assertTrue(merkleBits.get(20));
        assertTrue(merkleBits.get(30));

        // Modify the original BitSet
        originalBits.set(40);

        // Verify that the MerkleBits is not affected
        assertFalse(merkleBits.get(40));
    }

    @Test
    void testMutationMethodsThrowExceptions() {
        MerkleBits merkleBits = new MerkleBits(100);

        // Test set methods
        assertThrows(UnsupportedOperationException.class, () -> merkleBits.set(10));
        assertThrows(UnsupportedOperationException.class, () -> merkleBits.set(10, true));
        assertThrows(UnsupportedOperationException.class, () -> merkleBits.set(10, 20));
        assertThrows(UnsupportedOperationException.class, () -> merkleBits.set(10, 20, true));

        // Test clear methods
        assertThrows(UnsupportedOperationException.class, () -> merkleBits.clear(10));
        assertThrows(UnsupportedOperationException.class, () -> merkleBits.clear(10, 20));
        assertThrows(UnsupportedOperationException.class, () -> merkleBits.clear());

        // Test flip methods
        assertThrows(UnsupportedOperationException.class, () -> merkleBits.flip(10));
        assertThrows(UnsupportedOperationException.class, () -> merkleBits.flip(10, 20));

        // Test logical operation methods
        BitSet otherBits = new BitSet(100);
        assertThrows(UnsupportedOperationException.class, () -> merkleBits.and(otherBits));
        assertThrows(UnsupportedOperationException.class, () -> merkleBits.or(otherBits));
        assertThrows(UnsupportedOperationException.class, () -> merkleBits.xor(otherBits));
        assertThrows(UnsupportedOperationException.class, () -> merkleBits.andNot(otherBits));
    }

    @Test
    void testEqualsAndHashCode() {
        // Create two identical BitSets
        BitSet bits1 = new BitSet(100);
        bits1.set(10);
        bits1.set(20);

        BitSet bits2 = new BitSet(100);
        bits2.set(10);
        bits2.set(20);

        // Create MerkleBits from the BitSets
        MerkleBits merkleBits1 = new MerkleBits(bits1);
        MerkleBits merkleBits2 = new MerkleBits(bits2);

        // Test equals and hashCode
        assertEquals(merkleBits1, merkleBits2);
        assertEquals(merkleBits1.hashCode(), merkleBits2.hashCode());

        // Modify one of the BitSets
        bits2.set(30);
        MerkleBits merkleBits3 = new MerkleBits(bits2);

        // Test that they are no longer equal
        assertNotEquals(merkleBits1, merkleBits3);
    }
}
