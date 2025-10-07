package io.nosqlbench.vectordata.merklev2;

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

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

class Merklev2FooterTest {

    @Test
    void testCreateFooter() {
        // Create test data
        long chunkSize = 4096;
        long totalContentSize = 1024 * 1024;
        int totalChunks = 256;
        int leafCount = 256;
        int capLeaf = 256;
        int nodeCount = 511;
        int offset = 255;
        int internalNodeCount = 255;
        int bitSetSize = 32;

        // Create footer
        Merklev2Footer footer = Merklev2Footer.create(
                chunkSize,
                totalContentSize,
                totalChunks,
                leafCount,
                capLeaf,
                nodeCount,
                offset,
                internalNodeCount,
                bitSetSize
        );

        // Verify properties
        assertEquals(chunkSize, footer.chunkSize());
        assertEquals(totalContentSize, footer.totalContentSize());
        assertEquals(totalChunks, footer.totalChunks());
        assertEquals(leafCount, footer.leafCount());
        assertEquals(capLeaf, footer.capLeaf());
        assertEquals(nodeCount, footer.nodeCount());
        assertEquals(offset, footer.offset());
        assertEquals(internalNodeCount, footer.internalNodeCount());
        assertEquals(bitSetSize, footer.bitSetSize());
        assertEquals(Merklev2Footer.FIXED_FOOTER_SIZE, footer.footerLength());
    }

    @Test
    void testSerializeDeserialize() {
        // Create test data
        long chunkSize = 4096;
        long totalContentSize = 1024 * 1024;
        int totalChunks = 256;
        int leafCount = 256;
        int capLeaf = 256;
        int nodeCount = 511;
        int offset = 255;
        int internalNodeCount = 255;
        int bitSetSize = 32;

        // Create footer
        Merklev2Footer original = Merklev2Footer.create(
                chunkSize,
                totalContentSize,
                totalChunks,
                leafCount,
                capLeaf,
                nodeCount,
                offset,
                internalNodeCount,
                bitSetSize
        );

        // Serialize
        ByteBuffer buffer = original.toByteBuffer();

        // Deserialize
        Merklev2Footer deserialized = Merklev2Footer.fromByteBuffer(buffer);

        // Verify properties
        assertEquals(original.chunkSize(), deserialized.chunkSize());
        assertEquals(original.totalContentSize(), deserialized.totalContentSize());
        assertEquals(original.totalChunks(), deserialized.totalChunks());
        assertEquals(original.leafCount(), deserialized.leafCount());
        assertEquals(original.capLeaf(), deserialized.capLeaf());
        assertEquals(original.nodeCount(), deserialized.nodeCount());
        assertEquals(original.offset(), deserialized.offset());
        assertEquals(original.internalNodeCount(), deserialized.internalNodeCount());
        assertEquals(original.bitSetSize(), deserialized.bitSetSize());
        assertEquals(original.footerLength(), deserialized.footerLength());
    }

    @Test
    void testCreateFromMerkleShape() {
        // Create a MerkleShape
        long totalContentSize = 1024 * 1024;
        long chunkSize = 4096;
        BaseMerkleShape shape = new BaseMerkleShape(totalContentSize, chunkSize);

        // Create footer from shape
        Merklev2Footer footer = Merklev2Footer.create(shape);

        // Verify properties
        assertEquals(chunkSize, footer.chunkSize());
        assertEquals(totalContentSize, footer.totalContentSize());
        assertEquals(shape.getTotalChunks(), footer.totalChunks());
        assertEquals(shape.getLeafCount(), footer.leafCount());
        assertEquals(shape.getCapLeaf(), footer.capLeaf());
        assertEquals(shape.getNodeCount(), footer.nodeCount());
        assertEquals(shape.getOffset(), footer.offset());
        assertEquals(shape.getInternalNodeCount(), footer.internalNodeCount());
        assertEquals(1, footer.bitSetSize()); // Default bitSetSize
        assertEquals(Merklev2Footer.FIXED_FOOTER_SIZE, footer.footerLength());
    }

    @Test
    void testRoundTripWithMerkleShape() {
        // Create a MerkleShape
        long totalContentSize = 1024 * 1024;
        long chunkSize = 4096;
        BaseMerkleShape originalShape = new BaseMerkleShape(totalContentSize, chunkSize);

        // Create footer from shape
        Merklev2Footer footer = Merklev2Footer.create(originalShape);

        // Serialize
        ByteBuffer buffer = footer.toByteBuffer();

        // Deserialize
        Merklev2Footer deserialized = Merklev2Footer.fromByteBuffer(buffer);

        // Convert back to MerkleShape
        MerkleShape deserializedShape = deserialized.toMerkleShape();

        // Verify shape properties
        assertEquals(originalShape.getChunkSize(), deserializedShape.getChunkSize());
        assertEquals(originalShape.getTotalContentSize(), deserializedShape.getTotalContentSize());
    }
}