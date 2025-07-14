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


import io.nosqlbench.vectordata.merkle.MerkleFooter;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class MerkleFooterTest {

    @Test
    void testCreateFooter() {
        // Create test data
        long chunkSize = 4096;
        long totalSize = 1024 * 1024;

        // Create footer
        MerkleFooter footer = MerkleFooter.create(chunkSize, totalSize);

        // Verify properties
        assertEquals(chunkSize, footer.chunkSize());
        assertEquals(totalSize, footer.totalSize());
        assertEquals(MerkleFooter.FIXED_FOOTER_SIZE, footer.footerLength());
    }

    @Test
    void testSerializeDeserialize() {
        // Create test data
        long chunkSize = 4096;
        long totalSize = 1024 * 1024;

        // Create footer
        MerkleFooter original = MerkleFooter.create(chunkSize, totalSize);

        // Serialize
        ByteBuffer buffer = original.toByteBuffer();

        // Deserialize
        MerkleFooter deserialized = MerkleFooter.fromByteBuffer(buffer);

        // Verify properties
        assertEquals(original.chunkSize(), deserialized.chunkSize());
        assertEquals(original.totalSize(), deserialized.totalSize());
        assertEquals(original.footerLength(), deserialized.footerLength());
    }
}
