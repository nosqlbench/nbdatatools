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

package io.nosqlbench.slabtastic;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.*;

class NamespacesPageEntryTest implements SlabConstants {

    @Test
    void roundTripDefaultNamespace() {
        NamespacesPageEntry entry = new NamespacesPageEntry(NAMESPACE_DEFAULT, "", 1024L);
        ByteBuffer buf = ByteBuffer.allocate(entry.serializedSize());
        entry.writeTo(buf, 0);

        NamespacesPageEntry read = NamespacesPageEntry.readFrom(buf, 0);
        assertThat(read).isEqualTo(entry);
        assertThat(read.namespaceIndex()).isEqualTo(NAMESPACE_DEFAULT);
        assertThat(read.name()).isEmpty();
        assertThat(read.pagesPageOffset()).isEqualTo(1024L);
    }

    @Test
    void roundTripNamedNamespace() {
        NamespacesPageEntry entry = new NamespacesPageEntry((byte) 2, "vectors", 2048L);
        ByteBuffer buf = ByteBuffer.allocate(entry.serializedSize());
        entry.writeTo(buf, 0);

        NamespacesPageEntry read = NamespacesPageEntry.readFrom(buf, 0);
        assertThat(read).isEqualTo(entry);
        assertThat(read.namespaceIndex()).isEqualTo((byte) 2);
        assertThat(read.name()).isEqualTo("vectors");
        assertThat(read.pagesPageOffset()).isEqualTo(2048L);
    }

    @Test
    void roundTripAtNonZeroOffset() {
        NamespacesPageEntry entry = new NamespacesPageEntry((byte) 3, "metadata", 4096L);
        ByteBuffer buf = ByteBuffer.allocate(entry.serializedSize() + 20);
        entry.writeTo(buf, 10);

        NamespacesPageEntry read = NamespacesPageEntry.readFrom(buf, 10);
        assertThat(read).isEqualTo(entry);
    }

    @Test
    void serializedSizeCorrectness() {
        // 1 (index) + 1 (name length) + 0 (empty name) + 8 (offset) = 10
        NamespacesPageEntry empty = new NamespacesPageEntry(NAMESPACE_DEFAULT, "", 0L);
        assertThat(empty.serializedSize()).isEqualTo(10);

        // 1 + 1 + 7 ("vectors") + 8 = 17
        NamespacesPageEntry named = new NamespacesPageEntry((byte) 2, "vectors", 0L);
        assertThat(named.serializedSize()).isEqualTo(17);

        // Multi-byte UTF-8 characters
        NamespacesPageEntry unicode = new NamespacesPageEntry((byte) 3, "\u00e9", 0L);
        assertThat(unicode.serializedSize()).isEqualTo(1 + 1 + 2 + 8); // é = 2 bytes in UTF-8
    }

    @Test
    void validateRejectsIndexZero() {
        NamespacesPageEntry entry = new NamespacesPageEntry(NAMESPACE_INVALID, "test", 0L);
        assertThatThrownBy(entry::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("namespace index");
    }

    @Test
    void validateRejectsNameTooLong() {
        String longName = "x".repeat(NAMESPACE_MAX_NAME_LENGTH + 1);
        NamespacesPageEntry entry = new NamespacesPageEntry(NAMESPACE_DEFAULT, longName, 0L);
        assertThatThrownBy(entry::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("too long");
    }

    @Test
    void validateAcceptsMaxLengthName() {
        String maxName = "x".repeat(NAMESPACE_MAX_NAME_LENGTH);
        NamespacesPageEntry entry = new NamespacesPageEntry(NAMESPACE_DEFAULT, maxName, 0L);
        assertThatCode(entry::validate).doesNotThrowAnyException();
    }

    @Test
    void validateAcceptsNonDefaultIndex() {
        NamespacesPageEntry entry = new NamespacesPageEntry((byte) 127, "ns", 0L);
        assertThatCode(entry::validate).doesNotThrowAnyException();
    }
}
