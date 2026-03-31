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
import java.nio.ByteOrder;

import static org.assertj.core.api.Assertions.*;

class PageHeaderTest implements SlabConstants {

    @Test
    void roundTrip() {
        PageHeader header = new PageHeader(4096);
        ByteBuffer buf = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        header.writeTo(buf, 0);

        PageHeader read = PageHeader.readFrom(buf, 0);
        assertThat(read.pageSize()).isEqualTo(4096);
    }

    @Test
    void badMagicRejected() {
        ByteBuffer buf = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(0, 0xDEADBEEF);
        buf.putInt(4, 512);

        assertThatThrownBy(() -> PageHeader.readFrom(buf, 0))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Bad magic");
    }

    @Test
    void magicBytesAreSlabInFileOrder() {
        PageHeader header = new PageHeader(512);
        ByteBuffer buf = ByteBuffer.allocate(8);
        header.writeTo(buf, 0);

        // In file order, the first four bytes should spell "SLAB"
        assertThat(buf.get(0)).isEqualTo((byte) 'S');
        assertThat(buf.get(1)).isEqualTo((byte) 'L');
        assertThat(buf.get(2)).isEqualTo((byte) 'A');
        assertThat(buf.get(3)).isEqualTo((byte) 'B');
    }

    @Test
    void roundTripAtNonZeroOffset() {
        PageHeader header = new PageHeader(2048);
        ByteBuffer buf = ByteBuffer.allocate(24);
        header.writeTo(buf, 8);

        PageHeader read = PageHeader.readFrom(buf, 8);
        assertThat(read.pageSize()).isEqualTo(2048);
    }

    @Test
    void minPageSize() {
        PageHeader header = new PageHeader(512);
        ByteBuffer buf = ByteBuffer.allocate(8);
        header.writeTo(buf, 0);

        PageHeader read = PageHeader.readFrom(buf, 0);
        assertThat(read.pageSize()).isEqualTo(512);
    }
}
