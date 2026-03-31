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
import java.util.List;

import static org.assertj.core.api.Assertions.*;

class SlabPageTest implements SlabConstants {

    @Test
    void buildSerializeAndParseBack() {
        List<byte[]> records = List.of(
            "hello".getBytes(),
            "world".getBytes(),
            "slab".getBytes()
        );
        SlabPage page = new SlabPage(100L, PAGE_TYPE_DATA, records);

        ByteBuffer buf = page.toByteBuffer();
        assertThat(buf.remaining()).isEqualTo(page.serializedSize());

        SlabPage parsed = SlabPage.parseFrom(buf);
        assertThat(parsed.recordCount()).isEqualTo(3);
        assertThat(parsed.startOrdinal()).isEqualTo(100L);
        assertThat(parsed.pageType()).isEqualTo(PAGE_TYPE_DATA);

        for (int i = 0; i < records.size(); i++) {
            ByteBuffer rec = parsed.getRecord(i);
            byte[] bytes = new byte[rec.remaining()];
            rec.get(bytes);
            assertThat(bytes).isEqualTo(records.get(i));
        }
    }

    @Test
    void pageSizeIsMultipleOf512() {
        List<byte[]> records = List.of("a".getBytes());
        SlabPage page = new SlabPage(0L, PAGE_TYPE_DATA, records);
        assertThat(page.serializedSize() % PAGE_ALIGNMENT).isZero();
    }

    @Test
    void headerAndFooterPageSizeMatch() {
        List<byte[]> records = List.of("test".getBytes(), "data".getBytes());
        SlabPage page = new SlabPage(0L, PAGE_TYPE_DATA, records);
        ByteBuffer buf = page.toByteBuffer();

        PageHeader header = PageHeader.readFrom(buf, 0);
        PageFooter footer = PageFooter.readFrom(buf, page.serializedSize() - FOOTER_V1_SIZE);

        assertThat(header.pageSize()).isEqualTo(footer.pageSize());
        assertThat(header.pageSize()).isEqualTo(page.serializedSize());
    }

    @Test
    void fencePostOffsets() {
        byte[] rec0 = new byte[10];
        byte[] rec1 = new byte[20];
        byte[] rec2 = new byte[30];
        List<byte[]> records = List.of(rec0, rec1, rec2);
        SlabPage page = new SlabPage(0L, PAGE_TYPE_DATA, records);
        ByteBuffer buf = page.toByteBuffer();

        int pageSz = page.serializedSize();
        int offsetsStart = pageSz - FOOTER_V1_SIZE - 4 * OFFSET_ENTRY_SIZE;
        ByteBuffer le = buf.duplicate().order(ByteOrder.LITTLE_ENDIAN);

        // offsets[0] should be HEADER_SIZE
        assertThat(le.getInt(offsetsStart)).isEqualTo(HEADER_SIZE);
        // offsets[1] = HEADER_SIZE + 10
        assertThat(le.getInt(offsetsStart + 4)).isEqualTo(HEADER_SIZE + 10);
        // offsets[2] = HEADER_SIZE + 30
        assertThat(le.getInt(offsetsStart + 8)).isEqualTo(HEADER_SIZE + 30);
        // offsets[3] = HEADER_SIZE + 60
        assertThat(le.getInt(offsetsStart + 12)).isEqualTo(HEADER_SIZE + 60);
    }

    @Test
    void emptyPageRoundTrips() {
        SlabPage page = new SlabPage(0L, PAGE_TYPE_PAGES_PAGE, List.of());
        ByteBuffer buf = page.toByteBuffer();
        SlabPage parsed = SlabPage.parseFrom(buf);
        assertThat(parsed.recordCount()).isZero();
        assertThat(parsed.serializedSize() % PAGE_ALIGNMENT).isZero();
    }

    @Test
    void largeRecordsRoundTrip() {
        byte[] big = new byte[1000];
        for (int i = 0; i < big.length; i++) {
            big[i] = (byte) (i & 0xFF);
        }
        SlabPage page = new SlabPage(0L, PAGE_TYPE_DATA, List.of(big));
        ByteBuffer buf = page.toByteBuffer();
        SlabPage parsed = SlabPage.parseFrom(buf);

        ByteBuffer rec = parsed.getRecord(0);
        byte[] got = new byte[rec.remaining()];
        rec.get(got);
        assertThat(got).isEqualTo(big);
    }

    @Test
    void getRecordOutOfBoundsThrows() {
        SlabPage page = new SlabPage(0L, PAGE_TYPE_DATA, List.of("x".getBytes()));
        assertThatThrownBy(() -> page.getRecord(1))
            .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> page.getRecord(-1))
            .isInstanceOf(IndexOutOfBoundsException.class);
    }
}
