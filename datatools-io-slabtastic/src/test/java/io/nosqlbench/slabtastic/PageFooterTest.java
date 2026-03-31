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

class PageFooterTest implements SlabConstants {

    @Test
    void roundTripBasicValues() {
        PageFooter footer = new PageFooter(42L, 10, 512, PAGE_TYPE_DATA, NAMESPACE_DEFAULT, (short) FOOTER_V1_SIZE);
        ByteBuffer buf = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
        footer.writeTo(buf, 0);

        PageFooter read = PageFooter.readFrom(buf, 0);
        assertThat(read).isEqualTo(footer);
    }

    @Test
    void roundTripNegativeOrdinal() {
        PageFooter footer = new PageFooter(-100L, 5, 1024, PAGE_TYPE_DATA, NAMESPACE_DEFAULT, (short) FOOTER_V1_SIZE);
        ByteBuffer buf = ByteBuffer.allocate(16);
        footer.writeTo(buf, 0);

        PageFooter read = PageFooter.readFrom(buf, 0);
        assertThat(read.startOrdinal()).isEqualTo(-100L);
        assertThat(read).isEqualTo(footer);
    }

    @Test
    void roundTripMaxOrdinal() {
        long maxOrd = MAX_ORDINAL; // 2^39 - 1
        PageFooter footer = new PageFooter(maxOrd, 1, 512, PAGE_TYPE_DATA, NAMESPACE_DEFAULT, (short) FOOTER_V1_SIZE);
        ByteBuffer buf = ByteBuffer.allocate(16);
        footer.writeTo(buf, 0);

        PageFooter read = PageFooter.readFrom(buf, 0);
        assertThat(read.startOrdinal()).isEqualTo(maxOrd);
    }

    @Test
    void roundTripMinOrdinal() {
        long minOrd = MIN_ORDINAL; // -2^39
        PageFooter footer = new PageFooter(minOrd, 1, 512, PAGE_TYPE_DATA, NAMESPACE_DEFAULT, (short) FOOTER_V1_SIZE);
        ByteBuffer buf = ByteBuffer.allocate(16);
        footer.writeTo(buf, 0);

        PageFooter read = PageFooter.readFrom(buf, 0);
        assertThat(read.startOrdinal()).isEqualTo(minOrd);
    }

    @Test
    void roundTripMaxRecordCount() {
        int maxRec = MAX_RECORD_COUNT; // 2^24 - 1
        PageFooter footer = new PageFooter(0L, maxRec, 512, PAGE_TYPE_DATA, NAMESPACE_DEFAULT, (short) FOOTER_V1_SIZE);
        ByteBuffer buf = ByteBuffer.allocate(16);
        footer.writeTo(buf, 0);

        PageFooter read = PageFooter.readFrom(buf, 0);
        assertThat(read.recordCount()).isEqualTo(maxRec);
    }

    @Test
    void validateRejectsInvalidVersion() {
        PageFooter footer = new PageFooter(0L, 1, 512, PAGE_TYPE_DATA, NAMESPACE_INVALID, (short) FOOTER_V1_SIZE);
        assertThatThrownBy(footer::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("namespace index");
    }

    @Test
    void validateRejectsInvalidPageType() {
        PageFooter footer = new PageFooter(0L, 1, 512, PAGE_TYPE_INVALID, NAMESPACE_DEFAULT, (short) FOOTER_V1_SIZE);
        assertThatThrownBy(footer::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("page type");
    }

    @Test
    void validateRejectsSmallPageSize() {
        PageFooter footer = new PageFooter(0L, 1, 256, PAGE_TYPE_DATA, NAMESPACE_DEFAULT, (short) FOOTER_V1_SIZE);
        assertThatThrownBy(footer::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("below minimum");
    }

    @Test
    void validateRejectsUnalignedPageSize() {
        PageFooter footer = new PageFooter(0L, 1, 600, PAGE_TYPE_DATA, NAMESPACE_DEFAULT, (short) FOOTER_V1_SIZE);
        assertThatThrownBy(footer::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("not a multiple");
    }

    @Test
    void roundTripPagesPageType() {
        PageFooter footer = new PageFooter(0L, 3, 512, PAGE_TYPE_PAGES_PAGE, NAMESPACE_DEFAULT, (short) FOOTER_V1_SIZE);
        ByteBuffer buf = ByteBuffer.allocate(16);
        footer.writeTo(buf, 0);

        PageFooter read = PageFooter.readFrom(buf, 0);
        assertThat(read.pageType()).isEqualTo(PAGE_TYPE_PAGES_PAGE);
        read.validate(); // should not throw
    }

    @Test
    void roundTripAtNonZeroOffset() {
        PageFooter footer = new PageFooter(7L, 2, 1024, PAGE_TYPE_DATA, NAMESPACE_DEFAULT, (short) FOOTER_V1_SIZE);
        ByteBuffer buf = ByteBuffer.allocate(32);
        footer.writeTo(buf, 10);

        PageFooter read = PageFooter.readFrom(buf, 10);
        assertThat(read).isEqualTo(footer);
    }
}
