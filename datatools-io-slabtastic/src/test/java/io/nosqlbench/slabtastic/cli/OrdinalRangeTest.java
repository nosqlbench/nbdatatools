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

package io.nosqlbench.slabtastic.cli;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

/// Tests for {@link OrdinalRange} and its picocli converter.
class OrdinalRangeTest {

    private final OrdinalRange.Converter converter = new OrdinalRange.Converter();

    @Test
    void parseSingleCount() {
        OrdinalRange.Range r = converter.convert("10");
        assertThat(r.start()).isEqualTo(0);
        assertThat(r.end()).isEqualTo(10);
    }

    @Test
    void parseDotDotClosedInterval() {
        OrdinalRange.Range r = converter.convert("5..10");
        assertThat(r.start()).isEqualTo(5);
        assertThat(r.end()).isEqualTo(11); // inclusive → exclusive
    }

    @Test
    void parseHalfOpenBracketComma() {
        OrdinalRange.Range r = converter.convert("[5,10)");
        assertThat(r.start()).isEqualTo(5);
        assertThat(r.end()).isEqualTo(10);
    }

    @Test
    void parseClosedBracketComma() {
        OrdinalRange.Range r = converter.convert("[5,10]");
        assertThat(r.start()).isEqualTo(5);
        assertThat(r.end()).isEqualTo(11); // inclusive → exclusive
    }

    @Test
    void parseOpenBracketComma() {
        OrdinalRange.Range r = converter.convert("(5,10)");
        assertThat(r.start()).isEqualTo(6); // exclusive → inclusive
        assertThat(r.end()).isEqualTo(10);
    }

    @Test
    void parseHalfOpenRight() {
        OrdinalRange.Range r = converter.convert("(5,10]");
        assertThat(r.start()).isEqualTo(6); // exclusive → inclusive
        assertThat(r.end()).isEqualTo(11); // inclusive → exclusive
    }

    @Test
    void parseSingleBracket() {
        OrdinalRange.Range r = converter.convert("[7]");
        assertThat(r.start()).isEqualTo(7);
        assertThat(r.end()).isEqualTo(8);
    }

    @Test
    void parseBracketWithDotDot() {
        OrdinalRange.Range r = converter.convert("[3..8)");
        assertThat(r.start()).isEqualTo(3);
        assertThat(r.end()).isEqualTo(8);
    }

    @Test
    void rangeContains() {
        OrdinalRange.Range r = new OrdinalRange.Range(5, 10);
        assertThat(r.contains(4)).isFalse();
        assertThat(r.contains(5)).isTrue();
        assertThat(r.contains(9)).isTrue();
        assertThat(r.contains(10)).isFalse();
    }

    @Test
    void rangeSize() {
        OrdinalRange.Range r = new OrdinalRange.Range(5, 10);
        assertThat(r.size()).isEqualTo(5);
    }

    @Test
    void rejectEmptyString() {
        assertThatThrownBy(() -> converter.convert(""))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectNegativeStart() {
        assertThatThrownBy(() -> new OrdinalRange.Range(-1, 5))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectEndNotGreaterThanStart() {
        assertThatThrownBy(() -> new OrdinalRange.Range(5, 5))
            .isInstanceOf(IllegalArgumentException.class);
    }
}
