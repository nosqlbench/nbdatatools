/*
 * Copyright (c) nosqlbench
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.nosqlbench.command.common;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import picocli.CommandLine;

import static org.assertj.core.api.Assertions.*;

@DisplayName("RangeOption")
class RangeOptionTest {

    @Nested
    @DisplayName("Range Record")
    class RangeRecordTest {

        @Test
        @DisplayName("should create valid range with start < end")
        void shouldCreateValidRange() {
            RangeOption.Range range = new RangeOption.Range(10, 20);

            assertThat(range.start()).isEqualTo(10);
            assertThat(range.end()).isEqualTo(20);
            assertThat(range.size()).isEqualTo(10);
        }

        @Test
        @DisplayName("should reject negative start")
        void shouldRejectNegativeStart() {
            assertThatThrownBy(() -> new RangeOption.Range(-1, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be non-negative");
        }

        @Test
        @DisplayName("should reject start >= end")
        void shouldRejectStartGreaterOrEqualEnd() {
            assertThatThrownBy(() -> new RangeOption.Range(10, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be greater than start");

            assertThatThrownBy(() -> new RangeOption.Range(20, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be greater than start");
        }

        @Test
        @DisplayName("should calculate size correctly")
        void shouldCalculateSize() {
            assertThat(new RangeOption.Range(0, 100).size()).isEqualTo(100);
            assertThat(new RangeOption.Range(50, 150).size()).isEqualTo(100);
        }

        @Test
        @DisplayName("should check if start is zero")
        void shouldCheckZeroStart() {
            assertThat(new RangeOption.Range(0, 100).hasZeroStart()).isTrue();
            assertThat(new RangeOption.Range(10, 100).hasZeroStart()).isFalse();
        }

        @Test
        @DisplayName("should require zero start when needed")
        void shouldRequireZeroStart() {
            RangeOption.Range zeroStart = new RangeOption.Range(0, 100);
            assertThatNoException().isThrownBy(zeroStart::requireZeroStart);

            RangeOption.Range nonZeroStart = new RangeOption.Range(10, 100);
            assertThatThrownBy(nonZeroStart::requireZeroStart)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Range start must be 0");
        }

        @ParameterizedTest
        @CsvSource({
            "0, 10, 0, true",
            "0, 10, 5, true",
            "0, 10, 9, true",
            "0, 10, 10, false",
            "5, 15, 4, false",
            "5, 15, 5, true",
            "5, 15, 14, true",
            "5, 15, 15, false"
        })
        @DisplayName("should check if index is contained")
        void shouldCheckContains(long start, long end, long index, boolean expected) {
            RangeOption.Range range = new RangeOption.Range(start, end);
            assertThat(range.contains(index)).isEqualTo(expected);
        }

        @Test
        @DisplayName("should constrain range to total count")
        void shouldConstrainToTotalCount() {
            // Range fits within total
            RangeOption.Range range1 = new RangeOption.Range(0, 50);
            assertThat(range1.constrain(100)).isEqualTo(range1);

            // Range exceeds total - should truncate end
            RangeOption.Range range2 = new RangeOption.Range(0, 150);
            RangeOption.Range constrained = range2.constrain(100);
            assertThat(constrained.start()).isEqualTo(0);
            assertThat(constrained.end()).isEqualTo(100);

            // Partial overlap
            RangeOption.Range range3 = new RangeOption.Range(50, 150);
            RangeOption.Range constrained2 = range3.constrain(100);
            assertThat(constrained2.start()).isEqualTo(50);
            assertThat(constrained2.end()).isEqualTo(100);
        }

        @Test
        @DisplayName("should format toString as [start, end)")
        void shouldFormatToString() {
            assertThat(new RangeOption.Range(0, 100).toString()).isEqualTo("[0, 100)");
            assertThat(new RangeOption.Range(10, 50).toString()).isEqualTo("[10, 50)");
        }
    }

    @Nested
    @DisplayName("RangeConverter")
    class RangeConverterTest {

        private final RangeOption.RangeConverter converter = new RangeOption.RangeConverter();

        @ParameterizedTest
        @ValueSource(strings = {"100", "1000", "5"})
        @DisplayName("should parse simple count format 'n' as [0, n)")
        void shouldParseSimpleCount(String input) {
            long n = Long.parseLong(input);
            RangeOption.Range range = converter.convert(input);

            assertThat(range.start()).isEqualTo(0);
            assertThat(range.end()).isEqualTo(n);
        }

        @ParameterizedTest
        @CsvSource({
            "10..20, 10, 21",
            "0..99, 0, 100",
            "50..100, 50, 101"
        })
        @DisplayName("should parse inclusive range 'm..n' as [m, n+1)")
        void shouldParseInclusiveRange(String input, long expectedStart, long expectedEnd) {
            RangeOption.Range range = converter.convert(input);

            assertThat(range.start()).isEqualTo(expectedStart);
            assertThat(range.end()).isEqualTo(expectedEnd);
        }

        @ParameterizedTest
        @CsvSource({
            "'[10,20)', 10, 20",
            "'[0,100)', 0, 100",
            "'[50,150)', 50, 150"
        })
        @DisplayName("should parse half-open range '[m,n)' as [m, n)")
        void shouldParseHalfOpenRange(String input, long expectedStart, long expectedEnd) {
            RangeOption.Range range = converter.convert(input);

            assertThat(range.start()).isEqualTo(expectedStart);
            assertThat(range.end()).isEqualTo(expectedEnd);
        }

        @ParameterizedTest
        @ValueSource(strings = {"", "   ", "\t"})
        @DisplayName("should reject empty or whitespace input")
        void shouldRejectEmptyInput(String input) {
            assertThatThrownBy(() -> converter.convert(input))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot be empty");
        }

        @Test
        @DisplayName("should reject null input")
        void shouldRejectNullInput() {
            assertThatThrownBy(() -> converter.convert(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot be empty");
        }

        @ParameterizedTest
        @ValueSource(strings = {"abc", "10.5", "10..20..30", "[10,20", "10,20)", "[10-20)"})
        @DisplayName("should reject invalid formats")
        void shouldRejectInvalidFormats(String input) {
            assertThatThrownBy(() -> converter.convert(input))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid range format");
        }
    }

    @Nested
    @DisplayName("Picocli Integration")
    class PicocliIntegrationTest {

        @Test
        @DisplayName("should parse --range with simple count")
        void shouldParseSimpleCount() {
            DummyCommand command = new DummyCommand();
            new CommandLine(command).parseArgs("--range", "100");

            assertThat(command.rangeOption.getRange()).isNotNull();
            assertThat(command.rangeOption.getRangeStart()).isEqualTo(0);
            assertThat(command.rangeOption.getRangeEnd()).isEqualTo(100);
            assertThat(command.rangeOption.getRangeSize()).isEqualTo(100);
        }

        @Test
        @DisplayName("should parse --range with inclusive format")
        void shouldParseInclusiveRange() {
            DummyCommand command = new DummyCommand();
            new CommandLine(command).parseArgs("--range", "10..20");

            assertThat(command.rangeOption.getRange()).isNotNull();
            assertThat(command.rangeOption.getRangeStart()).isEqualTo(10);
            assertThat(command.rangeOption.getRangeEnd()).isEqualTo(21);
        }

        @Test
        @DisplayName("should parse --range with half-open format")
        void shouldParseHalfOpenRange() {
            DummyCommand command = new DummyCommand();
            new CommandLine(command).parseArgs("--range", "[10,20)");

            assertThat(command.rangeOption.getRange()).isNotNull();
            assertThat(command.rangeOption.getRangeStart()).isEqualTo(10);
            assertThat(command.rangeOption.getRangeEnd()).isEqualTo(20);
        }

        @Test
        @DisplayName("should handle no --range option")
        void shouldHandleNoRange() {
            DummyCommand command = new DummyCommand();
            new CommandLine(command).parseArgs();

            assertThat(command.rangeOption.getRange()).isNull();
            assertThat(command.rangeOption.isRangeSpecified()).isFalse();
            assertThat(command.rangeOption.getRangeStart()).isEqualTo(0);
            assertThat(command.rangeOption.getRangeEnd()).isEqualTo(Long.MAX_VALUE);
        }

        @Test
        @DisplayName("should reject invalid range during parsing")
        void shouldRejectInvalidRange() {
            DummyCommand command = new DummyCommand();
            CommandLine cmd = new CommandLine(command);

            int exitCode = cmd.execute("--range", "-5..10");
            assertThat(exitCode).isEqualTo(2); // Picocli error code
        }

        @Test
        @DisplayName("should provide effective range with total count")
        void shouldProvideEffectiveRange() {
            DummyCommand command = new DummyCommand();
            new CommandLine(command).parseArgs("--range", "10..50");

            RangeOption.Range effective = command.rangeOption.getEffectiveRange(30);
            assertThat(effective.start()).isEqualTo(10);
            assertThat(effective.end()).isEqualTo(30); // Constrained by total
        }

        @Test
        @DisplayName("should use full range when no option specified")
        void shouldUseFullRangeWhenNotSpecified() {
            DummyCommand command = new DummyCommand();
            new CommandLine(command).parseArgs();

            RangeOption.Range effective = command.rangeOption.getEffectiveRange(100);
            assertThat(effective.start()).isEqualTo(0);
            assertThat(effective.end()).isEqualTo(100);
        }

        @Test
        @DisplayName("toString should show range or 'all'")
        void shouldFormatToString() {
            DummyCommand withRange = new DummyCommand();
            new CommandLine(withRange).parseArgs("--range", "10..20");
            assertThat(withRange.rangeOption.toString()).isEqualTo("[10, 21)");

            DummyCommand withoutRange = new DummyCommand();
            new CommandLine(withoutRange).parseArgs();
            assertThat(withoutRange.rangeOption.toString()).isEqualTo("all");
        }
    }

    private static final class DummyCommand implements Runnable {
        @CommandLine.Mixin
        final RangeOption rangeOption = new RangeOption();

        @Override
        public void run() {
        }
    }
}
