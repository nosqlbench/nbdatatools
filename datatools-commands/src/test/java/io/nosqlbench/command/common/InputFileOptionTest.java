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
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.*;

@DisplayName("InputFileOption")
class InputFileOptionTest {

    @TempDir
    Path tempDir;

    @Nested
    @DisplayName("InputFile Record")
    class InputFileRecordTest {

        @Test
        @DisplayName("should create InputFile with spec only")
        void shouldCreateWithSpecOnly() {
            VectorDataSpec spec = VectorDataSpec.parse("test.fvec");
            InputFileOption.InputFile inputFile = new InputFileOption.InputFile(spec);

            assertThat(inputFile.spec()).isEqualTo(spec);
            assertThat(inputFile.inlineRangeSpec()).isNull();
            assertThat(inputFile.hasInlineRange()).isFalse();
        }

        @Test
        @DisplayName("should create InputFile with spec and range")
        void shouldCreateWithSpecAndRange() {
            VectorDataSpec spec = VectorDataSpec.parse("test.fvec");
            RangeOption.Range range = new RangeOption.Range(0, 100);
            InputFileOption.InputFile inputFile = new InputFileOption.InputFile(spec, range, "[0,100)");

            assertThat(inputFile.spec()).isEqualTo(spec);
            assertThat(inputFile.range()).isEqualTo(range);
            assertThat(inputFile.inlineRangeSpec()).isEqualTo("[0,100)");
            assertThat(inputFile.hasInlineRange()).isTrue();
        }

        @Test
        @DisplayName("should reject null spec")
        void shouldRejectNullSpec() {
            assertThatThrownBy(() -> new InputFileOption.InputFile(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot be null");
        }

        @Test
        @DisplayName("should normalize path")
        void shouldNormalizePath() throws IOException {
            Path file = Files.createFile(tempDir.resolve("test.fvec"));
            VectorDataSpec spec = VectorDataSpec.parse(file.toString());
            InputFileOption.InputFile inputFile = new InputFileOption.InputFile(spec);

            assertThat(inputFile.normalizedPath()).isAbsolute();
        }

        @Test
        @DisplayName("should check file existence")
        void shouldCheckExistence() throws IOException {
            Path existing = Files.createFile(tempDir.resolve("existing.txt"));
            Path nonExisting = tempDir.resolve("nonexisting.txt");

            assertThat(new InputFileOption.InputFile(VectorDataSpec.parse(existing.toString())).exists()).isTrue();
            assertThat(new InputFileOption.InputFile(VectorDataSpec.parse(nonExisting.toString())).exists()).isFalse();
        }

        @Test
        @DisplayName("should validate file exists")
        void shouldValidateExistence() throws IOException {
            Path existing = Files.createFile(tempDir.resolve("existing.txt"));
            Path nonExisting = tempDir.resolve("nonexisting.txt");

            InputFileOption.InputFile existingFile = new InputFileOption.InputFile(VectorDataSpec.parse(existing.toString()));
            assertThatNoException().isThrownBy(existingFile::validate);

            InputFileOption.InputFile nonExistingFile = new InputFileOption.InputFile(VectorDataSpec.parse(nonExisting.toString()));
            assertThatThrownBy(nonExistingFile::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("does not exist");
        }

        @Test
        @DisplayName("should implement CharSequence for spec")
        void shouldImplementCharSequence() {
            VectorDataSpec spec = VectorDataSpec.parse("/path/to/test.fvec");
            InputFileOption.InputFile inputFile = new InputFileOption.InputFile(spec);

            String specString = spec.toString();
            assertThat(inputFile.length()).isEqualTo(specString.length());
            assertThat(inputFile.charAt(0)).isEqualTo(specString.charAt(0));
            assertThat(inputFile.subSequence(0, 5).toString()).isEqualTo(specString.substring(0, 5));
        }

        @Test
        @DisplayName("toString should include inline range spec when present")
        void shouldFormatToStringWithRange() {
            VectorDataSpec spec = VectorDataSpec.parse("test.fvec");

            InputFileOption.InputFile withRange = new InputFileOption.InputFile(spec, new RangeOption.Range(0, 100), "[0,100)");
            assertThat(withRange.toString()).isEqualTo("test.fvec[0,100)");

            InputFileOption.InputFile withoutRange = new InputFileOption.InputFile(spec);
            assertThat(withoutRange.toString()).isEqualTo("test.fvec");
        }
    }

    @Nested
    @DisplayName("InputFileConverter")
    class InputFileConverterTest {

        private final InputFileOption.InputFileConverter converter = new InputFileOption.InputFileConverter();

        @Test
        @DisplayName("should parse plain file path")
        void shouldParsePlainPath() {
            InputFileOption.InputFile inputFile = converter.convert("test.fvec");

            assertThat(inputFile.spec().getLocalPath().orElseThrow().toString()).isEqualTo("test.fvec");
            assertThat(inputFile.inlineRangeSpec()).isNull();
        }

        @ParameterizedTest
        @CsvSource({
            "test.fvec[100], test.fvec, [100]",
            "test.fvec[10..20], test.fvec, [10..20]",
            "'test.fvec[0,100)', test.fvec, '[0,100)'",
            "/path/to/file.fvec[500], /path/to/file.fvec, [500]"
        })
        @DisplayName("should parse spec with inline range suffix")
        void shouldParseSpecWithRange(String input, String expectedSpec, String expectedRange) {
            InputFileOption.InputFile inputFile = converter.convert(input);

            assertThat(inputFile.spec().toString()).isEqualTo(expectedSpec);
            assertThat(inputFile.inlineRangeSpec()).isEqualTo(expectedRange);
        }

        @Test
        @DisplayName("should reject empty input")
        void shouldRejectEmptyInput() {
            assertThatThrownBy(() -> converter.convert(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot be empty");

            assertThatThrownBy(() -> converter.convert(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot be empty");
        }
    }

    @Nested
    @DisplayName("Picocli Integration")
    class PicocliIntegrationTest {

        @Test
        @DisplayName("should parse --input with plain path")
        void shouldParsePlainPath() throws IOException {
            Path file = Files.createFile(tempDir.resolve("test.fvec"));
            DummyCommand command = new DummyCommand();
            new CommandLine(command).parseArgs("--input", file.toString());

            assertThat(command.inputFileOption.getInputFile()).isNotNull();
            assertThat(command.inputFileOption.getInputPath()).isEqualTo(file);
            assertThat(command.inputFileOption.hasInlineRange()).isFalse();
        }

        @Test
        @DisplayName("should parse --input with inline range")
        void shouldParseSpecWithInlineRange() {
            DummyCommand command = new DummyCommand();
            new CommandLine(command).parseArgs("--input", "test.fvec[100]");

            assertThat(command.inputFileOption.getInputFile()).isNotNull();
            assertThat(command.inputFileOption.getInputPath().toString()).isEqualTo("test.fvec");
            assertThat(command.inputFileOption.hasInlineRange()).isTrue();
            assertThat(command.inputFileOption.getInlineRangeSpec()).isEqualTo("[100]");
        }

        @Test
        @DisplayName("should provide normalized path")
        void shouldProvideNormalizedPath() throws IOException {
            Path file = Files.createFile(tempDir.resolve("test.fvec"));
            DummyCommand command = new DummyCommand();
            new CommandLine(command).parseArgs("--input", file.toString());

            assertThat(command.inputFileOption.getNormalizedInputPath()).isAbsolute();
        }

        @Test
        @DisplayName("should validate file exists")
        void shouldValidateFileExists() throws IOException {
            Path existing = Files.createFile(tempDir.resolve("existing.txt"));
            DummyCommand command1 = new DummyCommand();
            new CommandLine(command1).parseArgs("--input", existing.toString());
            assertThatNoException().isThrownBy(command1.inputFileOption::validate);

            Path nonExisting = tempDir.resolve("nonexisting.txt");
            DummyCommand command2 = new DummyCommand();
            new CommandLine(command2).parseArgs("--input", nonExisting.toString());
            assertThatThrownBy(command2.inputFileOption::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("does not exist");
        }

        @Test
        @DisplayName("toString should format appropriately")
        void shouldFormatToString() {
            DummyCommand withRange = new DummyCommand();
            new CommandLine(withRange).parseArgs("--input", "test.fvec[100]");
            assertThat(withRange.inputFileOption.toString()).isEqualTo("test.fvec[100]");

            DummyCommand withoutRange = new DummyCommand();
            new CommandLine(withoutRange).parseArgs("--input", "test.fvec");
            assertThat(withoutRange.inputFileOption.toString()).isEqualTo("test.fvec");
        }
    }

    private static final class DummyCommand implements Runnable {
        @CommandLine.Mixin
        final InputFileOption inputFileOption = new InputFileOption();

        @Override
        public void run() {
        }
    }
}
