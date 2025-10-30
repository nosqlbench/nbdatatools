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
        @DisplayName("should create InputFile with path only")
        void shouldCreateWithPathOnly() {
            Path path = Path.of("test.txt");
            InputFileOption.InputFile inputFile = new InputFileOption.InputFile(path);

            assertThat(inputFile.path()).isEqualTo(path);
            assertThat(inputFile.inlineRangeSpec()).isNull();
            assertThat(inputFile.hasInlineRange()).isFalse();
        }

        @Test
        @DisplayName("should create InputFile with path and range")
        void shouldCreateWithPathAndRange() {
            Path path = Path.of("test.txt");
            InputFileOption.InputFile inputFile = new InputFileOption.InputFile(path, "100");

            assertThat(inputFile.path()).isEqualTo(path);
            assertThat(inputFile.inlineRangeSpec()).isEqualTo("100");
            assertThat(inputFile.hasInlineRange()).isTrue();
        }

        @Test
        @DisplayName("should reject null path")
        void shouldRejectNullPath() {
            assertThatThrownBy(() -> new InputFileOption.InputFile(null, "100"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot be null");
        }

        @Test
        @DisplayName("should normalize path")
        void shouldNormalizePath() throws IOException {
            Path file = Files.createFile(tempDir.resolve("test.txt"));
            InputFileOption.InputFile inputFile = new InputFileOption.InputFile(file);

            assertThat(inputFile.normalizedPath()).isAbsolute();
        }

        @Test
        @DisplayName("should check file existence")
        void shouldCheckExistence() throws IOException {
            Path existing = Files.createFile(tempDir.resolve("existing.txt"));
            Path nonExisting = tempDir.resolve("nonexisting.txt");

            assertThat(new InputFileOption.InputFile(existing).exists()).isTrue();
            assertThat(new InputFileOption.InputFile(nonExisting).exists()).isFalse();
        }

        @Test
        @DisplayName("should validate file exists")
        void shouldValidateExistence() throws IOException {
            Path existing = Files.createFile(tempDir.resolve("existing.txt"));
            Path nonExisting = tempDir.resolve("nonexisting.txt");

            InputFileOption.InputFile existingFile = new InputFileOption.InputFile(existing);
            assertThatNoException().isThrownBy(existingFile::validate);

            InputFileOption.InputFile nonExistingFile = new InputFileOption.InputFile(nonExisting);
            assertThatThrownBy(nonExistingFile::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("does not exist");
        }

        @Test
        @DisplayName("should implement CharSequence for path")
        void shouldImplementCharSequence() {
            Path path = Path.of("/path/to/test.txt");
            InputFileOption.InputFile inputFile = new InputFileOption.InputFile(path);

            String pathString = path.toString();
            assertThat(inputFile.length()).isEqualTo(pathString.length());
            assertThat(inputFile.charAt(0)).isEqualTo(pathString.charAt(0));
            assertThat(inputFile.subSequence(0, 5).toString()).isEqualTo(pathString.substring(0, 5));
        }

        @Test
        @DisplayName("toString should include inline range when present")
        void shouldFormatToStringWithRange() {
            Path path = Path.of("test.txt");

            InputFileOption.InputFile withRange = new InputFileOption.InputFile(path, "100");
            assertThat(withRange.toString()).contains("test.txt").contains("range: 100");

            InputFileOption.InputFile withoutRange = new InputFileOption.InputFile(path);
            assertThat(withoutRange.toString()).isEqualTo("test.txt");
        }
    }

    @Nested
    @DisplayName("InputFileConverter")
    class InputFileConverterTest {

        private final InputFileOption.InputFileConverter converter = new InputFileOption.InputFileConverter();

        @Test
        @DisplayName("should parse plain file path")
        void shouldParsePlainPath() {
            InputFileOption.InputFile inputFile = converter.convert("test.txt");

            assertThat(inputFile.path()).isEqualTo(Path.of("test.txt"));
            assertThat(inputFile.inlineRangeSpec()).isNull();
        }

        @ParameterizedTest
        @CsvSource({
            "test.txt:100, test.txt, 100",
            "test.txt:10..20, test.txt, 10..20",
            "'test.txt:[0,100)', test.txt, '[0,100)'",
            "/path/to/file.fvec:500, /path/to/file.fvec, 500"
        })
        @DisplayName("should parse path with inline range spec")
        void shouldParsePathWithRange(String input, String expectedPath, String expectedRange) {
            InputFileOption.InputFile inputFile = converter.convert(input);

            assertThat(inputFile.path()).isEqualTo(Path.of(expectedPath));
            assertThat(inputFile.inlineRangeSpec()).isEqualTo(expectedRange);
        }

        @Test
        @DisplayName("should handle Windows drive letters")
        void shouldHandleWindowsDriveLetters() {
            // Windows path without range
            InputFileOption.InputFile file1 = converter.convert("C:\\data\\test.txt");
            assertThat(file1.path().toString()).isEqualTo("C:\\data\\test.txt");
            assertThat(file1.inlineRangeSpec()).isNull();

            // Windows path with range (colon after drive letter should not be treated as range separator)
            InputFileOption.InputFile file2 = converter.convert("C:\\data\\test.txt:100");
            assertThat(file2.path().toString()).isEqualTo("C:\\data\\test.txt");
            assertThat(file2.inlineRangeSpec()).isEqualTo("100");
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
            Path file = Files.createFile(tempDir.resolve("test.txt"));
            DummyCommand command = new DummyCommand();
            new CommandLine(command).parseArgs("--input", file.toString());

            assertThat(command.inputFileOption.getInputFile()).isNotNull();
            assertThat(command.inputFileOption.getInputPath()).isEqualTo(file);
            assertThat(command.inputFileOption.hasInlineRange()).isFalse();
        }

        @Test
        @DisplayName("should parse --input with inline range")
        void shouldParsePathWithInlineRange() {
            DummyCommand command = new DummyCommand();
            new CommandLine(command).parseArgs("--input", "test.txt:100");

            assertThat(command.inputFileOption.getInputFile()).isNotNull();
            assertThat(command.inputFileOption.getInputPath().toString()).isEqualTo("test.txt");
            assertThat(command.inputFileOption.hasInlineRange()).isTrue();
            assertThat(command.inputFileOption.getInlineRangeSpec()).isEqualTo("100");
        }

        @Test
        @DisplayName("should provide normalized path")
        void shouldProvideNormalizedPath() throws IOException {
            Path file = Files.createFile(tempDir.resolve("test.txt"));
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
            new CommandLine(withRange).parseArgs("--input", "test.txt:100");
            assertThat(withRange.inputFileOption.toString()).contains("test.txt").contains("100");

            DummyCommand withoutRange = new DummyCommand();
            new CommandLine(withoutRange).parseArgs("--input", "test.txt");
            assertThat(withoutRange.inputFileOption.toString()).contains("test.txt");
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
