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

import io.nosqlbench.slabtastic.SlabWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

/// Tests for {@link SlabFileValidator}.
class SlabFileValidatorTest {

    @TempDir
    Path tempDir;

    @Test
    void validFileReturnsNoErrors() throws IOException {
        Path file = tempDir.resolve("valid.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write(0, "hello".getBytes());
            writer.write(1, "world".getBytes());
        }

        List<String> errors = SlabFileValidator.validate(file);
        assertThat(errors).isEmpty();
    }

    @Test
    void emptySlabFileReturnsNoErrors() throws IOException {
        Path file = tempDir.resolve("empty.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            // no records
        }

        List<String> errors = SlabFileValidator.validate(file);
        assertThat(errors).isEmpty();
    }

    @Test
    void tooSmallFileReturnsError() throws IOException {
        Path file = tempDir.resolve("tiny.slab");
        Files.write(file, new byte[10]);

        List<String> errors = SlabFileValidator.validate(file);
        assertThat(errors).hasSize(1);
        assertThat(errors.get(0)).contains("too small");
    }

    @Test
    void nonSlabFileReturnsError() throws IOException {
        Path file = tempDir.resolve("garbage.slab");
        Files.write(file, new byte[512]);

        List<String> errors = SlabFileValidator.validate(file);
        assertThat(errors).isNotEmpty();
    }

    @Test
    void requireValidThrowsOnInvalid() throws IOException {
        Path file = tempDir.resolve("bad.slab");
        Files.write(file, new byte[10]);

        assertThatThrownBy(() -> SlabFileValidator.requireValid(file))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("validation failed");
    }

    @Test
    void requireValidSucceedsOnValid() throws IOException {
        Path file = tempDir.resolve("good.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write(0, "test".getBytes());
        }

        assertThatCode(() -> SlabFileValidator.requireValid(file))
            .doesNotThrowAnyException();
    }
}
