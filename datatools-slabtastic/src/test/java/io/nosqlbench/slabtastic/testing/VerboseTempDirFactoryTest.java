package io.nosqlbench.slabtastic.testing;

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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VerboseTempDirFactoryTest {

    @Test
    void createsTempDirectoryUnderConfiguredBasePath() throws Exception {
        String previousTmpDir = System.getProperty("java.io.tmpdir");
        Path base = Paths.get("target", "verbose-tempdir-factory-base").toAbsolutePath().normalize();
        try {
            Files.createDirectories(base);
            System.setProperty("java.io.tmpdir", base.toString());

            Path tempDir = new VerboseTempDirFactory().createTempDirectory(null, null);

            assertNotNull(tempDir);
            assertTrue(Files.isDirectory(tempDir));
            assertTrue(tempDir.startsWith(base.toAbsolutePath().normalize()));
        } finally {
            restoreTmpDir(previousTmpDir);
            deleteQuietly(base);
        }
    }

    @Test
    void includesPathDetailsWhenConfiguredPathIsAFile() throws Exception {
        String previousTmpDir = System.getProperty("java.io.tmpdir");
        Path base = Paths.get("target", "verbose-tempdir-factory-file-case").toAbsolutePath().normalize();
        Files.createDirectories(base);
        Path file = base.resolve("tmpdir-is-file.tmp");
        try {
            Files.writeString(file, "not a directory");
            System.setProperty("java.io.tmpdir", file.toString());

            IOException ioe = assertThrows(
                IOException.class,
                () -> new VerboseTempDirFactory().createTempDirectory(null, null)
            );

            String message = ioe.getMessage();
            assertNotNull(message);
            assertTrue(message.contains("Failed to create default temp directory for tests"));
            assertTrue(message.contains(file.toString()));
            assertTrue(message.contains("resolved="));
        } finally {
            restoreTmpDir(previousTmpDir);
            deleteQuietly(file);
            deleteQuietly(base);
        }
    }

    private static void restoreTmpDir(String value) {
        if (value == null) {
            System.clearProperty("java.io.tmpdir");
        } else {
            System.setProperty("java.io.tmpdir", value);
        }
    }

    private static void deleteQuietly(Path path) {
        try {
            if (Files.isDirectory(path)) {
                try (Stream<Path> children = Files.list(path)) {
                    children.forEach(VerboseTempDirFactoryTest::deleteQuietly);
                }
            }
            Files.deleteIfExists(path);
        } catch (Exception ignored) {
        }
    }
}
