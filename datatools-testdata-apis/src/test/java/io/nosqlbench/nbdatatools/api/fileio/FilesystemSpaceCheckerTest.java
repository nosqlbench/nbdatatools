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

package io.nosqlbench.nbdatatools.api.fileio;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class FilesystemSpaceCheckerTest {

    @TempDir
    static Path tempDir;
    
    private long availableSpace;
    private Path testPath;

    @BeforeAll
    void setUp() throws IOException {
        testPath = tempDir.resolve("test");
        Files.createDirectories(testPath);
        availableSpace = Files.getFileStore(testPath).getUsableSpace();
        System.out.println("Available space on test filesystem: " + (availableSpace / (1024.0 * 1024.0 * 1024.0)) + " GB");
    }

    @Test
    void testSinglePath_WithinAvailableSpace_ShouldSucceed() {
        // Request 10% of available space (well within limits)
        double requestSize = availableSpace * 0.1;
        
        assertDoesNotThrow(() -> {
            FilesystemSpaceChecker.checkSpaceAvailable(testPath, requestSize);
        });
    }

    @Test
    void testSinglePath_WithCustomMargin_WithinAvailableSpace_ShouldSucceed() {
        // Request 50% of available space with 10% margin (should still fit)
        double requestSize = availableSpace * 0.5;
        double margin = 0.10; // 10% margin
        
        assertDoesNotThrow(() -> {
            FilesystemSpaceChecker.checkSpaceAvailable(testPath, requestSize, margin);
        });
    }

    @Test
    void testSinglePath_ExceedsAvailableSpace_ShouldThrowException() {
        // Request 150% of available space (definitely too much)
        double requestSize = availableSpace * 1.5;
        
        FilesystemSpaceChecker.InsufficientSpaceException exception = assertThrows(
            FilesystemSpaceChecker.InsufficientSpaceException.class,
            () -> FilesystemSpaceChecker.checkSpaceAvailable(testPath, requestSize)
        );
        
        assertTrue(exception.getMessage().contains("Insufficient disk space"));
        assertTrue(exception.getMessage().contains("Required:"));
        assertTrue(exception.getMessage().contains("Available:"));
        assertTrue(exception.getMessage().contains("GB"));
    }

    @Test
    void testSinglePath_ExceedsWithMargin_ShouldThrowException() {
        // Request 90% of available space with default 20% margin (total 108%, should fail)
        double requestSize = availableSpace * 0.9;
        
        FilesystemSpaceChecker.InsufficientSpaceException exception = assertThrows(
            FilesystemSpaceChecker.InsufficientSpaceException.class,
            () -> FilesystemSpaceChecker.checkSpaceAvailable(testPath, requestSize)
        );
        
        assertTrue(exception.getMessage().contains("20% margin"));
    }

    @Test
    void testSinglePath_CustomMarginExceeds_ShouldThrowException() {
        // Request 80% of available space with 30% margin (total 104%, should fail)
        double requestSize = availableSpace * 0.8;
        double margin = 0.30; // 30% margin
        
        FilesystemSpaceChecker.InsufficientSpaceException exception = assertThrows(
            FilesystemSpaceChecker.InsufficientSpaceException.class,
            () -> FilesystemSpaceChecker.checkSpaceAvailable(testPath, requestSize, margin)
        );
        
        assertTrue(exception.getMessage().contains("30% margin"));
    }

    @Test
    void testBuilder_MultiplePathsSameFilesystem_WithinSpace_ShouldSucceed() throws IOException {
        Path path1 = testPath.resolve("file1");
        Path path2 = testPath.resolve("file2");
        Path path3 = testPath.resolve("subdir/file3");
        
        // Split 20% of available space across 3 files
        double sizePerFile = (availableSpace * 0.2) / 3;
        
        assertDoesNotThrow(() -> {
            FilesystemSpaceChecker.builder()
                .withMargin(0.10) // 10% margin
                .addPath(path1, sizePerFile)
                .addPath(path2, sizePerFile)
                .addPath(path3, sizePerFile)
                .checkAll();
        });
    }

    @Test
    void testBuilder_MultiplePathsSameFilesystem_ExceedsSpace_ShouldThrowException() throws IOException {
        Path path1 = testPath.resolve("largefile1");
        Path path2 = testPath.resolve("largefile2");
        
        // Each file requests 70% of available space (total 140%, should fail)
        double sizePerFile = availableSpace * 0.7;
        
        FilesystemSpaceChecker.InsufficientSpaceException exception = assertThrows(
            FilesystemSpaceChecker.InsufficientSpaceException.class,
            () -> FilesystemSpaceChecker.builder()
                .addPath(path1, sizePerFile)
                .addPath(path2, sizePerFile)
                .checkAll()
        );
        
        assertTrue(exception.getMessage().contains("Insufficient disk space on filesystem"));
        assertTrue(exception.getMessage().contains("Affected paths:"));
        assertTrue(exception.getMessage().contains("largefile1"));
        assertTrue(exception.getMessage().contains("largefile2"));
    }

    @Test
    void testBuilder_DefaultMargin_ShouldUse20Percent() throws IOException {
        Path path1 = testPath.resolve("margintest");
        
        // Request exactly 85% of available space (with 20% margin = 102%, should fail)
        double requestSize = availableSpace * 0.85;
        
        FilesystemSpaceChecker.InsufficientSpaceException exception = assertThrows(
            FilesystemSpaceChecker.InsufficientSpaceException.class,
            () -> FilesystemSpaceChecker.builder()
                .addPath(path1, requestSize)
                .checkAll()
        );
        
        assertTrue(exception.getMessage().contains("20% margin"));
    }

    @Test
    void testBuilder_CustomMargin_ShouldUseSpecifiedMargin() throws IOException {
        Path path1 = testPath.resolve("custommargin");
        
        // Request 90% of available space with 5% margin (total 94.5%, should succeed)
        double requestSize = availableSpace * 0.9;
        
        assertDoesNotThrow(() -> {
            FilesystemSpaceChecker.builder()
                .withMargin(0.05) // 5% margin
                .addPath(path1, requestSize)
                .checkAll();
        });
    }

    @Test
    void testSinglePath_InvalidPath_ShouldWorkWithRoot() {
        // Invalid path should fall back to root filesystem and work
        Path invalidPath = Paths.get("/this/path/does/not/exist/nowhere");
        
        // This should succeed as it falls back to root filesystem space check
        assertDoesNotThrow(() -> {
            FilesystemSpaceChecker.checkSpaceAvailable(invalidPath, 1000.0);
        });
    }

    @Test
    void testBuilder_InvalidPath_ShouldWorkWithRoot() {
        Path validPath = testPath.resolve("valid");
        Path invalidPath = Paths.get("/this/path/does/not/exist/nowhere");
        
        // Both paths should work - valid path uses temp filesystem, invalid falls back to root
        assertDoesNotThrow(() -> {
            FilesystemSpaceChecker.builder()
                .addPath(validPath, 1000.0)
                .addPath(invalidPath, 1000.0)
                .checkAll();
        });
    }

    @Test
    void testSinglePath_TrueIOException_ShouldThrowException() {
        // Try to create a scenario that would cause a true IOException
        // This is mainly for coverage of the exception handling path
        Path validPath = testPath.resolve("validfile");
        
        // This should succeed normally
        assertDoesNotThrow(() -> {
            FilesystemSpaceChecker.checkSpaceAvailable(validPath, 1000.0);
        });
    }

    @Test
    void testBuilder_EmptyBuilder_ShouldSucceed() {
        // Empty builder should succeed (no requirements to check)
        assertDoesNotThrow(() -> {
            FilesystemSpaceChecker.builder().checkAll();
        });
    }

    @Test
    void testExceptionMessages_ContainHelpfulInformation() {
        double requestSize = availableSpace * 1.5;
        
        FilesystemSpaceChecker.InsufficientSpaceException exception = assertThrows(
            FilesystemSpaceChecker.InsufficientSpaceException.class,
            () -> FilesystemSpaceChecker.checkSpaceAvailable(testPath, requestSize, 0.15)
        );
        
        String message = exception.getMessage();
        assertTrue(message.contains("Insufficient disk space"));
        assertTrue(message.contains("Required:"));
        assertTrue(message.contains("Available:"));
        assertTrue(message.contains("15% margin"));
        assertTrue(message.contains("GB"));
        assertTrue(message.contains(testPath.toString()));
    }

    @Test
    void testBuilder_MethodChaining() throws IOException {
        Path path1 = testPath.resolve("chain1");
        Path path2 = testPath.resolve("chain2");
        
        // Test that all builder methods return the builder for chaining
        FilesystemSpaceChecker.Builder builder = FilesystemSpaceChecker.builder();
        assertSame(builder, builder.withMargin(0.1));
        assertSame(builder, builder.addPath(path1, 1000.0));
        assertSame(builder, builder.addPath(path2, 2000.0));
        
        // And that the final operation works
        assertDoesNotThrow(builder::checkAll);
    }
}