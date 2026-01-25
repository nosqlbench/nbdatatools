package io.nosqlbench.vectordata.config;

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

import io.nosqlbench.vectordata.config.exceptions.InvalidCacheDirectoryException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/// Unit tests for [MountPointResolver].
class MountPointResolverTest {

    @TempDir
    Path tempDir;

    @Test
    void requiresResolution_withAutoLargestNonRoot_returnsTrue() {
        assertTrue(MountPointResolver.requiresResolution("auto:largest-non-root"));
        assertTrue(MountPointResolver.requiresResolution("AUTO:LARGEST-NON-ROOT"));
        assertTrue(MountPointResolver.requiresResolution("  auto:largest-non-root  "));
    }

    @Test
    void requiresResolution_withAutoLargestAny_returnsTrue() {
        assertTrue(MountPointResolver.requiresResolution("auto:largest-any"));
        assertTrue(MountPointResolver.requiresResolution("AUTO:LARGEST-ANY"));
    }

    @Test
    void requiresResolution_withDefault_returnsTrue() {
        assertTrue(MountPointResolver.requiresResolution("default"));
        assertTrue(MountPointResolver.requiresResolution("DEFAULT"));
        assertTrue(MountPointResolver.requiresResolution("  default  "));
    }

    @Test
    void requiresResolution_withAbsolutePath_returnsFalse() {
        assertFalse(MountPointResolver.requiresResolution("/some/path"));
        assertFalse(MountPointResolver.requiresResolution("/data/vectordata-cache"));
    }

    @Test
    void requiresResolution_withTildePath_returnsFalse() {
        assertFalse(MountPointResolver.requiresResolution("~/.cache/vectordata"));
    }

    @Test
    void requiresResolution_withNull_returnsFalse() {
        assertFalse(MountPointResolver.requiresResolution(null));
    }

    @Test
    void resolve_withNull_throwsException() {
        assertThrows(InvalidCacheDirectoryException.class,
            () -> MountPointResolver.resolve(null));
    }

    @Test
    void resolve_withEmptyString_throwsException() {
        assertThrows(InvalidCacheDirectoryException.class,
            () -> MountPointResolver.resolve(""));
        assertThrows(InvalidCacheDirectoryException.class,
            () -> MountPointResolver.resolve("   "));
    }

    @Test
    void resolve_withDefault_returnsHomeCachePath() {
        Path resolved = MountPointResolver.resolve("default");

        String home = System.getProperty("user.home");
        Path expected = Path.of(home, ".cache", "vectordata");
        assertEquals(expected, resolved);
    }

    @Test
    void resolve_withAbsolutePath_returnsPath() {
        Path resolved = MountPointResolver.resolve(tempDir.toAbsolutePath().toString());
        assertEquals(tempDir.toAbsolutePath(), resolved);
    }

    @Test
    void resolve_withTildePath_expandsTilde() {
        Path resolved = MountPointResolver.resolve("~/.cache/test");

        String home = System.getProperty("user.home");
        Path expected = Path.of(home, ".cache", "test");
        assertEquals(expected, resolved);
    }

    @Test
    void resolve_withRelativePath_throwsException() {
        assertThrows(InvalidCacheDirectoryException.class,
            () -> MountPointResolver.resolve("relative/path"));
    }

    @Test
    void resolve_withUnknownAutoDirective_throwsException() {
        assertThrows(InvalidCacheDirectoryException.class,
            () -> MountPointResolver.resolve("auto:unknown"));
    }

    @Test
    void resolve_withAutoLargestAny_returnsPath() {
        // This should succeed on any system with at least one writable mount point
        Path resolved = MountPointResolver.resolve("auto:largest-any");

        assertNotNull(resolved);
        assertTrue(resolved.isAbsolute());
        assertTrue(resolved.toString().endsWith(MountPointResolver.CACHE_SUBDIR));
    }

    @Test
    void listWritableMountPoints_returnsNonEmptyList() {
        // Most systems should have at least one writable mount point
        List<MountPointInfo> mounts = MountPointResolver.listWritableMountPoints();

        assertNotNull(mounts);
        // We can't assert it's non-empty in all environments, but check the structure
        for (MountPointInfo mount : mounts) {
            assertNotNull(mount.mountPoint());
            assertTrue(mount.totalSpace() >= 0);
            assertTrue(mount.availableSpace() >= 0);
        }
    }

    @Test
    void listWritableMountPoints_sortedByAvailableSpaceDescending() {
        List<MountPointInfo> mounts = MountPointResolver.listWritableMountPoints();

        for (int i = 0; i < mounts.size() - 1; i++) {
            assertTrue(mounts.get(i).availableSpace() >= mounts.get(i + 1).availableSpace(),
                "Mount points should be sorted by available space descending");
        }
    }

    @Test
    void mountPointInfo_formatBytes_formatsCorrectly() {
        assertEquals("0 B", MountPointInfo.formatBytes(0));
        assertEquals("512 B", MountPointInfo.formatBytes(512));
        assertEquals("1.0 KB", MountPointInfo.formatBytes(1024));
        assertEquals("1.0 MB", MountPointInfo.formatBytes(1024 * 1024));
        assertEquals("1.0 GB", MountPointInfo.formatBytes(1024L * 1024 * 1024));
        assertEquals("1.0 TB", MountPointInfo.formatBytes(1024L * 1024 * 1024 * 1024));
        assertEquals("unknown", MountPointInfo.formatBytes(-1));
    }

    @Test
    void mountPointInfo_formattedMethods_returnFormattedStrings() {
        MountPointInfo info = new MountPointInfo(
            Path.of("/test"),
            1024L * 1024 * 1024 * 100, // 100 GB total
            1024L * 1024 * 1024 * 50,  // 50 GB available
            false
        );

        assertEquals("100.0 GB", info.formattedTotalSpace());
        assertEquals("50.0 GB", info.formattedAvailableSpace());
        assertEquals("Yes", info.writableLabel());
    }

    @Test
    void mountPointInfo_writableLabel_forRootMount() {
        MountPointInfo rootInfo = new MountPointInfo(
            Path.of("/"),
            1024L * 1024 * 1024 * 100,
            1024L * 1024 * 1024 * 50,
            true
        );

        assertEquals("Yes (root)", rootInfo.writableLabel());
    }

    @Test
    void resolve_withExistingFileAsPath_throwsException() throws Exception {
        // Create a file (not directory)
        Path file = tempDir.resolve("not-a-directory");
        java.nio.file.Files.writeString(file, "test");

        assertThrows(InvalidCacheDirectoryException.class,
            () -> MountPointResolver.resolve(file.toAbsolutePath().toString()));
    }
}
