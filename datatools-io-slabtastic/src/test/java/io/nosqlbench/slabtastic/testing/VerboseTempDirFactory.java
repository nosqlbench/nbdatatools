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

import org.junit.jupiter.api.extension.AnnotatedElementContext;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDirFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/// Temp directory factory that emits actionable diagnostics when temp-dir creation fails.
public class VerboseTempDirFactory implements TempDirFactory {

    private static final String TMPDIR_PROPERTY = "java.io.tmpdir";

    @Override
    public Path createTempDirectory(AnnotatedElementContext annotatedElementContext, ExtensionContext extensionContext)
        throws Exception
    {
        String configuredTmpDir = System.getProperty(TMPDIR_PROPERTY);
        if (configuredTmpDir == null || configuredTmpDir.isBlank()) {
            throw new IOException("Missing required system property '" + TMPDIR_PROPERTY + "'");
        }

        Path baseTempDir;
        try {
            baseTempDir = Paths.get(configuredTmpDir).toAbsolutePath().normalize();
        } catch (Exception e) {
            throw new IOException(
                "Invalid '" + TMPDIR_PROPERTY + "' value '" + configuredTmpDir + "'; unable to resolve as a path",
                e
            );
        }

        try {
            Files.createDirectories(baseTempDir);
        } catch (Exception e) {
            throw new IOException(buildFailureMessage(baseTempDir, configuredTmpDir, "failed to create base temp directory"), e);
        }

        if (!Files.isDirectory(baseTempDir)) {
            throw new IOException(buildFailureMessage(baseTempDir, configuredTmpDir, "base temp path is not a directory"));
        }
        if (!Files.isWritable(baseTempDir)) {
            throw new IOException(buildFailureMessage(baseTempDir, configuredTmpDir, "base temp directory is not writable"));
        }

        try {
            return Files.createTempDirectory(baseTempDir, "junit-");
        } catch (Exception e) {
            throw new IOException(buildFailureMessage(baseTempDir, configuredTmpDir, "failed to create test temp directory"), e);
        }
    }

    private static String buildFailureMessage(Path baseTempDir, String configuredTmpDir, String reason) {
        boolean exists = Files.exists(baseTempDir);
        boolean directory = exists && Files.isDirectory(baseTempDir);
        boolean writable = exists && Files.isWritable(baseTempDir);
        return "Failed to create default temp directory for tests: " + reason +
            " [java.io.tmpdir='" + configuredTmpDir + "'" +
            ", resolved='" + baseTempDir + "'" +
            ", exists=" + exists +
            ", isDirectory=" + directory +
            ", writable=" + writable +
            "]";
    }
}
