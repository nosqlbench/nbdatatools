package io.nosqlbench.command.analyze.subcommands;

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

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;

/// An OutputStream that writes to two destinations simultaneously.
///
/// This is used to duplicate stdout/stderr to log files while still
/// displaying output on the console. Similar to the Unix `tee` command.
///
/// ## Usage
///
/// ```java
/// // Set up tee for stdout
/// PrintStream originalOut = System.out;
/// FileOutputStream logFile = new FileOutputStream("stdout.txt");
/// TeeOutputStream tee = new TeeOutputStream(originalOut, logFile);
/// System.setOut(new PrintStream(tee, true));
///
/// // ... run code that prints to stdout ...
///
/// // Restore original stdout
/// System.setOut(originalOut);
/// tee.close();
/// ```
public final class TeeOutputStream extends OutputStream {

    private final OutputStream primary;
    private final OutputStream secondary;

    /// Creates a TeeOutputStream that writes to both streams.
    ///
    /// @param primary the primary output stream (e.g., original System.out)
    /// @param secondary the secondary output stream (e.g., file)
    public TeeOutputStream(OutputStream primary, OutputStream secondary) {
        this.primary = primary;
        this.secondary = secondary;
    }

    @Override
    public void write(int b) throws IOException {
        primary.write(b);
        secondary.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        primary.write(b);
        secondary.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        primary.write(b, off, len);
        secondary.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        primary.flush();
        secondary.flush();
    }

    @Override
    public void close() throws IOException {
        try {
            secondary.close();
        } finally {
            // Don't close primary (it's System.out/err)
        }
    }

    /// Helper class to manage tee setup and teardown for stdout/stderr.
    ///
    /// Use try-with-resources to ensure proper cleanup:
    /// ```java
    /// try (OutputCapture capture = OutputCapture.start(logDir)) {
    ///     // ... code that prints to stdout/stderr ...
    /// }
    /// // Original streams are automatically restored
    /// ```
    public static final class OutputCapture implements Closeable {

        private final PrintStream originalOut;
        private final PrintStream originalErr;
        private final FileOutputStream outFile;
        private final FileOutputStream errFile;
        private final TeeOutputStream outTee;
        private final TeeOutputStream errTee;

        private OutputCapture(PrintStream originalOut, PrintStream originalErr,
                              FileOutputStream outFile, FileOutputStream errFile,
                              TeeOutputStream outTee, TeeOutputStream errTee) {
            this.originalOut = originalOut;
            this.originalErr = originalErr;
            this.outFile = outFile;
            this.errFile = errFile;
            this.outTee = outTee;
            this.errTee = errTee;
        }

        /// Starts capturing stdout and stderr to log files.
        ///
        /// @param logDir directory for log files (stdout.txt, stderr.txt)
        /// @return OutputCapture that should be closed to restore streams
        /// @throws IOException if log files cannot be created
        public static OutputCapture start(Path logDir) throws IOException {
            // Ensure directory exists
            Files.createDirectories(logDir);

            Path stdoutPath = logDir.resolve("stdout.txt");
            Path stderrPath = logDir.resolve("stderr.txt");

            // Save original streams
            PrintStream originalOut = System.out;
            PrintStream originalErr = System.err;

            // Create file streams
            FileOutputStream outFile = new FileOutputStream(stdoutPath.toFile());
            FileOutputStream errFile = new FileOutputStream(stderrPath.toFile());

            // Create tee streams
            TeeOutputStream outTee = new TeeOutputStream(originalOut, outFile);
            TeeOutputStream errTee = new TeeOutputStream(originalErr, errFile);

            // Replace System streams
            System.setOut(new PrintStream(outTee, true));
            System.setErr(new PrintStream(errTee, true));

            System.out.println("Output logging enabled: " + logDir.toAbsolutePath());
            System.out.println("  stdout → " + stdoutPath.toAbsolutePath());
            System.out.println("  stderr → " + stderrPath.toAbsolutePath());
            System.out.println();

            return new OutputCapture(originalOut, originalErr, outFile, errFile, outTee, errTee);
        }

        @Override
        public void close() throws IOException {
            // Restore original streams first
            System.setOut(originalOut);
            System.setErr(originalErr);

            // Then close file streams
            IOException firstException = null;
            try {
                outTee.close();
            } catch (IOException e) {
                firstException = e;
            }
            try {
                errTee.close();
            } catch (IOException e) {
                if (firstException == null) {
                    firstException = e;
                }
            }

            if (firstException != null) {
                throw firstException;
            }
        }
    }
}
