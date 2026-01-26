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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class Test_CMD_analyze_check_endian {

    @TempDir
    Path tempDir;

    private final java.io.ByteArrayOutputStream outContent = new java.io.ByteArrayOutputStream();
    private final java.io.ByteArrayOutputStream errContent = new java.io.ByteArrayOutputStream();
    private final PrintStream originalOut = System.out;
    private final PrintStream originalErr = System.err;

    @BeforeEach
    public void setUp() {
        System.setOut(new PrintStream(outContent));
        System.setErr(new PrintStream(errContent));
    }

    @AfterEach
    public void tearDown() {
        System.setOut(originalOut);
        System.setErr(originalErr);
    }

    @Test
    public void testCheckEndianLittleEndian() throws IOException {
        Path file = writeXvec(tempDir.resolve("little.fvec"), ByteOrder.LITTLE_ENDIAN, 4, 4);

        CMD_analyze_check_endian cmd = new CMD_analyze_check_endian();
        int exitCode = new CommandLine(cmd).execute(file.toString());

        assertEquals(0, exitCode, "Little-endian file should pass");
        String output = outContent.toString();
        assertTrue(output.contains("Little-endian layout validated."),
            "Output should confirm little-endian validation");
    }

    @Test
    public void testCheckEndianBigEndian() throws IOException {
        Path file = writeXvec(tempDir.resolve("big.fvec"), ByteOrder.BIG_ENDIAN, 4, 4);

        CMD_analyze_check_endian cmd = new CMD_analyze_check_endian();
        int exitCode = new CommandLine(cmd).execute(file.toString());

        assertEquals(1, exitCode, "Big-endian file should fail");
        String error = errContent.toString();
        assertTrue(error.contains("big-endian"), "Error output should mention big-endian mismatch");
    }

    private Path writeXvec(Path file, ByteOrder headerOrder, int dimension, int elementWidth) throws IOException {
        try (OutputStream out = Files.newOutputStream(file)) {
            ByteBuffer header = ByteBuffer.allocate(4).order(headerOrder);
            header.putInt(dimension);
            out.write(header.array());

            byte[] payload = new byte[dimension * elementWidth];
            Arrays.fill(payload, (byte) 1);
            out.write(payload);
        }
        return file;
    }
}
