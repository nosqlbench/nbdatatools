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

import com.google.gson.Gson;
import io.nosqlbench.slabtastic.SlabReader;
import picocli.CommandLine;

import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;

/// Gets records from a slabtastic file by ordinal.
///
/// Supports multiple output formats: ascii (default), hex dump, raw bytes,
/// UTF-8, JSON string, and JSONL. Missing ordinals are reported to stderr.
///
/// Output overrides `--as-hex` and `--as-base64` render record bytes in hex
/// or base64 with trailing newlines, regardless of the format setting.
@CommandLine.Command(
    name = "get",
    header = "Extract records by ordinal",
    description = "Gets records from a slabtastic file for given ordinals.",
    exitCodeList = {"0: All ordinals found", "1: Some ordinals missing", "2: Error"}
)
public class CMD_slab_get implements Callable<Integer> {

    /// Output format for retrieved records.
    public enum OutputFormat { ascii, hex, raw, utf8, json, jsonl }

    @CommandLine.Parameters(index = "0", description = "Path to the slabtastic file")
    private Path file;

    @CommandLine.Option(names = {"-o", "--ordinals"}, required = true, split = ",",
        description = "Comma-separated ordinals or range specifiers to retrieve")
    private List<String> ordinalSpecs;

    @CommandLine.Option(names = {"-f", "--format"}, defaultValue = "ascii",
        description = "Output format: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})")
    private OutputFormat format;

    @CommandLine.Option(names = {"--as-hex"},
        description = "Output bytes as hex with space between each byte, trailing newline per record")
    private boolean asHex;

    @CommandLine.Option(names = {"--as-base64"},
        description = "Output bytes as base64 with trailing newline per record")
    private boolean asBase64;

    @CommandLine.Option(names = {"--namespace", "-n"}, defaultValue = "",
        description = "Namespace to read from (default: default namespace)")
    private String namespace;

    @Override
    public Integer call() {
        if (asHex && asBase64) {
            System.err.println("Error: --as-hex and --as-base64 are mutually exclusive");
            return 2;
        }

        // Expand ordinal specs into a flat list of ordinals
        List<Long> ordinals;
        try {
            ordinals = expandOrdinalSpecs(ordinalSpecs);
        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
            return 2;
        }

        int missing = 0;
        try (SlabReader reader = new SlabReader(file)) {
            Gson gson = (format == OutputFormat.json || format == OutputFormat.jsonl) ? new Gson() : null;

            for (long ordinal : ordinals) {
                Optional<ByteBuffer> result = reader.get(namespace, ordinal);
                if (result.isEmpty()) {
                    System.err.printf("ordinal %d: not found%n", ordinal);
                    missing++;
                    continue;
                }

                ByteBuffer data = result.get();
                byte[] bytes = new byte[data.remaining()];
                data.get(bytes);

                if (asHex) {
                    printAsHex(bytes);
                } else if (asBase64) {
                    printAsBase64(bytes);
                } else {
                    printFormatted(ordinal, bytes, gson);
                }
            }
            return missing > 0 ? 1 : 0;
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return 2;
        }
    }

    /// Expands a list of ordinal specifiers into individual ordinals.
    ///
    /// Each specifier is either a bare long value or a range specifier
    /// supported by {@link OrdinalRange.Converter} (e.g. `0..9`,
    /// `[5,10)`, `[3]`). Bare ordinals are added directly; ranges are
    /// expanded into all ordinals in `[start, end)`.
    ///
    /// @param specs the raw specifier strings
    /// @return the expanded list of ordinals
    /// @throws IllegalArgumentException if a specifier cannot be parsed
    static List<Long> expandOrdinalSpecs(List<String> specs) {
        List<Long> result = new ArrayList<>();
        OrdinalRange.Converter converter = new OrdinalRange.Converter();
        for (String spec : specs) {
            String trimmed = spec.trim();
            try {
                result.add(Long.parseLong(trimmed));
            } catch (NumberFormatException e) {
                OrdinalRange.Range range = converter.convert(trimmed);
                for (long ord = range.start(); ord < range.end(); ord++) {
                    result.add(ord);
                }
            }
        }
        return result;
    }

    /// Prints record bytes as uppercase hex with spaces and a trailing newline.
    private void printAsHex(byte[] bytes) {
        HexFormat hex = HexFormat.of().withUpperCase();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < bytes.length; i++) {
            if (i > 0) sb.append(' ');
            sb.append(hex.toHexDigits(bytes[i]));
        }
        System.out.println(sb);
    }

    /// Prints record bytes as base64 with a trailing newline.
    private void printAsBase64(byte[] bytes) {
        System.out.println(Base64.getEncoder().encodeToString(bytes));
    }

    /// Prints a record in the configured format.
    private void printFormatted(long ordinal, byte[] bytes, Gson gson) {
        switch (format) {
            case hex -> {
                System.out.printf("ordinal %d (%d bytes):%n", ordinal, bytes.length);
                printHexDump(bytes, System.out);
            }
            case raw -> {
                try {
                    System.out.write(bytes);
                    System.out.flush();
                } catch (java.io.IOException e) {
                    System.err.println("Error writing raw output: " + e.getMessage());
                }
            }
            case ascii, utf8 -> {
                System.out.printf("ordinal %d: %s%n", ordinal, new String(bytes));
            }
            case json -> {
                String text = new String(bytes, StandardCharsets.UTF_8);
                System.out.printf("ordinal %d: %s%n", ordinal, gson.toJson(text));
            }
            case jsonl -> {
                String text = new String(bytes, StandardCharsets.UTF_8).trim();
                System.out.println(gson.toJson(text));
            }
        }
    }

    private static void printHexDump(byte[] bytes, PrintStream out) {
        HexFormat hex = HexFormat.of().withUpperCase();
        for (int i = 0; i < bytes.length; i += 16) {
            out.printf("  %04x  ", i);
            int end = Math.min(i + 16, bytes.length);
            StringBuilder hexPart = new StringBuilder();
            StringBuilder ascPart = new StringBuilder();
            for (int j = i; j < end; j++) {
                hexPart.append(hex.toHexDigits(bytes[j])).append(' ');
                ascPart.append(bytes[j] >= 0x20 && bytes[j] < 0x7F ? (char) bytes[j] : '.');
            }
            out.printf("%-48s  |%s|%n", hexPart, ascPart);
        }
    }
}
