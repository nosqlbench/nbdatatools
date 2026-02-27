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
import com.google.gson.JsonParser;
import io.nosqlbench.slabtastic.SlabConstants;
import io.nosqlbench.slabtastic.SlabReader;
import io.nosqlbench.slabtastic.SlabWriter;
import picocli.CommandLine;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Optional;
import java.util.concurrent.Callable;

/// Exports records from a slabtastic file to an output file or stdout.
///
/// Supports multiple output formats including raw bytes, text, hex dump,
/// slab-to-slab copy, and structured formats (JSON, JSONL, CSV, TSV, YAML).
/// An optional ordinal range filter allows exporting a subset of records.
///
/// Output overrides `--as-hex` and `--as-base64` render record bytes in hex
/// or base64 with trailing newlines, regardless of the format setting.
///
/// By default, parsability is checked for structured formats. Use
/// `--skip-validation` to disable this check if you trust the data.
@CommandLine.Command(
    name = "export",
    header = "Export records from a slab file",
    description = "Exports records from a slabtastic file to a file or stdout in various formats.",
    exitCodeList = {"0: Success", "1: Error"}
)
public class CMD_slab_export implements Callable<Integer>, SlabConstants {

    /// Creates a new export command instance.
    public CMD_slab_export() {}

    /// Output format for exported records.
    public enum ExportFormat {
        /// Raw bytes, no transformation
        raw,
        /// Text with trailing newlines
        text,
        /// Null-terminated C strings
        cstrings,
        /// Slab-to-slab copy
        slab,
        /// JSON values
        json,
        /// JSON lines (one value per line)
        jsonl,
        /// Comma-separated values
        csv,
        /// Tab-separated values
        tsv,
        /// YAML documents
        yaml,
        /// Hex dump with offset headers
        hex,
        /// UTF-8 text with ordinal prefix
        utf8,
        /// ASCII text with ordinal prefix
        ascii
    }

    @CommandLine.Parameters(index = "0", description = "Slab file to read")
    private Path source;

    @CommandLine.Option(names = {"--to"},
        description = "Output file; stdout if omitted")
    private Path outputPath;

    @CommandLine.Option(names = {"--range"},
        description = "Ordinal range filter (formats: n, m..n, [m,n), [m,n], (m,n), (m,n], [n])",
        converter = OrdinalRange.Converter.class)
    private OrdinalRange.Range range;

    @CommandLine.Option(names = {"--format"}, defaultValue = "raw",
        description = "Output format: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})")
    private ExportFormat format;

    @CommandLine.Option(names = {"--skip-validation"},
        description = "Disable parsability checking for structured formats")
    private boolean skipValidation;

    @CommandLine.Option(names = {"--force", "-f"},
        description = "Overwrite output file if it already exists")
    private boolean force;

    @CommandLine.Option(names = {"--preferred-page-size", "--page-size"}, defaultValue = "65536",
        description = "Page size for slab format output (default: ${DEFAULT-VALUE})")
    private int pageSize;

    @CommandLine.Option(names = {"--min-page-size"}, defaultValue = "512",
        description = "Minimum page size and alignment granularity in bytes (default: ${DEFAULT-VALUE})")
    private int minPageSize;

    @CommandLine.Option(names = {"--page-alignment"},
        description = "Align pages to preferred page size boundaries")
    private boolean pageAlignment;

    @CommandLine.Option(names = {"--max-page-size"},
        description = "Maximum allowed page size in bytes (default: no limit)")
    private Integer maxPageSize;

    @CommandLine.Option(names = {"--as-hex"},
        description = "Output bytes as hex with space between each byte, trailing newline per record")
    private boolean asHex;

    @CommandLine.Option(names = {"--as-base64"},
        description = "Output bytes as base64 with trailing newline per record")
    private boolean asBase64;

    @CommandLine.Option(names = {"--namespace", "-n"}, defaultValue = "",
        description = "Namespace to export from; required when the file has named namespaces")
    private String namespace;

    @CommandLine.Option(names = {"--progress"},
        description = "Print progress counters to stderr during the export")
    private boolean progress;

    @Override
    public Integer call() {
        try {
            if (asHex && asBase64) {
                System.err.println("Error: --as-hex and --as-base64 are mutually exclusive");
                return 1;
            }

            if (!Files.exists(source)) {
                System.err.println("Error: Source file does not exist: " + source);
                return 1;
            }

            if (outputPath != null && Files.exists(outputPath) && !force) {
                System.err.println("Error: Output file already exists: " + outputPath + " (use --force to overwrite)");
                return 1;
            }

            // Resolve namespace before any read operations
            try (SlabReader probe = new SlabReader(source)) {
                String resolved = NamespaceResolver.resolveForRead(namespace, probe);
                if (resolved == null) {
                    System.err.println("Error: " + NamespaceResolver.formatNamespaceHint(probe));
                    return 1;
                }
                namespace = resolved;
            }

            if (format == ExportFormat.slab && !asHex && !asBase64) {
                return exportToSlab();
            }

            return exportToStream();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }

    /// Exports records into a new slab file.
    private int exportToSlab() throws IOException {
        if (outputPath == null) {
            System.err.println("Error: --to is required for slab format output");
            return 1;
        }

        var config = maxPageSize != null
            ? new SlabWriter.SlabWriterConfig(pageSize, minPageSize, pageAlignment, maxPageSize)
            : new SlabWriter.SlabWriterConfig(pageSize, minPageSize, pageAlignment);
        try (SlabReader reader = new SlabReader(source);
             SlabWriter writer = SlabWriter.createWithBufferNaming(outputPath, config)) {

            long written = 0;
            for (SlabReader.PageSummary ps : reader.pages(namespace)) {
                for (int i = 0; i < ps.recordCount(); i++) {
                    long ordinal = ps.startOrdinal() + i;
                    if (range != null && !range.contains(ordinal)) {
                        continue;
                    }
                    Optional<ByteBuffer> data = reader.get(namespace, ordinal);
                    if (data.isEmpty()) continue;
                    ByteBuffer buf = data.get();
                    byte[] bytes = new byte[buf.remaining()];
                    buf.get(bytes);
                    writer.write(namespace, ordinal, bytes);
                    written++;
                }
            }

            System.err.printf("Exported %,d records to %s (slab format)%n", written, outputPath);
        }
        return 0;
    }

    /// Exports records to a stream (file or stdout).
    private int exportToStream() throws IOException {
        try (SlabReader reader = new SlabReader(source);
             OutputStream out = openOutput()) {

            Gson gson = (format == ExportFormat.json || format == ExportFormat.jsonl) ? new Gson() : null;
            HexFormat hexFmt = (format == ExportFormat.hex) ? HexFormat.of().withUpperCase() : null;
            long written = 0;

            for (SlabReader.PageSummary ps : reader.pages(namespace)) {
                for (int i = 0; i < ps.recordCount(); i++) {
                    long ordinal = ps.startOrdinal() + i;
                    if (range != null && !range.contains(ordinal)) {
                        continue;
                    }
                    Optional<ByteBuffer> data = reader.get(namespace, ordinal);
                    if (data.isEmpty()) continue;
                    ByteBuffer buf = data.get();
                    byte[] bytes = new byte[buf.remaining()];
                    buf.get(bytes);

                    if (asHex) {
                        writeAsHex(out, bytes);
                    } else if (asBase64) {
                        writeAsBase64(out, bytes);
                    } else {
                        writeFormatted(out, bytes, ordinal, gson, hexFmt);
                    }
                    written++;
                }
            }

            out.flush();
            System.err.printf("Exported %,d records%n", written);
        }
        return 0;
    }

    /// Writes a record as uppercase hex with spaces between bytes and a trailing newline.
    private void writeAsHex(OutputStream out, byte[] bytes) throws IOException {
        HexFormat hex = HexFormat.of().withUpperCase();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < bytes.length; i++) {
            if (i > 0) sb.append(' ');
            sb.append(hex.toHexDigits(bytes[i]));
        }
        sb.append('\n');
        out.write(sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    /// Writes a record as base64 with a trailing newline.
    private void writeAsBase64(OutputStream out, byte[] bytes) throws IOException {
        String encoded = Base64.getEncoder().encodeToString(bytes);
        out.write((encoded + "\n").getBytes(StandardCharsets.UTF_8));
    }

    /// Writes a single record in the configured format.
    private void writeFormatted(OutputStream out, byte[] bytes, long ordinal,
                                Gson gson, HexFormat hexFmt) throws IOException {
        switch (format) {
            case raw -> {
                out.write(bytes);
            }
            case text -> {
                out.write(bytes);
                // Add newline if the record doesn't already end with one
                if (bytes.length == 0 || bytes[bytes.length - 1] != '\n') {
                    out.write('\n');
                }
            }
            case cstrings -> {
                out.write(bytes);
                if (bytes.length == 0 || bytes[bytes.length - 1] != 0) {
                    out.write(0);
                }
            }
            case json -> {
                String text = new String(bytes, StandardCharsets.UTF_8);
                if (!skipValidation) {
                    validateJson(text, ordinal);
                }
                out.write(text.getBytes(StandardCharsets.UTF_8));
                out.write('\n');
            }
            case jsonl -> {
                String text = new String(bytes, StandardCharsets.UTF_8).trim();
                if (!skipValidation) {
                    validateJson(text, ordinal);
                }
                // Wrap as a JSON string value
                String jsonStr = gson.toJson(text);
                out.write(jsonStr.getBytes(StandardCharsets.UTF_8));
                out.write('\n');
            }
            case csv, tsv, yaml -> {
                out.write(bytes);
            }
            case hex -> {
                String header = "ordinal %d (%d bytes):\n".formatted(ordinal, bytes.length);
                out.write(header.getBytes(StandardCharsets.UTF_8));
                writeHexDump(out, bytes, hexFmt);
            }
            case utf8, ascii -> {
                String header = "ordinal %d: ".formatted(ordinal);
                out.write(header.getBytes(StandardCharsets.UTF_8));
                out.write(bytes);
                out.write('\n');
            }
            default -> out.write(bytes);
        }
    }

    private void validateJson(String text, long ordinal) throws IOException {
        try {
            JsonParser.parseString(text.trim());
        } catch (Exception e) {
            throw new IOException("Invalid JSON at ordinal " + ordinal + ": " + e.getMessage());
        }
    }

    private void writeHexDump(OutputStream out, byte[] bytes, HexFormat hex) throws IOException {
        for (int i = 0; i < bytes.length; i += 16) {
            StringBuilder sb = new StringBuilder();
            sb.append("  %04x  ".formatted(i));
            int end = Math.min(i + 16, bytes.length);
            StringBuilder hexPart = new StringBuilder();
            StringBuilder ascPart = new StringBuilder();
            for (int j = i; j < end; j++) {
                hexPart.append(hex.toHexDigits(bytes[j])).append(' ');
                ascPart.append(bytes[j] >= 0x20 && bytes[j] < 0x7F ? (char) bytes[j] : '.');
            }
            sb.append("%-48s  |%s|\n".formatted(hexPart, ascPart));
            out.write(sb.toString().getBytes(StandardCharsets.UTF_8));
        }
    }

    private OutputStream openOutput() throws IOException {
        if (outputPath == null) {
            return System.out;
        }
        return Files.newOutputStream(outputPath,
            StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
    }
}
