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

import com.google.gson.JsonParser;
import io.nosqlbench.slabtastic.SlabConstants;
import io.nosqlbench.slabtastic.SlabReader;
import io.nosqlbench.slabtastic.SlabWriter;
import org.snakeyaml.engine.v2.api.Load;
import org.snakeyaml.engine.v2.api.LoadSettings;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;

/// Imports records from a non-slab source file into a new slabtastic file.
///
/// Supports text (newline-terminated), cstrings (null-terminated), and
/// structured record formats including JSON, JSONL, CSV, TSV, and YAML.
/// Slab-to-slab imports are also supported when the source file has a
/// `.slab` extension or `--format slab` is specified.
///
/// Delimiters are included in the record data — every byte of the original
/// file is preserved. Records are assigned sequential ordinals starting
/// from `--start-ordinal`.
///
/// Format auto-detection:
/// 1. If `--format` is set explicitly, use it
/// 2. Check source file extension against well-known map
/// 3. Fall through to content scanning (first 8192 bytes)
@CommandLine.Command(
    name = "import",
    header = "Import records from a non-slab file",
    description = "Imports records from text files, null-terminated files, structured formats, or other slab files into a slabtastic file.",
    exitCodeList = {"0: Success", "1: Error"}
)
public class CMD_slab_import implements Callable<Integer>, SlabConstants {

    /// Creates a new instance of the import subcommand.
    public CMD_slab_import() {}

    @CommandLine.Parameters(index = "0", description = "Target slabtastic file to create or append to")
    private Path target;

    @CommandLine.Option(names = {"--from"}, required = true,
        description = "Source file to import from")
    private Path source;

    @CommandLine.Option(names = {"--force", "-f"},
        description = "Overwrite target if it already exists (when not in append mode)")
    private boolean force;

    @CommandLine.Option(names = {"--preferred-page-size", "--page-size"}, defaultValue = "65536",
        description = "Preferred page size in bytes (default: ${DEFAULT-VALUE})")
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

    @CommandLine.Option(names = {"--start-ordinal"}, defaultValue = "-1",
        description = "Starting ordinal for imported records (default: 0, or auto-detected in append mode)")
    private long startOrdinal;

    @CommandLine.Option(names = {"--format"},
        description = "Source format: ${COMPLETION-CANDIDATES}")
    private SourceFormat format;

    @CommandLine.Option(names = {"--append"},
        description = "Append to target if it already exists")
    private boolean appendMode;

    @CommandLine.Option(names = {"--namespace", "-n"}, defaultValue = "",
        description = "Namespace to import into (default: default namespace)")
    private String namespace;

    @CommandLine.Option(names = {"--progress"},
        description = "Print progress counters to stderr during the import")
    private boolean progress;

    /// Format of the source file.
    enum SourceFormat {
        text,
        cstrings,
        slab,
        json,
        jsonl,
        csv,
        tsv,
        yaml
    }

    /// Well-known file extension to format mapping.
    private static final Map<String, SourceFormat> EXTENSION_MAP = Map.of(
        ".txt", SourceFormat.text,
        ".slab", SourceFormat.slab,
        ".json", SourceFormat.json,
        ".jsonl", SourceFormat.jsonl,
        ".csv", SourceFormat.csv,
        ".tsv", SourceFormat.tsv,
        ".yaml", SourceFormat.yaml,
        ".yml", SourceFormat.yaml
    );

    @Override
    public Integer call() {
        try {
            if (!Files.exists(source)) {
                System.err.println("Error: Source file does not exist: " + source);
                return 1;
            }

            if (appendMode) {
                // Append mode: target may or may not exist
                if (Files.exists(target)) {
                    // Auto-detect start ordinal from existing file
                    if (startOrdinal == -1) {
                        try (SlabReader reader = new SlabReader(target)) {
                            List<SlabReader.PageSummary> pages = reader.pages(namespace);
                            if (!pages.isEmpty()) {
                                SlabReader.PageSummary last = pages.getLast();
                                startOrdinal = last.startOrdinal() + last.recordCount();
                            } else {
                                startOrdinal = 0;
                            }
                        }
                    }
                } else {
                    // Append to non-existent file: just create new
                    if (startOrdinal == -1) startOrdinal = 0;
                }
            } else {
                if (startOrdinal == -1) startOrdinal = 0;
                if (Files.exists(target) && !force) {
                    System.err.println("Error: Target file already exists: " + target + " (use --force to overwrite)");
                    return 1;
                }
            }

            SourceFormat detectedFormat = detectFormat();
            if (detectedFormat == null) {
                return 1;
            }

            long recordCount = switch (detectedFormat) {
                case slab -> importFromSlab();
                case text -> importDelimited((byte) '\n');
                case cstrings -> importDelimited((byte) '\0');
                case json -> importJson();
                case jsonl -> importJsonl();
                case csv -> importCsv();
                case tsv -> importTsv();
                case yaml -> importYaml();
            };

            System.out.printf("Imported %,d records from %s (format: %s) into %s%n",
                recordCount, source, detectedFormat.name(), target);
            return 0;
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }

    /// Detects the source file format based on explicit `--format`, file
    /// extension, or content scanning.
    ///
    /// @return the detected format, or null if detection fails
    /// @throws IOException if the source file cannot be read
    SourceFormat detectFormat() throws IOException {
        // 1. Explicit --format flag
        if (format != null) return format;

        // 2. Well-known file extension
        String fileName = source.getFileName().toString().toLowerCase();
        for (Map.Entry<String, SourceFormat> entry : EXTENSION_MAP.entrySet()) {
            if (fileName.endsWith(entry.getKey())) {
                return entry.getValue();
            }
        }

        // 3. Content scanning
        return autoDetectFromContent();
    }

    /// Scans the first 8192 bytes of the source file to determine record
    /// format.
    ///
    /// @return the detected format, or null if no delimiters are found
    /// @throws IOException if the file cannot be read
    private SourceFormat autoDetectFromContent() throws IOException {
        long fileSize = Files.size(source);
        if (fileSize == 0) {
            return SourceFormat.text;
        }

        int scanSize = (int) Math.min(fileSize, 8192);
        byte[] sample = new byte[scanSize];
        try (FileChannel ch = FileChannel.open(source, StandardOpenOption.READ)) {
            ByteBuffer buf = ByteBuffer.wrap(sample);
            int totalRead = 0;
            while (totalRead < scanSize) {
                int read = ch.read(buf);
                if (read < 0) break;
                totalRead += read;
            }
        }

        boolean hasNull = false;
        boolean hasNewline = false;
        for (int i = 0; i < scanSize; i++) {
            if (sample[i] == 0) {
                hasNull = true;
            } else if (sample[i] == '\n') {
                hasNewline = true;
            }
        }

        if (hasNull) return SourceFormat.cstrings;
        if (hasNewline) return SourceFormat.text;

        System.err.println("Error: Cannot auto-detect record format; use --format to specify");
        return null;
    }

    /// Opens a writer, either for append or for new creation.
    ///
    /// In non-append mode, uses buffer naming to write to a `.buffer`
    /// temp file and atomically rename on close.
    private SlabWriter openWriter() throws IOException {
        var config = maxPageSize != null
            ? new SlabWriter.SlabWriterConfig(pageSize, minPageSize, pageAlignment, maxPageSize)
            : new SlabWriter.SlabWriterConfig(pageSize, minPageSize, pageAlignment);
        if (appendMode && Files.exists(target)) {
            return SlabWriter.openForAppend(target, config);
        }
        return SlabWriter.createWithBufferNaming(target, config);
    }

    /// Imports records from a slab source file, preserving ordinals.
    ///
    /// @return the number of records imported
    /// @throws IOException if an I/O error occurs
    private long importFromSlab() throws IOException {
        try (SlabReader sourceReader = new SlabReader(source)) {
            List<SlabReader.PageSummary> pages = sourceReader.pages(namespace);
            long sourceRecords = sourceReader.recordCount(namespace);

            if (sourceRecords == 0) {
                try (SlabWriter writer = openWriter()) {
                    // no records
                }
                return 0;
            }

            try (SlabWriter writer = openWriter()) {
                long written = 0;
                for (SlabReader.PageSummary ps : pages) {
                    for (int i = 0; i < ps.recordCount(); i++) {
                        long ordinal = ps.startOrdinal() + i;
                        Optional<ByteBuffer> data = sourceReader.get(namespace, ordinal);
                        if (data.isEmpty()) {
                            System.err.printf("WARNING: Missing record at ordinal %d in source%n", ordinal);
                            continue;
                        }
                        ByteBuffer buf = data.get();
                        byte[] bytes = new byte[buf.remaining()];
                        buf.get(bytes);
                        writer.write(namespace, ordinal, bytes);
                        written++;
                    }
                }
                return written;
            }
        }
    }

    /// Imports delimiter-separated records from a source file.
    ///
    /// Delimiters are included in the record data. If the file does not end
    /// with the delimiter, the trailing bytes form a valid final record
    /// without a delimiter.
    ///
    /// @param delimiter the byte used to split records
    /// @return the number of records imported
    /// @throws IOException if an I/O error occurs
    private long importDelimited(byte delimiter) throws IOException {
        long fileSize = Files.size(source);
        if (fileSize == 0) {
            try (SlabWriter writer = openWriter()) {
                // no records
            }
            return 0;
        }

        try (SlabWriter writer = openWriter();
             FileChannel ch = FileChannel.open(source, StandardOpenOption.READ)) {

            long ordinal = startOrdinal;
            int chunkSize = (int) Math.min(fileSize, 8 * 1024 * 1024);
            ByteBuffer readBuf = ByteBuffer.allocate(chunkSize);
            byte[] carryOver = null;
            long position = 0;

            while (position < fileSize) {
                readBuf.clear();
                int toRead = (int) Math.min(chunkSize, fileSize - position);
                readBuf.limit(toRead);

                int totalRead = 0;
                while (totalRead < toRead) {
                    int read = ch.read(readBuf, position + totalRead);
                    if (read < 0) break;
                    totalRead += read;
                }
                readBuf.flip();

                byte[] data = new byte[totalRead];
                readBuf.get(data);

                byte[] working;
                if (carryOver != null) {
                    working = new byte[carryOver.length + data.length];
                    System.arraycopy(carryOver, 0, working, 0, carryOver.length);
                    System.arraycopy(data, 0, working, carryOver.length, data.length);
                    carryOver = null;
                } else {
                    working = data;
                }

                int searchFrom = 0;
                while (searchFrom < working.length) {
                    int delimIdx = indexOf(working, delimiter, searchFrom);
                    if (delimIdx >= 0) {
                        int recordLen = delimIdx - searchFrom + 1;
                        byte[] record = new byte[recordLen];
                        System.arraycopy(working, searchFrom, record, 0, recordLen);
                        writer.write(namespace, ordinal++, record);
                        searchFrom = delimIdx + 1;
                    } else {
                        if (position + totalRead >= fileSize) {
                            int remaining = working.length - searchFrom;
                            if (remaining > 0) {
                                byte[] record = new byte[remaining];
                                System.arraycopy(working, searchFrom, record, 0, remaining);
                                writer.write(namespace, ordinal++, record);
                            }
                        } else {
                            int remaining = working.length - searchFrom;
                            carryOver = new byte[remaining];
                            System.arraycopy(working, searchFrom, carryOver, 0, remaining);
                        }
                        break;
                    }
                }

                position += totalRead;
            }

            return ordinal - startOrdinal;
        }
    }

    /// Imports JSON records from a source file.
    ///
    /// Each top-level JSON value (object, array, string, number, boolean,
    /// null) becomes one record. The raw bytes of each value (including
    /// leading whitespace) are preserved in the record data. Each value is
    /// validated as parsable JSON using `JsonParser.parseString()`.
    ///
    /// @return the number of records imported
    /// @throws IOException if an I/O error occurs or the JSON is invalid
    private long importJson() throws IOException {
        byte[] content = Files.readAllBytes(source);
        if (content.length == 0) {
            try (SlabWriter writer = openWriter()) {
                // no records
            }
            return 0;
        }

        try (SlabWriter writer = openWriter()) {
            return importJsonByDepthTracking(content, writer);
        }
    }

    /// Imports JSON by tracking brace/bracket depth to find top-level value
    /// boundaries. Includes all whitespace and delimiters in record data.
    private long importJsonByDepthTracking(byte[] content, SlabWriter writer) throws IOException {
        long ordinal = startOrdinal;
        int i = 0;
        int len = content.length;

        while (i < len) {
            // Skip whitespace before a value
            int valueStart = i;
            while (i < len && isJsonWhitespace(content[i])) {
                i++;
            }
            if (i >= len) {
                break;
            }

            int jsonStart = i;
            byte c = content[i];

            if (c == '{' || c == '[') {
                // Object or array: find matching close
                int depth = 0;
                boolean inString = false;
                boolean escaped = false;
                while (i < len) {
                    byte b = content[i];
                    if (escaped) {
                        escaped = false;
                    } else if (b == '\\' && inString) {
                        escaped = true;
                    } else if (b == '"') {
                        inString = !inString;
                    } else if (!inString) {
                        if (b == '{' || b == '[') depth++;
                        else if (b == '}' || b == ']') {
                            depth--;
                            if (depth == 0) {
                                i++;
                                break;
                            }
                        }
                    }
                    i++;
                }
            } else if (c == '"') {
                // String value
                i++;
                boolean escaped = false;
                while (i < len) {
                    byte b = content[i];
                    if (escaped) {
                        escaped = false;
                    } else if (b == '\\') {
                        escaped = true;
                    } else if (b == '"') {
                        i++;
                        break;
                    }
                    i++;
                }
            } else {
                // Number, true, false, null — read until whitespace or structural char
                while (i < len && !isJsonWhitespace(content[i])
                    && content[i] != ',' && content[i] != '}' && content[i] != ']') {
                    i++;
                }
            }

            // Record includes leading whitespace through the value
            int recordEnd = i;
            byte[] record = new byte[recordEnd - valueStart];
            System.arraycopy(content, valueStart, record, 0, record.length);

            // Validate the JSON portion
            String jsonPortion = new String(content, jsonStart, i - jsonStart, StandardCharsets.UTF_8).trim();
            try {
                JsonParser.parseString(jsonPortion);
            } catch (Exception e) {
                throw new IOException("Invalid JSON at byte offset " + jsonStart + ": " + e.getMessage());
            }

            writer.write(namespace, ordinal++, record);
        }

        return ordinal - startOrdinal;
    }

    private static boolean isJsonWhitespace(byte b) {
        return b == ' ' || b == '\t' || b == '\n' || b == '\r';
    }

    /// Imports JSONL records from a source file.
    ///
    /// Each line becomes one record. Each line is validated as parsable JSON
    /// using `JsonParser.parseString()`. Line delimiters are included in the
    /// record data.
    ///
    /// @return the number of records imported
    /// @throws IOException if an I/O error occurs or any line is not valid JSON
    private long importJsonl() throws IOException {
        byte[] content = Files.readAllBytes(source);
        if (content.length == 0) {
            try (SlabWriter writer = openWriter()) {
                // no records
            }
            return 0;
        }

        try (SlabWriter writer = openWriter()) {
            long ordinal = startOrdinal;
            int lineStart = 0;

            for (int i = 0; i <= content.length; i++) {
                if (i == content.length || content[i] == '\n') {
                    int recordEnd = (i < content.length) ? i + 1 : i;
                    if (recordEnd > lineStart) {
                        byte[] record = new byte[recordEnd - lineStart];
                        System.arraycopy(content, lineStart, record, 0, record.length);

                        // Validate JSON (trim whitespace for validation only)
                        String line = new String(content, lineStart,
                            (i < content.length ? i : i) - lineStart, StandardCharsets.UTF_8).trim();
                        if (!line.isEmpty()) {
                            try {
                                JsonParser.parseString(line);
                            } catch (Exception e) {
                                throw new IOException("Invalid JSON at line starting at byte " + lineStart
                                    + ": " + e.getMessage());
                            }
                            writer.write(namespace, ordinal++, record);
                        }
                    }
                    lineStart = recordEnd;
                }
            }

            return ordinal - startOrdinal;
        }
    }

    /// Imports CSV records from a source file.
    ///
    /// Each line becomes one record, with RFC 4180 quoted-field awareness:
    /// newlines inside double-quoted fields do not split records. Line
    /// delimiters are included in the record data.
    ///
    /// @return the number of records imported
    /// @throws IOException if an I/O error occurs
    private long importCsv() throws IOException {
        byte[] content = Files.readAllBytes(source);
        if (content.length == 0) {
            try (SlabWriter writer = openWriter()) {
                // no records
            }
            return 0;
        }

        try (SlabWriter writer = openWriter()) {
            long ordinal = startOrdinal;
            int recordStart = 0;
            boolean inQuote = false;

            for (int i = 0; i < content.length; i++) {
                byte b = content[i];
                if (inQuote) {
                    if (b == '"') {
                        // Check for escaped quote ("")
                        if (i + 1 < content.length && content[i + 1] == '"') {
                            i++; // skip escaped quote
                        } else {
                            inQuote = false;
                        }
                    }
                } else {
                    if (b == '"') {
                        inQuote = true;
                    } else if (b == '\n') {
                        int recordEnd = i + 1;
                        byte[] record = new byte[recordEnd - recordStart];
                        System.arraycopy(content, recordStart, record, 0, record.length);
                        writer.write(namespace, ordinal++, record);
                        recordStart = recordEnd;
                    }
                }
            }

            // Trailing content without newline
            if (recordStart < content.length) {
                byte[] record = new byte[content.length - recordStart];
                System.arraycopy(content, recordStart, record, 0, record.length);
                writer.write(namespace, ordinal++, record);
            }

            return ordinal - startOrdinal;
        }
    }

    /// Imports TSV records from a source file.
    ///
    /// Each line becomes one record. No quoting awareness — tabs are purely
    /// field separators within a line. Line delimiters are included in the
    /// record data.
    ///
    /// @return the number of records imported
    /// @throws IOException if an I/O error occurs
    private long importTsv() throws IOException {
        return importDelimited((byte) '\n');
    }

    /// Imports YAML records from a source file.
    ///
    /// Record boundaries are at YAML document boundaries (`---`). Each
    /// document is validated as parsable YAML using SnakeYAML Engine.
    /// Document separators are included in the record data.
    ///
    /// @return the number of records imported
    /// @throws IOException if an I/O error occurs or any document is not valid YAML
    private long importYaml() throws IOException {
        byte[] content = Files.readAllBytes(source);
        if (content.length == 0) {
            try (SlabWriter writer = openWriter()) {
                // no records
            }
            return 0;
        }

        try (SlabWriter writer = openWriter()) {
            long ordinal = startOrdinal;
            String text = new String(content, StandardCharsets.UTF_8);

            // Split on document boundaries (--- at start of line)
            // Keep the separator in the record data
            List<String> documents = splitYamlDocuments(text);

            LoadSettings settings = LoadSettings.builder().build();
            Load yamlLoader = new Load(settings);

            for (String doc : documents) {
                String trimmed = doc.trim();
                if (trimmed.isEmpty()) continue;

                // Remove leading --- for validation (if present)
                String toValidate = trimmed;
                if (toValidate.startsWith("---")) {
                    toValidate = toValidate.substring(3).trim();
                }
                // Remove trailing ... (document end marker)
                if (toValidate.endsWith("...")) {
                    toValidate = toValidate.substring(0, toValidate.length() - 3).trim();
                }

                if (!toValidate.isEmpty()) {
                    try {
                        yamlLoader.loadFromString(doc.trim());
                    } catch (Exception e) {
                        throw new IOException("Invalid YAML document at ordinal " + ordinal
                            + ": " + e.getMessage());
                    }
                }

                byte[] record = doc.getBytes(StandardCharsets.UTF_8);
                writer.write(namespace, ordinal++, record);
            }

            return ordinal - startOrdinal;
        }
    }

    /// Splits YAML text into documents, preserving the `---` separators in
    /// each document's content.
    private List<String> splitYamlDocuments(String text) {
        List<String> docs = new java.util.ArrayList<>();
        String[] lines = text.split("\n", -1);
        StringBuilder current = new StringBuilder();

        for (String line : lines) {
            if (line.equals("---") && !current.isEmpty()) {
                docs.add(current.toString());
                current = new StringBuilder();
            }
            if (!current.isEmpty()) {
                current.append("\n");
            }
            current.append(line);
        }

        if (!current.isEmpty()) {
            String remaining = current.toString();
            if (!remaining.trim().isEmpty()) {
                docs.add(remaining);
            }
        }

        return docs;
    }

    /// Finds the first occurrence of a byte in an array starting from an offset.
    ///
    /// @param data   the byte array to search
    /// @param target the byte to find
    /// @param from   the index to start searching from
    /// @return the index of the byte, or -1 if not found
    private static int indexOf(byte[] data, byte target, int from) {
        for (int i = from; i < data.length; i++) {
            if (data[i] == target) {
                return i;
            }
        }
        return -1;
    }
}
