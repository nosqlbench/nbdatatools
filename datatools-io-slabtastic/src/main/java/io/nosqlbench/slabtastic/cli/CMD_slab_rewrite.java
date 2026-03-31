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

import io.nosqlbench.slabtastic.SlabConstants;
import io.nosqlbench.slabtastic.SlabReader;
import io.nosqlbench.slabtastic.SlabWriter;
import picocli.CommandLine;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;

/// Rewrites a slabtastic file into a new file with clean page alignment,
/// monotonic ordinal order, and no unreferenced pages.
///
/// This command subsumes the former `repack` and `reorder` commands. It
/// always produces a clean output file — there is no short-circuit for
/// files that are already ordered. By default, the source file is validated
/// for structural consistency before reading; use `--skip-check` to bypass.
///
/// The output is written to a `.buffer` temp file and atomically renamed
/// on success. Use `--progress` to print ongoing record counts to stderr.
@CommandLine.Command(
    name = "rewrite",
    header = "Rewrite a slabtastic file",
    description = "Writes a new file from an existing one, repacking pages, eliding unused pages, and reordering for monotonicity.",
    exitCodeList = {"0: Success", "1: Error"}
)
public class CMD_slab_rewrite implements Callable<Integer>, SlabConstants {

    /// Creates a new instance of the rewrite subcommand.
    public CMD_slab_rewrite() {}

    @CommandLine.Parameters(index = "0", description = "Source slabtastic file")
    private Path source;

    @CommandLine.Parameters(index = "1", description = "Destination file")
    private Path dest;

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

    @CommandLine.Option(names = {"-f", "--force"}, description = "Overwrite destination if it exists")
    private boolean force;

    @CommandLine.Option(names = {"--skip-check"},
        description = "Skip pre-flight structural validation of the source file")
    private boolean skipCheck;

    @CommandLine.Option(names = {"--namespace", "-n"},
        description = "Namespace to rewrite; if omitted, all namespaces are rewritten")
    private String namespace;

    @CommandLine.Option(names = {"--progress"},
        description = "Print progress counters to stderr during the rewrite")
    private boolean progress;

    @Override
    public Integer call() {
        if (Files.exists(dest) && !force) {
            System.err.println("Destination already exists: " + dest + " (use --force to overwrite)");
            return 1;
        }

        // Pre-flight validation
        if (!skipCheck) {
            List<String> errors = SlabFileValidator.validate(source);
            if (!errors.isEmpty()) {
                System.err.println("Source file validation failed:");
                for (String error : errors) {
                    System.err.println("  " + error);
                }
                System.err.println("Use --skip-check to bypass validation.");
                return 1;
            }
        }

        try (SlabReader reader = new SlabReader(source)) {
            // Determine which namespaces to rewrite
            Set<String> namespacesToRewrite;
            if (namespace != null) {
                namespacesToRewrite = Set.of(namespace);
            } else {
                namespacesToRewrite = reader.namespaces();
            }

            var config = maxPageSize != null
                ? new SlabWriter.SlabWriterConfig(pageSize, minPageSize, pageAlignment, maxPageSize)
                : new SlabWriter.SlabWriterConfig(pageSize, minPageSize, pageAlignment);

            try (SlabWriter writer = SlabWriter.createWithBufferNaming(dest, config)) {
                long totalWritten = 0;
                int totalPages = 0;

                for (String ns : namespacesToRewrite) {
                    List<SlabReader.PageSummary> pages = reader.pages(ns);
                    long written = 0;
                    long lastProgressTime = System.nanoTime();

                    for (SlabReader.PageSummary ps : pages) {
                        for (int i = 0; i < ps.recordCount(); i++) {
                            long ordinal = ps.startOrdinal() + i;
                            Optional<ByteBuffer> data = reader.get(ns, ordinal);
                            if (data.isEmpty()) {
                                System.err.printf("WARNING: Missing record at ordinal %d in namespace '%s'%n",
                                    ordinal, ns);
                                continue;
                            }
                            ByteBuffer buf = data.get();
                            byte[] bytes = new byte[buf.remaining()];
                            buf.get(bytes);
                            writer.write(ns, ordinal, bytes);
                            written++;

                            if (progress && (written % 1_000_000 == 0
                                || System.nanoTime() - lastProgressTime >= 1_000_000_000L)) {
                                System.err.printf("Processing namespace '%s': %,d records ...%n",
                                    ns, written);
                                lastProgressTime = System.nanoTime();
                            }
                        }
                    }
                    totalWritten += written;
                    totalPages += pages.size();
                }
                if (namespacesToRewrite.size() > 1) {
                    System.out.printf("Rewrote %,d records from %d pages across %d namespaces into %s%n",
                        totalWritten, totalPages, namespacesToRewrite.size(), dest);
                } else {
                    System.out.printf("Rewrote %,d records from %d pages into %s%n",
                        totalWritten, totalPages, dest);
                }
            }
            return 0;
        } catch (Exception e) {
            System.err.printf("Error rewriting %s → %s: %s%n", source, dest, e.getMessage());
            return 1;
        }
    }
}
