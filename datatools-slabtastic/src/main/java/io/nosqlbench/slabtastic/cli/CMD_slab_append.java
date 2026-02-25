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
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;

/// Appends records from a source slabtastic file onto the end of a target
/// slabtastic file.
///
/// Before reading any records, the source file is validated for structural
/// integrity using {@link SlabFileValidator}. If validation fails, the
/// command aborts with an error.
///
/// The target file is opened in append mode: existing data pages are
/// preserved, new data pages are written after the old pages page, and a
/// new pages page is written at the end referencing both old and new pages.
/// The old pages page becomes dead data.
///
/// All ordinals in the source file must be strictly greater than the
/// maximum ordinal already present in the target file. Use `--progress`
/// to print ongoing record counts to stderr.
@CommandLine.Command(
    name = "append",
    header = "Append records from another slab file",
    description = "Appends all records from a source slabtastic file onto the end of a target file.",
    exitCodeList = {"0: Success", "1: Error"}
)
public class CMD_slab_append implements Callable<Integer>, SlabConstants {

    @CommandLine.Parameters(index = "0", description = "Target slabtastic file to append to")
    private Path target;

    @CommandLine.Option(names = {"--from"}, required = true,
        description = "Source slabtastic file to read records from")
    private Path source;

    @CommandLine.Option(names = {"--preferred-page-size", "--page-size"}, defaultValue = "65536",
        description = "Preferred page size for new pages in bytes (default: ${DEFAULT-VALUE})")
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

    @CommandLine.Option(names = {"--namespace", "-n"}, defaultValue = "",
        description = "Namespace to read from and write to (default: default namespace)")
    private String namespace;

    @CommandLine.Option(names = {"--progress"},
        description = "Print progress counters to stderr during the append")
    private boolean progress;

    @Override
    public Integer call() {
        try {
            SlabFileValidator.requireValid(source);
        } catch (IllegalStateException e) {
            System.err.println("Error: Source file is not well formed: " + e.getMessage());
            return 1;
        }

        try (SlabReader sourceReader = new SlabReader(source)) {
            List<SlabReader.PageSummary> pages = sourceReader.pages(namespace);
            long sourceRecords = sourceReader.recordCount(namespace);

            if (sourceRecords == 0) {
                System.out.println("Source file has no records; nothing to append.");
                return 0;
            }

            var config = maxPageSize != null
                ? new SlabWriter.SlabWriterConfig(pageSize, minPageSize, pageAlignment, maxPageSize)
                : new SlabWriter.SlabWriterConfig(pageSize, minPageSize, pageAlignment);
            try (SlabWriter writer = SlabWriter.openForAppend(target, config)) {
                long written = 0;
                int pagesDone = 0;
                long lastProgressTime = System.nanoTime();

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

                        if (progress && (written % 1_000_000 == 0
                            || System.nanoTime() - lastProgressTime >= 1_000_000_000L)) {
                            System.err.printf("Processing: %,d records (%d pages) ...%n",
                                written, pagesDone);
                            lastProgressTime = System.nanoTime();
                        }
                    }
                    pagesDone++;
                }
                System.out.printf("Appended %,d records from %s into %s%n",
                    written, source, target);
            }
            return 0;
        } catch (Exception e) {
            System.err.printf("Error appending from %s: %s%n", source, e.getMessage());
            return 1;
        }
    }
}
