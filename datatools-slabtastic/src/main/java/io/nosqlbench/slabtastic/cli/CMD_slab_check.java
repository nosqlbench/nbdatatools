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

import io.nosqlbench.slabtastic.*;
import picocli.CommandLine;

import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/// Checks a slabtastic file for structural errors or inconsistencies.
///
/// Delegates structural validation to {@link SlabFileValidator} and provides
/// verbose per-page progress output when requested. Validation runs three
/// passes:
///
/// **Pass 1 — Index-driven** (existing): reads the tail footer, locates the
/// pages page (or namespaces page), and validates each indexed data page.
///
/// **Pass 2 — Forward traversal**: walks from offset 0 using page headers
/// to jump page-by-page through the file, collecting all pages found.
///
/// **Pass 3 — Cross-check**: verifies that every index entry appears in the
/// forward traversal and every forward-traversal data page appears in the
/// index. Detects orphan pages and missing index entries.
@CommandLine.Command(
    name = "check",
    header = "Validate file integrity",
    description = "Checks a slabtastic file for errors or inconsistencies.",
    exitCodeList = {"0: File is valid", "1: Errors found", "2: Cannot read file"}
)
public class CMD_slab_check implements Callable<Integer>, SlabConstants {

    @CommandLine.Parameters(index = "0", description = "Path to the slabtastic file")
    private Path file;

    @CommandLine.Option(names = {"-v", "--verbose"}, description = "Show progress for each page")
    private boolean verbose;

    @Override
    public Integer call() {
        PrintStream out = System.out;
        PrintStream err = System.err;

        out.println("Checking: " + file + " (" + fileSize() + " bytes)");

        if (!verbose) {
            // Non-verbose mode: delegate entirely to validator
            List<String> errors = SlabFileValidator.validate(file);
            if (errors.isEmpty()) {
                out.println("All checks passed.");
                return 0;
            }
            for (String error : errors) {
                err.println("ERROR: " + error);
            }
            return errors.stream().anyMatch(e -> e.startsWith("File too small")
                || e.startsWith("Pages-page footer invalid")
                || e.startsWith("File does not end with")
                || e.startsWith("Pages-page pageSize")
                || e.startsWith("Cannot")) ? 2 : 1;
        }

        // Verbose mode: run page-by-page with progress
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ)) {
            long fileSize = ch.size();

            if (fileSize < FOOTER_V1_SIZE) {
                err.println("ERROR: File too small to contain a footer (" + fileSize + " bytes)");
                return 2;
            }

            ByteBuffer tailBuf = SlabFileValidator.readAt(ch, fileSize - FOOTER_V1_SIZE, FOOTER_V1_SIZE);
            PageFooter pagesFooter = PageFooter.readFrom(tailBuf, 0);

            try {
                pagesFooter.validate();
            } catch (IllegalStateException e) {
                err.println("ERROR: Pages-page footer invalid: " + e.getMessage());
                return 2;
            }

            if (pagesFooter.pageType() != PAGE_TYPE_PAGES_PAGE) {
                err.println("ERROR: File does not end with a pages page (type=" + pagesFooter.pageType() + ")");
                return 2;
            }

            int pagesPageSize = pagesFooter.pageSize();
            long pagesPageOffset = fileSize - pagesPageSize;

            if (pagesPageOffset < 0) {
                err.println("ERROR: Pages-page pageSize (" + pagesPageSize
                    + ") exceeds file size (" + fileSize + ")");
                return 2;
            }

            ByteBuffer pagesPageBuf = SlabFileValidator.readAt(ch, pagesPageOffset, pagesPageSize);
            SlabPage pagesPage;
            try {
                pagesPage = SlabPage.parseFrom(pagesPageBuf);
            } catch (Exception e) {
                err.println("ERROR: Cannot parse pages page: " + e.getMessage());
                return 2;
            }

            out.printf("Pages page OK: %d entries, %d bytes at offset %d%n",
                pagesPage.recordCount(), pagesPageSize, pagesPageOffset);

            List<PagesPageEntry> entries = new ArrayList<>();
            for (int i = 0; i < pagesPage.recordCount(); i++) {
                ByteBuffer rec = pagesPage.getRecord(i);
                byte[] recBytes = new byte[rec.remaining()];
                rec.get(recBytes);
                entries.add(PagesPageEntry.readFrom(ByteBuffer.wrap(recBytes), 0));
            }

            // Run full validation for non-page-specific checks
            List<String> allErrors = SlabFileValidator.validate(file);

            // Print per-page progress
            int pageErrors = 0;
            for (int i = 0; i < entries.size(); i++) {
                PagesPageEntry entry = entries.get(i);
                out.printf("  Page %d: ordinal=%d, offset=%d ... ", i, entry.startOrdinal(), entry.fileOffset());

                // Count errors specific to this page
                String pagePrefix = "Page " + i + ":";
                long errorsForPage = allErrors.stream().filter(e -> e.startsWith(pagePrefix)).count();

                if (errorsForPage == 0) {
                    out.println("OK");
                } else {
                    out.println("ERRORS");
                    pageErrors += errorsForPage;
                }
            }

            // Print all errors
            for (String error : allErrors) {
                err.println("ERROR: " + error);
            }

            if (allErrors.isEmpty()) {
                out.println("All checks passed.");
                return 0;
            } else {
                err.printf("%d error(s) found.%n", allErrors.size());
                return 1;
            }
        } catch (Exception e) {
            err.println("ERROR: " + e.getMessage());
            return 2;
        }
    }

    private long fileSize() {
        try {
            return java.nio.file.Files.size(file);
        } catch (Exception e) {
            return -1;
        }
    }
}
