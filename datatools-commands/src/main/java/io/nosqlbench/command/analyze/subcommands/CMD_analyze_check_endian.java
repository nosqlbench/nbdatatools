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

import io.nosqlbench.readers.ReaderUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/// Check that xvec header dimensions use the expected little-endian layout
@CommandLine.Command(name = "check-endian",
    header = "Verify xvec files use little-endian dimension headers",
    description = "Inspects xvec files to ensure vector counts are encoded with the expected little-endian order",
    exitCodeList = {"0: success", "1: validation error"})
public class CMD_analyze_check_endian implements Callable<Integer> {

    private static final Logger logger = LogManager.getLogger(CMD_analyze_check_endian.class);

    @CommandLine.Parameters(arity = "1..*", paramLabel = "FILE",
        description = "One or more xvec files (.fvec, .ivec, .bvec, .dvec) to inspect")
    private List<Path> files = new ArrayList<>();

    @Override
    public Integer call() {
        boolean hadError = false;
        for (Path file : files) {
            System.out.printf("Analyzing %s%n", file);
            if (!Files.exists(file)) {
                System.err.printf("  Error: File not found. Please verify the path and try again.%n");
                hadError = true;
                continue;
            }

            int elementWidth = resolveElementWidth(file);
            if (elementWidth <= 0) {
                System.err.printf("  Error: Unsupported file extension. Expected .fvec/.ivec/.bvec/.dvec variants.%n");
                hadError = true;
                continue;
            }

            System.out.printf("  Step 1: Element width detected as %d bytes%n", elementWidth);

            try {
                System.out.println("  Step 2: Evaluating header endianness assumptions...");
                ReaderUtils.EndianCheckResult result = ReaderUtils.checkXvecEndianness(file, elementWidth);

                if (result.isEndianMismatch()) {
                    System.out.println("    Result: Detected big-endian encoded vector counts (mismatch).");
                    System.err.printf("  Issue: %s appears to be big-endian encoded (dimension %d, vectors %d).%n",
                        file,
                        result.getBigEndianDimension(),
                        result.getBigEndianVectorCount());
                    System.err.println("  Suggestion: Re-run the export or conversion tooling with little-endian output enabled.");
                    hadError = true;
                    continue;
                }

                if (!result.isLittleEndianValid()) {
                    String reason = result.getLittleEndianFailureReason();
                    System.out.println("    Result: Unable to confirm little-endian layout.");
                    System.err.printf("  Issue: Could not validate little-endian header for %s.%n", file);
                    if (reason != null) {
                        System.err.printf("  Details: %s%n", reason);
                    }
                    System.err.println("  Suggestion: Verify the file is complete and follows the xvec specification.");
                    hadError = true;
                    continue;
                }

                System.out.println("    Result: Little-endian layout validated.");

                System.out.printf("%s: dimension %d, vectors %d%n",
                    file,
                    result.getLittleEndianDimension(),
                    result.getLittleEndianVectorCount());

                if (result.isBigEndianValid()) {
                    System.out.printf("  Step 3: Big-endian reinterpretation would report dimension %d, vectors %d.%n",
                        result.getBigEndianDimension(),
                        result.getBigEndianVectorCount());
                } else if (result.getBigEndianFailureReason() != null) {
                    logger.debug("Big-endian interpretation rejected for {}: {}", file,
                        result.getBigEndianFailureReason());
                    System.out.println("  Step 3: Big-endian reinterpretation rejected (see debug logs).");
                }

                System.out.println("  Step 4: Endianness check complete.\n");
            } catch (IOException e) {
                System.err.printf("  Error: Failed to inspect %s: %s%n", file, e.getMessage());
                System.err.println("  Suggestion: Ensure the file is readable and in a supported format.");
                hadError = true;
            }
        }

        return hadError ? 1 : 0;
    }

    private int resolveElementWidth(Path file) {
        String name = file.getFileName().toString().toLowerCase();
        if (name.endsWith(".fvec") || name.endsWith(".fvecs")
            || name.endsWith(".ivec") || name.endsWith(".ivecs")
            || name.endsWith(".bvec") || name.endsWith(".bvecs")
            || name.endsWith(".xvec") || name.endsWith(".xvecs")) {
            return 4;
        }
        if (name.endsWith(".dvec") || name.endsWith(".dvecs")) {
            return 8;
        }
        return -1;
    }
}
