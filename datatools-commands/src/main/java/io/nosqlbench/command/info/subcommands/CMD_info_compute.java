package io.nosqlbench.command.info.subcommands;

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

import io.nosqlbench.vshapes.ComputeMode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.util.concurrent.Callable;

/// Show compute environment capabilities including SIMD/CPU information.
///
/// This command displays detailed information about the runtime environment
/// including Java version, CPU SIMD capabilities (AVX-512F, AVX2, AVX, SSE),
/// Panama Vector API availability, and the selected compute mode.
///
/// ## Usage
///
/// ```bash
/// nbvectors info compute
/// ```
///
/// ## Output
///
/// Shows a detailed report including:
/// - Java runtime version
/// - Operating system information
/// - CPU SIMD capability flags (AVX-512F, AVX2, AVX)
/// - Panama Vector API status and vector width
/// - Selected compute mode and expected performance
///
/// ## Example Output
///
/// ```
/// ═══════════════════════════════════════════════════════════
///               COMPUTE MODE CAPABILITY REPORT
/// ═══════════════════════════════════════════════════════════
///
/// Runtime Environment:
///   Java Version:        25 (25.0.1)
///   OS:                  Linux amd64
///
/// CPU Capabilities:
///   Detection Available: Yes
///   AVX-512F:            ✓ Supported
///   AVX2:                ✓ Supported
///   AVX:                 ✓ Supported
///   Best SIMD:           AVX-512F (512-bit SIMD)
///
/// Panama Vector API:
///   Available:           ✓ Yes
///   Preferred Width:     512 bits
///   Float Lanes:         16 per vector
///   Double Lanes:        8 per vector
///
/// Selected Mode:
///   Mode:                Panama AVX-512F
///   Description:         512-bit SIMD vectorization (AVX-512 Foundation)
///   Float Throughput:    16x scalar
/// ```
@CommandLine.Command(
    name = "compute",
    header = "Show compute environment capabilities",
    description = "Displays SIMD/CPU capabilities and Panama Vector API status.",
    exitCodeList = {
        "0: Success"
    }
)
public class CMD_info_compute implements Callable<Integer> {

    private static final Logger logger = LogManager.getLogger(CMD_info_compute.class);

    @CommandLine.Option(
        names = {"--short", "-s"},
        description = "Show only a one-line summary"
    )
    private boolean shortOutput = false;

    @Override
    public Integer call() {
        if (shortOutput) {
            System.out.println(ComputeMode.getModeSummary());
        } else {
            System.out.print(ComputeMode.getCapabilityReport());

            // Check for misconfiguration and print warning
            if (ComputeMode.isPanamaMisconfigured()) {
                System.out.println();
                System.out.println("⚠ WARNING: " + ComputeMode.getPanamaMisconfigurationMessage());
            }
        }
        return 0;
    }
}
