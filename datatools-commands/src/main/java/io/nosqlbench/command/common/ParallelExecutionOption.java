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
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.nosqlbench.command.common;

import picocli.CommandLine;

/**
 * Shared parallel execution options.
 * Provides standard {@code --parallel} and {@code --threads} options for commands
 * that support concurrent processing.
 */
public class ParallelExecutionOption {

    @CommandLine.Option(
        names = {"-p", "--parallel"},
        description = "Enable parallel processing (auto-sizes based on available CPU cores and memory)"
    )
    private boolean parallel = false;

    @CommandLine.Option(
        names = {"--threads"},
        description = "Number of parallel threads (default: auto-detect based on CPU cores, always leaves 1 core free)"
    )
    private Integer explicitThreads;

    /**
     * Checks if parallel execution is enabled.
     *
     * @return true if parallel is enabled
     */
    public boolean isParallel() {
        return parallel;
    }

    /**
     * Gets the explicitly specified thread count, if any.
     *
     * @return the thread count, or null if auto-detect should be used
     */
    public Integer getExplicitThreads() {
        return explicitThreads;
    }

    /**
     * Calculates the optimal thread count based on available CPU cores.
     * Always leaves at least 1 core free for system processes.
     *
     * @return the optimal thread count
     */
    public int getOptimalThreadCount() {
        int availableCores = Runtime.getRuntime().availableProcessors();

        if (explicitThreads != null) {
            return Math.max(1, explicitThreads);
        } else if (parallel) {
            // Auto-detect: use all but 1 core (leaving 1 free for system)
            return Math.max(1, availableCores - 1);
        } else {
            // Sequential mode
            return 1;
        }
    }

    /**
     * Checks if the user explicitly specified more threads than available cores.
     *
     * @return true if thread count exceeds available cores
     */
    public boolean exceedsAvailableCores() {
        if (explicitThreads == null) {
            return false;
        }
        return explicitThreads >= Runtime.getRuntime().availableProcessors();
    }

    /**
     * Gets the effective execution mode (parallel or sequential).
     *
     * @return true if parallel execution should be used
     */
    public boolean isEffectivelyParallel() {
        return parallel || explicitThreads != null;
    }
}
