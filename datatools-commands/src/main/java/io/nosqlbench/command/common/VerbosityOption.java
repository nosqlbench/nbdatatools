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
 * Shared verbosity control options.
 * Provides standard {@code -v/--verbose} and {@code -q/--quiet} flags for controlling
 * command output verbosity.
 */
public class VerbosityOption {

    @CommandLine.Option(
        names = {"-v", "--verbose"},
        description = "Enable verbose output"
    )
    private boolean verbose = false;

    @CommandLine.Option(
        names = {"-q", "--quiet"},
        description = "Suppress all output except errors"
    )
    private boolean quiet = false;

    /**
     * Checks if verbose mode is enabled.
     *
     * @return true if verbose is enabled
     */
    public boolean isVerbose() {
        return verbose;
    }

    /**
     * Checks if quiet mode is enabled.
     *
     * @return true if quiet is enabled
     */
    public boolean isQuiet() {
        return quiet;
    }

    /**
     * Checks if normal (non-verbose, non-quiet) output should be shown.
     *
     * @return true if normal output should be shown
     */
    public boolean showNormalOutput() {
        return !quiet;
    }

    /**
     * Checks if verbose messages should be shown.
     *
     * @return true if verbose messages should be shown
     */
    public boolean showVerbose() {
        return verbose && !quiet;
    }

    /**
     * Validates that verbose and quiet are not both enabled.
     *
     * @throws IllegalStateException if both verbose and quiet are enabled
     */
    public void validate() {
        if (verbose && quiet) {
            throw new IllegalStateException(
                "Cannot specify both --verbose and --quiet options"
            );
        }
    }
}
