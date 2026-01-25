package io.nosqlbench.vectordata.config.exceptions;

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

/// Exception thrown when auto:* resolution cannot find a suitable mount point.
///
/// This exception is thrown when:
/// - auto:largest-non-root is specified but no non-root writable mount points exist
/// - auto:largest-any is specified but no writable mount points exist at all
/// - No mount points have sufficient space or write permissions
public class NoSuitableMountPointException extends RuntimeException {

    private static final String MESSAGE_TEMPLATE =
        "\n" +
        "No suitable mount point found for cache directory.\n" +
        "\n" +
        "Attempted resolution: %s\n" +
        "Reason: %s\n" +
        "\n" +
        "Alternatives:\n" +
        "  1. Use 'auto:largest-any' to include the root filesystem\n" +
        "  2. Specify an explicit path: cache_dir: /path/to/cache\n" +
        "  3. Use 'default' to use ~/.cache/vectordata\n" +
        "  4. Run 'nbvectors config list-mounts' to see available mount points\n";

    private final String autoDirective;
    private final String reason;

    /// Constructs a NoSuitableMountPointException with details about the failed resolution.
    ///
    /// @param autoDirective the auto directive that was being resolved (e.g., "auto:largest-non-root")
    /// @param reason a description of why no suitable mount point was found
    public NoSuitableMountPointException(String autoDirective, String reason) {
        super(String.format(MESSAGE_TEMPLATE, autoDirective, reason));
        this.autoDirective = autoDirective;
        this.reason = reason;
    }

    /// Returns the auto directive that could not be resolved.
    ///
    /// @return the auto directive (e.g., "auto:largest-non-root")
    public String getAutoDirective() {
        return autoDirective;
    }

    /// Returns the reason why resolution failed.
    ///
    /// @return a description of why no suitable mount point was found
    public String getReason() {
        return reason;
    }
}
