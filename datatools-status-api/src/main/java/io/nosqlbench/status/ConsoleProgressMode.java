/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.nosqlbench.status;

import java.util.Locale;

/**
 * Console status output modes for {@code nb.status.sink} system property.
 * This enum defines the supported modes for displaying task progress and status information.
 *
 * <p>Supported modes:</p>
 * <ul>
 *   <li><strong>AUTO</strong> - Automatically selects the best mode based on console availability</li>
 *   <li><strong>PANEL</strong> - Interactive TUI panel (requires console support)</li>
 *   <li><strong>LOG</strong> - Simple text-based logging output</li>
 *   <li><strong>OFF</strong> - Disables all status output</li>
 * </ul>
 */
public enum ConsoleProgressMode {
    /**
     * Automatically selects the best mode based on console availability.
     * If a console is detected, uses PANEL mode; otherwise falls back to LOG mode.
     */
    AUTO("auto"),

    /**
     * Interactive TUI panel mode using JLine3 for real-time status updates.
     * Provides hierarchical task display with progress bars and interactive controls.
     */
    PANEL("panel"),

    /**
     * Simple text-based logging mode using SLF4J.
     * Outputs status updates as log messages with progress bars.
     */
    LOG("log"),

    /**
     * Disables all status output.
     * No sinks are attached and no progress information is displayed.
     */
    OFF("off");

    private final String propertyValue;

    ConsoleProgressMode(String propertyValue) {
        this.propertyValue = propertyValue;
    }

    /**
     * Returns the system property value corresponding to this mode.
     * This value is used for the {@code nb.status.sink} system property.
     *
     * @return the property value string (e.g., "auto", "panel", "log", "off")
     */
    public String getPropertyValue() {
        return propertyValue;
    }

    /**
     * Parses a string value into a ConsoleProgressMode, accepting various aliases and synonyms.
     * This method is case-insensitive and trims whitespace.
     *
     * <p>Supported values and aliases:</p>
     * <ul>
     *   <li><strong>AUTO:</strong> "auto", "" (empty string)</li>
     *   <li><strong>PANEL:</strong> "panel", "tui", "console"</li>
     *   <li><strong>LOG:</strong> "log", "logger", "text"</li>
     *   <li><strong>OFF:</strong> "off", "none", "disable", "disabled", "false"</li>
     * </ul>
     *
     * @param value the string value to parse (may be null)
     * @return the corresponding ConsoleProgressMode, or null if the input is null
     * @throws IllegalArgumentException if the value is not recognized
     */
    public static ConsoleProgressMode fromString(String value) {
        if (value == null) {
            return null;
        }
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        switch (normalized) {
            case "panel":
            case "tui":
            case "console":
                return PANEL;
            case "log":
            case "logger":
            case "text":
                return LOG;
            case "off":
            case "none":
            case "disable":
            case "disabled":
            case "false":
                return OFF;
            case "auto":
            case "":
                return AUTO;
            default:
                throw new IllegalArgumentException(
                    "Unrecognized status mode '" + value + "'. Expected one of: auto, panel, log, off.");
        }
    }

    /**
     * Returns the enum constant name in lowercase for display purposes.
     *
     * @return lowercase name (e.g., "auto", "panel", "log", "off")
     */
    @Override
    public String toString() {
        return propertyValue;
    }
}
