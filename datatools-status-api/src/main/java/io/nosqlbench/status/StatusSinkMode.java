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
 * Status sink output modes for {@code nb.status.sink} system property.
 * This enum defines the supported modes for displaying task progress and status information,
 * and controls the logging configuration for the entire application.
 *
 * <p><strong>IMPORTANT:</strong> The status sink mode is the single source of truth for
 * logging configuration. When a mode is initialized, it automatically configures the logging
 * framework to match the output requirements of that mode.</p>
 *
 * <p>Supported modes:</p>
 * <ul>
 *   <li><strong>AUTO</strong> - Automatically selects the best mode based on console availability</li>
 *   <li><strong>PANEL</strong> - Interactive TUI panel (requires console support, configures logging to LogBuffer)</li>
 *   <li><strong>LOG</strong> - Simple text-based logging output (uses standard console appenders)</li>
 *   <li><strong>OFF</strong> - Disables all status output and logging</li>
 * </ul>
 */
public enum StatusSinkMode {
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

    StatusSinkMode(String propertyValue) {
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
     * Parses a string value into a StatusSinkMode, accepting various aliases and synonyms.
     * This method is case-insensitive and trims whitespace.
     *
     * <p>Supported values and aliases:</p>
     * <ul>
     *   <li><strong>AUTO:</strong> "auto", "default", "" (empty string)</li>
     *   <li><strong>PANEL:</strong> "panel", "tui", "console"</li>
     *   <li><strong>LOG:</strong> "log", "logger", "text"</li>
     *   <li><strong>OFF:</strong> "off", "none", "disable", "disabled", "false"</li>
     * </ul>
     *
     * @param value the string value to parse (may be null)
     * @return the corresponding StatusSinkMode, or null if the input is null
     * @throws IllegalArgumentException if the value is not recognized
     */
    public static StatusSinkMode fromString(String value) {
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
            case "default":
            case "":
                return AUTO;
            default:
                throw new IllegalArgumentException(
                    "Unrecognized status mode '" + value + "'. Expected one of: auto, panel, log, off.");
        }
    }

    /**
     * Initializes the status sink mode and configures logging as the FIRST action in main()
     * (after optional command line parsing).
     *
     * <p>This method MUST be called before any other classes are loaded that have static Logger fields.
     * It validates that the calling class has no Logger fields, reads the {@code nb.status.sink}
     * system property, resolves AUTO mode to a concrete mode, and configures the logging framework.</p>
     *
     * <p><strong>CRITICAL:</strong> Call this immediately after command line parsing in your main() method:</p>
     * <pre>{@code
     * public static void main(String[] args) {
     *     // Optional: Parse command line args to set -Dnb.status.sink=MODE
     *     StatusSinkMode.initializeEarly();  // MUST BE CALLED BEFORE ANY LOGGER FIELDS LOAD!
     *     // ... rest of your code
     * }
     * }</pre>
     *
     * <p>The method uses {@link LoggerFieldValidator} to ensure no Logger fields exist in the
     * calling class at initialization time. If any are found, an {@link IllegalStateException}
     * is thrown with details about the offending fields.</p>
     *
     * @return the resolved status sink mode that was configured
     * @throws IllegalStateException if Logger fields are detected in the calling class
     */
    public static StatusSinkMode initializeEarly() {
        // Validate that calling class has no Logger fields yet
        Class<?> callingClass = StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE)
            .walk(frames -> frames
                .skip(1)  // Skip initializeEarly itself
                .findFirst()
                .map(StackWalker.StackFrame::getDeclaringClass)
                .orElse(null)
            );

        LoggerFieldValidator.validateNoLoggerFieldsExist(callingClass);

        String modeProp = System.getProperty("nb.status.sink", "auto");
        StatusSinkMode mode;

        try {
            mode = StatusSinkMode.fromString(modeProp);
            if (mode == null) {
                mode = StatusSinkMode.AUTO;
            }
        } catch (IllegalArgumentException e) {
            mode = StatusSinkMode.AUTO;
        }

        // Resolve AUTO to concrete mode
        if (mode == StatusSinkMode.AUTO) {
            mode = (System.console() != null) ? StatusSinkMode.PANEL : StatusSinkMode.LOG;
        }

        // Persist the resolved mode
        System.setProperty("nb.status.sink", mode.getPropertyValue());

        // Configure logging IMMEDIATELY
        mode.configureLogging();

        return mode;
    }

    /**
     * Configures the logging framework for this mode. This method should be called
     * once during application initialization to set up logging appropriately.
     *
     * <p><strong>NOTE:</strong> You should normally use {@link #initializeEarly()} instead
     * of calling this method directly.</p>
     *
     * <p>Configuration by mode:</p>
     * <ul>
     *   <li><strong>PANEL:</strong> Installs LogBuffer appender, removes console appenders</li>
     *   <li><strong>LOG:</strong> Uses default logging configuration (console appenders)</li>
     *   <li><strong>OFF:</strong> Disables all logging output</li>
     *   <li><strong>AUTO:</strong> Should be resolved to a concrete mode before calling this</li>
     * </ul>
     */
    public void configureLogging() {
        switch (this) {
            case PANEL:
                io.nosqlbench.status.sinks.ConsolePanelLogIntercept.configure(
                    io.nosqlbench.status.sinks.OutputMode.INTERACTIVE
                );
                break;
            case LOG:
                // Use default logging configuration - do nothing
                break;
            case OFF:
                // Disable logging
                disableAllLogging();
                break;
            case AUTO:
                throw new IllegalStateException("AUTO mode must be resolved to a concrete mode before configuring logging");
        }
    }

    private static void disableAllLogging() {
        try {
            org.apache.logging.log4j.core.LoggerContext context =
                (org.apache.logging.log4j.core.LoggerContext) org.apache.logging.log4j.LogManager.getContext(false);
            org.apache.logging.log4j.core.config.Configuration config = context.getConfiguration();
            org.apache.logging.log4j.core.config.LoggerConfig rootConfig = config.getRootLogger();

            // Remove all appenders
            for (String appenderName : new java.util.ArrayList<>(rootConfig.getAppenders().keySet())) {
                rootConfig.removeAppender(appenderName);
            }

            // Set level to OFF
            rootConfig.setLevel(org.apache.logging.log4j.Level.OFF);
            context.updateLoggers();
        } catch (Exception e) {
            // Best effort - ignore if logging framework not available
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
