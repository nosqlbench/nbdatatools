package io.nosqlbench.command.common;

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

import picocli.CommandLine;

import java.util.ArrayList;
import java.util.List;

/// Utility class for formatting command line information from picocli CommandSpec
public class CommandLineFormatter {

    /// Get the full command line string from the root command to the current command
    /// @param spec The CommandSpec of the current command
    /// @return The full command line (e.g., "nbvectors analyze verify_knn")
    public static String getFullCommandName(CommandLine.Model.CommandSpec spec) {
        List<String> parts = new ArrayList<>();

        // Walk up the command hierarchy to get all command names
        CommandLine.Model.CommandSpec current = spec;
        while (current != null) {
            String name = current.name();
            if (name != null && !name.isEmpty()) {
                parts.add(0, name);  // Add to front to get root-to-leaf order
            }
            current = current.parent();
        }

        return String.join(" ", parts);
    }

    /// Format the full command line with arguments for display
    /// @param spec The CommandSpec of the current command
    /// @param args The original command line arguments
    /// @return The formatted command line string
    public static String formatCommandLine(CommandLine.Model.CommandSpec spec, String[] args) {
        String baseCommand = getFullCommandName(spec);
        if (args != null && args.length > 0) {
            // Filter out the command names from args since we already have them
            List<String> argList = new ArrayList<>();
            int skipCount = baseCommand.split("\\s+").length - 1; // Skip all but root command
            for (int i = Math.max(0, skipCount); i < args.length; i++) {
                argList.add(args[i]);
            }
            if (!argList.isEmpty()) {
                return baseCommand + " " + String.join(" ", argList);
            }
        }
        return baseCommand;
    }

    /// Print the command line to standard output
    /// @param spec The CommandSpec of the current command
    public static void printCommandLine(CommandLine.Model.CommandSpec spec) {
        String commandLine = getFullCommandName(spec);
        spec.commandLine().getOut().printf("%s%n", commandLine);
    }

    /// Print the command line to standard output with original arguments
    /// @param spec The CommandSpec of the current command
    /// @param args The original command line arguments
    public static void printCommandLine(CommandLine.Model.CommandSpec spec, String[] args) {
        String commandLine = formatCommandLine(spec, args);
        spec.commandLine().getOut().printf("%s%n", commandLine);
    }
}
