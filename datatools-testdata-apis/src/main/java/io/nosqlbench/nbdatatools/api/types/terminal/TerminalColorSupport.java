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

package io.nosqlbench.nbdatatools.api.types.terminal;

import java.util.Map;
import java.util.function.Function;

/// Helper class for determining terminal color support based on environment variables.
///
/// This class provides methods to detect the color depth supported by the current terminal
/// by examining environment variables, primarily COLORTERM and TERM.
///
/// The detection logic follows this order:
/// 1. Check COLORTERM environment variable
/// 2. If COLORTERM is not definitive, check TERM environment variable
/// 3. If neither provides clear information, default to NOCOLOR
///
/// ```
/// ┌─────────────────────┬─────────────────────┬─────────────────────┐
/// │ Environment Check   │ Values              │ Color Depth         │
/// ├─────────────────────┼─────────────────────┼─────────────────────┤
/// │ COLORTERM           │ "truecolor"         │ ANSI24BITCOLOR      │
/// │                     │ "24bit"             │ ANSI24BITCOLOR      │
/// │                     │ "256color"          │ ANSI256COLOR        │
/// │                     │ other non-null      │ ANSI8COLOR          │
/// ├─────────────────────┼─────────────────────┼─────────────────────┤
/// │ TERM                │ contains "256color" │ ANSI256COLOR        │
/// │                     │ contains "color"    │ ANSI8COLOR          │
/// │                     │ other               │ NOCOLOR             │
/// └─────────────────────┴─────────────────────┴─────────────────────┘
/// ```
public class TerminalColorSupport {

    /// Default constructor for the TerminalColorSupport utility class.
    /// This class provides static methods for detecting terminal color support.
    public TerminalColorSupport() {
        // Default constructor
    }

    // Default environment variable accessor
    private static final Function<String, String> DEFAULT_ENV_ACCESSOR = System::getenv;

    /// Detects the color depth supported by the current terminal.
    ///
    /// This method examines environment variables to determine the level of color support.
    /// It first checks COLORTERM, and if that doesn't provide definitive information,
    /// it falls back to checking TERM.
    ///
    /// @return the detected ColorDepth enum value
    public static ColorDepth detectColorDepth() {
        return detectColorDepth(DEFAULT_ENV_ACCESSOR);
    }

    /// Detects the color depth supported by the terminal using the provided environment variables.
    ///
    /// This method is primarily for testing purposes, allowing environment variables to be injected.
    ///
    /// @param envVars a map of environment variable names to values
    /// @return the detected ColorDepth enum value
    public static ColorDepth detectColorDepth(Map<String, String> envVars) {
        return detectColorDepth(envVars::get);
    }

    /// Detects the color depth supported by the terminal using the provided environment accessor.
    ///
    /// This method is primarily for testing purposes, allowing environment variables to be injected.
    ///
    /// @param envAccessor a function that takes an environment variable name and returns its value
    /// @return the detected ColorDepth enum value
    public static ColorDepth detectColorDepth(Function<String, String> envAccessor) {
        // First check COLORTERM environment variable
        String colorTerm = envAccessor.apply("COLORTERM");
        if (colorTerm != null) {
            colorTerm = colorTerm.toLowerCase();
            if (colorTerm.contains("truecolor") || colorTerm.contains("24bit")) {
                return ColorDepth.ANSI24BITCOLOR;
            } else if (colorTerm.contains("256color")) {
                return ColorDepth.ANSI256COLOR;
            } else {
                // COLORTERM is set but doesn't indicate advanced color support
                // We'll assume basic ANSI color support
                return ColorDepth.ANSI8COLOR;
            }
        }

        // If COLORTERM is not set or not definitive, check TERM
        String term = envAccessor.apply("TERM");
        if (term != null) {
            term = term.toLowerCase();
            if (term.contains("256color")) {
                return ColorDepth.ANSI256COLOR;
            } else if (term.contains("color")) {
                return ColorDepth.ANSI8COLOR;
            }
        }

        // Default to no color support if we couldn't determine it
        return ColorDepth.NOCOLOR;
    }

    /// Gets the current terminal's color depth.
    ///
    /// This is a convenience method that calls detectColorDepth().
    ///
    /// @return the current terminal's ColorDepth
    public static ColorDepth getCurrentColorDepth() {
        return detectColorDepth();
    }
}
