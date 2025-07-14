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

/// Represents the color depth support level of a terminal.
///
/// This enum provides different levels of terminal color support:
/// - NOCOLOR: No color support
/// - ANSI8COLOR: Basic 8-color ANSI support
/// - ANSI256COLOR: Extended 256-color ANSI support
/// - ANSI24BITCOLOR: Full 24-bit true color support (16.7 million colors)
///
/// ```
/// ┌─────────────────┬───────────────────────────────────────┐
/// │ Color Depth     │ Description                           │
/// ├─────────────────┼───────────────────────────────────────┤
/// │ NOCOLOR         │ No color support                      │
/// │ ANSI8COLOR      │ Basic 8 colors                        │
/// │ ANSI256COLOR    │ Extended 256 colors                   │
/// │ ANSI24BITCOLOR  │ True color (16.7 million colors)      │
/// └─────────────────┴───────────────────────────────────────┘
/// ```
public enum ColorDepth {
    /// No color support
    NOCOLOR,
    
    /// Basic 8-color ANSI support
    ANSI8COLOR,
    
    /// Extended 256-color ANSI support
    ANSI256COLOR,
    
    /// Full 24-bit true color support (16.7 million colors)
    ANSI24BITCOLOR;
    
    /// Returns true if this color depth supports at least basic ANSI colors.
    ///
    /// @return true if color is supported, false otherwise
    public boolean supportsColor() {
        return this != NOCOLOR;
    }
    
    /// Returns true if this color depth supports 256 colors.
    ///
    /// @return true if 256 colors are supported, false otherwise
    public boolean supports256Colors() {
        return this == ANSI256COLOR || this == ANSI24BITCOLOR;
    }
    
    /// Returns true if this color depth supports 24-bit true color.
    ///
    /// @return true if 24-bit color is supported, false otherwise
    public boolean supportsTrueColor() {
        return this == ANSI24BITCOLOR;
    }
}