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

/// This package provides utilities for working with terminal capabilities and features.
///
/// ## REQUIREMENTS
///
/// - Provide a way to detect terminal color support based on environment variables
/// - Support different levels of color depth: none, 8-color ANSI, 256-color ANSI, and 24-bit true color
/// - Check COLORTERM environment variable first, then fall back to TERM if necessary
/// - Return an enum type that indicates the level of color support
///
/// ## IMPLEMENTATION NOTES
///
/// The color depth detection follows this order of precedence:
/// 1. Check COLORTERM environment variable
///    - "truecolor" or "24bit" → ANSI24BITCOLOR
///    - "256color" → ANSI256COLOR
///    - any other non-null value → ANSI8COLOR
/// 2. If COLORTERM is not set, check TERM environment variable
///    - contains "256color" → ANSI256COLOR
///    - contains "color" → ANSI8COLOR
///    - any other value → NOCOLOR
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
///
/// The API is designed to be testable by allowing environment variables to be injected
/// either through a Map or a Function accessor, which makes it easier to test without
/// modifying the actual system environment.
package io.nosqlbench.nbdatatools.api.types.terminal;