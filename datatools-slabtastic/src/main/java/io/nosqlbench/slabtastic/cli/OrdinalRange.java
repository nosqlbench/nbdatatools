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

package io.nosqlbench.slabtastic.cli;

import picocli.CommandLine;

/// Self-contained ordinal range specification for slabtastic CLI commands.
///
/// Represents a half-open interval `[start, end)` with a picocli type
/// converter that supports all standard range forms used in nbdatatools:
///
/// - `n` — shorthand for `[0, n)` (first n ordinals)
/// - `m..n` — closed interval, equivalent to `[m, n+1)` (m to n inclusive)
/// - `[m,n)` — half-open interval (m inclusive, n exclusive)
/// - `[m,n]` — closed interval (m inclusive, n inclusive)
/// - `(m,n)` — open interval (m exclusive, n exclusive)
/// - `(m,n]` — half-open interval (m exclusive, n inclusive)
/// - `[n]` — single ordinal, equivalent to `[n, n+1)`
///
/// Brackets and commas may also use `..` as separator: `[m..n)`.
public class OrdinalRange {

    private OrdinalRange() {}

    /// Immutable range specification representing a half-open interval
    /// `[start, end)`.
    ///
    /// @param start the inclusive start ordinal (must be non-negative)
    /// @param end   the exclusive end ordinal (must be greater than start)
    public record Range(long start, long end) {

        /// Validates that start is non-negative and end is greater than start.
        public Range {
            if (start < 0) {
                throw new IllegalArgumentException("Range start must be non-negative: " + start);
            }
            if (end <= start) {
                throw new IllegalArgumentException(
                    "Range end must be greater than start: [" + start + ", " + end + ")");
            }
        }

        /// Returns the number of ordinals in this range.
        public long size() {
            return end - start;
        }

        /// Returns whether the given ordinal falls within this range.
        ///
        /// @param ordinal the ordinal to check
        /// @return true if `start <= ordinal < end`
        public boolean contains(long ordinal) {
            return ordinal >= start && ordinal < end;
        }

        @Override
        public String toString() {
            return "[" + start + ", " + end + ")";
        }
    }

    /// Picocli type converter for {@link Range} specifications.
    ///
    /// Supports formats: `n`, `m..n`, `[m,n)`, `[m,n]`, `(m,n)`, `(m,n]`,
    /// `[n]`, and bracket forms with `..` separator.
    public static class Converter implements CommandLine.ITypeConverter<Range> {

        @Override
        public Range convert(String value) {
            if (value == null || value.trim().isEmpty()) {
                throw new IllegalArgumentException("Range specification cannot be empty");
            }

            String trimmed = value.trim();

            try {
                if ((trimmed.startsWith("[") || trimmed.startsWith("(")) &&
                    (trimmed.endsWith("]") || trimmed.endsWith(")"))) {
                    return parseBracketNotation(trimmed, value);
                } else if (trimmed.contains("..")) {
                    return parseDotDot(trimmed, value);
                } else {
                    long end = Long.parseLong(trimmed);
                    return new Range(0, end);
                }
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                    "Invalid range format: " + value + ". Could not parse numbers: " + e.getMessage());
            }
        }

        private Range parseBracketNotation(String trimmed, String original) {
            boolean startClosed = trimmed.startsWith("[");
            boolean endClosed = trimmed.endsWith("]");

            String inner = trimmed.substring(1, trimmed.length() - 1).trim();
            long start;
            long end;

            if (inner.contains(",")) {
                String[] parts = inner.split(",");
                if (parts.length != 2) {
                    throw new IllegalArgumentException(
                        "Invalid range format: " + original + ". Expected: [start,end)");
                }
                start = Long.parseLong(parts[0].trim());
                end = Long.parseLong(parts[1].trim());
            } else if (inner.contains("..")) {
                String[] parts = inner.split("\\.\\.");
                if (parts.length != 2) {
                    throw new IllegalArgumentException(
                        "Invalid range format: " + original + ". Expected: [start..end)");
                }
                start = Long.parseLong(parts[0].trim());
                end = Long.parseLong(parts[1].trim());
            } else {
                long index = Long.parseLong(inner);
                if (!startClosed || !endClosed) {
                    throw new IllegalArgumentException(
                        "Single-value range must be specified as [n]. Got: " + original);
                }
                return new Range(index, index + 1);
            }

            if (!startClosed) {
                start += 1;
            }
            if (endClosed) {
                end += 1;
            }
            return new Range(start, end);
        }

        private Range parseDotDot(String trimmed, String original) {
            String[] parts = trimmed.split("\\.\\.");
            if (parts.length != 2) {
                throw new IllegalArgumentException(
                    "Invalid range format: " + original + ". Expected: start..end");
            }
            long start = Long.parseLong(parts[0].trim());
            long end = Long.parseLong(parts[1].trim()) + 1;
            return new Range(start, end);
        }
    }
}
