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
 * Shared range specification option using {@link Range} record with automatic parsing.
 * All supporting types are inner classes for self-contained encapsulation.
 */
public class RangeOption {

    /**
     * Immutable range specification representing a half-open interval [start, end).
     *
     * @param start the inclusive start index (must be non-negative)
     * @param end   the exclusive end index (must be greater than start)
     */
    public record Range(long start, long end) {

        /**
         * Compact constructor with validation.
         */
        public Range {
            if (start < 0) {
                throw new IllegalArgumentException("Range start must be non-negative: " + start);
            }
            if (end <= start) {
                throw new IllegalArgumentException(
                    "Range end must be greater than start: [" + start + ", " + end + ")"
                );
            }
        }

        /**
         * Gets the size of this range (number of elements).
         */
        public long size() {
            return end - start;
        }

        /**
         * Checks if the range start is zero.
         */
        public boolean hasZeroStart() {
            return start == 0;
        }

        /**
         * Validates that the range start is zero.
         *
         * @throws IllegalStateException if start is not zero
         */
        public void requireZeroStart() {
            if (start != 0) {
                throw new IllegalStateException(
                    "Range start must be 0. Non-zero range start is not yet supported. Got: " + this
                );
            }
        }

        /**
         * Applies range constraints to a total count.
         *
         * @param totalCount the total number of elements available
         * @return a new Range constrained to the available elements
         */
        public Range constrain(long totalCount) {
            long effectiveStart = Math.min(start, totalCount);
            long effectiveEnd = Math.min(end, totalCount);
            return new Range(effectiveStart, Math.max(effectiveStart, effectiveEnd));
        }

        /**
         * Checks if an index is within this range.
         */
        public boolean contains(long index) {
            return index >= start && index < end;
        }

        /**
         * Returns a debuggable string representation in the format "[start, end)".
         */
        @Override
        public String toString() {
            return "[" + start + ", " + end + ")";
        }
    }

    /**
     * Picocli type converter for {@link Range} specifications.
     * Supports formats: n, m..n, [m,n)
     */
    public static class RangeConverter implements CommandLine.ITypeConverter<Range> {

        @Override
        public Range convert(String value) {
            if (value == null || value.trim().isEmpty()) {
                throw new IllegalArgumentException("Range specification cannot be empty");
            }

            String trimmed = value.trim();

            try {
                // Format: [m,n) - half-open interval notation
                if (trimmed.startsWith("[") && trimmed.endsWith(")")) {
                    String inner = trimmed.substring(1, trimmed.length() - 1);
                    String[] parts = inner.split(",");
                    if (parts.length != 2) {
                        throw new IllegalArgumentException(
                            "Invalid range format: " + value + ". Expected: [start,end)"
                        );
                    }
                    long start = Long.parseLong(parts[0].trim());
                    long end = Long.parseLong(parts[1].trim());
                    return new Range(start, end);
                }
                // Format: m..n - closed interval (inclusive)
                else if (trimmed.contains("..")) {
                    String[] parts = trimmed.split("\\.\\.");
                    if (parts.length != 2) {
                        throw new IllegalArgumentException(
                            "Invalid range format: " + value + ". Expected: start..end"
                        );
                    }
                    long start = Long.parseLong(parts[0].trim());
                    long end = Long.parseLong(parts[1].trim()) + 1; // Make exclusive
                    return new Range(start, end);
                }
                // Format: n - from 0 to n-1
                else {
                    long end = Long.parseLong(trimmed);
                    return new Range(0, end);
                }
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                    "Invalid range format: " + value + ". Could not parse numbers: " + e.getMessage()
                );
            }
        }
    }

    @CommandLine.Option(
        names = {"--range"},
        description = "Range of elements to process. Formats: 'n' (0 to n-1), 'm..n' (m to n inclusive), '[m,n)' (m to n-1)",
        converter = RangeConverter.class
    )
    private Range range;

    /**
     * Gets the parsed Range record.
     * Parsing happens automatically via picocli - no manual parse() call needed.
     */
    public Range getRange() {
        return range;
    }

    /**
     * Checks if a range was specified.
     */
    public boolean isRangeSpecified() {
        return range != null;
    }

    /**
     * Gets the range start (inclusive).
     */
    public long getRangeStart() {
        return range != null ? range.start() : 0;
    }

    /**
     * Gets the range end (exclusive).
     */
    public long getRangeEnd() {
        return range != null ? range.end() : Long.MAX_VALUE;
    }

    /**
     * Gets the range size (number of elements in the range).
     */
    public long getRangeSize() {
        return range != null ? range.size() : Long.MAX_VALUE;
    }

    /**
     * Applies range constraints to a total count.
     */
    public Range getEffectiveRange(long totalCount) {
        if (range == null) {
            return new Range(0, totalCount);
        }
        return range.constrain(totalCount);
    }

    /**
     * Validates that the range start is zero.
     */
    public void requireZeroStart() {
        if (range != null) {
            range.requireZeroStart();
        }
    }

    /**
     * Returns string representation of the range.
     */
    @Override
    public String toString() {
        return range != null ? range.toString() : "all";
    }
}
