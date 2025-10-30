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
 * Shared value range options for min/max specifications.
 * All supporting types are inner classes for self-contained encapsulation.
 */
public class ValueRangeOption {

    /**
     * Immutable value range specification for numeric bounds in vector generation.
     * Represents a range [min, max) for generating random values.
     *
     * @param min the minimum value (inclusive)
     * @param max the maximum value (exclusive for floats, inclusive for display)
     */
    public record ValueRange(float min, float max) {

        /**
         * Default value range [0.0, 1.0).
         */
        public static final ValueRange DEFAULT = new ValueRange(0.0f, 1.0f);

        /**
         * Compact constructor with validation.
         */
        public ValueRange {
            if (min >= max) {
                throw new IllegalArgumentException(
                    "min (" + min + ") must be less than max (" + max + ")"
                );
            }
        }

        /**
         * Gets the range size (max - min).
         */
        public float size() {
            return max - min;
        }

        /**
         * Checks if a value is within the range [min, max).
         */
        public boolean contains(float value) {
            return value >= min && value < max;
        }

        /**
         * Normalizes a value from [0, 1] to [min, max].
         * Useful for converting uniform random values to the desired range.
         */
        public float denormalize(float normalizedValue) {
            return min + normalizedValue * (max - min);
        }

        /**
         * Normalizes a value from [min, max] to [0, 1].
         */
        public float normalize(float value) {
            return (value - min) / (max - min);
        }

        /**
         * Returns a debuggable string representation.
         */
        @Override
        public String toString() {
            return "[" + min + ", " + max + ")";
        }
    }

    @CommandLine.Option(
        names = {"--min"},
        description = "Minimum value for vector generation",
        defaultValue = "0.0"
    )
    private float min = 0.0f;

    @CommandLine.Option(
        names = {"--max"},
        description = "Maximum value for vector generation",
        defaultValue = "1.0"
    )
    private float max = 1.0f;

    /**
     * Gets the ValueRange record constructed from min/max options.
     * Validation happens when the record is created.
     */
    public ValueRange getValueRange() {
        return new ValueRange(min, max);
    }

    /**
     * Gets the minimum value.
     */
    public float getMin() {
        return min;
    }

    /**
     * Gets the maximum value.
     */
    public float getMax() {
        return max;
    }

    /**
     * Gets the range size (max - min).
     */
    public float getRange() {
        return max - min;
    }

    /**
     * Validates that min is less than max.
     */
    public void validate() {
        getValueRange(); // Will throw if invalid
    }

    /**
     * Checks if a value is within the range [min, max).
     */
    public boolean contains(float value) {
        return getValueRange().contains(value);
    }

    /**
     * Normalizes a value from [0, 1] to [min, max].
     */
    public float denormalize(float normalizedValue) {
        return getValueRange().denormalize(normalizedValue);
    }

    /**
     * Returns string representation of the range.
     */
    @Override
    public String toString() {
        return getValueRange().toString();
    }
}
