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
 * Shared vector specification options for dimension and count.
 * All supporting types are inner classes for self-contained encapsulation.
 */
public class VectorSpecOption {

    /**
     * Immutable vector specification defining dimensionality and count.
     * Used for vector generation and processing operations.
     *
     * @param dimension the number of elements per vector (must be positive)
     * @param count     the number of vectors to process/generate (must be positive)
     */
    public record VectorSpec(int dimension, int count) {

        /**
         * Compact constructor with validation.
         */
        public VectorSpec {
            if (dimension <= 0) {
                throw new IllegalArgumentException("Dimension must be positive, got: " + dimension);
            }
            if (count <= 0) {
                throw new IllegalArgumentException("Count must be positive, got: " + count);
            }
        }

        /**
         * Gets the total number of scalar values (dimension × count).
         */
        public long totalElements() {
            return (long) dimension * count;
        }

        /**
         * Calculates the memory requirement for float vectors.
         */
        public long memoryRequirementBytes() {
            return totalElements() * 4L;
        }

        /**
         * Calculates the memory requirement for vectors of a specific element size.
         */
        public long memoryRequirementBytes(int bytesPerElement) {
            return totalElements() * bytesPerElement;
        }

        /**
         * Returns a debuggable string representation.
         */
        @Override
        public String toString() {
            return count + " vectors × " + dimension + " dimensions";
        }
    }

    @CommandLine.Option(
        names = {"-d", "--dimension"},
        description = "Vector dimension (number of elements per vector)",
        required = true
    )
    private int dimension;

    @CommandLine.Option(
        names = {"-n", "--count"},
        description = "Number of vectors to process/generate",
        required = true
    )
    private int count;

    /**
     * Gets the VectorSpec record constructed from dimension/count options.
     * Validation happens when the record is created.
     */
    public VectorSpec getVectorSpec() {
        return new VectorSpec(dimension, count);
    }

    /**
     * Gets the dimension.
     */
    public int getDimension() {
        return dimension;
    }

    /**
     * Gets the count.
     */
    public int getCount() {
        return count;
    }

    /**
     * Validates the options.
     */
    public void validate() {
        getVectorSpec(); // Will throw if invalid
    }

    /**
     * Returns string representation of the vector spec.
     */
    @Override
    public String toString() {
        return getVectorSpec().toString();
    }
}
