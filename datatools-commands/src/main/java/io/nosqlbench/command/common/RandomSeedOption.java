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
 * Shared random seed option using {@link Seed} record with automatic parsing.
 * All supporting types are inner classes for self-contained encapsulation.
 */
public class RandomSeedOption {

    /**
     * Immutable random seed specification.
     * When not specified or value is null, current time is used for non-deterministic behavior.
     *
     * @param value the seed value, or null for auto-generated seed
     */
    public record Seed(Long value) {

        /**
         * Creates a Seed with a specific value.
         */
        public Seed(long value) {
            this(Long.valueOf(value));
        }

        /**
         * Creates a Seed that will use current time.
         */
        public Seed() {
            this((Long) null);
        }

        /**
         * Gets the effective seed value, generating one from current time if needed.
         */
        public long effective() {
            return value != null ? value : System.currentTimeMillis();
        }

        /**
         * Checks if this seed was explicitly specified (vs. auto-generated).
         */
        public boolean isExplicit() {
            return value != null;
        }

        /**
         * Returns a debuggable string representation.
         */
        @Override
        public String toString() {
            return value != null ? String.valueOf(value) : "auto (time-based)";
        }
    }

    /**
     * Picocli type converter for {@link Seed} specifications.
     */
    public static class SeedConverter implements CommandLine.ITypeConverter<Seed> {

        @Override
        public Seed convert(String value) {
            if (value == null || value.trim().isEmpty()) {
                return new Seed(); // Auto-generated seed
            }

            try {
                long seedValue = Long.parseLong(value.trim());
                return new Seed(seedValue);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                    "Invalid seed value: " + value + ". Must be a valid long integer."
                );
            }
        }
    }

    @CommandLine.Option(
        names = {"-s", "--seed"},
        description = "Random seed for generation/shuffling (default: current time)",
        converter = SeedConverter.class
    )
    private Seed seed;

    /**
     * Gets the Seed record.
     * Parsing happens automatically via picocli - no manual parse() call needed.
     */
    public Seed getSeedRecord() {
        return seed != null ? seed : new Seed();
    }

    /**
     * Gets the effective seed value, using current time if not specified.
     */
    public long getSeed() {
        return getSeedRecord().effective();
    }

    /**
     * Checks if a seed was explicitly specified by the user.
     */
    public boolean isSeedSpecified() {
        return seed != null && seed.isExplicit();
    }

    /**
     * Returns string representation of the seed.
     */
    @Override
    public String toString() {
        return getSeedRecord().toString();
    }
}
