package io.nosqlbench.nbvectors.util;

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


import org.apache.commons.rng.RandomProviderState;
import org.apache.commons.rng.RestorableUniformRandomProvider;
import org.apache.commons.rng.UniformRandomProvider;
import org.apache.commons.rng.simple.RandomSource;
import org.apache.commons.rng.sampling.CollectionSampler;
import org.apache.commons.rng.sampling.distribution.ContinuousSampler;
import org.apache.commons.rng.sampling.distribution.ContinuousUniformSampler;

import java.util.Collection;
import java.util.List;

/**
 * Provides high-quality random number generators for various applications.
 * Based on Apache Commons RNG which offers state-of-the-art PRNGs with 
 * excellent statistical properties.
 */
public class RandomGenerators {
    
    /**
     * Available PRNG algorithms with different characteristics.
     * XO_SHI_RO_256_PP is recommended for most applications due to its
     * excellent statistical properties and speed on modern x64_64 architectures.
     */
    public enum Algorithm {
        /**
         * XorShiro256++ algorithm - 256-bit state, extremely fast with excellent statistical properties
         * Period: 2^256 - 1
         * Recommendation: General purpose, excellent for high-dimensional applications
         */
        XO_SHI_RO_256_PP(RandomSource.XO_SHI_RO_256_PP),
        
        /**
         * XorShiro128++ algorithm - 128-bit state, extremely fast with good statistical properties
         * Period: 2^128 - 1
         * Recommendation: When memory footprint is a concern
         */
        XO_SHI_RO_128_PP(RandomSource.XO_SHI_RO_128_PP),
        
        /**
         * SplitMix64 algorithm - 64-bit state, extremely fast with acceptable statistical properties
         * Period: 2^64
         * Recommendation: When minimal state is desired
         */
        SPLIT_MIX_64(RandomSource.SPLIT_MIX_64),
        
        /**
         * Mersenne Twister (MT) algorithm - 19937-bit state, good statistical properties
         * Period: 2^19937 - 1
         * Recommendation: Legacy applications or when extremely long period is required
         */
        MT(RandomSource.MT),
        
        /**
         * KISS algorithm - 128-bit state, good statistical properties
         * Recommendation: Alternative to XorShiro128++ with different characteristics
         */
        KISS(RandomSource.KISS);
        
        private final RandomSource source;
        
        Algorithm(RandomSource source) {
            this.source = source;
        }
        
        RandomSource getSource() {
            return source;
        }
    }
    
    /**
     * Creates a new random number generator with the specified algorithm and seed.
     * 
     * @param algorithm The PRNG algorithm to use
     * @param seed The seed for deterministic random generation
     * @return A uniform random provider
     */
    public static RestorableUniformRandomProvider create(Algorithm algorithm, long seed) {
        return (RestorableUniformRandomProvider) algorithm.getSource().create(seed);
    }
    
    /**
     * Creates a new random number generator with the recommended algorithm and specified seed.
     * Uses XO_SHI_RO_256_PP by default for its excellent statistical properties.
     * 
     * @param seed The seed for deterministic random generation
     * @return A uniform random provider
     */
    public static RestorableUniformRandomProvider create(long seed) {
        return create(Algorithm.XO_SHI_RO_256_PP, seed);
    }
    
    /**
     * Creates a sampler that returns random elements from a collection.
     * 
     * @param <T> The type of elements in the collection
     * @param collection The collection to sample from
     * @param rng The random number generator
     * @return A sampler that returns random elements from the collection
     */
    public static <T> CollectionSampler<T> createCollectionSampler(Collection<T> collection, 
                                                                  UniformRandomProvider rng) {
        return new CollectionSampler<>(rng, collection);
    }
    
    /**
     * Shuffles a list in-place using the Fisher-Yates algorithm with a high-quality RNG.
     * This implementation has better statistical properties than Java's Collections.shuffle().
     * 
     * @param <T> The type of elements in the list
     * @param list The list to shuffle
     * @param rng The random number generator
     */
    public static <T> void shuffle(List<T> list, UniformRandomProvider rng) {
        int size = list.size();
        // Fisher-Yates shuffle with high-quality random numbers
        for (int i = size - 1; i > 0; i--) {
            int j = rng.nextInt(i + 1);
            T temp = list.get(i);
            list.set(i, list.get(j));
            list.set(j, temp);
        }
    }
    
    /**
     * Creates a continuous uniform sampler for the specified range.
     * 
     * @param rng The random number generator
     * @param lower The lower bound (inclusive)
     * @param upper The upper bound (exclusive)
     * @return A continuous uniform sampler
     */
    public static ContinuousSampler createUniformSampler(UniformRandomProvider rng, 
                                                        double lower, double upper) {
        return new ContinuousUniformSampler(rng, lower, upper);
    }
    
    /**
     * State-saving utility that captures the current state of a restorable RNG.
     * 
     * @param rng The random number generator
     * @return The RandomProviderState object that can be used to restore the RNG
     */
    public static RandomProviderState saveState(RestorableUniformRandomProvider rng) {
        return rng.saveState();
    }
    
    /**
     * State-restoring utility that restores a previously saved state.
     * 
     * @param rng The random number generator
     * @param state The previously saved RandomProviderState
     */
    public static void restoreState(RestorableUniformRandomProvider rng, RandomProviderState state) {
        rng.restoreState(state);
    }
}
