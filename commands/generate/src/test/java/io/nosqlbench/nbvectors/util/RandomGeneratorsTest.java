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


import io.nosqlbench.command.generate.RandomGenerators;
import org.apache.commons.rng.RandomProviderState;
import org.apache.commons.rng.RestorableUniformRandomProvider;
import org.apache.commons.rng.UniformRandomProvider;
import org.apache.commons.rng.sampling.CollectionSampler;
import org.apache.commons.rng.sampling.distribution.ContinuousSampler;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Test class for RandomGenerators utility
 */
class RandomGeneratorsTest {

    @Test
    void testCreateWithAlgorithm() {
        // Create RNGs with different algorithms but same seed
        long seed = 12345L;
        RestorableUniformRandomProvider rng1 = RandomGenerators.create(RandomGenerators.Algorithm.XO_SHI_RO_256_PP, seed);
        RestorableUniformRandomProvider rng2 = RandomGenerators.create(RandomGenerators.Algorithm.XO_SHI_RO_128_PP, seed);
        
        // Different algorithms should produce different sequences even with same seed
        assertNotEquals(rng1.nextInt(), rng2.nextInt());
    }
    
    @Test
    void testCreateDefaultAlgorithm() {
        // Default algorithm should be XO_SHI_RO_256_PP
        long seed = 54321L;
        RestorableUniformRandomProvider rng1 = RandomGenerators.create(seed);
        RestorableUniformRandomProvider rng2 = RandomGenerators.create(RandomGenerators.Algorithm.XO_SHI_RO_256_PP, seed);
        
        // Should produce identical sequences with same seed
        assertEquals(rng1.nextInt(), rng2.nextInt());
        assertEquals(rng1.nextInt(), rng2.nextInt());
        assertEquals(rng1.nextInt(), rng2.nextInt());
    }
    
    @Test
    void testSaveAndRestoreState() {
        // Create RNG and generate some numbers
        RestorableUniformRandomProvider rng = RandomGenerators.create(42L);
        int a1 = rng.nextInt();
        int b1 = rng.nextInt();
        
        // Save state
        RandomProviderState  savedState = RandomGenerators.saveState(rng);
        
        // Generate more numbers
        int c1 = rng.nextInt();
        int d1 = rng.nextInt();
        
        // Restore state
        RandomGenerators.restoreState(rng, savedState);
        
        // Should reproduce the same sequence from the saved point
        int c2 = rng.nextInt();
        int d2 = rng.nextInt();
        
        assertEquals(c1, c2, "RNG should reproduce the same values after state restoration");
        assertEquals(d1, d2, "RNG should reproduce the same values after state restoration");
    }
    
    @Test
    void testCreateCollectionSampler() {
        // Create a collection and sampler
        List<String> items = Arrays.asList("A", "B", "C", "D", "E");
        UniformRandomProvider rng = RandomGenerators.create(789L);
        CollectionSampler<String> sampler = RandomGenerators.createCollectionSampler(items, rng);
        
        // Collect many samples
        List<String> samples = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            samples.add(sampler.sample());
        }
        
        // All items should be represented
        assertThat(samples).containsAll(items);
        
        // Check frequency is roughly uniform (within reasonable bounds for 1000 samples)
        for (String item : items) {
            long count = samples.stream().filter(s -> s.equals(item)).count();
            assertThat(count).isBetween(150L, 250L);
        }
    }
    
    @Test
    void testShuffle() {
        // Create list to shuffle
        List<Integer> original = IntStream.range(0, 100).boxed().collect(Collectors.toList());
        List<Integer> shuffled = new ArrayList<>(original);
        
        // Create RNG and shuffle
        UniformRandomProvider rng = RandomGenerators.create(101L);
        RandomGenerators.shuffle(shuffled, rng);
        
        // Shuffled list should contain same elements but in different order
        assertThat(shuffled).containsExactlyInAnyOrderElementsOf(original);
        assertThat(shuffled).isNotEqualTo(original);
        
        // Multiple shuffles with same seed should be identical
        List<Integer> shuffled1 = new ArrayList<>(original);
        List<Integer> shuffled2 = new ArrayList<>(original);
        
        RandomGenerators.shuffle(shuffled1, RandomGenerators.create(202L));
        RandomGenerators.shuffle(shuffled2, RandomGenerators.create(202L));
        
        assertThat(shuffled1).isEqualTo(shuffled2);
    }
    
    @Test
    void testUniformSampler() {
        // Create uniform sampler for the range [0, 10)
        UniformRandomProvider rng = RandomGenerators.create(303L);
        ContinuousSampler sampler = RandomGenerators.createUniformSampler(rng, 0, 10);
        
        // Generate many samples
        List<Double> samples = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            samples.add(sampler.sample());
        }
        
        // All samples should be in range [0, 10)
        assertThat(samples).allSatisfy(sample -> {
            assertThat(sample).isGreaterThanOrEqualTo(0.0);
            assertThat(sample).isLessThan(10.0);
        });
        
        // Mean should be close to 5.0
        double mean = samples.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        assertThat(mean).isCloseTo(5.0, within(0.5));
    }
    
    private org.assertj.core.data.Offset<Double> within(double tolerance) {
        return org.assertj.core.data.Offset.offset(tolerance);
    }
}
