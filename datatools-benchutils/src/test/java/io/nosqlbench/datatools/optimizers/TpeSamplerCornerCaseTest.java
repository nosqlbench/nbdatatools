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

package io.nosqlbench.datatools.optimizers;

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.*;

/// Corner-case and numerical extrema tests for [TpeSampler].
///
/// These tests exercise boundary conditions, degenerate inputs, and extreme
/// numeric values to shake out crashes, NaN propagation, and silent
/// degradation in the TPE algorithm.
class TpeSamplerCornerCaseTest {

    // =====================================================================
    // Structural edge cases: degenerate parameter spaces
    // =====================================================================

    @Test
    void singleValueParam_alwaysSuggestsThatValue() {
        // Parameter with exactly 1 choice — there is no "exploration" to do.
        LinkedHashMap<String, String[]> space = new LinkedHashMap<>();
        space.put("x", new String[]{"only"});
        TpeSampler sampler = new TpeSampler(space);

        Map<String, String> s = sampler.suggest().orElseThrow();
        assertThat(s.get("x")).isEqualTo("only");
        sampler.addTrial(s, 42.0);

        // After 1 trial the entire space (size=1) is exhausted
        assertThat(sampler.suggest()).isEmpty();
        assertThat(sampler.spaceSize()).isEqualTo(1);
    }

    @Test
    void mixedSingleAndMultiValueParams_onlyMultiValueVaries() {
        // One fixed param plus one that varies — the fixed param should
        // always be its only value, and duplicate avoidance should work
        // only on the varying param.
        LinkedHashMap<String, String[]> space = new LinkedHashMap<>();
        space.put("fixed", new String[]{"X"});
        space.put("vary", new String[]{"A", "B", "C"});
        TpeSampler sampler = new TpeSampler(space);

        Set<String> seenVary = new HashSet<>();
        for (int i = 0; i < 3; i++) {
            Map<String, String> s = sampler.suggest().orElseThrow();
            assertThat(s.get("fixed")).isEqualTo("X");
            seenVary.add(s.get("vary"));
            sampler.addTrial(s, i * 10.0);
        }
        assertThat(seenVary).containsExactlyInAnyOrder("A", "B", "C");
        assertThat(sampler.suggest()).isEmpty();
    }

    @Test
    void singleParamSpace_tpeWorksWithOneAxis() {
        // Only one parameter — no cross-parameter product.
        LinkedHashMap<String, String[]> space = new LinkedHashMap<>();
        space.put("color", new String[]{"red", "green", "blue", "yellow", "purple",
            "orange", "pink", "brown", "black", "white"});
        TpeSampler sampler = new TpeSampler(space,
            TpeSampler::defaultGamma, TpeSampler::defaultWeights, 1.0, 3, 24, 42);

        for (int i = 0; i < 10; i++) {
            Map<String, String> s = sampler.suggest().orElseThrow();
            assertThat(s).containsKey("color");
            double score = "red".equals(s.get("color")) ? 100 : 10;
            sampler.addTrial(s, score);
        }
        assertThat(sampler.getBestTrial().orElseThrow().params().get("color")).isEqualTo("red");
    }

    @Test
    void highDimensionalSpace_doesNotCrash() {
        // 8 parameters × 3 values each = 6561 configs.
        // Verify TPE can handle many dimensions without error.
        LinkedHashMap<String, String[]> space = new LinkedHashMap<>();
        for (int d = 0; d < 8; d++) {
            space.put("p" + d, new String[]{"a", "b", "c"});
        }
        TpeSampler sampler = new TpeSampler(space,
            TpeSampler::defaultGamma, TpeSampler::defaultWeights, 1.0, 5, 24, 99);

        for (int i = 0; i < 30; i++) {
            Map<String, String> s = sampler.suggest().orElseThrow();
            assertThat(s).hasSize(8);
            double score = 0;
            for (int d = 0; d < 8; d++) {
                if ("a".equals(s.get("p" + d))) score += 10;
            }
            sampler.addTrial(s, score);
        }
        assertThat(sampler.trialCount()).isEqualTo(30);
    }

    // =====================================================================
    // nStartupTrials boundary conditions
    // =====================================================================

    @Test
    void nStartupTrials_zero_firstSuggestStillWorks() {
        // nStartupTrials=0 means TPE should be used from the very first
        // suggestion. With zero history, the sampler must not crash.
        LinkedHashMap<String, String[]> space = new LinkedHashMap<>();
        space.put("x", new String[]{"A", "B", "C"});
        space.put("y", new String[]{"1", "2", "3"});

        TpeSampler sampler = new TpeSampler(space,
            TpeSampler::defaultGamma, TpeSampler::defaultWeights, 1.0, 0, 24, 42);

        // Should not throw — must produce a valid suggestion even with no history
        Map<String, String> first = sampler.suggest().orElseThrow();
        assertThat(first).containsKeys("x", "y");
        sampler.addTrial(first, 50.0);

        // Second suggestion with 1 trial in history
        Map<String, String> second = sampler.suggest().orElseThrow();
        assertThat(second).containsKeys("x", "y");
        assertThat(second).isNotEqualTo(first);
    }

    @Test
    void nStartupTrials_one_transitionsToTpeAfterOneTrial() {
        LinkedHashMap<String, String[]> space = new LinkedHashMap<>();
        space.put("x", new String[]{"A", "B", "C", "D", "E"});
        space.put("y", new String[]{"1", "2", "3", "4", "5"});

        TpeSampler sampler = new TpeSampler(space,
            TpeSampler::defaultGamma, TpeSampler::defaultWeights, 1.0, 1, 24, 42);

        // First suggestion is random (history.size()=0 < 1)
        Map<String, String> first = sampler.suggest().orElseThrow();
        sampler.addTrial(first, 100.0);

        // Second and beyond use TPE (history.size()=1 >= 1)
        // With only 1 trial, gamma(1)=1, nBelow=1, above is empty.
        // The above group is modeled by just the prior kernel.
        for (int i = 0; i < 10; i++) {
            Map<String, String> s = sampler.suggest().orElseThrow();
            sampler.addTrial(s, 50.0 + i);
        }
        assertThat(sampler.trialCount()).isEqualTo(11);
    }

    // =====================================================================
    // Score extrema: identical, negative, huge, infinity, NaN
    // =====================================================================

    @Test
    void allIdenticalScores_doesNotCrash() {
        // When all trials have the same score, the below/above split is
        // arbitrary (sort is stable). The sampler should still function.
        LinkedHashMap<String, String[]> space = new LinkedHashMap<>();
        space.put("x", new String[]{"A", "B", "C", "D", "E"});
        space.put("y", new String[]{"1", "2", "3", "4"});

        TpeSampler sampler = new TpeSampler(space,
            TpeSampler::defaultGamma, TpeSampler::defaultWeights, 1.0, 5, 24, 42);

        for (int i = 0; i < 15; i++) {
            Map<String, String> s = sampler.suggest().orElseThrow();
            sampler.addTrial(s, 42.0); // every trial scores identically
        }

        // Best trial has the universal score
        assertThat(sampler.getBestTrial().orElseThrow().score()).isEqualTo(42.0);
        // Can still suggest
        assertThat(sampler.suggest()).isPresent();
    }

    @Test
    void negativeScores_handledCorrectly() {
        // All scores are negative — the sampler should still identify the
        // least-negative as "best."
        LinkedHashMap<String, String[]> space = new LinkedHashMap<>();
        space.put("x", new String[]{"A", "B", "C", "D"});
        space.put("y", new String[]{"1", "2", "3", "4", "5"});

        TpeSampler sampler = new TpeSampler(space,
            TpeSampler::defaultGamma, TpeSampler::defaultWeights, 1.0, 5, 24, 42);

        for (int i = 0; i < 15; i++) {
            Map<String, String> s = sampler.suggest().orElseThrow();
            double score = "A".equals(s.get("x")) ? -10 : -1000;
            sampler.addTrial(s, score);
        }

        TpeSampler.Trial best = sampler.getBestTrial().orElseThrow();
        assertThat(best.score()).isEqualTo(-10.0);
        assertThat(best.params().get("x")).isEqualTo("A");
    }

    @Test
    void extremeScores_maxAndMinValue() {
        // Trials with Double.MAX_VALUE and -Double.MAX_VALUE as scores.
        LinkedHashMap<String, String[]> space = new LinkedHashMap<>();
        space.put("x", new String[]{"hi", "lo", "mid"});
        space.put("y", new String[]{"1", "2", "3", "4", "5", "6", "7", "8"});

        TpeSampler sampler = new TpeSampler(space,
            TpeSampler::defaultGamma, TpeSampler::defaultWeights, 1.0, 5, 24, 42);

        for (int i = 0; i < 15; i++) {
            Map<String, String> s = sampler.suggest().orElseThrow();
            double score = switch (s.get("x")) {
                case "hi" -> Double.MAX_VALUE;
                case "lo" -> -Double.MAX_VALUE;
                default -> 0.0;
            };
            sampler.addTrial(s, score);
        }

        assertThat(sampler.getBestTrial().orElseThrow().score()).isEqualTo(Double.MAX_VALUE);
        // Should still produce valid suggestions
        assertThat(sampler.suggest()).isPresent();
    }

    @Test
    void infinityScores_sortCorrectly() {
        // +Infinity and -Infinity as scores.
        LinkedHashMap<String, String[]> space = new LinkedHashMap<>();
        space.put("x", new String[]{"pos", "neg", "zero"});
        space.put("y", new String[]{"1", "2", "3", "4", "5", "6", "7", "8"});

        TpeSampler sampler = new TpeSampler(space,
            TpeSampler::defaultGamma, TpeSampler::defaultWeights, 1.0, 5, 24, 42);

        for (int i = 0; i < 15; i++) {
            Map<String, String> s = sampler.suggest().orElseThrow();
            double score = switch (s.get("x")) {
                case "pos" -> Double.POSITIVE_INFINITY;
                case "neg" -> Double.NEGATIVE_INFINITY;
                default -> 0.0;
            };
            sampler.addTrial(s, score);
        }

        assertThat(sampler.getBestTrial().orElseThrow().score()).isEqualTo(Double.POSITIVE_INFINITY);
        // Sorted list should have +Inf first, -Inf last
        List<TpeSampler.Trial> sorted = sampler.getAllTrialsSorted();
        assertThat(sorted.getFirst().score()).isEqualTo(Double.POSITIVE_INFINITY);
        assertThat(sorted.getLast().score()).isEqualTo(Double.NEGATIVE_INFINITY);
        // Can still suggest
        assertThat(sampler.suggest()).isPresent();
    }

    @Test
    void nanScore_doesNotPoisonSubsequentTrials() {
        // A trial with NaN score should not crash the sampler or corrupt
        // future suggestions. NaN comparisons are tricky — verify the
        // sampler degrades gracefully.
        LinkedHashMap<String, String[]> space = new LinkedHashMap<>();
        space.put("x", new String[]{"A", "B", "C", "D"});
        space.put("y", new String[]{"1", "2", "3", "4", "5", "6", "7", "8"});

        TpeSampler sampler = new TpeSampler(space,
            TpeSampler::defaultGamma, TpeSampler::defaultWeights, 1.0, 5, 24, 42);

        // First 6 normal trials
        for (int i = 0; i < 6; i++) {
            Map<String, String> s = sampler.suggest().orElseThrow();
            sampler.addTrial(s, 50.0 + i);
        }
        // Inject a NaN score
        sampler.addTrial(Map.of("x", "A", "y", "1"), Double.NaN);

        // Subsequent suggestions should not crash
        for (int i = 0; i < 5; i++) {
            assertThatCode(() -> {
                Optional<Map<String, String>> s = sampler.suggest();
                s.ifPresent(params -> sampler.addTrial(params, 60.0));
            }).doesNotThrowAnyException();
        }
    }

    @Test
    void mixedExtremeScores_noCrashOrNaN() {
        // Mix of normal, +Inf, -Inf, MAX_VALUE in the same trial history.
        LinkedHashMap<String, String[]> space = new LinkedHashMap<>();
        space.put("x", new String[]{"A", "B", "C", "D", "E"});
        space.put("y", new String[]{"1", "2", "3", "4", "5", "6"});

        TpeSampler sampler = new TpeSampler(space,
            TpeSampler::defaultGamma, TpeSampler::defaultWeights, 1.0, 3, 24, 42);

        double[] scores = {100, -50, Double.MAX_VALUE, 0, Double.POSITIVE_INFINITY,
            -Double.MAX_VALUE, 1e-300, Double.NEGATIVE_INFINITY, 42, 1e15};

        for (int i = 0; i < 10; i++) {
            Map<String, String> s = sampler.suggest().orElseThrow();
            sampler.addTrial(s, scores[i]);
        }

        // TPE should still produce a suggestion
        assertThat(sampler.suggest()).isPresent();
        // Best should be +Inf
        assertThat(sampler.getBestTrial().orElseThrow().score())
            .isEqualTo(Double.POSITIVE_INFINITY);
    }

    // =====================================================================
    // Prior weight extrema
    // =====================================================================

    @Test
    void zeroPriorWeight_doesNotCrash() {
        // With prior_weight=0, the prior kernel contributes nothing.
        // Observation kernels become one-hot. Should still work.
        LinkedHashMap<String, String[]> space = new LinkedHashMap<>();
        space.put("x", new String[]{"A", "B", "C", "D"});
        space.put("y", new String[]{"1", "2", "3", "4", "5"});

        TpeSampler sampler = new TpeSampler(space,
            TpeSampler::defaultGamma, TpeSampler::defaultWeights, 0.0, 5, 24, 42);

        for (int i = 0; i < 15; i++) {
            Map<String, String> s = sampler.suggest().orElseThrow();
            sampler.addTrial(s, "A".equals(s.get("x")) ? 100 : 10);
        }

        assertThat(sampler.getBestTrial().orElseThrow().params().get("x")).isEqualTo("A");
    }

    @Test
    void tinyPriorWeight_doesNotUnderflow() {
        // prior_weight = 1e-300. The fill in buildKernels is ~1e-300/nKernels,
        // very close to zero. Verify no NaN or crash.
        LinkedHashMap<String, String[]> space = new LinkedHashMap<>();
        space.put("x", new String[]{"A", "B", "C"});
        space.put("y", new String[]{"1", "2", "3", "4", "5", "6", "7", "8"});

        TpeSampler sampler = new TpeSampler(space,
            TpeSampler::defaultGamma, TpeSampler::defaultWeights, 1e-300, 5, 24, 42);

        for (int i = 0; i < 15; i++) {
            Map<String, String> s = sampler.suggest().orElseThrow();
            sampler.addTrial(s, i * 10.0);
        }
        assertThat(sampler.trialCount()).isEqualTo(15);
    }

    @Test
    void hugePriorWeight_doesNotOverflow() {
        // prior_weight = 1e15. The fill dominates the +1 observation increment,
        // making all kernels nearly uniform. Should not overflow.
        LinkedHashMap<String, String[]> space = new LinkedHashMap<>();
        space.put("x", new String[]{"A", "B", "C"});
        space.put("y", new String[]{"1", "2", "3", "4", "5", "6", "7", "8"});

        TpeSampler sampler = new TpeSampler(space,
            TpeSampler::defaultGamma, TpeSampler::defaultWeights, 1e15, 5, 24, 42);

        for (int i = 0; i < 15; i++) {
            Map<String, String> s = sampler.suggest().orElseThrow();
            sampler.addTrial(s, i * 10.0);
        }
        assertThat(sampler.trialCount()).isEqualTo(15);
    }

    // =====================================================================
    // Custom gamma edge cases
    // =====================================================================

    @Test
    void customGamma_returnsZero_clampedToOne() {
        // gamma always returns 0 → nBelow is clamped to 1
        LinkedHashMap<String, String[]> space = new LinkedHashMap<>();
        space.put("x", new String[]{"A", "B", "C", "D"});
        space.put("y", new String[]{"1", "2", "3", "4", "5"});

        TpeSampler sampler = new TpeSampler(space,
            n -> 0, TpeSampler::defaultWeights, 1.0, 5, 24, 42);

        for (int i = 0; i < 15; i++) {
            Map<String, String> s = sampler.suggest().orElseThrow();
            sampler.addTrial(s, i * 10.0);
        }
        assertThat(sampler.trialCount()).isEqualTo(15);
    }

    @Test
    void customGamma_returnsN_clampedToNMinusOne() {
        // gamma returns n (all trials below → nothing above). Clamped to n-1.
        LinkedHashMap<String, String[]> space = new LinkedHashMap<>();
        space.put("x", new String[]{"A", "B", "C", "D"});
        space.put("y", new String[]{"1", "2", "3", "4", "5"});

        TpeSampler sampler = new TpeSampler(space,
            n -> n, TpeSampler::defaultWeights, 1.0, 5, 24, 42);

        for (int i = 0; i < 15; i++) {
            Map<String, String> s = sampler.suggest().orElseThrow();
            sampler.addTrial(s, i * 10.0);
        }
        assertThat(sampler.trialCount()).isEqualTo(15);
    }

    @Test
    void customGamma_returnsMoreThanN_clampedSafely() {
        // gamma returns Integer.MAX_VALUE → clamped to n-1
        LinkedHashMap<String, String[]> space = new LinkedHashMap<>();
        space.put("x", new String[]{"A", "B", "C", "D"});
        space.put("y", new String[]{"1", "2", "3", "4", "5"});

        TpeSampler sampler = new TpeSampler(space,
            n -> Integer.MAX_VALUE, TpeSampler::defaultWeights, 1.0, 5, 24, 42);

        for (int i = 0; i < 15; i++) {
            Map<String, String> s = sampler.suggest().orElseThrow();
            sampler.addTrial(s, i * 10.0);
        }
        assertThat(sampler.trialCount()).isEqualTo(15);
    }

    @Test
    void customGamma_returnsNegative_clampedToOne() {
        // gamma returns -100 → clamped to 1
        LinkedHashMap<String, String[]> space = new LinkedHashMap<>();
        space.put("x", new String[]{"A", "B", "C", "D"});
        space.put("y", new String[]{"1", "2", "3", "4", "5"});

        TpeSampler sampler = new TpeSampler(space,
            n -> -100, TpeSampler::defaultWeights, 1.0, 5, 24, 42);

        for (int i = 0; i < 15; i++) {
            Map<String, String> s = sampler.suggest().orElseThrow();
            sampler.addTrial(s, i * 10.0);
        }
        assertThat(sampler.trialCount()).isEqualTo(15);
    }

    // =====================================================================
    // Heavy duplicate pressure
    // =====================================================================

    @Test
    void heavyDuplicatePressure_exhaustsTinySpaceGracefully() {
        // Space of 4 configs. After exhausting all, suggest returns empty.
        // Before that, every suggestion must be unique.
        LinkedHashMap<String, String[]> space = new LinkedHashMap<>();
        space.put("x", new String[]{"A", "B"});
        space.put("y", new String[]{"1", "2"});

        TpeSampler sampler = new TpeSampler(space,
            TpeSampler::defaultGamma, TpeSampler::defaultWeights, 1.0, 2, 24, 42);

        Set<Map<String, String>> seen = new HashSet<>();
        for (int i = 0; i < 4; i++) {
            Map<String, String> s = sampler.suggest().orElseThrow();
            assertThat(seen).doesNotContain(s);
            seen.add(s);
            sampler.addTrial(s, i * 10.0);
        }
        assertThat(sampler.suggest()).isEmpty();
    }

    @Test
    void nearExhaustion_lastConfigIsDiscoverable() {
        // Space of 6 configs. After 5 trials, there's exactly 1 remaining.
        // TPE or random fallback must find it.
        LinkedHashMap<String, String[]> space = new LinkedHashMap<>();
        space.put("x", new String[]{"A", "B", "C"});
        space.put("y", new String[]{"1", "2"});

        TpeSampler sampler = new TpeSampler(space,
            TpeSampler::defaultGamma, TpeSampler::defaultWeights, 1.0, 3, 24, 42);

        Set<Map<String, String>> seen = new HashSet<>();
        for (int i = 0; i < 5; i++) {
            Map<String, String> s = sampler.suggest().orElseThrow();
            seen.add(s);
            sampler.addTrial(s, i * 10.0);
        }

        // The 6th (last) config must be findable
        Map<String, String> last = sampler.suggest().orElseThrow();
        assertThat(seen).doesNotContain(last);
        sampler.addTrial(last, 999.0);

        // Now truly exhausted
        assertThat(sampler.suggest()).isEmpty();
    }

    // =====================================================================
    // addTrial with values not in the declared space
    // =====================================================================

    @Test
    void addTrial_withUnknownParamValue_doesNotCrash() {
        // If a trial is added with a value not in the declared space
        // (e.g., from external data), the sampler should not crash.
        // The unrecognized value simply doesn't match any category in
        // buildKernels, so that observation kernel stays uniform.
        LinkedHashMap<String, String[]> space = new LinkedHashMap<>();
        space.put("x", new String[]{"A", "B", "C"});
        space.put("y", new String[]{"1", "2", "3", "4", "5", "6", "7", "8"});

        TpeSampler sampler = new TpeSampler(space,
            TpeSampler::defaultGamma, TpeSampler::defaultWeights, 1.0, 3, 24, 42);

        // Normal startup trials
        for (int i = 0; i < 5; i++) {
            Map<String, String> s = sampler.suggest().orElseThrow();
            sampler.addTrial(s, 50.0);
        }

        // Inject a trial with an out-of-space value
        sampler.addTrial(Map.of("x", "UNKNOWN", "y", "999"), 75.0);

        // Subsequent suggestions must not crash
        for (int i = 0; i < 5; i++) {
            Map<String, String> s = sampler.suggest().orElseThrow();
            sampler.addTrial(s, 60.0);
        }
        assertThat(sampler.trialCount()).isEqualTo(11);
    }

    // =====================================================================
    // Monotonic score ladders: verify sorting and splitting correctness
    // =====================================================================

    @Test
    void descendingScores_bestTrialIsFirst() {
        LinkedHashMap<String, String[]> space = new LinkedHashMap<>();
        space.put("x", new String[]{"A", "B", "C", "D", "E"});
        TpeSampler sampler = new TpeSampler(space);

        sampler.addTrial(Map.of("x", "A"), 500);
        sampler.addTrial(Map.of("x", "B"), 400);
        sampler.addTrial(Map.of("x", "C"), 300);
        sampler.addTrial(Map.of("x", "D"), 200);
        sampler.addTrial(Map.of("x", "E"), 100);

        assertThat(sampler.getAllTrialsSorted())
            .extracting(TpeSampler.Trial::score)
            .containsExactly(500.0, 400.0, 300.0, 200.0, 100.0);
    }

    @Test
    void ascendingScores_bestTrialIsLast() {
        LinkedHashMap<String, String[]> space = new LinkedHashMap<>();
        space.put("x", new String[]{"A", "B", "C", "D", "E"});
        TpeSampler sampler = new TpeSampler(space);

        sampler.addTrial(Map.of("x", "A"), 100);
        sampler.addTrial(Map.of("x", "B"), 200);
        sampler.addTrial(Map.of("x", "C"), 300);
        sampler.addTrial(Map.of("x", "D"), 400);
        sampler.addTrial(Map.of("x", "E"), 500);

        assertThat(sampler.getBestTrial().orElseThrow().score()).isEqualTo(500.0);
        assertThat(sampler.getBestTrial().orElseThrow().params().get("x")).isEqualTo("E");
    }

    // =====================================================================
    // Duplicate addTrial: same config, different scores
    // =====================================================================

    @Test
    void duplicateTrials_allRecordedButSuggestSkipsThem() {
        // Adding the same config multiple times should all be recorded,
        // but suggest() should still skip configs that appear in history.
        LinkedHashMap<String, String[]> space = new LinkedHashMap<>();
        space.put("x", new String[]{"A", "B"});
        space.put("y", new String[]{"1", "2", "3"});
        // Space = 6

        TpeSampler sampler = new TpeSampler(space);

        // Add "A,1" three times with different scores
        sampler.addTrial(Map.of("x", "A", "y", "1"), 10);
        sampler.addTrial(Map.of("x", "A", "y", "1"), 50);
        sampler.addTrial(Map.of("x", "A", "y", "1"), 90);

        assertThat(sampler.trialCount()).isEqualTo(3);
        // suggest() should never return {A, 1} since it's already tried
        for (int i = 0; i < 5; i++) {
            Map<String, String> s = sampler.suggest().orElseThrow();
            assertThat(s).isNotEqualTo(Map.of("x", "A", "y", "1"));
            sampler.addTrial(s, 42.0);
        }
    }

    // =====================================================================
    // defaultWeights numerical edge cases
    // =====================================================================

    @Test
    void defaultWeights_allPositive() {
        // Verify no weight is ever zero or negative for any n
        for (int n = 1; n <= 200; n++) {
            double[] w = TpeSampler.defaultWeights(n);
            assertThat(w).hasSize(n);
            for (int i = 0; i < n; i++) {
                assertThat(w[i]).as("weight[%d] for n=%d", i, n).isGreaterThan(0.0);
            }
        }
    }

    @Test
    void defaultWeights_monotonicallyNondecreasing() {
        // For any n, weights should be non-decreasing (ramp then flat).
        for (int n : new int[]{25, 26, 30, 50, 100, 200}) {
            double[] w = TpeSampler.defaultWeights(n);
            for (int i = 1; i < n; i++) {
                assertThat(w[i]).as("weight[%d] for n=%d", i, n)
                    .isGreaterThanOrEqualTo(w[i - 1]);
            }
        }
    }

    @Test
    void defaultWeights_sumIsStable() {
        // Verify the sum of weights is reasonable (no NaN, not zero).
        for (int n : new int[]{1, 10, 25, 50, 100, 500, 1000}) {
            double[] w = TpeSampler.defaultWeights(n);
            double sum = 0;
            for (double v : w) sum += v;
            assertThat(sum).as("sum for n=%d", n).isGreaterThan(0.0);
            assertThat(Double.isNaN(sum)).as("NaN sum for n=%d", n).isFalse();
            assertThat(Double.isInfinite(sum)).as("Inf sum for n=%d", n).isFalse();
        }
    }

    // =====================================================================
    // Rapid regime transitions
    // =====================================================================

    @Test
    void regimeSwitch_tpeAdaptsWhenOptimumFlips() {
        // Phase 1: A is best. Phase 2: B is best. Phase 3: C is best.
        // The sampler should eventually find each regime's optimum.
        LinkedHashMap<String, String[]> space = new LinkedHashMap<>();
        space.put("x", new String[]{"A", "B", "C"});
        space.put("pad", new String[]{"1", "2", "3", "4", "5", "6", "7", "8",
            "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20"});
        // Space = 60

        TpeSampler sampler = new TpeSampler(space,
            TpeSampler::defaultGamma, TpeSampler::defaultWeights, 1.0, 3, 24, 42);

        // Phase 1: A dominant
        for (int i = 0; i < 15; i++) {
            Map<String, String> s = sampler.suggest().orElseThrow();
            double score = switch (s.get("x")) {
                case "A" -> 100;
                case "B" -> 10;
                default -> 5;
            };
            sampler.addTrial(s, score);
        }

        // Phase 2: B dominant
        for (int i = 0; i < 15; i++) {
            Map<String, String> s = sampler.suggest().orElseThrow();
            double score = switch (s.get("x")) {
                case "B" -> 200;
                case "A" -> 5;
                default -> 10;
            };
            sampler.addTrial(s, score);
        }

        // Phase 3: C dominant
        for (int i = 0; i < 15; i++) {
            Map<String, String> s = sampler.suggest().orElseThrow();
            double score = switch (s.get("x")) {
                case "C" -> 300;
                case "B" -> 10;
                default -> 5;
            };
            sampler.addTrial(s, score);
        }

        // Best trial overall should be from phase 3 (C, score 300)
        assertThat(sampler.getBestTrial().orElseThrow().score()).isEqualTo(300.0);
        assertThat(sampler.getBestTrial().orElseThrow().params().get("x")).isEqualTo("C");
    }

    // =====================================================================
    // Very small score differences
    // =====================================================================

    @Test
    void tinyScoreDifferences_doesNotLoseDiscrimination() {
        // Scores differ by 1e-10 — the sampler should still correctly
        // identify the best trial and not treat them as identical.
        LinkedHashMap<String, String[]> space = new LinkedHashMap<>();
        space.put("x", new String[]{"A", "B", "C", "D", "E"});
        TpeSampler sampler = new TpeSampler(space);

        sampler.addTrial(Map.of("x", "A"), 1.0);
        sampler.addTrial(Map.of("x", "B"), 1.0 + 1e-10);
        sampler.addTrial(Map.of("x", "C"), 1.0 + 2e-10);
        sampler.addTrial(Map.of("x", "D"), 1.0 + 3e-10);
        sampler.addTrial(Map.of("x", "E"), 1.0 + 4e-10);

        assertThat(sampler.getBestTrial().orElseThrow().params().get("x")).isEqualTo("E");
        assertThat(sampler.getAllTrialsSorted().getFirst().params().get("x")).isEqualTo("E");
        assertThat(sampler.getAllTrialsSorted().getLast().params().get("x")).isEqualTo("A");
    }
}
