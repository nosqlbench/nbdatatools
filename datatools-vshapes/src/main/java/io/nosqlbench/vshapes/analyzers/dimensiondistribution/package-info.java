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

/// # Dimension Distribution Analyzer
///
/// This package contains the dimension distribution analyzer, which extracts
/// per-dimension statistical distributions from vector data to build a
/// [VectorSpaceModel][io.nosqlbench.vshapes.model.VectorSpaceModel].
///
/// ## Architecture
///
/// ```text
/// ┌─────────────────────────────────────────────────────────────────────────┐
/// │                    DIMENSION DISTRIBUTION ANALYSIS                      │
/// └─────────────────────────────────────────────────────────────────────────┘
///
///   Input Data                    Processing                      Output
///  ┌───────────┐              ┌──────────────┐              ┌──────────────┐
///  │ float[][] │──────────────│ Per-Dimension│──────────────│VectorSpace   │
///  │  chunks   │              │ Accumulation │              │   Model      │
///  └───────────┘              └──────────────┘              └──────────────┘
///        │                          │                             │
///        ▼                          ▼                             ▼
///   Vector values              Welford's +                  ComponentModel
///   per dimension              Reservoir                    per dimension
/// ```
///
/// ## Key Insight
///
/// This analyzer treats each dimension independently, fitting a separate
/// distribution model (Gaussian, Uniform, or Empirical) to each dimension's
/// values. This captures the statistical "shape" of the vector space.
///
/// ```text
///   Source Vectors                          Per-Dimension Models
///  ┌─────────────────┐                     ┌─────────────────────┐
///  │ v₀ = [x₀,y₀,z₀] │                     │ dim[0]: Gaussian    │
///  │ v₁ = [x₁,y₁,z₁] │  ───► Analyze ───►  │   mean=0.5, σ=1.2   │
///  │ v₂ = [x₂,y₂,z₂] │                     │ dim[1]: Uniform     │
///  │       ...       │                     │   min=-1, max=1     │
///  │ vₙ = [xₙ,yₙ,zₙ] │                     │ dim[2]: Gaussian    │
///  └─────────────────┘                     │   mean=5.0, σ=0.3   │
///                                          └─────────────────────┘
/// ```
///
/// ## Key Components
///
/// | Class | Purpose |
/// |-------|---------|
/// | [DimensionDistributionAnalyzer] | Main analyzer implementing StreamingAnalyzer |
/// | [OnlineAccumulator] | Thread-safe statistics accumulator with reservoir sampling |
///
/// ## Algorithms Used
///
/// ### Welford's Online Algorithm
///
/// Computes mean and variance in a single pass with numerical stability:
///
/// ```text
/// For each value x:
///   count++
///   delta = x - mean
///   mean += delta / count
///   M2 += delta * (x - mean)
///
/// variance = M2 / count
/// ```
///
/// ### Reservoir Sampling
///
/// Maintains a representative sample of k elements from a stream of unknown size:
///
/// ```text
/// For element i (0-indexed):
///   if i < k:
///     reservoir[i] = element
///   else:
///     j = random(0, i)
///     if j < k:
///       reservoir[j] = element
/// ```
///
/// ## Thread Safety
///
/// All accumulators use [ReentrantLock][java.util.concurrent.locks.ReentrantLock]
/// for fine-grained locking, allowing concurrent chunk processing from multiple threads.
///
/// ## Usage
///
/// ```java
/// // Register with harness
/// AnalyzerHarness harness = new AnalyzerHarness();
/// harness.register("dimension-distribution");  // SPI lookup
///
/// // Or instantiate directly
/// harness.register(new DimensionDistributionAnalyzer());
///
/// // Run analysis
/// AnalysisResults results = harness.run(source, 1000);
///
/// // Get extracted model
/// VectorSpaceModel model = results.getResult("dimension-distribution", VectorSpaceModel.class);
/// ```
///
/// @see io.nosqlbench.vshapes.stream.StreamingAnalyzer
/// @see io.nosqlbench.vshapes.model.VectorSpaceModel
package io.nosqlbench.vshapes.analyzers.dimensiondistribution;
