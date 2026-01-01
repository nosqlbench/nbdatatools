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

/// # Dimension Statistics Analyzer
///
/// This package contains the dimension statistics analyzer, which computes
/// per-dimension statistical moments from vector data without fitting
/// distribution models.
///
/// ## Architecture
///
/// ```text
/// ┌─────────────────────────────────────────────────────────────────────────┐
/// │                    DIMENSION STATISTICS PIPELINE                        │
/// └─────────────────────────────────────────────────────────────────────────┘
///
///   Input Data                    Processing                      Output
///  ┌───────────┐              ┌──────────────┐              ┌──────────────┐
///  │ float[][] │──────────────│ Per-Dimension│──────────────│  Statistics  │
///  │  chunks   │              │ Accumulation │              │    Model     │
///  └───────────┘              └──────────────┘              └──────────────┘
///        │                          │                             │
///        ▼                          ▼                             ▼
///   Vector values              Welford's                    DimensionStats
///   per dimension              Algorithm                    per dimension
/// ```
///
/// ## Key Components
///
/// | Class | Purpose |
/// |-------|---------||
/// | [DimensionStatisticsAnalyzer] | Main analyzer implementing StreamingAnalyzer |
/// | [DimensionStatisticsModel] | Output model containing per-dimension statistics |
/// | [OnlineMomentsAccumulator] | Thread-safe online moments calculator |
///
/// ## Statistics Computed
///
/// For each dimension, the analyzer computes:
///
/// | Statistic | Description |
/// |-----------|-------------|
/// | count | Number of observations |
/// | min/max | Observed range |
/// | mean | Arithmetic mean (μ) |
/// | variance | Population variance (σ²) |
/// | stdDev | Standard deviation (σ) |
/// | skewness | Asymmetry measure (γ) |
/// | kurtosis | Tail heaviness (κ) |
///
/// ## Usage
///
/// ```java
/// AnalyzerHarness harness = new AnalyzerHarness();
/// harness.register("dimension-statistics");
///
/// AnalysisResults results = harness.run(source, 1000);
/// DimensionStatisticsModel model = results.getResult("dimension-statistics",
///     DimensionStatisticsModel.class);
///
/// // Examine individual dimensions
/// for (int d = 0; d < model.dimensions(); d++) {
///     DimensionStatistics stats = model.getStatistics(d);
///     System.out.printf("Dim %d: μ=%.2f σ=%.2f γ=%.2f κ=%.2f%n",
///         d, stats.mean(), stats.stdDev(), stats.skewness(), stats.kurtosis());
/// }
/// ```
///
/// @see io.nosqlbench.vshapes.stream.StreamingAnalyzer
/// @see io.nosqlbench.vshapes.extract.DimensionStatistics
package io.nosqlbench.vshapes.analyzers.dimensionstatistics;
