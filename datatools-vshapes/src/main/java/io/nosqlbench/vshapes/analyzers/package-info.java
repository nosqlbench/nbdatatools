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

/// # Analyzers Package
///
/// This package contains streaming analyzers that process vector data and produce
/// descriptive models. Analyzers are the bridge between raw data and the statistical
/// models that describe it.
///
/// ## Architectural Boundary
///
/// ```text
/// ┌─────────────────────────────────────────────────────────────────────────────┐
/// │                              vshapes module                                 │
/// │                         (understanding vector spaces)                       │
/// ├─────────────────────────────────────────────────────────────────────────────┤
/// │                                                                             │
/// │   ┌─────────────┐         ┌─────────────┐         ┌─────────────┐          │
/// │   │  Raw Data   │ ──────▶ │  Analyzers  │ ──────▶ │   Models    │          │
/// │   │  float[][]  │         │  (this pkg) │         │ (vshapes)   │          │
/// │   └─────────────┘         └─────────────┘         └─────────────┘          │
/// │                                                          │                  │
/// └──────────────────────────────────────────────────────────│──────────────────┘
///                                                            │
///                                                            ▼
/// ┌─────────────────────────────────────────────────────────────────────────────┐
/// │                              virtdata module                                │
/// │                           (generating variates)                             │
/// ├─────────────────────────────────────────────────────────────────────────────┤
/// │                                                                             │
/// │   ┌─────────────┐         ┌─────────────┐         ┌─────────────┐          │
/// │   │   Models    │ ──────▶ │ Generators  │ ──────▶ │  Variates   │          │
/// │   │ (from above)│         │ (virtdata)  │         │  float[][]  │          │
/// │   └─────────────┘         └─────────────┘         └─────────────┘          │
/// │                                                                             │
/// └─────────────────────────────────────────────────────────────────────────────┘
/// ```
///
/// ## Key Principle
///
/// **Analyzers produce models. Generators consume models.**
///
/// - Analyzers in vshapes extract statistical descriptions from data
/// - Models are pure data structures describing the shape of a vector space
/// - Generators in virtdata use models to produce synthetic variates
///
/// This separation ensures:
/// 1. Models can be serialized and shared without generation logic
/// 2. Generation strategies can vary independently of analysis
/// 3. The vshapes module focuses purely on understanding data
///
/// ## Available Analyzers
///
/// | Analyzer | Model Produced | Purpose |
/// |----------|----------------|---------|
/// | `DimensionDistributionAnalyzer` | `VectorSpaceModel` | Per-dimension distribution fitting |
/// | `DimensionStatisticsAnalyzer` | `DimensionStatisticsModel` | Per-dimension statistical moments |
///
/// ## Usage
///
/// ```java
/// // Analysis happens in vshapes
/// AnalyzerHarness harness = new AnalyzerHarness();
/// harness.register("dimension-distribution");
/// AnalysisResults results = harness.run(dataSource, 1000);
/// VectorSpaceModel model = results.getResult("dimension-distribution", VectorSpaceModel.class);
///
/// // Generation happens in virtdata (separate module)
/// VectorGenerator generator = VectorGeneratorIO.get("dimension-distribution", model);
/// float[] vector = generator.generate(index);
/// ```
///
/// @see io.nosqlbench.vshapes.stream.StreamingAnalyzer
/// @see io.nosqlbench.vshapes.stream.AnalyzerHarness
package io.nosqlbench.vshapes.analyzers;
