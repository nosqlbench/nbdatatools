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

/// # Virtdata - Variate Generation
///
/// This package contains generators that consume models from vshapes and produce
/// synthetic variates. It is the generation counterpart to vshapes' analysis.
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
/// │   │  float[][]  │         │             │         │ (pure data) │          │
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
/// │   │ (from above)│         │ (this pkg)  │         │  float[][]  │          │
/// │   └─────────────┘         └─────────────┘         └─────────────┘          │
/// │                                                                             │
/// └─────────────────────────────────────────────────────────────────────────────┘
/// ```
///
/// ## Key Principle
///
/// **Models describe. Generators produce.**
///
/// - Models from vshapes are pure data structures (parameters, statistics)
/// - Generators in virtdata interpret models and produce samples
/// - Sampling algorithms (inverse CDF, etc.) live here, not in models
///
/// This separation ensures:
/// 1. Models can be serialized/shared without generation logic
/// 2. Different generation strategies can use the same model
/// 3. Analysis and generation are independently testable
///
/// ## Tensor Hierarchy Alignment
///
/// This package aligns with the tensor model hierarchy from vshapes:
///
/// | vshapes Model | virtdata Sampler/Generator |
/// |---------------|----------------------------|
/// | ScalarModel | ScalarSampler |
/// | VectorModel | VectorGenerator |
///
/// ## Generator Types
///
/// | Generator | Model Consumed | Output |
/// |-----------|----------------|--------|
/// | DimensionDistributionGenerator | VectorModel | Deterministic vectors |
/// | ScalarDimensionDistributionGenerator | VectorModel | Streaming vectors |
///
/// ## Sampling Strategies
///
/// Generators use various sampling techniques:
///
/// | Strategy | Description |
/// |----------|-------------|
/// | Inverse CDF | Transform uniform samples via inverse cumulative distribution |
/// | Stratified | Divide [0,1] into strata for even coverage |
/// | Truncated | Reject samples outside bounds |
///
/// ## Usage
///
/// ```java
/// // Model comes from vshapes analysis
/// VectorSpaceModel model = ... // from analyzer or deserialization
///
/// // Generator uses model to produce variates
/// VectorGenerator generator = VectorGeneratorIO.get("dimension-distribution", model);
///
/// // Generate deterministic vectors by index
/// float[] v0 = generator.generate(0);
/// float[] v1 = generator.generate(1);
///
/// // Or use streaming generator
/// VectorGenerator streaming = VectorGeneratorIO.get("scalar-dimension-distribution", model);
/// float[] sample = streaming.generate(randomIndex);
/// ```
///
/// @see io.nosqlbench.vshapes.model.VectorSpaceModel
/// @see VectorGenerator
/// @see VectorGeneratorIO
package io.nosqlbench.datatools.virtdata;
