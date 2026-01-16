package io.nosqlbench.datatools.virtdata;

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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation used to declare the model type that a VectorGenerator supports.
 *
 * <p>This annotation provides compile-time type safety by referencing the concrete
 * model class from the vshapes module. The generator implementation must be able
 * to consume models of this type.
 *
 * <p>This enables:
 * <ul>
 *   <li><b>Compile-time binding</b> - The annotation won't compile if the model class doesn't exist</li>
 *   <li><b>Runtime discovery</b> - Factory can find generators that support a given model type</li>
 *   <li><b>Type safety</b> - Clear contract between analyzers (producers) and generators (consumers)</li>
 * </ul>
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * @GeneratorName("vector-gen")
 * @ModelType(VectorSpaceModel.class)
 * public class VectorGen implements VectorGenerator<VectorSpaceModel> {
 *
 *     @Override
 *     public void initialize(VectorSpaceModel model) {
 *         // Configure generator from model
 *     }
 * }
 * }</pre>
 *
 * <h2>Relationship to Analyzers</h2>
 *
 * <p>This creates a symmetric relationship with streaming analyzers:
 * <pre>{@code
 * Analyzer (vshapes)                    Generator (virtdata)
 * ─────────────────                     ────────────────────
 * @AnalyzerName("model-extractor")      @GeneratorName("vector-gen")
 * StreamingAnalyzer<VectorSpaceModel>   @ModelType(VectorSpaceModel.class)
 *        │                              VectorGenerator<VectorSpaceModel>
 *        │                                      │
 *        └──────► VectorSpaceModel ◄────────────┘
 *                 (shared contract)
 * }</pre>
 *
 * @see VectorGenerator
 * @see VectorGeneratorIO
 * @see GeneratorName
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ModelType {
    /**
     * The model class that this generator supports.
     *
     * <p>This should be the concrete model type from the vshapes module,
     * such as {@code VectorSpaceModel.class}.
     *
     * @return the supported model class
     */
    Class<?> value();
}
