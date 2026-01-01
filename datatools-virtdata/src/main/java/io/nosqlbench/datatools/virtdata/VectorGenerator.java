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

import java.util.function.LongFunction;

/**
 * Interface for vector generators that produce vectors from ordinals.
 *
 * <p>Generators consume a model (extracted by analyzers) and produce vectors
 * deterministically from ordinal values. This creates a symmetric relationship
 * with streaming analyzers:
 *
 * <pre>{@code
 * ┌─────────────────────────────────────────────────────────────────────────┐
 * │                        DATA FLOW                                        │
 * └─────────────────────────────────────────────────────────────────────────┘
 *
 *   Source Data              Model                    Generated Data
 *  ┌───────────┐         ┌───────────┐              ┌───────────┐
 *  │ float[][] │──────►  │     M     │  ──────────► │ float[][] │
 *  │ (vectors) │         │  (model)  │              │ (vectors) │
 *  └───────────┘         └───────────┘              └───────────┘
 *        │                     │                          │
 *        ▼                     │                          ▼
 *  StreamingAnalyzer<M>        │               VectorGenerator<M>
 *  - accept(chunks)            │               - initialize(model)
 *  - complete() → M ───────────┘               - apply(ordinal) → vector
 * }</pre>
 *
 * <h2>SPI Registration</h2>
 *
 * <p>Generators are discovered via ServiceLoader. To register a generator:
 * <ol>
 *   <li>Implement this interface</li>
 *   <li>Add {@link GeneratorName} annotation with a unique name</li>
 *   <li>Add {@link ModelType} annotation with the supported model class</li>
 *   <li>Provide a public no-args constructor</li>
 *   <li>Register in META-INF/services/io.nosqlbench.datatools.virtdata.VectorGenerator</li>
 * </ol>
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * // Discover and create a generator
 * VectorGenerator<VectorSpaceModel> gen = VectorGeneratorIO.get("vector-gen", VectorSpaceModel.class);
 * gen.initialize(model);
 *
 * // Generate vectors
 * float[] v42 = gen.apply(42L);
 * float[][] batch = gen.generateBatch(0, 1000);
 * }</pre>
 *
 * @param <M> the model type this generator consumes
 * @see GeneratorName
 * @see ModelType
 * @see VectorGeneratorIO
 */
public interface VectorGenerator<M> extends LongFunction<float[]> {

    /**
     * Returns the unique type identifier for this generator.
     *
     * <p>This should match the value in the {@link GeneratorName} annotation.
     *
     * @return the generator type identifier
     */
    String getGeneratorType();

    /**
     * Returns a human-readable description of this generator.
     *
     * @return description of the generator's purpose and behavior
     */
    default String getDescription() {
        return "Vector generator: " + getGeneratorType();
    }

    /**
     * Initializes the generator with a model.
     *
     * <p>This method must be called before any generation methods.
     * It configures the generator based on the model's parameters.
     *
     * @param model the model to initialize from
     * @throws IllegalStateException if already initialized
     * @throws IllegalArgumentException if the model is invalid
     */
    void initialize(M model);

    /**
     * Returns whether this generator has been initialized.
     *
     * @return true if initialize() has been called
     */
    boolean isInitialized();

    /**
     * Returns the model this generator was initialized with.
     *
     * @return the model, or null if not initialized
     */
    M model();

    /**
     * Generates a vector for the given ordinal.
     *
     * <p>This method is deterministic: the same ordinal always produces
     * the same vector.
     *
     * @param ordinal the vector ordinal
     * @return an M-dimensional float array
     * @throws IllegalStateException if not initialized
     */
    @Override
    float[] apply(long ordinal);

    /**
     * Generates a vector into an existing array.
     *
     * <p>This avoids allocation overhead when generating many vectors.
     *
     * @param ordinal the vector ordinal
     * @param target the target array to write into
     * @param offset the offset within the target array
     * @throws IllegalStateException if not initialized
     */
    void generateInto(long ordinal, float[] target, int offset);

    /**
     * Generates a vector as double precision.
     *
     * @param ordinal the vector ordinal
     * @return an M-dimensional double array
     * @throws IllegalStateException if not initialized
     */
    double[] applyAsDouble(long ordinal);

    /**
     * Generates a vector into an existing double array.
     *
     * @param ordinal the vector ordinal
     * @param target the target array to write into
     * @param offset the offset within the target array
     * @throws IllegalStateException if not initialized
     */
    void generateIntoDouble(long ordinal, double[] target, int offset);

    /**
     * Generates a batch of vectors for consecutive ordinals.
     *
     * @param startOrdinal the starting ordinal
     * @param count the number of vectors to generate
     * @return a 2D array where result[i] is the vector for ordinal (startOrdinal + i)
     * @throws IllegalStateException if not initialized
     */
    float[][] generateBatch(long startOrdinal, int count);

    /**
     * Generates a flat batch of vectors for consecutive ordinals.
     *
     * <p>Vectors are stored contiguously: [v0_d0, v0_d1, ..., v1_d0, v1_d1, ...]
     *
     * @param startOrdinal the starting ordinal
     * @param count the number of vectors to generate
     * @return a flat array containing count * dimensions floats
     * @throws IllegalStateException if not initialized
     */
    float[] generateFlatBatch(long startOrdinal, int count);

    /**
     * Returns the dimensionality of generated vectors.
     *
     * @return the number of dimensions
     * @throws IllegalStateException if not initialized
     */
    int dimensions();

    /**
     * Returns the number of unique vectors in the space.
     *
     * @return the number of unique vectors
     * @throws IllegalStateException if not initialized
     */
    long uniqueVectors();
}
