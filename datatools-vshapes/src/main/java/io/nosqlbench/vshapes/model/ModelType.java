package io.nosqlbench.vshapes.model;

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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares the serialization type name for a {@link ScalarModel} implementation.
 *
 * <p>This annotation enables polymorphic JSON serialization by providing:
 * <ul>
 *   <li>A type discriminator field in the JSON output</li>
 *   <li>Type validation during deserialization</li>
 *   <li>Type-specific field serialization (no extraneous null fields)</li>
 * </ul>
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * @ModelType("normal")
 * public class NormalScalarModel implements ScalarModel {
 *     // Only normal-specific fields: mean, stdDev, lower, upper
 * }
 *
 * @ModelType("uniform")
 * public class UniformScalarModel implements ScalarModel {
 *     // Only uniform-specific fields: lower, upper
 * }
 * }</pre>
 *
 * <h2>JSON Output</h2>
 *
 * <p>The annotated type name appears as a "type" field in serialized JSON:
 *
 * <pre>{@code
 * {
 *   "type": "normal",
 *   "mean": 0.0,
 *   "std_dev": 1.0
 * }
 * }</pre>
 *
 * <h2>Type Validation</h2>
 *
 * <p>During deserialization, the type field is validated against the expected
 * type to ensure compatibility. If a mismatch is detected, an
 * {@link IllegalArgumentException} is thrown.
 *
 * @see ScalarModel
 * @see ScalarModelTypeAdapterFactory
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ModelType {
    /**
     * The type name used in JSON serialization.
     *
     * <p>This value must be unique across all ScalarModel implementations
     * and should be lowercase with underscores for multi-word names
     * (e.g., "normal", "uniform", "beta_prime", "pearson_iv").
     *
     * @return the type discriminator string
     */
    String value();
}
