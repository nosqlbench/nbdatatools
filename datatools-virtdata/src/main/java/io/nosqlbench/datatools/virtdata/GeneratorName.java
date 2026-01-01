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
 * Annotation used to mark a VectorGenerator implementation with its unique name.
 *
 * <p>This annotation is used by {@link VectorGeneratorIO} to discover and load
 * generators by name via the ServiceLoader mechanism.
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * @GeneratorName("vector-gen")
 * @ModelType(VectorSpaceModel.class)
 * public class VectorGen implements VectorGenerator<VectorSpaceModel> {
 *     // ...
 * }
 * }</pre>
 *
 * @see VectorGenerator
 * @see VectorGeneratorIO
 * @see ModelType
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface GeneratorName {
    /**
     * The unique name identifying this generator.
     *
     * <p>This name is used to look up the generator via {@link VectorGeneratorIO#get(String)}.
     * It should match the value returned by {@link VectorGenerator#getGeneratorType()}.
     *
     * @return the generator name
     */
    String value();
}
