package io.nosqlbench.vshapes.stream;

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
 * Annotation used to mark a StreamingAnalyzer implementation with its unique name.
 *
 * <p>This annotation is used by {@link StreamingAnalyzerIO} to discover and load
 * analyzers by name via the ServiceLoader mechanism.
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * @AnalyzerName("model-extractor")
 * public class StreamingModelExtractor implements StreamingAnalyzer<VectorSpaceModel> {
 *     // ...
 * }
 * }</pre>
 *
 * @see StreamingAnalyzer
 * @see StreamingAnalyzerIO
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface AnalyzerName {
    /**
     * The unique name identifying this analyzer.
     *
     * <p>This name is used to look up the analyzer via {@link StreamingAnalyzerIO#get(String)}.
     * It should match the value returned by {@link StreamingAnalyzer#getAnalyzerType()}.
     *
     * @return the analyzer name
     */
    String value();
}
