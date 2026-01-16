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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Factory class for discovering and loading StreamingAnalyzer implementations via SPI.
 *
 * <p>Uses {@link ServiceLoader} to discover all available analyzers and provides
 * methods to find analyzers by name or list all available analyzers.
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * // Get a specific analyzer by name
 * Optional<StreamingAnalyzer<?>> analyzer = StreamingAnalyzerIO.get("model-extractor");
 *
 * // List all available analyzer names
 * List<String> names = StreamingAnalyzerIO.getAvailableNames();
 *
 * // Get all analyzers
 * List<StreamingAnalyzer<?>> all = StreamingAnalyzerIO.getAll();
 *
 * // Use with AnalyzerHarness
 * AnalyzerHarness harness = new AnalyzerHarness();
 * StreamingAnalyzerIO.get("model-extractor").ifPresent(harness::register);
 * StreamingAnalyzerIO.get("stats").ifPresent(harness::register);
 * }</pre>
 *
 * <h2>Registering Analyzers</h2>
 *
 * <p>To register an analyzer for SPI discovery:
 * <ol>
 *   <li>Implement {@link StreamingAnalyzer}</li>
 *   <li>Add the {@link AnalyzerName} annotation</li>
 *   <li>Provide a public no-args constructor</li>
 *   <li>Register in META-INF/services/io.nosqlbench.vshapes.stream.StreamingAnalyzer</li>
 * </ol>
 *
 * @see StreamingAnalyzer
 * @see AnalyzerName
 * @see AnalyzerHarness
 */
public final class StreamingAnalyzerIO {

    private static final ServiceLoader<StreamingAnalyzer> serviceLoader =
        ServiceLoader.load(StreamingAnalyzer.class);

    private StreamingAnalyzerIO() {
    }

    /**
     * Gets an analyzer by name.
     *
     * <p>The name is matched against the {@link AnalyzerName} annotation value
     * or the {@link StreamingAnalyzer#getAnalyzerType()} method.
     *
     * @param name the analyzer name to find
     * @return an Optional containing a new instance of the analyzer, or empty if not found
     */
    public static Optional<StreamingAnalyzer<?>> get(String name) {
        return providers()
            .filter(provider -> matchesName(provider, name))
            .findFirst()
            .map(ServiceLoader.Provider::get);
    }

    /**
     * Gets an analyzer by name with type casting.
     *
     * @param name the analyzer name to find
     * @param modelType the expected model type class
     * @param <M> the model type
     * @return an Optional containing a new instance of the analyzer, or empty if not found
     */
    @SuppressWarnings("unchecked")
    public static <M> Optional<StreamingAnalyzer<M>> get(String name, Class<M> modelType) {
        return get(name).map(a -> (StreamingAnalyzer<M>) a);
    }

    /**
     * Gets all available analyzers.
     *
     * <p>Each call returns new instances of all registered analyzers.
     *
     * @return list of all available analyzer instances
     */
    public static List<StreamingAnalyzer<?>> getAll() {
        List<StreamingAnalyzer<?>> result = new ArrayList<>();
        for (ServiceLoader.Provider<StreamingAnalyzer> provider : serviceLoader.stream().toList()) {
            result.add(provider.get());
        }
        return result;
    }

    /**
     * Gets the names of all available analyzers.
     *
     * @return list of analyzer names
     */
    public static List<String> getAvailableNames() {
        List<String> names = new ArrayList<>();
        for (ServiceLoader.Provider<StreamingAnalyzer> provider : serviceLoader.stream().toList()) {
            String name = getAnalyzerName(provider);
            if (name != null) {
                names.add(name);
            }
        }
        return names;
    }

    /**
     * Checks if an analyzer with the given name is available.
     *
     * @param name the analyzer name to check
     * @return true if an analyzer with the given name exists
     */
    public static boolean isAvailable(String name) {
        return providers().anyMatch(provider -> matchesName(provider, name));
    }

    /**
     * Reloads the service loader to pick up newly available analyzers.
     *
     * <p>This is useful in dynamic environments where analyzers may be added at runtime.
     */
    public static void reload() {
        serviceLoader.reload();
    }

    private static java.util.stream.Stream<ServiceLoader.Provider<StreamingAnalyzer>> providers() {
        return StreamSupport.stream(serviceLoader.stream().spliterator(), false);
    }

    private static boolean matchesName(ServiceLoader.Provider<StreamingAnalyzer> provider, String name) {
        String analyzerName = getAnalyzerName(provider);
        return name.equals(analyzerName);
    }

    private static String getAnalyzerName(ServiceLoader.Provider<StreamingAnalyzer> provider) {
        Class<?> type = provider.type();

        // First check the @AnalyzerName annotation
        AnalyzerName annotation = type.getAnnotation(AnalyzerName.class);
        if (annotation != null) {
            return annotation.value();
        }

        // Fall back to instantiating and calling getAnalyzerType()
        // This is less efficient but provides compatibility
        try {
            StreamingAnalyzer<?> instance = provider.get();
            return instance.getAnalyzerType();
        } catch (Exception e) {
            return null;
        }
    }
}
