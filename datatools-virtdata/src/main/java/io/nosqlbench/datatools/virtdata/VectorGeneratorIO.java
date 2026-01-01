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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Factory class for discovering and loading VectorGenerator implementations via SPI.
 *
 * <p>Uses {@link ServiceLoader} to discover all available generators and provides
 * methods to find generators by name or by supported model type.
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * // Get a generator by name
 * Optional<VectorGenerator<?>> gen = VectorGeneratorIO.get("vector-gen");
 *
 * // Get a generator for a specific model type
 * Optional<VectorGenerator<?>> gen = VectorGeneratorIO.getForModel(VectorSpaceModel.class);
 *
 * // Get and initialize in one step
 * VectorGenerator<VectorSpaceModel> gen = VectorGeneratorIO.create("vector-gen", model);
 *
 * // List available generators
 * List<String> names = VectorGeneratorIO.getAvailableNames();
 * }</pre>
 *
 * <h2>Discovering Generators for Models</h2>
 *
 * <p>When you have a model extracted by an analyzer, you can find a compatible generator:
 *
 * <pre>{@code
 * // Model from analyzer
 * VectorSpaceModel model = results.getResult("model-extractor", VectorSpaceModel.class);
 *
 * // Find generator that supports this model type
 * VectorGenerator<?> gen = VectorGeneratorIO.getForModel(VectorSpaceModel.class)
 *     .orElseThrow(() -> new RuntimeException("No generator for VectorSpaceModel"));
 * gen.initialize(model);
 *
 * // Generate vectors
 * float[] vector = gen.apply(42L);
 * }</pre>
 *
 * @see VectorGenerator
 * @see GeneratorName
 * @see ModelType
 */
public final class VectorGeneratorIO {

    @SuppressWarnings("rawtypes")
    private static final ServiceLoader<VectorGenerator> serviceLoader =
        ServiceLoader.load(VectorGenerator.class);

    private VectorGeneratorIO() {
    }

    /**
     * Gets a generator by name.
     *
     * <p>The name is matched against the {@link GeneratorName} annotation value
     * or the {@link VectorGenerator#getGeneratorType()} method.
     *
     * @param name the generator name to find
     * @return an Optional containing a new instance of the generator, or empty if not found
     */
    public static Optional<VectorGenerator<?>> get(String name) {
        return providers()
            .filter(provider -> matchesName(provider, name))
            .findFirst()
            .map(ServiceLoader.Provider::get);
    }

    /**
     * Gets a generator by name with type casting.
     *
     * @param name the generator name to find
     * @param modelType the expected model type class
     * @param <M> the model type
     * @return an Optional containing a new instance of the generator, or empty if not found
     */
    @SuppressWarnings("unchecked")
    public static <M> Optional<VectorGenerator<M>> get(String name, Class<M> modelType) {
        return get(name).map(g -> (VectorGenerator<M>) g);
    }

    /**
     * Gets a generator that supports the given model type.
     *
     * <p>The model type is matched against the {@link ModelType} annotation.
     *
     * @param modelClass the model class to find a generator for
     * @return an Optional containing a new instance of the generator, or empty if not found
     */
    public static Optional<VectorGenerator<?>> getForModel(Class<?> modelClass) {
        return providers()
            .filter(provider -> matchesModelType(provider, modelClass))
            .findFirst()
            .map(ServiceLoader.Provider::get);
    }

    /**
     * Gets a generator that supports the given model type with type casting.
     *
     * @param modelClass the model class to find a generator for
     * @param <M> the model type
     * @return an Optional containing a new instance of the generator, or empty if not found
     */
    @SuppressWarnings("unchecked")
    public static <M> Optional<VectorGenerator<M>> getForModel(Class<M> modelClass, Class<M> ignored) {
        return getForModel(modelClass).map(g -> (VectorGenerator<M>) g);
    }

    /**
     * Creates and initializes a generator by name.
     *
     * @param name the generator name
     * @param model the model to initialize with
     * @param <M> the model type
     * @return the initialized generator
     * @throws IllegalArgumentException if no generator with the given name exists
     */
    @SuppressWarnings("unchecked")
    public static <M> VectorGenerator<M> create(String name, M model) {
        VectorGenerator<M> generator = (VectorGenerator<M>) get(name)
            .orElseThrow(() -> new IllegalArgumentException(
                "No generator found with name: " + name +
                ". Available: " + getAvailableNames()));
        generator.initialize(model);
        return generator;
    }

    /**
     * Creates and initializes a generator for the given model.
     *
     * <p>Finds a generator that supports the model's type and initializes it.
     *
     * @param model the model to create a generator for
     * @param <M> the model type
     * @return the initialized generator
     * @throws IllegalArgumentException if no generator supports the model type
     */
    @SuppressWarnings("unchecked")
    public static <M> VectorGenerator<M> createForModel(M model) {
        Class<?> modelClass = model.getClass();
        VectorGenerator<M> generator = (VectorGenerator<M>) getForModel(modelClass)
            .orElseThrow(() -> new IllegalArgumentException(
                "No generator found for model type: " + modelClass.getName() +
                ". Available generators support: " + getSupportedModelTypes()));
        generator.initialize(model);
        return generator;
    }

    /**
     * Gets all available generators.
     *
     * <p>Each call returns new instances of all registered generators.
     *
     * @return list of all available generator instances
     */
    @SuppressWarnings("rawtypes")
    public static List<VectorGenerator<?>> getAll() {
        List<VectorGenerator<?>> result = new ArrayList<>();
        for (ServiceLoader.Provider<VectorGenerator> provider : serviceLoader.stream().collect(Collectors.toList())) {
            result.add(provider.get());
        }
        return result;
    }

    /**
     * Gets the names of all available generators.
     *
     * @return list of generator names
     */
    @SuppressWarnings("rawtypes")
    public static List<String> getAvailableNames() {
        List<String> names = new ArrayList<>();
        for (ServiceLoader.Provider<VectorGenerator> provider : serviceLoader.stream().collect(Collectors.toList())) {
            String name = getGeneratorName(provider);
            if (name != null) {
                names.add(name);
            }
        }
        return names;
    }

    /**
     * Gets the model types supported by available generators.
     *
     * @return list of supported model class names
     */
    @SuppressWarnings("rawtypes")
    public static List<String> getSupportedModelTypes() {
        List<String> types = new ArrayList<>();
        for (ServiceLoader.Provider<VectorGenerator> provider : serviceLoader.stream().collect(Collectors.toList())) {
            ModelType annotation = provider.type().getAnnotation(ModelType.class);
            if (annotation != null) {
                types.add(annotation.value().getSimpleName());
            }
        }
        return types;
    }

    /**
     * Checks if a generator with the given name is available.
     *
     * @param name the generator name to check
     * @return true if a generator with the given name exists
     */
    public static boolean isAvailable(String name) {
        return providers().anyMatch(provider -> matchesName(provider, name));
    }

    /**
     * Checks if a generator supporting the given model type is available.
     *
     * @param modelClass the model class to check
     * @return true if a generator supporting the model type exists
     */
    public static boolean supportsModel(Class<?> modelClass) {
        return providers().anyMatch(provider -> matchesModelType(provider, modelClass));
    }

    /**
     * Reloads the service loader to pick up newly available generators.
     */
    public static void reload() {
        serviceLoader.reload();
    }

    @SuppressWarnings("rawtypes")
    private static java.util.stream.Stream<ServiceLoader.Provider<VectorGenerator>> providers() {
        return StreamSupport.stream(serviceLoader.stream().spliterator(), false);
    }

    @SuppressWarnings("rawtypes")
    private static boolean matchesName(ServiceLoader.Provider<VectorGenerator> provider, String name) {
        String generatorName = getGeneratorName(provider);
        return name.equals(generatorName);
    }

    @SuppressWarnings("rawtypes")
    private static boolean matchesModelType(ServiceLoader.Provider<VectorGenerator> provider, Class<?> modelClass) {
        ModelType annotation = provider.type().getAnnotation(ModelType.class);
        if (annotation != null) {
            return annotation.value().equals(modelClass);
        }
        return false;
    }

    @SuppressWarnings("rawtypes")
    private static String getGeneratorName(ServiceLoader.Provider<VectorGenerator> provider) {
        Class<?> type = provider.type();

        // First check the @GeneratorName annotation
        GeneratorName annotation = type.getAnnotation(GeneratorName.class);
        if (annotation != null) {
            return annotation.value();
        }

        // Fall back to instantiating and calling getGeneratorType()
        try {
            VectorGenerator<?> instance = provider.get();
            return instance.getGeneratorType();
        } catch (Exception e) {
            return null;
        }
    }
}
