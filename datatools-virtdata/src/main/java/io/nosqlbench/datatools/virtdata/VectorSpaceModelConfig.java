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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/**
 * JSON-serializable configuration for a VectorSpaceModel.
 *
 * <h2>Purpose</h2>
 *
 * <p>This class provides a JSON representation of vector space model parameters,
 * enabling configuration to be loaded from or saved to JSON files. It supports
 * both uniform distributions (same parameters for all dimensions) and per-dimension
 * custom distributions.
 *
 * <h2>JSON Schema</h2>
 *
 * <p>Uniform distribution (all dimensions share same parameters):
 * <pre>{@code
 * {
 *   "unique_vectors": 1000000,
 *   "dimensions": 128,
 *   "mean": 0.0,
 *   "std_dev": 1.0,
 *   "lower_bound": -1.0,    // optional, for truncation
 *   "upper_bound": 1.0      // optional, for truncation
 * }
 * }</pre>
 *
 * <p>Per-dimension distributions:
 * <pre>{@code
 * {
 *   "unique_vectors": 1000000,
 *   "components": [
 *     {"mean": 0.0, "std_dev": 1.0},
 *     {"mean": 0.5, "std_dev": 0.5, "lower_bound": 0.0, "upper_bound": 1.0},
 *     ...
 *   ]
 * }
 * }</pre>
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * // Load from file
 * VectorSpaceModel model = VectorSpaceModelConfig.loadFromFile(Path.of("config.json"));
 *
 * // Load from reader
 * try (Reader reader = Files.newBufferedReader(path)) {
 *     VectorSpaceModel model = VectorSpaceModelConfig.load(reader);
 * }
 *
 * // Save to file
 * VectorSpaceModelConfig.saveToFile(model, Path.of("config.json"));
 *
 * // Create programmatically and save
 * VectorSpaceModelConfig config = new VectorSpaceModelConfig();
 * config.setUniqueVectors(1_000_000);
 * config.setDimensions(128);
 * config.setMean(0.0);
 * config.setStdDev(1.0);
 * config.setLowerBound(-1.0);
 * config.setUpperBound(1.0);
 * VectorSpaceModelConfig.saveToFile(config, Path.of("config.json"));
 * }</pre>
 *
 * @see VectorSpaceModel
 */
public class VectorSpaceModelConfig {

    private static final Gson GSON = new GsonBuilder()
            .setPrettyPrinting()
            .serializeNulls()
            .create();

    @SerializedName("unique_vectors")
    private Long uniqueVectors;

    @SerializedName("dimensions")
    private Integer dimensions;

    /** Mean for uniform distribution across all dimensions */
    @SerializedName("mean")
    private Double mean;

    /** Standard deviation for uniform distribution across all dimensions */
    @SerializedName("std_dev")
    private Double stdDev;

    /** Lower truncation bound (optional) */
    @SerializedName("lower_bound")
    private Double lowerBound;

    /** Upper truncation bound (optional) */
    @SerializedName("upper_bound")
    private Double upperBound;

    /** Per-dimension component configurations (alternative to uniform) */
    @SerializedName("components")
    private ComponentConfig[] components;

    /**
     * Configuration for a single dimension's Gaussian distribution.
     */
    public static class ComponentConfig {
        @SerializedName("mean")
        private Double mean;

        @SerializedName("std_dev")
        private Double stdDev;

        @SerializedName("lower_bound")
        private Double lowerBound;

        @SerializedName("upper_bound")
        private Double upperBound;

        public ComponentConfig() {
        }

        public ComponentConfig(double mean, double stdDev) {
            this.mean = mean;
            this.stdDev = stdDev;
        }

        public ComponentConfig(double mean, double stdDev, double lowerBound, double upperBound) {
            this.mean = mean;
            this.stdDev = stdDev;
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
        }

        public Double getMean() {
            return mean;
        }

        public void setMean(Double mean) {
            this.mean = mean;
        }

        public Double getStdDev() {
            return stdDev;
        }

        public void setStdDev(Double stdDev) {
            this.stdDev = stdDev;
        }

        public Double getLowerBound() {
            return lowerBound;
        }

        public void setLowerBound(Double lowerBound) {
            this.lowerBound = lowerBound;
        }

        public Double getUpperBound() {
            return upperBound;
        }

        public void setUpperBound(Double upperBound) {
            this.upperBound = upperBound;
        }

        public boolean isTruncated() {
            return lowerBound != null && upperBound != null;
        }

        /**
         * Converts this configuration to a GaussianComponentModel.
         *
         * @return the corresponding GaussianComponentModel
         * @throws IllegalArgumentException if required fields are missing
         */
        public GaussianComponentModel toComponentModel() {
            Objects.requireNonNull(mean, "mean is required");
            Objects.requireNonNull(stdDev, "std_dev is required");

            if (isTruncated()) {
                return new GaussianComponentModel(mean, stdDev, lowerBound, upperBound);
            } else {
                return new GaussianComponentModel(mean, stdDev);
            }
        }

        /**
         * Creates a ComponentConfig from a GaussianComponentModel.
         *
         * @param model the source model
         * @return the corresponding ComponentConfig
         */
        public static ComponentConfig fromComponentModel(GaussianComponentModel model) {
            if (model.isTruncated()) {
                return new ComponentConfig(model.mean(), model.stdDev(), model.lower(), model.upper());
            } else {
                return new ComponentConfig(model.mean(), model.stdDev());
            }
        }
    }

    public VectorSpaceModelConfig() {
    }

    public Long getUniqueVectors() {
        return uniqueVectors;
    }

    public void setUniqueVectors(Long uniqueVectors) {
        this.uniqueVectors = uniqueVectors;
    }

    public Integer getDimensions() {
        return dimensions;
    }

    public void setDimensions(Integer dimensions) {
        this.dimensions = dimensions;
    }

    public Double getMean() {
        return mean;
    }

    public void setMean(Double mean) {
        this.mean = mean;
    }

    public Double getStdDev() {
        return stdDev;
    }

    public void setStdDev(Double stdDev) {
        this.stdDev = stdDev;
    }

    public Double getLowerBound() {
        return lowerBound;
    }

    public void setLowerBound(Double lowerBound) {
        this.lowerBound = lowerBound;
    }

    public Double getUpperBound() {
        return upperBound;
    }

    public void setUpperBound(Double upperBound) {
        this.upperBound = upperBound;
    }

    public ComponentConfig[] getComponents() {
        return components;
    }

    public void setComponents(ComponentConfig[] components) {
        this.components = components;
    }

    /**
     * Returns whether this config uses uniform distribution (same for all dimensions)
     * or per-dimension custom distributions.
     *
     * @return true if using per-dimension components, false if uniform
     */
    public boolean hasPerDimensionComponents() {
        return components != null && components.length > 0;
    }

    /**
     * Returns whether this config has truncation bounds.
     *
     * @return true if lower_bound and upper_bound are specified
     */
    public boolean isTruncated() {
        return lowerBound != null && upperBound != null;
    }

    /**
     * Converts this configuration to a VectorSpaceModel.
     *
     * @return the corresponding VectorSpaceModel
     * @throws IllegalArgumentException if required fields are missing or invalid
     */
    public VectorSpaceModel toVectorSpaceModel() {
        Objects.requireNonNull(uniqueVectors, "unique_vectors is required");

        if (hasPerDimensionComponents()) {
            // Per-dimension configuration
            GaussianComponentModel[] models = new GaussianComponentModel[components.length];
            for (int i = 0; i < components.length; i++) {
                models[i] = components[i].toComponentModel();
            }
            return new VectorSpaceModel(uniqueVectors, models);
        } else {
            // Uniform configuration
            Objects.requireNonNull(dimensions, "dimensions is required for uniform configuration");
            double m = mean != null ? mean : 0.0;
            double s = stdDev != null ? stdDev : 1.0;

            if (isTruncated()) {
                return new VectorSpaceModel(uniqueVectors, dimensions, m, s, lowerBound, upperBound);
            } else {
                return new VectorSpaceModel(uniqueVectors, dimensions, m, s);
            }
        }
    }

    /**
     * Creates a VectorSpaceModelConfig from a VectorSpaceModel.
     *
     * @param model the source model
     * @return the corresponding configuration
     */
    public static VectorSpaceModelConfig fromVectorSpaceModel(VectorSpaceModel model) {
        VectorSpaceModelConfig config = new VectorSpaceModelConfig();
        config.setUniqueVectors(model.uniqueVectors());

        GaussianComponentModel[] componentModels = model.componentModels();

        // Check if all components are identical (uniform)
        boolean allSame = true;
        GaussianComponentModel first = componentModels[0];
        for (int i = 1; i < componentModels.length; i++) {
            if (!componentModels[i].equals(first)) {
                allSame = false;
                break;
            }
        }

        if (allSame) {
            // Use uniform representation
            config.setDimensions(model.dimensions());
            config.setMean(first.mean());
            config.setStdDev(first.stdDev());
            if (first.isTruncated()) {
                config.setLowerBound(first.lower());
                config.setUpperBound(first.upper());
            }
        } else {
            // Use per-dimension representation
            ComponentConfig[] components = new ComponentConfig[componentModels.length];
            for (int i = 0; i < componentModels.length; i++) {
                components[i] = ComponentConfig.fromComponentModel(componentModels[i]);
            }
            config.setComponents(components);
        }

        return config;
    }

    /**
     * Loads a VectorSpaceModelConfig from JSON.
     *
     * @param json the JSON string
     * @return the parsed configuration
     */
    public static VectorSpaceModelConfig fromJson(String json) {
        return GSON.fromJson(json, VectorSpaceModelConfig.class);
    }

    /**
     * Loads a VectorSpaceModelConfig from a Reader.
     *
     * @param reader the reader providing JSON
     * @return the parsed configuration
     */
    public static VectorSpaceModelConfig fromJson(Reader reader) {
        return GSON.fromJson(reader, VectorSpaceModelConfig.class);
    }

    /**
     * Serializes this configuration to JSON.
     *
     * @return the JSON string
     */
    public String toJson() {
        return GSON.toJson(this);
    }

    /**
     * Writes this configuration as JSON to a Writer.
     *
     * @param writer the target writer
     */
    public void toJson(Writer writer) {
        GSON.toJson(this, writer);
    }

    /**
     * Loads a VectorSpaceModel directly from a JSON file.
     *
     * @param path the path to the JSON file
     * @return the loaded VectorSpaceModel
     * @throws IOException if the file cannot be read
     */
    public static VectorSpaceModel loadFromFile(Path path) throws IOException {
        try (Reader reader = Files.newBufferedReader(path)) {
            return load(reader);
        }
    }

    /**
     * Loads a VectorSpaceModel from a Reader providing JSON.
     *
     * @param reader the reader providing JSON
     * @return the loaded VectorSpaceModel
     */
    public static VectorSpaceModel load(Reader reader) {
        VectorSpaceModelConfig config = fromJson(reader);
        return config.toVectorSpaceModel();
    }

    /**
     * Loads a VectorSpaceModel from a JSON string.
     *
     * @param json the JSON string
     * @return the loaded VectorSpaceModel
     */
    public static VectorSpaceModel load(String json) {
        VectorSpaceModelConfig config = fromJson(json);
        return config.toVectorSpaceModel();
    }

    /**
     * Saves a VectorSpaceModel to a JSON file.
     *
     * @param model the model to save
     * @param path the target file path
     * @throws IOException if the file cannot be written
     */
    public static void saveToFile(VectorSpaceModel model, Path path) throws IOException {
        VectorSpaceModelConfig config = fromVectorSpaceModel(model);
        try (Writer writer = Files.newBufferedWriter(path)) {
            config.toJson(writer);
        }
    }

    /**
     * Saves a VectorSpaceModelConfig to a JSON file.
     *
     * @param config the configuration to save
     * @param path the target file path
     * @throws IOException if the file cannot be written
     */
    public static void saveToFile(VectorSpaceModelConfig config, Path path) throws IOException {
        try (Writer writer = Files.newBufferedWriter(path)) {
            config.toJson(writer);
        }
    }

    /**
     * Saves a VectorSpaceModel to a Writer as JSON.
     *
     * @param model the model to save
     * @param writer the target writer
     */
    public static void save(VectorSpaceModel model, Writer writer) {
        VectorSpaceModelConfig config = fromVectorSpaceModel(model);
        config.toJson(writer);
    }

    @Override
    public String toString() {
        return toJson();
    }
}
