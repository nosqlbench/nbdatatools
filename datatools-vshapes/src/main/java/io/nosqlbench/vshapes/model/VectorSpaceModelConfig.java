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
 * custom distributions with polymorphic model types.
 *
 * <h2>JSON Schema</h2>
 *
 * <p>Uniform Gaussian distribution (all dimensions share same parameters):
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
 * <p>Per-dimension distributions (heterogeneous model types):
 * <pre>{@code
 * {
 *   "unique_vectors": 1000000,
 *   "components": [
 *     {"type": "gaussian", "mean": 0.0, "std_dev": 1.0},
 *     {"type": "uniform", "lower": 0.0, "upper": 1.0},
 *     {"type": "gaussian", "mean": 0.5, "std_dev": 0.5, "lower_bound": 0.0, "upper_bound": 1.0},
 *     ...
 *   ]
 * }
 * }</pre>
 *
 * @see VectorSpaceModel
 * @see ComponentModel
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

    /** Mean for uniform Gaussian distribution across all dimensions */
    @SerializedName("mean")
    private Double mean;

    /** Standard deviation for uniform Gaussian distribution across all dimensions */
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
     * Configuration for a single dimension's distribution model.
     * Supports polymorphic model types via the "type" field.
     */
    public static class ComponentConfig {
        @SerializedName("type")
        private String type = "gaussian";  // default to gaussian for backward compatibility

        @SerializedName("mean")
        private Double mean;

        @SerializedName("std_dev")
        private Double stdDev;

        @SerializedName("lower")
        private Double lower;

        @SerializedName("upper")
        private Double upper;

        @SerializedName("lower_bound")
        private Double lowerBound;

        @SerializedName("upper_bound")
        private Double upperBound;

        public ComponentConfig() {
        }

        /**
         * Creates a Gaussian component config.
         */
        public ComponentConfig(double mean, double stdDev) {
            this.type = "gaussian";
            this.mean = mean;
            this.stdDev = stdDev;
        }

        /**
         * Creates a truncated Gaussian component config.
         */
        public ComponentConfig(double mean, double stdDev, double lowerBound, double upperBound) {
            this.type = "gaussian";
            this.mean = mean;
            this.stdDev = stdDev;
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
        }

        /**
         * Creates a Uniform component config.
         */
        public static ComponentConfig uniform(double lower, double upper) {
            ComponentConfig config = new ComponentConfig();
            config.type = "uniform";
            config.lower = lower;
            config.upper = upper;
            return config;
        }

        public String getType() {
            return type != null ? type : "gaussian";
        }

        public void setType(String type) {
            this.type = type;
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

        public Double getLower() {
            return lower;
        }

        public void setLower(Double lower) {
            this.lower = lower;
        }

        public Double getUpper() {
            return upper;
        }

        public void setUpper(Double upper) {
            this.upper = upper;
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
         * Converts this configuration to a ComponentModel.
         *
         * @return the corresponding ComponentModel
         * @throws IllegalArgumentException if required fields are missing or type is unknown
         */
        public ComponentModel toComponentModel() {
            String modelType = getType();

            switch (modelType) {
                case "gaussian":
                    Objects.requireNonNull(mean, "mean is required for gaussian");
                    Objects.requireNonNull(stdDev, "std_dev is required for gaussian");
                    if (isTruncated()) {
                        return new GaussianComponentModel(mean, stdDev, lowerBound, upperBound);
                    } else {
                        return new GaussianComponentModel(mean, stdDev);
                    }

                case "uniform":
                    Double lo = lower != null ? lower : lowerBound;
                    Double hi = upper != null ? upper : upperBound;
                    Objects.requireNonNull(lo, "lower is required for uniform");
                    Objects.requireNonNull(hi, "upper is required for uniform");
                    return new UniformComponentModel(lo, hi);

                default:
                    throw new IllegalArgumentException("Unknown component model type: " + modelType);
            }
        }

        /**
         * Creates a ComponentConfig from a ComponentModel.
         *
         * @param model the source model
         * @return the corresponding ComponentConfig
         */
        public static ComponentConfig fromComponentModel(ComponentModel model) {
            ComponentConfig config = new ComponentConfig();
            config.type = model.getModelType();

            if (model instanceof GaussianComponentModel) {
                GaussianComponentModel gaussian = (GaussianComponentModel) model;
                config.mean = gaussian.getMean();
                config.stdDev = gaussian.getStdDev();
                if (gaussian.isTruncated()) {
                    config.lowerBound = gaussian.lower();
                    config.upperBound = gaussian.upper();
                }
            } else if (model instanceof UniformComponentModel) {
                UniformComponentModel uniform = (UniformComponentModel) model;
                config.lower = uniform.getLower();
                config.upper = uniform.getUpper();
            } else if (model instanceof EmpiricalComponentModel) {
                EmpiricalComponentModel empirical = (EmpiricalComponentModel) model;
                config.mean = empirical.getMean();
                config.stdDev = empirical.getStdDev();
                config.lowerBound = empirical.getMin();
                config.upperBound = empirical.getMax();
            } else if (model instanceof CompositeComponentModel) {
                // Composite models don't have simple scalar properties
                // The type field is sufficient for identification
            } else {
                throw new IllegalArgumentException("Unknown component model type: " + model.getClass().getName());
            }

            return config;
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
     * Returns whether this config uses per-dimension custom distributions.
     */
    public boolean hasPerDimensionComponents() {
        return components != null && components.length > 0;
    }

    /**
     * Returns whether this config has truncation bounds.
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
            ComponentModel[] models = new ComponentModel[components.length];
            for (int i = 0; i < components.length; i++) {
                models[i] = components[i].toComponentModel();
            }
            return new VectorSpaceModel(uniqueVectors, models);
        } else {
            // Uniform Gaussian configuration
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

        ComponentModel[] componentModels = model.componentModels();

        // Check if all components are identical Gaussian (can use compact uniform format)
        boolean canUseUniform = model.isAllGaussian();
        if (canUseUniform) {
            GaussianComponentModel first = (GaussianComponentModel) componentModels[0];
            for (int i = 1; i < componentModels.length; i++) {
                if (!componentModels[i].equals(first)) {
                    canUseUniform = false;
                    break;
                }
            }

            if (canUseUniform) {
                // Use uniform representation
                config.setDimensions(model.dimensions());
                config.setMean(first.getMean());
                config.setStdDev(first.getStdDev());
                if (first.isTruncated()) {
                    config.setLowerBound(first.lower());
                    config.setUpperBound(first.upper());
                }
                return config;
            }
        }

        // Use per-dimension representation
        ComponentConfig[] components = new ComponentConfig[componentModels.length];
        for (int i = 0; i < componentModels.length; i++) {
            components[i] = ComponentConfig.fromComponentModel(componentModels[i]);
        }
        config.setComponents(components);

        return config;
    }

    /**
     * Loads a VectorSpaceModelConfig from JSON.
     */
    public static VectorSpaceModelConfig fromJson(String json) {
        return GSON.fromJson(json, VectorSpaceModelConfig.class);
    }

    /**
     * Loads a VectorSpaceModelConfig from a Reader.
     */
    public static VectorSpaceModelConfig fromJson(Reader reader) {
        return GSON.fromJson(reader, VectorSpaceModelConfig.class);
    }

    /**
     * Serializes this configuration to JSON.
     */
    public String toJson() {
        return GSON.toJson(this);
    }

    /**
     * Writes this configuration as JSON to a Writer.
     */
    public void toJson(Writer writer) {
        GSON.toJson(this, writer);
    }

    /**
     * Loads a VectorSpaceModel directly from a JSON file.
     */
    public static VectorSpaceModel loadFromFile(Path path) throws IOException {
        try (Reader reader = Files.newBufferedReader(path)) {
            return load(reader);
        }
    }

    /**
     * Loads a VectorSpaceModel from a Reader providing JSON.
     */
    public static VectorSpaceModel load(Reader reader) {
        VectorSpaceModelConfig config = fromJson(reader);
        return config.toVectorSpaceModel();
    }

    /**
     * Loads a VectorSpaceModel from a JSON string.
     */
    public static VectorSpaceModel load(String json) {
        VectorSpaceModelConfig config = fromJson(json);
        return config.toVectorSpaceModel();
    }

    /**
     * Saves a VectorSpaceModel to a JSON file.
     */
    public static void saveToFile(VectorSpaceModel model, Path path) throws IOException {
        VectorSpaceModelConfig config = fromVectorSpaceModel(model);
        try (Writer writer = Files.newBufferedWriter(path)) {
            config.toJson(writer);
        }
    }

    /**
     * Saves a VectorSpaceModelConfig to a JSON file.
     */
    public static void saveToFile(VectorSpaceModelConfig config, Path path) throws IOException {
        try (Writer writer = Files.newBufferedWriter(path)) {
            config.toJson(writer);
        }
    }

    /**
     * Saves a VectorSpaceModel to a Writer as JSON.
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
