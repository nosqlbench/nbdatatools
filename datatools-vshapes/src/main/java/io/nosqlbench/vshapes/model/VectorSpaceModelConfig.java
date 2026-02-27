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
 * @see ScalarModel
 */
public class VectorSpaceModelConfig {

    private static final Gson GSON = new GsonBuilder()
            .setPrettyPrinting()
            .registerTypeAdapterFactory(ScalarModelTypeAdapterFactory.create())
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
     *
     * <p><b>Supported Model Types:</b></p>
     * <ul>
     *   <li><b>normal</b>: Normal (Gaussian) distribution - mean, std_dev, optional bounds</li>
     *   <li><b>uniform</b>: Uniform distribution - lower, upper</li>
     *   <li><b>beta</b>: Beta distribution - alpha, beta, lower, upper</li>
     *   <li><b>gamma</b>: Gamma distribution - shape, scale, shift</li>
     *   <li><b>inverse_gamma</b>: Inverse Gamma distribution - shape, scale</li>
     *   <li><b>student_t</b>: Student's t distribution - nu, mu, sigma</li>
     *   <li><b>pearson_iv</b>: Pearson Type IV distribution - m, nu, a, lambda</li>
     *   <li><b>beta_prime</b>: Beta Prime distribution - alpha, beta, scale</li>
     *   <li><b>empirical</b>: Empirical histogram - bins, counts, min, max</li>
     *   <li><b>composite</b>: Composite/mixture model - sub_models, weights</li>
     * </ul>
     */
    public static class ComponentConfig {
        @SerializedName("type")
        private String type = "normal";  // default to normal distribution

        // Normal distribution parameters
        @SerializedName("mean")
        private Double mean;

        @SerializedName("std_dev")
        private Double stdDev;

        // Uniform distribution parameters
        @SerializedName("lower")
        private Double lower;

        @SerializedName("upper")
        private Double upper;

        // Truncation bounds (for normal, gamma, etc.)
        @SerializedName("lower_bound")
        private Double lowerBound;

        @SerializedName("upper_bound")
        private Double upperBound;

        // Beta distribution parameters
        @SerializedName("alpha")
        private Double alpha;

        @SerializedName("beta")
        private Double beta;

        // Gamma distribution parameters
        @SerializedName("shape")
        private Double shape;

        @SerializedName("scale")
        private Double scale;

        @SerializedName("shift")
        private Double shift;

        // Student's t distribution parameters
        @SerializedName("nu")
        private Double nu;

        @SerializedName("mu")
        private Double mu;

        @SerializedName("sigma")
        private Double sigma;

        // Pearson IV parameters
        @SerializedName("m")
        private Double m;

        @SerializedName("a")
        private Double a;

        @SerializedName("lambda")
        private Double lambda;

        // Empirical histogram
        @SerializedName("bins")
        private double[] bins;

        @SerializedName("counts")
        private int[] counts;

        @SerializedName("min")
        private Double min;

        @SerializedName("max")
        private Double max;

        // Composite/mixture model parameters
        @SerializedName("sub_models")
        private ComponentConfig[] subModels;

        @SerializedName("weights")
        private double[] weights;

        /// Creates a default ComponentConfig.
        public ComponentConfig() {
        }

        /// Creates a Normal component config.
        ///
        /// @param mean the mean value
        /// @param stdDev the standard deviation
        public ComponentConfig(double mean, double stdDev) {
            this.type = "normal";
            this.mean = mean;
            this.stdDev = stdDev;
        }

        /// Creates a truncated Normal component config.
        ///
        /// @param mean the mean value
        /// @param stdDev the standard deviation
        /// @param lowerBound the lower truncation bound
        /// @param upperBound the upper truncation bound
        public ComponentConfig(double mean, double stdDev, double lowerBound, double upperBound) {
            this.type = "normal";
            this.mean = mean;
            this.stdDev = stdDev;
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
        }

        /// Creates a Uniform component config.
        ///
        /// @param lower the lower bound
        /// @param upper the upper bound
        /// @return a new uniform ComponentConfig
        public static ComponentConfig uniform(double lower, double upper) {
            ComponentConfig config = new ComponentConfig();
            config.type = "uniform";
            config.lower = lower;
            config.upper = upper;
            return config;
        }

        /// Returns the distribution type.
        ///
        /// @return the value
        public String getType() {
            return type != null ? type : "normal";
        }

        /// Sets the distribution type.
        ///
        /// @param type the value to set
        public void setType(String type) {
            this.type = type;
        }

        /// Returns the mean.
        ///
        /// @return the value
        public Double getMean() {
            return mean;
        }

        /// Sets the mean.
        ///
        /// @param mean the value to set
        public void setMean(Double mean) {
            this.mean = mean;
        }

        /// Returns the standard deviation.
        ///
        /// @return the value
        public Double getStdDev() {
            return stdDev;
        }

        /// Sets the standard deviation.
        ///
        /// @param stdDev the value to set
        public void setStdDev(Double stdDev) {
            this.stdDev = stdDev;
        }

        /// Returns the lower bound for uniform distributions.
        ///
        /// @return the value
        public Double getLower() {
            return lower;
        }

        /// Sets the lower bound for uniform distributions.
        ///
        /// @param lower the value to set
        public void setLower(Double lower) {
            this.lower = lower;
        }

        /// Returns the upper bound for uniform distributions.
        ///
        /// @return the value
        public Double getUpper() {
            return upper;
        }

        /// Sets the upper bound for uniform distributions.
        ///
        /// @param upper the value to set
        public void setUpper(Double upper) {
            this.upper = upper;
        }

        /// Returns the lower truncation bound.
        ///
        /// @return the value
        public Double getLowerBound() {
            return lowerBound;
        }

        /// Sets the lower truncation bound.
        ///
        /// @param lowerBound the value to set
        public void setLowerBound(Double lowerBound) {
            this.lowerBound = lowerBound;
        }

        /// Returns the upper truncation bound.
        ///
        /// @return the value
        public Double getUpperBound() {
            return upperBound;
        }

        /// Sets the upper truncation bound.
        ///
        /// @param upperBound the value to set
        public void setUpperBound(Double upperBound) {
            this.upperBound = upperBound;
        }

        /// Returns whether this component has truncation bounds.
        ///
        /// @return the value
        public boolean isTruncated() {
            return lowerBound != null && upperBound != null;
        }

        /// Converts this configuration to a ScalarModel.
        ///
        /// @return the corresponding ScalarModel
        /// @throws IllegalArgumentException if required fields are missing or type is unknown
        public ScalarModel toComponentModel() {
            String modelType = getType();

            switch (modelType) {
                case "normal":
                    Objects.requireNonNull(mean, "mean is required for normal");
                    Objects.requireNonNull(stdDev, "std_dev is required for normal");
                    if (isTruncated()) {
                        return new NormalScalarModel(mean, stdDev, lowerBound, upperBound);
                    } else {
                        return new NormalScalarModel(mean, stdDev);
                    }

                case "uniform":
                    Double lo = lower != null ? lower : lowerBound;
                    Double hi = upper != null ? upper : upperBound;
                    Objects.requireNonNull(lo, "lower is required for uniform");
                    Objects.requireNonNull(hi, "upper is required for uniform");
                    return new UniformScalarModel(lo, hi);

                case "beta":
                    Objects.requireNonNull(alpha, "alpha is required for beta");
                    Objects.requireNonNull(beta, "beta is required for beta");
                    double betaLower = lower != null ? lower : 0.0;
                    double betaUpper = upper != null ? upper : 1.0;
                    return new BetaScalarModel(alpha, beta, betaLower, betaUpper);

                case "gamma":
                    Objects.requireNonNull(shape, "shape is required for gamma");
                    Objects.requireNonNull(scale, "scale is required for gamma");
                    double gammaLocation = shift != null ? shift : 0.0;
                    return new GammaScalarModel(shape, scale, gammaLocation);

                case "inverse_gamma":
                    Objects.requireNonNull(shape, "shape is required for inverse_gamma");
                    Objects.requireNonNull(scale, "scale is required for inverse_gamma");
                    return new InverseGammaScalarModel(shape, scale);

                case "student_t":
                    Objects.requireNonNull(nu, "nu is required for student_t");
                    double tLocation = mu != null ? mu : 0.0;
                    double tScale = sigma != null ? sigma : 1.0;
                    return new StudentTScalarModel(nu, tLocation, tScale);

                case "pearson_iv":
                    Objects.requireNonNull(m, "m is required for pearson_iv");
                    Objects.requireNonNull(nu, "nu is required for pearson_iv");
                    Objects.requireNonNull(a, "a is required for pearson_iv");
                    Objects.requireNonNull(lambda, "lambda is required for pearson_iv");
                    return new PearsonIVScalarModel(m, nu, a, lambda);

                case "beta_prime":
                    Objects.requireNonNull(alpha, "alpha is required for beta_prime");
                    Objects.requireNonNull(beta, "beta is required for beta_prime");
                    double bpScale = scale != null ? scale : 1.0;
                    return new BetaPrimeScalarModel(alpha, beta, bpScale);

                case "empirical":
                    Objects.requireNonNull(bins, "bins is required for empirical");
                    Objects.requireNonNull(mean, "mean is required for empirical");
                    Objects.requireNonNull(stdDev, "std_dev is required for empirical");
                    // bins contains bin edges, create a simple uniform CDF
                    double[] cdf = new double[bins.length];
                    for (int i = 0; i < cdf.length; i++) {
                        cdf[i] = (double) (i + 1) / cdf.length;
                    }
                    return new EmpiricalScalarModel(bins, cdf, mean, stdDev);

                case "composite":
                    Objects.requireNonNull(subModels, "sub_models is required for composite");
                    Objects.requireNonNull(weights, "weights is required for composite");
                    if (subModels.length != weights.length) {
                        throw new IllegalArgumentException(
                            "sub_models and weights must have same length");
                    }
                    java.util.List<ScalarModel> components = new java.util.ArrayList<>();
                    for (ComponentConfig subConfig : subModels) {
                        components.add(subConfig.toComponentModel());
                    }
                    return new CompositeScalarModel(components, weights);

                default:
                    throw new IllegalArgumentException("Unknown component model type: " + modelType);
            }
        }

        /**
         * Creates a ComponentConfig from a ScalarModel.
         *
         * <p>For simple (1-component) CompositeScalarModels, this method unwraps
         * to the underlying model for backwards-compatible serialization. This
         * supports the unified model representation where all dimensions are
         * internally CompositeScalarModels, while maintaining the same JSON format.
         *
         * @param model the source model
         * @return the corresponding ComponentConfig
         */
        public static ComponentConfig fromComponentModel(ScalarModel model) {
            // Unwrap simple composites for backwards-compatible serialization
            if (model instanceof CompositeScalarModel composite && composite.isSimple()) {
                model = composite.unwrap();
            }

            ComponentConfig config = new ComponentConfig();
            config.type = model.getModelType();

            if (model instanceof NormalScalarModel normal) {
                config.mean = normal.getMean();
                config.stdDev = normal.getStdDev();
                if (normal.isTruncated()) {
                    config.lowerBound = normal.lower();
                    config.upperBound = normal.upper();
                }
            } else if (model instanceof UniformScalarModel uniform) {
                config.lower = uniform.getLower();
                config.upper = uniform.getUpper();
            } else if (model instanceof BetaScalarModel beta) {
                config.alpha = beta.getAlpha();
                config.beta = beta.getBeta();
                config.lower = beta.getLower();
                config.upper = beta.getUpper();
            } else if (model instanceof GammaScalarModel gamma) {
                config.shape = gamma.getShape();
                config.scale = gamma.getScale();
                config.shift = gamma.getLocation();
            } else if (model instanceof InverseGammaScalarModel invGamma) {
                config.shape = invGamma.getShape();
                config.scale = invGamma.getScale();
            } else if (model instanceof StudentTScalarModel studentT) {
                config.nu = studentT.getDegreesOfFreedom();
                config.mu = studentT.getLocation();
                config.sigma = studentT.getScale();
            } else if (model instanceof PearsonIVScalarModel pearson) {
                config.m = pearson.getM();
                config.nu = pearson.getNu();
                config.a = pearson.getA();
                config.lambda = pearson.getLambda();
            } else if (model instanceof BetaPrimeScalarModel betaPrime) {
                config.alpha = betaPrime.getAlpha();
                config.beta = betaPrime.getBeta();
                config.scale = betaPrime.getScale();
            } else if (model instanceof EmpiricalScalarModel empirical) {
                config.mean = empirical.getMean();
                config.stdDev = empirical.getStdDev();
                config.min = empirical.getMin();
                config.max = empirical.getMax();
                config.bins = empirical.getBinEdges();
            } else if (model instanceof CompositeScalarModel composite) {
                // Serialize multi-component composite models with their sub-models and weights
                ScalarModel[] scalarModels = composite.getScalarModels();
                double[] compositeWeights = composite.getWeights();
                config.subModels = new ComponentConfig[scalarModels.length];
                for (int i = 0; i < scalarModels.length; i++) {
                    config.subModels[i] = fromComponentModel(scalarModels[i]);
                }
                config.weights = compositeWeights;
            } else {
                throw new IllegalArgumentException("Unknown component model type: " + model.getClass().getName());
            }

            return config;
        }
    }

    /// Creates a new empty VectorSpaceModelConfig.
    public VectorSpaceModelConfig() {
    }

    /// Returns the unique vector count.
    /// @return the unique vectors
    public Long getUniqueVectors() {
        return uniqueVectors;
    }

    /// Sets the unique vector count.
    /// @param uniqueVectors the unique vectors
    public void setUniqueVectors(Long uniqueVectors) {
        this.uniqueVectors = uniqueVectors;
    }

    /// Returns the dimension count.
    /// @return the dimensions
    public Integer getDimensions() {
        return dimensions;
    }

    /// Sets the number of dimensions.
    ///
    /// @param dimensions the value to set
    public void setDimensions(Integer dimensions) {
        this.dimensions = dimensions;
    }

    /// Returns the mean.
    ///
    /// @return the value
    public Double getMean() {
        return mean;
    }

    /// Sets the mean.
    ///
    /// @param mean the value to set
    public void setMean(Double mean) {
        this.mean = mean;
    }

    /// Returns the standard deviation.
    ///
    /// @return the value
    public Double getStdDev() {
        return stdDev;
    }

    /// Sets the standard deviation.
    ///
    /// @param stdDev the value to set
    public void setStdDev(Double stdDev) {
        this.stdDev = stdDev;
    }

    /// Returns the lower truncation bound.
    ///
    /// @return the value
    public Double getLowerBound() {
        return lowerBound;
    }

    /// Sets the lower truncation bound.
    ///
    /// @param lowerBound the value to set
    public void setLowerBound(Double lowerBound) {
        this.lowerBound = lowerBound;
    }

    /// Returns the upper truncation bound.
    ///
    /// @return the value
    public Double getUpperBound() {
        return upperBound;
    }

    /// Sets the upper truncation bound.
    ///
    /// @param upperBound the value to set
    public void setUpperBound(Double upperBound) {
        this.upperBound = upperBound;
    }

    /// Returns the per-dimension component configurations.
    ///
    /// @return the value
    public ComponentConfig[] getComponents() {
        return components;
    }

    /// Sets the per-dimension component configurations.
    ///
    /// @param components the component configurations
    public void setComponents(ComponentConfig[] components) {
        this.components = components;
    }

    /// Returns whether this config uses per-dimension custom distributions.
    ///
    /// @return true if per-dimension components are configured
    public boolean hasPerDimensionComponents() {
        return components != null && components.length > 0;
    }

    /// Returns whether this config has truncation bounds.
    ///
    /// @return true if both lower and upper bounds are set
    public boolean isTruncated() {
        return lowerBound != null && upperBound != null;
    }

    /// Converts this configuration to a VectorSpaceModel.
    ///
    /// @return the corresponding VectorSpaceModel
    /// @throws IllegalArgumentException if required fields are missing or invalid
    public VectorSpaceModel toVectorSpaceModel() {
        Objects.requireNonNull(uniqueVectors, "unique_vectors is required");

        if (hasPerDimensionComponents()) {
            // Per-dimension configuration
            ScalarModel[] models = new ScalarModel[components.length];
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

    /// Creates a VectorSpaceModelConfig from a VectorSpaceModel.
    ///
    /// @param model the source model
    /// @return the corresponding configuration
    public static VectorSpaceModelConfig fromVectorSpaceModel(VectorSpaceModel model) {
        VectorSpaceModelConfig config = new VectorSpaceModelConfig();
        config.setUniqueVectors(model.uniqueVectors());

        ScalarModel[] scalarModels = model.scalarModels();

        // Check if all components are identical Normal (can use compact uniform format)
        boolean canUseUniform = model.isAllNormal();
        if (canUseUniform) {
            NormalScalarModel first = (NormalScalarModel) scalarModels[0];
            for (int i = 1; i < scalarModels.length; i++) {
                if (!scalarModels[i].equals(first)) {
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
        ComponentConfig[] components = new ComponentConfig[scalarModels.length];
        for (int i = 0; i < scalarModels.length; i++) {
            components[i] = ComponentConfig.fromComponentModel((ScalarModel) scalarModels[i]);
        }
        config.setComponents(components);

        return config;
    }

    /// Loads a VectorSpaceModelConfig from JSON.
    ///
    /// @param json the JSON string
    /// @return the deserialized config
    public static VectorSpaceModelConfig fromJson(String json) {
        return GSON.fromJson(json, VectorSpaceModelConfig.class);
    }

    /// Loads a VectorSpaceModelConfig from a Reader.
    ///
    /// @param reader the reader providing JSON
    /// @return the deserialized config
    public static VectorSpaceModelConfig fromJson(Reader reader) {
        return GSON.fromJson(reader, VectorSpaceModelConfig.class);
    }

    /// Serializes this configuration to JSON.
    ///
    /// @return the JSON string
    public String toJson() {
        return GSON.toJson(this);
    }

    /// Writes this configuration as JSON to a Writer.
    ///
    /// @param writer the target writer
    public void toJson(Writer writer) {
        GSON.toJson(this, writer);
    }

    /// Loads a VectorSpaceModel directly from a JSON file.
    ///
    /// @param path the file path
    /// @return the loaded model
    /// @throws IOException if reading fails
    public static VectorSpaceModel loadFromFile(Path path) throws IOException {
        try (Reader reader = Files.newBufferedReader(path)) {
            return load(reader);
        }
    }

    /// Loads a VectorSpaceModel from a Reader providing JSON.
    ///
    /// @param reader the reader providing JSON
    /// @return the loaded model
    public static VectorSpaceModel load(Reader reader) {
        VectorSpaceModelConfig config = fromJson(reader);
        return config.toVectorSpaceModel();
    }

    /// Loads a VectorSpaceModel from a JSON string.
    ///
    /// @param json the JSON string
    /// @return the loaded model
    public static VectorSpaceModel load(String json) {
        VectorSpaceModelConfig config = fromJson(json);
        return config.toVectorSpaceModel();
    }

    /// Saves a VectorSpaceModel to a JSON file.
    ///
    /// @param model the model to save
    /// @param path the target file path
    /// @throws IOException if writing fails
    public static void saveToFile(VectorSpaceModel model, Path path) throws IOException {
        VectorSpaceModelConfig config = fromVectorSpaceModel(model);
        try (Writer writer = Files.newBufferedWriter(path)) {
            config.toJson(writer);
        }
    }

    /// Saves a VectorSpaceModelConfig to a JSON file.
    ///
    /// @param config the config to save
    /// @param path the target file path
    /// @throws IOException if writing fails
    public static void saveToFile(VectorSpaceModelConfig config, Path path) throws IOException {
        try (Writer writer = Files.newBufferedWriter(path)) {
            config.toJson(writer);
        }
    }

    /// Saves a VectorSpaceModel to a Writer as JSON.
    ///
    /// @param model the model to save
    /// @param writer the target writer
    public static void save(VectorSpaceModel model, Writer writer) {
        VectorSpaceModelConfig config = fromVectorSpaceModel(model);
        config.toJson(writer);
    }

    @Override
    public String toString() {
        return toJson();
    }
}
