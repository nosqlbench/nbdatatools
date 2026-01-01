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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Describes the shape and configuration of a vector dataspace.
 *
 * <h2>Purpose</h2>
 *
 * <p>This record encapsulates the fundamental properties of a vector dataset:
 * <ul>
 *   <li><b>cardinality</b> - The total number of vectors in the dataset</li>
 *   <li><b>dimensionality</b> - The number of dimensions per vector</li>
 *   <li><b>parameters</b> - Additional configuration/metadata as key-value pairs</li>
 * </ul>
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * // Basic shape
 * DataspaceShape shape = new DataspaceShape(1_000_000, 128);
 *
 * // With parameters
 * DataspaceShape shape = new DataspaceShape(1_000_000, 128, Map.of(
 *     "source", "glove-100",
 *     "normalized", true
 * ));
 *
 * // Access properties
 * long n = shape.cardinality();
 * int d = shape.dimensionality();
 * boolean normalized = shape.getParameter("normalized", Boolean.class, false);
 * }</pre>
 *
 * @param cardinality the number of vectors in the dataspace
 * @param dimensionality the number of dimensions per vector
 * @param parameters additional configuration parameters
 */
public record DataspaceShape(
    long cardinality,
    int dimensionality,
    Map<String, Object> parameters
) {

    /**
     * Creates a DataspaceShape with validation.
     */
    public DataspaceShape {
        if (cardinality < 0) {
            throw new IllegalArgumentException("cardinality must be non-negative, got: " + cardinality);
        }
        if (dimensionality <= 0) {
            throw new IllegalArgumentException("dimensionality must be positive, got: " + dimensionality);
        }
        parameters = parameters == null
            ? Collections.emptyMap()
            : Collections.unmodifiableMap(new HashMap<>(parameters));
    }

    /**
     * Creates a DataspaceShape with no additional parameters.
     *
     * @param cardinality the number of vectors
     * @param dimensionality the number of dimensions per vector
     */
    public DataspaceShape(long cardinality, int dimensionality) {
        this(cardinality, dimensionality, Collections.emptyMap());
    }

    /**
     * Gets a typed parameter value with a default.
     *
     * @param key the parameter key
     * @param type the expected type
     * @param defaultValue the default if not present or wrong type
     * @param <T> the parameter type
     * @return the parameter value or default
     */
    @SuppressWarnings("unchecked")
    public <T> T getParameter(String key, Class<T> type, T defaultValue) {
        Object value = parameters.get(key);
        if (value != null && type.isAssignableFrom(value.getClass())) {
            return (T) value;
        }
        return defaultValue;
    }

    /**
     * Gets a string parameter with a default.
     */
    public String getStringParameter(String key, String defaultValue) {
        return getParameter(key, String.class, defaultValue);
    }

    /**
     * Gets an integer parameter with a default.
     */
    public int getIntParameter(String key, int defaultValue) {
        Object value = parameters.get(key);
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return defaultValue;
    }

    /**
     * Gets a boolean parameter with a default.
     */
    public boolean getBooleanParameter(String key, boolean defaultValue) {
        return getParameter(key, Boolean.class, defaultValue);
    }

    /**
     * Checks if a parameter exists.
     */
    public boolean hasParameter(String key) {
        return parameters.containsKey(key);
    }

    /**
     * Creates a new DataspaceShape with an additional parameter.
     *
     * @param key the parameter key
     * @param value the parameter value
     * @return a new DataspaceShape with the added parameter
     */
    public DataspaceShape withParameter(String key, Object value) {
        Objects.requireNonNull(key, "key cannot be null");
        Map<String, Object> newParams = new HashMap<>(parameters);
        newParams.put(key, value);
        return new DataspaceShape(cardinality, dimensionality, newParams);
    }

    /**
     * Creates a new DataspaceShape with additional parameters.
     *
     * @param additionalParams the parameters to add
     * @return a new DataspaceShape with the added parameters
     */
    public DataspaceShape withParameters(Map<String, Object> additionalParams) {
        if (additionalParams == null || additionalParams.isEmpty()) {
            return this;
        }
        Map<String, Object> newParams = new HashMap<>(parameters);
        newParams.putAll(additionalParams);
        return new DataspaceShape(cardinality, dimensionality, newParams);
    }

    @Override
    public String toString() {
        return String.format("DataspaceShape[cardinality=%d, dimensionality=%d, parameters=%s]",
            cardinality, dimensionality, parameters);
    }
}
