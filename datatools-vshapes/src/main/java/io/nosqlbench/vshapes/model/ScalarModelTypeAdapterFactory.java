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
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.internal.Streams;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

/**
 * GSON TypeAdapterFactory for polymorphic {@link ScalarModel} serialization.
 *
 * <p>This factory provides type-discriminated JSON serialization for ScalarModel
 * implementations, ensuring each model type has:
 * <ul>
 *   <li>A "type" field in the JSON output for identification</li>
 *   <li>Only type-specific fields (no null/extraneous fields)</li>
 *   <li>Type validation during deserialization</li>
 * </ul>
 *
 * <h2>Architecture</h2>
 *
 * <pre>{@code
 * ┌─────────────────────────────────────────────────────────────────────────┐
 * │                    POLYMORPHIC SERIALIZATION FLOW                       │
 * └─────────────────────────────────────────────────────────────────────────┘
 *
 *  SERIALIZE                              DESERIALIZE
 *  ─────────                              ───────────
 *  NormalScalarModel                      { "type": "normal", ... }
 *        │                                         │
 *        ▼                                         ▼
 *  1. Get @ModelType("normal")            1. Read "type" field
 *  2. Serialize type-specific fields      2. Lookup registered class
 *  3. Add "type" field to JSON            3. Validate type matches
 *        │                                4. Deserialize with delegate
 *        ▼                                         │
 *  {                                               ▼
 *    "type": "normal",                    NormalScalarModel
 *    "mean": 0.0,
 *    "std_dev": 1.0
 *  }
 * }</pre>
 *
 * <h2>Registration</h2>
 *
 * <p>Model types are registered via the {@link #registerType} method. Each
 * registered type must have a unique type name from its {@link ModelType}
 * annotation.
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * ScalarModelTypeAdapterFactory factory = ScalarModelTypeAdapterFactory.create();
 * Gson gson = new GsonBuilder()
 *     .registerTypeAdapterFactory(factory)
 *     .create();
 *
 * // Serialize
 * ScalarModel model = new NormalScalarModel(0.0, 1.0);
 * String json = gson.toJson(model, ScalarModel.class);
 * // {"type":"normal","mean":0.0,"std_dev":1.0}
 *
 * // Deserialize
 * ScalarModel restored = gson.fromJson(json, ScalarModel.class);
 * }</pre>
 *
 * @see ModelType
 * @see ScalarModel
 */
public final class ScalarModelTypeAdapterFactory implements TypeAdapterFactory {

    private static final String TYPE_FIELD = "type";

    private final Map<String, Class<? extends ScalarModel>> typeToClass = new HashMap<>();
    private final Map<Class<? extends ScalarModel>, String> classToType = new HashMap<>();

    private ScalarModelTypeAdapterFactory() {
    }

    /**
     * Creates a new factory with all standard ScalarModel types registered.
     *
     * @return a configured factory
     */
    public static ScalarModelTypeAdapterFactory create() {
        ScalarModelTypeAdapterFactory factory = new ScalarModelTypeAdapterFactory();

        // Register all known ScalarModel implementations
        factory.registerType(NormalScalarModel.class);
        factory.registerType(UniformScalarModel.class);
        factory.registerType(BetaScalarModel.class);
        factory.registerType(GammaScalarModel.class);
        factory.registerType(InverseGammaScalarModel.class);
        factory.registerType(StudentTScalarModel.class);
        factory.registerType(PearsonIVScalarModel.class);
        factory.registerType(BetaPrimeScalarModel.class);
        factory.registerType(EmpiricalScalarModel.class);
        factory.registerType(CompositeScalarModel.class);

        return factory;
    }

    /**
     * Registers a ScalarModel implementation type.
     *
     * <p>The type name is extracted from the {@link ModelType} annotation
     * on the class, or falls back to {@link ScalarModel#getModelType()}.
     *
     * @param modelClass the model class to register
     * @throws IllegalArgumentException if the class has no ModelType annotation
     *         or if the type name is already registered
     */
    public void registerType(Class<? extends ScalarModel> modelClass) {
        ModelType annotation = modelClass.getAnnotation(ModelType.class);
        String typeName;

        if (annotation != null) {
            typeName = annotation.value();
        } else {
            // Fallback: try to get type from static field or instantiate
            try {
                java.lang.reflect.Field field = modelClass.getField("MODEL_TYPE");
                typeName = (String) field.get(null);
            } catch (Exception e) {
                throw new IllegalArgumentException(
                    "Class " + modelClass.getName() + " has no @ModelType annotation " +
                    "and no MODEL_TYPE static field");
            }
        }

        if (typeToClass.containsKey(typeName)) {
            throw new IllegalArgumentException(
                "Type '" + typeName + "' is already registered to " +
                typeToClass.get(typeName).getName());
        }

        typeToClass.put(typeName, modelClass);
        classToType.put(modelClass, typeName);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
        // Only handle ScalarModel and its subclasses
        if (!ScalarModel.class.isAssignableFrom(type.getRawType())) {
            return null;
        }

        return new TypeAdapter<T>() {
            @Override
            public void write(JsonWriter out, T value) throws IOException {
                if (value == null) {
                    out.nullValue();
                    return;
                }

                ScalarModel model = (ScalarModel) value;
                String typeName = classToType.get(value.getClass());

                if (typeName == null) {
                    // Fallback to getModelType() method
                    typeName = model.getModelType();
                }

                // Get the delegate adapter for the ACTUAL concrete type
                TypeAdapter<T> concreteDelegate = (TypeAdapter<T>) gson.getDelegateAdapter(
                    ScalarModelTypeAdapterFactory.this,
                    TypeToken.get(value.getClass())
                );

                // Serialize to intermediate string with lenient mode to handle Infinity/NaN
                StringWriter stringWriter = new StringWriter();
                JsonWriter lenientWriter = new JsonWriter(stringWriter);
                lenientWriter.setLenient(true);
                concreteDelegate.write(lenientWriter, value);
                lenientWriter.close();

                // Parse back to JsonElement
                JsonElement tree = JsonParser.parseString(stringWriter.toString());

                if (tree.isJsonObject()) {
                    JsonObject obj = tree.getAsJsonObject();

                    // Create new object with "type" first
                    JsonObject result = new JsonObject();
                    result.addProperty(TYPE_FIELD, typeName);

                    // Copy all other properties
                    for (Map.Entry<String, JsonElement> entry : obj.entrySet()) {
                        if (!TYPE_FIELD.equals(entry.getKey())) {
                            result.add(entry.getKey(), entry.getValue());
                        }
                    }

                    // Write with lenient mode enabled for Infinity/NaN support
                    boolean wasLenient = out.isLenient();
                    out.setLenient(true);
                    try {
                        Streams.write(result, out);
                    } finally {
                        out.setLenient(wasLenient);
                    }
                } else {
                    // Write directly with lenient mode
                    boolean wasLenient = out.isLenient();
                    out.setLenient(true);
                    try {
                        concreteDelegate.write(out, value);
                    } finally {
                        out.setLenient(wasLenient);
                    }
                }
            }

            @Override
            public T read(JsonReader in) throws IOException {
                JsonElement element = JsonParser.parseReader(in);

                if (element.isJsonNull()) {
                    return null;
                }

                JsonObject obj = element.getAsJsonObject();

                // Extract type field
                if (!obj.has(TYPE_FIELD)) {
                    throw new IllegalArgumentException(
                        "Missing '" + TYPE_FIELD + "' field in JSON: " + obj);
                }

                String typeName = obj.get(TYPE_FIELD).getAsString();
                Class<? extends ScalarModel> targetClass = typeToClass.get(typeName);

                if (targetClass == null) {
                    throw new IllegalArgumentException(
                        "Unknown model type: '" + typeName + "'. " +
                        "Known types: " + typeToClass.keySet());
                }

                // Deserialize using the concrete type's adapter with lenient mode
                // (fromJsonTree doesn't support lenient mode, so we serialize to string and re-parse)
                TypeAdapter<? extends ScalarModel> targetAdapter =
                    gson.getDelegateAdapter(ScalarModelTypeAdapterFactory.this,
                                            TypeToken.get(targetClass));

                String jsonString = obj.toString();
                JsonReader lenientReader = new JsonReader(new StringReader(jsonString));
                lenientReader.setLenient(true);
                return (T) targetAdapter.read(lenientReader);
            }
        };
    }

    /**
     * Returns the type name for a given model class.
     *
     * @param modelClass the model class
     * @return the type name, or null if not registered
     */
    public String getTypeName(Class<? extends ScalarModel> modelClass) {
        return classToType.get(modelClass);
    }

    /**
     * Returns the model class for a given type name.
     *
     * @param typeName the type name
     * @return the model class, or null if not registered
     */
    public Class<? extends ScalarModel> getModelClass(String typeName) {
        return typeToClass.get(typeName);
    }
}
