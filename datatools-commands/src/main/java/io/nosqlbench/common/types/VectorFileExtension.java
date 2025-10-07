package io.nosqlbench.common.types;

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


import io.nosqlbench.nbdatatools.api.services.FileType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Enum representing the default file extensions for vector files
 * used in Merkle tree creation and other operations.
 */
public enum VectorFileExtension {
    IVEC(new String[]{".ivec", ".ivecs"}, FileType.xvec, int[].class),
    FVEC(new String[]{".fvec", ".fvecs"}, FileType.xvec, float[].class),
    BVEC(new String[]{".bvec", ".bvecs"}, FileType.xvec, byte[].class),
    HDF5(new String[]{".hdf5"}, FileType.xvec, float[].class),
    PARQUET(new String[]{".parquet"}, FileType.parquet, float[].class),
    CSV(new String[]{".csv"}, FileType.csv, float[].class);

    private final String[] extensions;
    private final FileType fileType;
    private final Class<?> dataType;

    VectorFileExtension(String[] extensions, FileType fileType, Class<?> dataType) {
        this.extensions = extensions;
        this.fileType = fileType;
        this.dataType = dataType;
    }

    /**
     * Get the primary file extension string (the first one in the array).
     *
     * @return The primary file extension string including the dot prefix
     */
    public String getExtension() {
        return extensions[0];
    }

    /**
     * Get all file extension strings for this enum value.
     *
     * @return Array of file extension strings including the dot prefix
     */
    public String[] getExtensions() {
        return extensions;
    }

    /**
     * Get the file type associated with this extension.
     *
     * @return The file type
     */
    public FileType getFileType() {
        return fileType;
    }

    /**
     * Get the data type associated with this extension.
     *
     * @return The data type class
     */
    public Class<?> getDataType() {
        return dataType;
    }

    /**
     * Get a set of all the extension strings from all enum values.
     *
     * @return A set containing all the extension strings
     */
    public static Set<String> getAllExtensions() {
        return Arrays.stream(values())
                .flatMap(e -> Arrays.stream(e.getExtensions()))
                .collect(Collectors.toSet());
    }

    /**
     * Find a VectorFileExtension by its extension string.
     *
     * @param extension The extension string (with or without the dot prefix)
     * @return The matching VectorFileExtension, or null if not found
     */
    public static VectorFileExtension fromExtension(String extension) {
        // Ensure the extension has a dot prefix
        String normalizedExtension = extension.startsWith(".") ? extension : "." + extension;

        // Find the matching enum value by checking all extensions for each enum
        return Arrays.stream(values())
                .filter(e -> Arrays.stream(e.getExtensions())
                        .anyMatch(ext -> ext.equalsIgnoreCase(normalizedExtension)))
                .findFirst()
                .orElse(null);
    }
}
