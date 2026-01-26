/*
 * Copyright (c) nosqlbench
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.nosqlbench.command.common;

import java.util.Optional;
import java.util.regex.Pattern;

/// Parsed dataset profile facet spec with separator information.
public final class DatasetProfileFacetSpec {

    public enum Separator {
        COLON(':'),
        DOT('.');

        private final char symbol;

        Separator(char symbol) {
            this.symbol = symbol;
        }

        public char symbol() {
            return symbol;
        }
    }

    private final String datasetRef;
    private final String profileName;
    private final String facetName;
    private final Separator separator;

    private DatasetProfileFacetSpec(String datasetRef, String profileName,
                                    String facetName, Separator separator) {
        this.datasetRef = datasetRef;
        this.profileName = profileName;
        this.facetName = facetName;
        this.separator = separator;
    }

    public String datasetRef() {
        return datasetRef;
    }

    public String profileName() {
        return profileName;
    }

    public String facetName() {
        return facetName;
    }

    public Separator separator() {
        return separator;
    }

    public static Optional<DatasetProfileFacetSpec> tryParse(String spec) {
        if (spec == null) {
            return Optional.empty();
        }
        String trimmed = spec.trim();
        if (trimmed.isEmpty()) {
            return Optional.empty();
        }
        Optional<Separator> separator = detectSeparator(trimmed);
        if (separator.isEmpty()) {
            return Optional.empty();
        }
        String[] parts = split(trimmed, separator.get());
        if (parts.length != 3) {
            return Optional.empty();
        }
        String dataset = parts[0].trim();
        String profile = parts[1].trim();
        String facet = parts[2].trim();
        if (dataset.isEmpty() || profile.isEmpty() || facet.isEmpty()) {
            return Optional.empty();
        }
        if (dataset.contains(".")) {
            return Optional.empty();
        }
        return Optional.of(new DatasetProfileFacetSpec(dataset, profile, facet, separator.get()));
    }

    public static DatasetProfileFacetSpec parseRequired(String spec, String originalSpec) {
        if (spec == null) {
            throw new IllegalArgumentException("Facet spec must not be null");
        }
        String trimmed = spec.trim();
        Optional<Separator> separator = detectSeparator(trimmed);
        if (separator.isEmpty()) {
            throw invalidFacetSpec(originalSpec);
        }
        String[] parts = split(trimmed, separator.get());
        if (parts.length != 3) {
            throw invalidFacetSpec(originalSpec);
        }
        String dataset = parts[0].trim();
        String profile = parts[1].trim();
        String facet = parts[2].trim();
        if (dataset.isEmpty() || profile.isEmpty() || facet.isEmpty()) {
            throw new IllegalArgumentException(
                "Facet spec components must not be empty, got: " + originalSpec);
        }
        if (dataset.contains(".")) {
            throw new IllegalArgumentException(
                "Dataset names must not contain '.', got: '" + dataset + "'");
        }
        return new DatasetProfileFacetSpec(dataset, profile, facet, separator.get());
    }

    private static IllegalArgumentException invalidFacetSpec(String originalSpec) {
        return new IllegalArgumentException(
            "Facet spec requires format '<ref>.<profile>.<facet>' or '<ref>:<profile>:<facet>', got: " + originalSpec);
    }

    private static Optional<Separator> detectSeparator(String spec) {
        int colonCount = countChar(spec, ':');
        if (colonCount == 2) {
            return Optional.of(Separator.COLON);
        }
        if (colonCount != 0) {
            return Optional.empty();
        }
        int dotCount = countChar(spec, '.');
        if (dotCount == 2) {
            return Optional.of(Separator.DOT);
        }
        return Optional.empty();
    }

    private static int countChar(String spec, char needle) {
        int count = 0;
        for (int i = 0; i < spec.length(); i++) {
            if (spec.charAt(i) == needle) {
                count++;
            }
        }
        return count;
    }

    private static String[] split(String spec, Separator separator) {
        String sep = Pattern.quote(String.valueOf(separator.symbol()));
        return spec.split(sep, 3);
    }
}
