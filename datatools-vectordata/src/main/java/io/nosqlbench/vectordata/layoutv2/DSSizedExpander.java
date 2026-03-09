package io.nosqlbench.vectordata.layoutv2;

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


import io.nosqlbench.vectordata.spec.datasets.types.TestDataKind;
import io.nosqlbench.vectordata.utils.UnitConversions;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/// Expands `sized` profile specifications into concrete profile entries.
///
/// The `sized` key in a dataset.yaml `profiles` section enables declarative
/// generation of multiple profiles from range specifications. Two forms are
/// supported:
///
/// **Simple list form** — profiles inherit all views from the default profile:
/// ```yaml
/// sized: [10m, 20m, 100m..400m/100m]
/// ```
///
/// **Object form with facets template** — profiles use the template with
/// `${range}` and `${profile}` variable substitution:
/// ```yaml
/// sized:
///   ranges: ["0m..400m/10m"]
///   facets:
///     base_vectors: "profiles/base/base_vectors.hvec:${range}"
///     query_vectors: profiles/base/query_vectors.hvec
/// ```
///
/// ## Range specification forms
///
/// | Form | Example | Expansion |
/// |------|---------|-----------|
/// | Simple value | `10m` | One profile with base_count 10,000,000 |
/// | Linear range (step) | `100m..400m/100m` | Profiles at each step: 100m, 200m, 300m, 400m |
/// | Linear range (count) | `0m..400m/10` | 10 equal divisions (bare number = count) |
/// | Fibonacci | `fib:1m..400m` | Fibonacci multiples of start within range |
/// | Geometric | `mul:1m..400m/2` | Compound by factor (doubling) |
///
/// Generated profiles are sorted smallest to largest.
///
/// @see DSProfileGroup
/// @see DSProfile
public class DSSizedExpander {

    /// Pattern for range specs: `start..end/step`
    private static final Pattern RANGE_SPEC = Pattern.compile(
        "^(?<prefix>fib:|mul:)?(?<start>[\\d_]+[a-zA-Z]*)\\.\\.(?<end>[\\d_]+[a-zA-Z]*)(?:/(?<divisor>[\\d_]+[a-zA-Z]*))?$"
    );

    /// Private constructor — utility class
    private DSSizedExpander() {
    }

    /// Expands a `sized` specification into profile entries and adds them to the profile group.
    ///
    /// @param sizedData The raw `sized` value from the YAML profiles map — either a List or a Map
    /// @param defaultProfile The default profile to inherit views and maxk from (may be null)
    /// @param profileGroup The profile group to add expanded profiles into
    public static void expand(Object sizedData, DSProfile defaultProfile, DSProfileGroup profileGroup) {
        List<String> rangeSpecs;
        Map<String, String> facetTemplates = null;

        if (sizedData instanceof List<?>) {
            // Simple list form: sized: [10m, 20m, 100m..400m/100m]
            rangeSpecs = new ArrayList<>();
            for (Object item : (List<?>) sizedData) {
                rangeSpecs.add(item.toString());
            }
        } else if (sizedData instanceof Map<?, ?>) {
            // Object form: sized: { ranges: [...], facets: { ... } }
            Map<?, ?> sizedMap = (Map<?, ?>) sizedData;
            Object rangesObj = sizedMap.get("ranges");
            if (rangesObj instanceof List<?>) {
                rangeSpecs = new ArrayList<>();
                for (Object item : (List<?>) rangesObj) {
                    rangeSpecs.add(item.toString());
                }
            } else if (rangesObj instanceof String) {
                rangeSpecs = List.of((String) rangesObj);
            } else {
                throw new RuntimeException("sized.ranges must be a list of range specs, got: " + rangesObj);
            }
            Object facetsObj = sizedMap.get("facets");
            if (facetsObj instanceof Map<?, ?>) {
                facetTemplates = new LinkedHashMap<>();
                for (Map.Entry<?, ?> entry : ((Map<?, ?>) facetsObj).entrySet()) {
                    String key = entry.getKey().toString();
                    // Normalize facet key alias
                    String canonicalKey = TestDataKind.fromOptionalString(key)
                        .map(TestDataKind::name)
                        .orElse(key);
                    facetTemplates.put(canonicalKey, entry.getValue().toString());
                }
            }
        } else {
            throw new RuntimeException("sized must be a list or map, got: " + sizedData.getClass().getSimpleName());
        }

        // Expand all range specs into size values, sorted ascending
        TreeMap<Long, String> sizePoints = new TreeMap<>();
        for (String spec : rangeSpecs) {
            for (SizedEntry entry : parseRangeSpec(spec)) {
                sizePoints.put(entry.baseCount, entry.name);
            }
        }

        // Generate a profile for each size point
        for (Map.Entry<Long, String> entry : sizePoints.entrySet()) {
            long baseCount = entry.getKey();
            String profileName = entry.getValue();

            if (baseCount <= 0) {
                continue; // skip zero-count profiles
            }

            DSProfile profile = new DSProfile();
            profile.setName(profileName);
            profile.setBaseCount(baseCount);

            if (facetTemplates != null) {
                // Object form: apply facet templates with variable substitution
                String rangeWindow = "[0.." + baseCount + ")";
                for (Map.Entry<String, String> facetEntry : facetTemplates.entrySet()) {
                    String facetKey = facetEntry.getKey();
                    String template = facetEntry.getValue();
                    String resolved = template
                        .replace("${range}", rangeWindow)
                        .replace("${profile}", profileName);
                    DSSource source = DSSource.fromData(resolved);
                    DSView view = new DSView(facetKey, source, source.getWindow());
                    profile.put(facetKey, view);
                }
            }

            // Inherit from default profile
            if (defaultProfile != null) {
                defaultProfile.forEach((viewName, viewDef) -> profile.putIfAbsent(viewName, viewDef));
                if (profile.getMaxk() == null && defaultProfile.getMaxk() != null) {
                    profile.setMaxk(defaultProfile.getMaxk());
                }
                // base_count is never inherited
            }

            profileGroup.addProfile(profileName, profile);
        }
    }

    /// Parses a single range specification string into a list of sized entries.
    ///
    /// @param spec A range spec like "10m", "100m..400m/100m", "fib:1m..400m", or "0m..400m/10"
    /// @return A list of sized entries, each with a name and count
    static List<SizedEntry> parseRangeSpec(String spec) {
        spec = spec.trim();
        Matcher m = RANGE_SPEC.matcher(spec);

        if (!m.matches()) {
            // Simple single value: "10m"
            long count = parseCount(spec);
            return List.of(new SizedEntry(formatCount(count), count));
        }

        String prefix = m.group("prefix");
        long start = parseCount(m.group("start"));
        long end = parseCount(m.group("end"));
        String divisorStr = m.group("divisor");

        if (prefix != null && prefix.equals("fib:")) {
            return expandFibonacci(start, end);
        }
        if (prefix != null && prefix.equals("mul:")) {
            long factor = divisorStr != null ? parseCount(divisorStr) : 2;
            return expandGeometric(start, end, factor);
        }

        // Linear range
        if (divisorStr == null) {
            // No divisor — just start and end as two entries
            List<SizedEntry> entries = new ArrayList<>();
            entries.add(new SizedEntry(formatCount(start), start));
            if (end != start) {
                entries.add(new SizedEntry(formatCount(end), end));
            }
            return entries;
        }

        long divisor = parseCountRaw(divisorStr);
        boolean isStep = hasSuffix(divisorStr);

        if (isStep) {
            // Linear range with absolute step: 100m..400m/100m
            return expandLinearStep(start, end, divisor);
        } else {
            // Linear range with count: 0m..400m/10
            return expandLinearCount(start, end, divisor);
        }
    }

    /// Expands a linear range with absolute step size.
    ///
    /// For `100m..400m/100m`, generates entries at 100m, 200m, 300m, 400m.
    /// When start is 0, the first step value is used as the starting point.
    ///
    /// @param start The start of the range (inclusive)
    /// @param end The end of the range (inclusive)
    /// @param step The step size
    /// @return Expanded entries sorted ascending
    private static List<SizedEntry> expandLinearStep(long start, long end, long step) {
        List<SizedEntry> entries = new ArrayList<>();
        long current = start == 0 ? step : start;
        while (current <= end) {
            entries.add(new SizedEntry(formatCount(current), current));
            current += step;
        }
        return entries;
    }

    /// Expands a linear range divided into equal parts.
    ///
    /// For `0m..400m/10`, generates 10 equally-spaced entries
    /// from the first division to the end.
    ///
    /// @param start The start of the range
    /// @param end The end of the range
    /// @param count The number of divisions
    /// @return Expanded entries sorted ascending
    private static List<SizedEntry> expandLinearCount(long start, long end, long count) {
        List<SizedEntry> entries = new ArrayList<>();
        if (count <= 0) {
            return entries;
        }
        long step = (end - start) / count;
        if (step <= 0) {
            return entries;
        }
        for (long i = 1; i <= count; i++) {
            long value = start + step * i;
            entries.add(new SizedEntry(formatCount(value), value));
        }
        return entries;
    }

    /// Expands Fibonacci multiples of start within the given range.
    ///
    /// Generates entries at `start * fib(n)` for Fibonacci numbers
    /// where the result is within `[start, end]`.
    ///
    /// @param start The base value and starting point
    /// @param end The upper bound (inclusive)
    /// @return Expanded entries sorted ascending
    private static List<SizedEntry> expandFibonacci(long start, long end) {
        List<SizedEntry> entries = new ArrayList<>();
        long a = 1;
        long b = 1;
        while (start * a <= end) {
            long value = start * a;
            if (value >= start) {
                entries.add(new SizedEntry(formatCount(value), value));
            }
            long next = a + b;
            a = b;
            b = next;
        }
        return entries;
    }

    /// Expands geometric (multiplicative) progression within the given range.
    ///
    /// Generates entries by repeatedly multiplying by the factor,
    /// starting at `start` and going up to `end`.
    ///
    /// @param start The starting value
    /// @param end The upper bound (inclusive)
    /// @param factor The multiplication factor
    /// @return Expanded entries sorted ascending
    private static List<SizedEntry> expandGeometric(long start, long end, long factor) {
        List<SizedEntry> entries = new ArrayList<>();
        long current = start;
        while (current <= end) {
            entries.add(new SizedEntry(formatCount(current), current));
            long next = current * factor;
            if (next <= current) {
                break; // overflow or no progress
            }
            current = next;
        }
        return entries;
    }

    /// Parses a count string with SI suffixes into its numeric value.
    ///
    /// @param value The count string, e.g. "10m", "400m", "1000"
    /// @return The numeric count value
    private static long parseCount(String value) {
        String normalized = value.trim().replaceAll("_", "");
        return UnitConversions.longCountFor(normalized)
            .orElseThrow(() -> new RuntimeException("invalid sized count: " + value));
    }

    /// Parses a count string raw — returns the numeric value without normalizing.
    /// Used to get the divisor value which may or may not have a suffix.
    ///
    /// @param value The count string
    /// @return The numeric count value
    private static long parseCountRaw(String value) {
        return parseCount(value);
    }

    /// Checks whether a count string has a letter suffix (like m, k, etc.)
    /// to distinguish step values from count values in range specs.
    ///
    /// @param value The count string
    /// @return true if the string ends with an alphabetic suffix
    private static boolean hasSuffix(String value) {
        String trimmed = value.trim().replaceAll("_", "");
        return !trimmed.isEmpty() && Character.isLetter(trimmed.charAt(trimmed.length() - 1));
    }

    /// Formats a numeric count back to a human-friendly suffix notation.
    ///
    /// Uses the largest SI suffix that divides evenly:
    /// - 10000000 becomes "10m"
    /// - 1000000000 becomes "1b"
    /// - 1500000 becomes "1500000" (no clean suffix)
    ///
    /// @param count The numeric count
    /// @return A human-friendly string representation
    static String formatCount(long count) {
        if (count <= 0) {
            return String.valueOf(count);
        }
        // Try suffixes from largest to smallest
        if (count >= 1_000_000_000_000L && count % 1_000_000_000_000L == 0) {
            return (count / 1_000_000_000_000L) + "t";
        }
        if (count >= 1_000_000_000L && count % 1_000_000_000L == 0) {
            return (count / 1_000_000_000L) + "b";
        }
        if (count >= 1_000_000L && count % 1_000_000L == 0) {
            return (count / 1_000_000L) + "m";
        }
        if (count >= 1_000L && count % 1_000L == 0) {
            return (count / 1_000L) + "k";
        }
        return String.valueOf(count);
    }

    /// A single expanded size point with its profile name and base vector count.
    static class SizedEntry {
        private final String name;
        private final long baseCount;

        /// Creates a new SizedEntry.
        /// @param name The profile name (e.g. "10m", "100m")
        /// @param baseCount The base vector count for this profile
        SizedEntry(String name, long baseCount) {
            this.name = name;
            this.baseCount = baseCount;
        }

        /// Gets the profile name.
        /// @return The profile name
        String name() {
            return name;
        }

        /// Gets the base vector count.
        /// @return The base vector count
        long baseCount() {
            return baseCount;
        }
    }
}
