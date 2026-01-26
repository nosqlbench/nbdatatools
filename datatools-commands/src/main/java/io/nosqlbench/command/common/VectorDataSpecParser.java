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

import picocli.CommandLine;

/// Parses a VectorDataSpec with an optional bracketed range suffix.
public final class VectorDataSpecParser {

    private VectorDataSpecParser() {
    }

    public record Parsed(VectorDataSpec spec, RangeOption.Range range, String rangeSpec) {
        public boolean hasRange() {
            return range != null;
        }
    }

    public static Parsed parse(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            throw new CommandLine.TypeConversionException("Vector data spec cannot be empty");
        }

        String trimmed = raw.trim();
        String specPart = trimmed;
        RangeOption.Range range = null;
        String rangeSpec = null;

        if (trimmed.endsWith("]") || trimmed.endsWith(")")) {
            int openIndex = Math.max(trimmed.lastIndexOf('['), trimmed.lastIndexOf('('));
            if (openIndex > 0) {
                String candidate = trimmed.substring(openIndex);
                try {
                    range = new RangeOption.RangeConverter().convert(candidate);
                } catch (RuntimeException e) {
                    throw new CommandLine.TypeConversionException(
                        "Invalid range specification: " + candidate + " (" + e.getMessage() + ")");
                }
                rangeSpec = candidate;
                specPart = trimmed.substring(0, openIndex).trim();
                if (specPart.isEmpty()) {
                    throw new CommandLine.TypeConversionException("Vector data spec cannot be empty");
                }
            }
        }

        try {
            VectorDataSpec spec = new VectorDataSpecConverter().convert(specPart);
            return new Parsed(spec, range, rangeSpec);
        } catch (CommandLine.TypeConversionException e) {
            throw e;
        } catch (Exception e) {
            throw new CommandLine.TypeConversionException(
                "Invalid vector data spec: " + specPart + " (" + e.getMessage() + ")");
        }
    }
}
