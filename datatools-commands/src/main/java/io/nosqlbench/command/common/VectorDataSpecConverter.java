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

/// Picocli type converter for VectorDataSpec.
///
/// This converter enables automatic parsing of vector data specifications
/// from command line arguments. Register it with a command or parameter:
///
/// ```java
/// @CommandLine.Parameters(converter = VectorDataSpecConverter.class)
/// private VectorDataSpec vectors;
///
/// // Or for an option:
/// @CommandLine.Option(names = {"--vectors"}, converter = VectorDataSpecConverter.class)
/// private VectorDataSpec vectors;
/// ```
public class VectorDataSpecConverter implements CommandLine.ITypeConverter<VectorDataSpec> {

    /// Creates a new VectorDataSpecConverter instance.
    public VectorDataSpecConverter() {
    }

    @Override
    public VectorDataSpec convert(String value) throws Exception {
        try {
            return VectorDataSpec.parse(value);
        } catch (VectorDataSpec.IncompleteDatasetSpecException e) {
            // Provide helpful error for missing facet component
            throw new CommandLine.TypeConversionException(e.getMessage());
        } catch (VectorDataSpec.AmbiguousDatasetBaseException e) {
            // Provide helpful suggestions for ambiguous dataset base URLs
            String baseUrl = e.getBaseUrl();
            String suggestion = buildAmbiguousSuggestion(baseUrl);
            throw new CommandLine.TypeConversionException(e.getMessage() + "\n" + suggestion);
        } catch (IllegalArgumentException e) {
            throw new CommandLine.TypeConversionException(e.getMessage());
        }
    }

    /// Build a suggestion message for ambiguous dataset base URLs
    private String buildAmbiguousSuggestion(String baseUrl) {
        StringBuilder sb = new StringBuilder();
        sb.append("\nTo specify a facet, try one of these commands:\n");
        sb.append("  --vectors ").append(baseUrl).append("dataset.yaml  (then select facet)\n");
        sb.append("\nOr use a catalog facet specification:\n");
        sb.append("  --vectors facet.<dataset>.<profile>.base\n");
        sb.append("  --vectors facet.<dataset>.<profile>.query\n");
        sb.append("  --vectors facet.<dataset>.<profile>.indices\n");
        sb.append("  --vectors facet.<dataset>.<profile>.distances\n");
        sb.append("\nColon separators are also accepted for compatibility:\n");
        sb.append("  --vectors facet:<dataset>:<profile>:base\n");
        sb.append("\nOr shorthand (if the dataset is in the catalog):\n");
        sb.append("  --vectors <dataset>.<profile>.base\n");
        return sb.toString();
    }
}
