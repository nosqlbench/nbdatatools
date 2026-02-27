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

/// Picocli type converter for DatasetSpec.
///
/// This converter enables automatic parsing of dataset specifications
/// from command line arguments. Register it with a command or parameter:
///
/// ```java
/// @CommandLine.Option(names = {"--dataset"}, converter = DatasetSpecConverter.class)
/// private DatasetSpec dataset;
/// ```
///
/// ## Supported Formats
///
/// - `sift-128` - Catalog dataset name
/// - `./mydata` - Local directory containing dataset.yaml
/// - `./mydata/dataset.yaml` - Direct path to dataset.yaml
/// - `https://example.com/datasets/sift/` - Remote dataset base URL
/// - `https://example.com/datasets/sift/dataset.yaml` - Remote dataset.yaml URL
public class DatasetSpecConverter implements CommandLine.ITypeConverter<DatasetSpec> {

    /// Creates a new DatasetSpecConverter instance.
    public DatasetSpecConverter() {
    }

    @Override
    public DatasetSpec convert(String value) throws Exception {
        try {
            return DatasetSpec.parse(value);
        } catch (IllegalArgumentException e) {
            throw new CommandLine.TypeConversionException(e.getMessage());
        }
    }
}
