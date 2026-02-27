package io.nosqlbench.vectordata.discovery.metadata;

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

import java.util.List;
import java.util.Optional;

/// Describes the schema of metadata records in a predicate test dataset.
///
/// A metadata layout defines the ordered set of fields that make up each
/// metadata record. Field descriptors carry names, types, and for
/// {@link FieldType#ENUM ENUM} fields, the allowed values.
public interface MetadataLayout {

    /// Returns the ordered list of field descriptors.
    ///
    /// @return the field descriptors
    List<FieldDescriptor> getFields();

    /// Returns the number of fields in this layout.
    ///
    /// @return the field count
    int getFieldCount();

    /// Returns the field descriptor at the given index.
    ///
    /// @param index the zero-based field index
    /// @return the field descriptor
    /// @throws IndexOutOfBoundsException if the index is out of range
    FieldDescriptor getField(int index);

    /// Looks up a field descriptor by name.
    ///
    /// @param name the field name
    /// @return the field descriptor, or empty if no field has that name
    Optional<FieldDescriptor> getFieldByName(String name);
}
