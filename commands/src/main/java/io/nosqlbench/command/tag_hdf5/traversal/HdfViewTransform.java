package io.nosqlbench.command.tag_hdf5.traversal;

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


import io.jhdf.CommittedDatatype;
import io.jhdf.api.Dataset;
import io.jhdf.api.Group;
import io.jhdf.api.Node;

/// This interface allows for transformation of the HDF5 file structure.
public interface HdfViewTransform {

    /// return the replacement node, or null to do nothing
    /// @param node the node to transform
    /// @return the replacement node, or null to do nothing
    Node transform(Node node);

    /// return the replacement group, or null to do nothing
    /// @param group the group to transform
    /// @return the replacement group, or null to do nothing
    Group transform(Group group);

    /// return the replacement dataset, or null to do nothing
    /// @param dataset the dataset to transform
    /// @return the replacement dataset, or null to do nothing
    Dataset transform(Dataset dataset);

    /// return the replacement committed datatype, or null to do nothing
    /// @param cdt the committed datatype to transform
    /// @return the replacement committed datatype, or null to do nothing
    CommittedDatatype transform(CommittedDatatype cdt);
}
