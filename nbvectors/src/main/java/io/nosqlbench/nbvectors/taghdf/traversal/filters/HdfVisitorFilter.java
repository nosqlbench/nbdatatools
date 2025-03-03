package io.nosqlbench.nbvectors.taghdf.traversal.filters;

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
import io.jhdf.HdfFile;
import io.jhdf.api.Attribute;
import io.jhdf.api.Dataset;
import io.jhdf.api.Group;
import io.jhdf.api.Node;

public interface HdfVisitorFilter {

  boolean enterNode(Node node);

  boolean leaveNode(Node node);

  boolean dataset(Dataset dataset);

  boolean attribute(Attribute attribute);

  boolean committedDataType(CommittedDatatype cdt);

  boolean enterGroup(Group group);

  boolean enterFile(HdfFile file);

  boolean leaveFile(HdfFile file);

  boolean leaveGroup(Group group);
}
