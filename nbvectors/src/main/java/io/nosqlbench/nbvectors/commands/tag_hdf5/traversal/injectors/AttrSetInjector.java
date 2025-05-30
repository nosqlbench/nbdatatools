package io.nosqlbench.nbvectors.commands.tag_hdf5.traversal.injectors;

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


import io.jhdf.api.Node;

import java.util.List;

/// An experimental injector which sets attributes on nodes
public class AttrSetInjector extends BaseHdfVisitorInjector {

  /// Create a new attribute injector
  public AttrSetInjector() {
  }

  /// @return a list of nodes to _add_ or null or an empty list to do nothing
  @Override
  public List<Node> enterNode(Node node) {
    return super.enterNode(node);
  }
}
