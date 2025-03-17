package io.nosqlbench.nbvectors.commands.tag_hdf5.traversal.visitors;

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


import io.jhdf.WritableHdfFile;
import io.jhdf.api.Node;
import io.nosqlbench.nbvectors.commands.tag_hdf5.attrtypes.AttrSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/// When the node of one of the fully qualified attributes
/// in the map is seen, it is set as an attribute on that node
/// and removed from the map.
///
/// When there are remaining attrs in the map at the max, an error should be thrown.
public class HdfSetAttributesVisitor extends BaseHdfVisitor {

  private final Map<String, Map<String, AttrSet>> attrs = new HashMap<>();
  private final WritableHdfFile out;

  /// create a set-attributes visitor
  /// @param out the file to write to
  /// @param specifiers the attributes to set
  public HdfSetAttributesVisitor(WritableHdfFile out, List<String> specifiers) {
    this.out = out;
    for (String specifier : specifiers) {
      AttrSet entry = AttrSet.parse(specifier);
      Map<String, AttrSet> nodemap = attrs.computeIfAbsent(entry.attrname().path(),
          k -> new HashMap<>());
      nodemap.put(entry.attrname().attr(), entry);
    }
  }


  /// {@inheritDoc}
  @Override
  public void enterNode(Node node) {
    if (attrs.containsKey(node.getName())) {
      out.putAttribute(node.getName(), node.getName());

      Map<String, AttrSet> nodemap = attrs.remove(node.getName());
      for (AttrSet entry : nodemap.values()) {
        System.out.println("setting attr: " + entry);
      }
    }
    super.enterNode(node);
  }

  /// {@inheritDoc}
  @Override
  public void finish() {
    if (!attrs.isEmpty()) {
      throw new RuntimeException("Not all attributes were set: " + attrs);
    }

  }


}
