package io.nosqlbench.command.tag_hdf5.traversalv2;

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
import io.jhdf.api.*;

import java.util.List;
import java.util.Map;

/// experimental implementation of a transform walker
public class TransformWalker {
  private final WritableGroup out;
  private final HdfTransformer transformer;
  private final boolean walknew;

  /// create a new transform walker
  /// @param out the file to write to
  /// @param transformer the transformer to use
  /// @param walknew whether to walk new elements
  /// @see HdfTransformer
  public TransformWalker(WritableGroup out, HdfTransformer transformer, boolean walknew) {
    this.out = out;
    this.transformer = transformer;
    this.walknew = walknew;
  }

  //  TODO: Should traversal re-traverse new elements?

  /// traverse a node and its children, calling the appropriate methods on the visitor.
  /// @param node the node to traverse
  /// @param parent the parent node of the node to traverse
  public void traverseNode(Node node, Node parent) {

    Map<String, Attribute> attributes1 = node.getAttributes();
    for (Attribute attribute : attributes1.values()) {
      List<Attribute> attributes = transformer.transformAttribute(parent, attribute);
      attributes = attributes == null ? List.of(attribute) : attributes;
      for (Attribute attr : attributes) {
        out.putAttribute(attr.getName(), attr.getData());
      }
    }

    switch (node) {
      case HdfFile file -> {
        for (Node child : file.getChildren().values()) {
          traverseNode(child, node);
        }
      }
      case Group group -> {
        List<Group> update = transformer.transformGroup(parent, group);
        update = update == null ? List.of(group) : update;
        for (Group updatedGroup : update) {
          out.putGroup(updatedGroup.getName());
        }
      }
      case Attribute attribute -> {
        List<Attribute> attributes = transformer.transform(attribute);
        //        attributes = attributes == null ? List.of(attribute) : attributes;
        for (Attribute updatedAttribute : attributes == null ? List.of(attribute) : attributes) {
          if (attributes != null && walknew) {
            for (Attribute a : attributes) {
//              traverseNode(a);
            }
          }
          out.putAttribute(updatedAttribute.getName(), updatedAttribute.getData());
        }
      }
      case Dataset dataset -> {
        List<Dataset> datasets = transformer.transform(dataset);
        datasets = datasets == null ? List.of(dataset) : datasets;
        for (Dataset updatedDataset : datasets) {
          out.putDataset(updatedDataset.getName(), updatedDataset.getData());
        }
      }
      case CommittedDatatype cdt -> {
        List<CommittedDatatype> cdtypes = transformer.transform(cdt);
        cdtypes = cdtypes == null ? List.of(cdt) : cdtypes;
        for (CommittedDatatype updatedCdt : cdtypes) {
          throw new RuntimeException(
              "Unsupported type until HDF API supports it " + "(CommittedDataType)");
        }
      }
      default -> throw new RuntimeException("Unrecognized node type: " + node);
    }
  }

}
