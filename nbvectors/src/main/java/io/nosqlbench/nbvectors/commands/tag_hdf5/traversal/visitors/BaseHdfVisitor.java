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


import io.jhdf.CommittedDatatype;
import io.jhdf.HdfFile;
import io.jhdf.api.Attribute;
import io.jhdf.api.Dataset;
import io.jhdf.api.Group;
import io.jhdf.api.Node;

import java.util.LinkedList;

/// Extend this class if you simply want to implement the
/// [HdfVisitor] methods you care about
public abstract class BaseHdfVisitor implements HdfVisitor {

  /// create a base visitor
  BaseHdfVisitor() {
  }

  /// {@inheritDoc}
  @Override
  public void enterNode(Node node) {

  }

  /// {@inheritDoc}
  @Override
  public void leaveNode(Node node) {

  }

  /// {@inheritDoc}
  @Override
  public void dataset(Dataset dataset) {

  }

  /// {@inheritDoc}
  @Override
  public void attribute(Node node, Attribute attribute) {

  }

  /// {@inheritDoc}
  @Override
  public void committedDataType(CommittedDatatype cdt) {

  }

  /// {@inheritDoc}
  @Override
  public void enterGroup(Group group) {

  }

  /// {@inheritDoc}
  @Override
  public void enterFile(HdfFile file) {

  }

  /// {@inheritDoc}
  @Override
  public void leaveFile(HdfFile file) {

  }

  /// {@inheritDoc}
  @Override
  public void leaveGroup(Group group) {

  }

  /// compute the full attr name
  /// @param node the node to compute the name for
  /// @param attribute the attribute to append
  /// @return the full attribute name
  protected String fullAttrName(Node node, Attribute attribute) {
    return fullName(node)+"."+attribute.getName();
  }

  /// compute the full attr name
  /// @param node the node to compute the name for
  /// @param attrName the attribute name to append
  /// @return the full attribute name
  protected String fullAttrName(Node node, String attrName) {
    return fullName(node)+"."+attrName;
  }

  /// compute the full name of a node
  /// @param node the node to compute the name for
  /// @return the full name of the node
  protected String fullName(Node node) {
    LinkedList<String> names = new LinkedList<>();
    Node thisnode = node;
    while (thisnode != null) {
      names.addFirst(thisnode.getName());
      thisnode = thisnode.getParent();
      if (thisnode.getParent() == thisnode)
        thisnode = null;
    }
    return String.join("/", names);
  }
}
