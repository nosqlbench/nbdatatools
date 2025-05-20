package io.nosqlbench.command.tag_hdf5.traversal.visitors;

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
import io.jhdf.api.Attribute;
import io.jhdf.api.Dataset;
import io.jhdf.api.Group;
import io.jhdf.api.Node;

import java.util.Arrays;
import java.util.LinkedList;

/// An implementation of {@link HdfVisitor} which prints out details
public class HdfPrintVisitor extends BaseHdfVisitor {
  LinkedList<String> names = new LinkedList<>();

  /// {@inheritDoc}
  @Override
  public void enterNode(Node node) {
    names.addLast(node.getName());
    String path = String.join("/", names);
    System.out.println("name:" + path + "\ntype: " + node.getType().name());
  }

  /// {@inheritDoc}
  @Override
  public void leaveNode(Node node) {
    names.removeLast();
  }

  /// {@inheritDoc}
  @Override
  public void dataset(Dataset dataset) {
    System.out.println("""
            dims: DIMS
            type: HDFTYPE
        javatype: JAVATYPE
        datasize: DATASIZE
         storage: STORAGESIZE
        """.replaceAll("DIMS", Arrays.toString(dataset.getDimensions()))
        .replaceAll("HDFTYPE", dataset.getDataType().getClass().getSimpleName())
        .replaceAll("JAVATYPE", dataset.getJavaType().getTypeName())
        .replaceAll("DATASIZE", String.valueOf(dataset.getSize()))
        .replaceAll("STORAGESIZE", String.valueOf(dataset.getStorageInBytes())));
  }

  /// {@inheritDoc}
  @Override
  public void attribute(Node node, Attribute attribute) {
    System.out.println(attribute.toString());
  }

  /// {@inheritDoc}
  @Override
  public void committedDataType(CommittedDatatype cdt) {
    System.out.println("""
          datatype: HDFTYPE
        """.replaceAll("HDFTYPE", cdt.getDataType().getClass().getSimpleName()));
  }

  /// {@inheritDoc}
  @Override
  public void enterGroup(Group group) {
    System.out.println(group.toString());
  }

  /// {@inheritDoc}
  @Override
  public void finish() {
  }

}
