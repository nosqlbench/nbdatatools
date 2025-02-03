package io.nosqlbench.nbvectors.taghdf.traversal.visitors;

import io.jhdf.CommittedDatatype;
import io.jhdf.api.Attribute;
import io.jhdf.api.Dataset;
import io.jhdf.api.Group;
import io.jhdf.api.Node;

import java.util.Arrays;
import java.util.LinkedList;

public class HdfPrintVisitor extends BaseHdfVisitor {
  LinkedList<String> names = new LinkedList<>();

  @Override
  public void enterNode(Node node) {
    names.addLast(node.getName());
    String path = String.join("/", names);
    System.out.println("name:" + path + "\ntype: " + node.getType().name());
  }

  @Override
  public void leaveNode(Node node) {
    names.removeLast();
  }

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

  @Override
  public void attribute(Node node, Attribute attribute) {
    System.out.println(attribute.toString());
  }

  @Override
  public void committedDataType(CommittedDatatype cdt) {
    System.out.println("""
          datatype: HDFTYPE
        """.replaceAll("HDFTYPE", cdt.getDataType().getClass().getSimpleName()));

  }

  @Override
  public void enterGroup(Group group) {
    System.out.println(group.toString());
  }

  @Override
  public void finish() {
  }


}
