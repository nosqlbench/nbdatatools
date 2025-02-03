package io.nosqlbench.nbvectors.taghdf.traversal.visitors;

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
  @Override
  public void enterNode(Node node) {

  }

  @Override
  public void leaveNode(Node node) {

  }

  @Override
  public void dataset(Dataset dataset) {

  }

  @Override
  public void attribute(Node node, Attribute attribute) {

  }

  @Override
  public void committedDataType(CommittedDatatype cdt) {

  }

  @Override
  public void enterGroup(Group group) {

  }

  @Override
  public void enterFile(HdfFile file) {

  }

  @Override
  public void leaveFile(HdfFile file) {

  }

  @Override
  public void leaveGroup(Group group) {

  }

  protected String fullAttrName(Node node, Attribute attribute) {
    return fullName(node)+"."+attribute.getName();
  }

  protected String fullAttrName(Node node, String attrName) {
    return fullName(node)+"."+attrName;
  }

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
