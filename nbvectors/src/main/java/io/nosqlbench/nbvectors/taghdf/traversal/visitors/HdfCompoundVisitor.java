package io.nosqlbench.nbvectors.taghdf.traversal.visitors;

import io.jhdf.CommittedDatatype;
import io.jhdf.HdfFile;
import io.jhdf.api.Attribute;
import io.jhdf.api.Dataset;
import io.jhdf.api.Group;
import io.jhdf.api.Node;

import java.util.ArrayList;
import java.util.List;

public class HdfCompoundVisitor implements HdfVisitor {

  private final List<HdfVisitor> traversers = new ArrayList<>();

  public HdfCompoundVisitor add(HdfVisitor traverser) {
    traversers.add(traverser);
    return this;
  }

  @Override
  public void enterNode(Node node) {
    for (HdfVisitor traverser : traversers) {
      traverser.enterNode(node);
    }
  }

  @Override
  public void leaveNode(Node node) {
    for (HdfVisitor traverser : traversers) {
      traverser.leaveNode(node);
    }
  }

  @Override
  public void dataset(Dataset dataset) {
    for (HdfVisitor traverser : traversers) {
      traverser.dataset(dataset);
    }

  }

  @Override
  public void attribute(Node node, Attribute attribute) {
    for (HdfVisitor traverser : traversers) {
      traverser.attribute(node, attribute);
    }

  }

  @Override
  public void committedDataType(CommittedDatatype cdt) {
    for (HdfVisitor traverser : traversers) {
      traverser.committedDataType(cdt);
    }

  }

  @Override
  public void enterGroup(Group group) {
    for (HdfVisitor traverser : traversers) {
      traverser.enterGroup(group);
    }
  }

  @Override
  public void enterFile(HdfFile file) {
    for (HdfVisitor traverser : traversers) {
      traverser.enterFile(file);
    }

  }

  @Override
  public void leaveFile(HdfFile file) {
    for (HdfVisitor traverser : traversers) {
      traverser.leaveFile(file);
    }

  }

  @Override
  public void leaveGroup(Group group) {
    for (HdfVisitor traverser : traversers) {
      traverser.leaveGroup(group);
    }

  }

  @Override
  public void finish() {
    for (HdfVisitor traverser : traversers) {
      traverser.finish();
    }
  }
}
