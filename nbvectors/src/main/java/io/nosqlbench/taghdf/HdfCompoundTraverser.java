package io.nosqlbench.taghdf;

import io.jhdf.CommittedDatatype;
import io.jhdf.HdfFile;
import io.jhdf.api.Attribute;
import io.jhdf.api.Dataset;
import io.jhdf.api.Group;
import io.jhdf.api.Node;

import java.util.ArrayList;
import java.util.List;

public class HdfCompoundTraverser implements HdfTraverser {

  private final List<HdfTraverser> traversers = new ArrayList<>();

  public HdfCompoundTraverser add(HdfTraverser traverser) {
    traversers.add(traverser);
    return this;
  }

  @Override
  public void enterNode(Node node) {
    for (HdfTraverser traverser : traversers) {
      traverser.enterNode(node);
    }
  }

  @Override
  public void leaveNode(Node node) {
    for (HdfTraverser traverser : traversers) {
      traverser.leaveNode(node);
    }
  }

  @Override
  public void dataset(Dataset dataset) {
    for (HdfTraverser traverser : traversers) {
      traverser.dataset(dataset);
    }

  }

  @Override
  public void attribute(Attribute attribute) {
    for (HdfTraverser traverser : traversers) {
      traverser.attribute(attribute);
    }

  }

  @Override
  public void committedDataType(CommittedDatatype cdt) {
    for (HdfTraverser traverser : traversers) {
      traverser.committedDataType(cdt);
    }

  }

  @Override
  public void enterGroup(Group group) {
    for (HdfTraverser traverser : traversers) {
      traverser.enterGroup(group);
    }
  }

  @Override
  public void enterFile(HdfFile file) {
    for (HdfTraverser traverser : traversers) {
      traverser.enterFile(file);
    }

  }

  @Override
  public void leaveFile(HdfFile file) {
    for (HdfTraverser traverser : traversers) {
      traverser.leaveFile(file);
    }

  }

  @Override
  public void leaveGroup(Group group) {
    for (HdfTraverser traverser : traversers) {
      traverser.leaveGroup(group);
    }

  }
}
