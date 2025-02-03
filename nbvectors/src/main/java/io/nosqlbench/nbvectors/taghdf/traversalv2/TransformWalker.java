package io.nosqlbench.nbvectors.taghdf.traversalv2;

import io.jhdf.CommittedDatatype;
import io.jhdf.HdfFile;
import io.jhdf.api.*;

import java.util.List;
import java.util.Map;

public class TransformWalker {
  private final WritableGroup out;
  private final HdfTransformer transformer;
  private final boolean walknew;

  public TransformWalker(WritableGroup out, HdfTransformer transformer, boolean walknew) {
    this.out = out;
    this.transformer = transformer;
    this.walknew = walknew;
  }

  ///  TODO: Should traversal re-traverse new elements?
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
//    return out;
  }

}