package io.nosqlbench.nbvectors.taghdf.traversalv2;

import io.jhdf.CommittedDatatype;
import io.jhdf.HdfFile;
import io.jhdf.api.*;

import java.util.List;

/// One challenge to making many changes to an HDF5 file is that the input and output are
/// effectively stream-oriented for large files. The file can't always simply be buffered in
/// memory and then written out. A hybrid approach will be needed to improve this in the future,
/// where the structure of the file is read and retained. Then lightweight structural changes
/// are buffered and sequenced, interleaved with more batch-oriented changes (or none) for datasets.
///
///
/// These methods allow for modification of the HDF5 file structure. By default, no transformations
/// are performed. When a non-null List of elements is returned, they replace the original element.
///
/// All transform methods should return null if they do not wish to transform the subnode in any
/// way.
public interface HdfTransformer {

  default List<HdfFile> transform(HdfFile file) {
    return null;
  }

  default List<Group> transform(Group group) {
    return null;
  }

  default List<Dataset> transform(Dataset dataset) {
    return null;
  }

  default List<Attribute> transform(Attribute attribute) {
    return null;
  }

  default List<CommittedDatatype> transform(CommittedDatatype cdt) {
    return null;
  }

  default List<Node> transform(Node node) {
    return null;
  }

  default List<Attribute> transformAttribute(Node parent, Attribute attribute) {
    return null;
  }

  default List<Group> transformGroup(Node parent, Group group) {
    return null;
  }

  ;
}
