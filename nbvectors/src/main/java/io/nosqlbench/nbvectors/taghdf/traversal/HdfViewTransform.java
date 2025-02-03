package io.nosqlbench.nbvectors.taghdf.traversal;

import io.jhdf.CommittedDatatype;
import io.jhdf.api.Dataset;
import io.jhdf.api.Group;
import io.jhdf.api.Node;

public interface HdfViewTransform {
    Node transform(Node node);
    Group transform(Group group);
    Dataset transform(Dataset dataset);
    CommittedDatatype transform(CommittedDatatype cdt);
}
