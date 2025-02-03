package io.nosqlbench.nbvectors.taghdf.traversal.visitors;

import io.jhdf.WritableHdfFile;
import io.jhdf.api.Node;
import io.nosqlbench.nbvectors.taghdf.attrtypes.AttrSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/// When the node of one of the fully-qualified attributes
/// in the map is seen, it is set as an attribute on that node
/// and removed from the map.
///
/// When there are remaining attrs in the map at the end, an error should be thrown.
public class HdfSetAttributesVisitor extends BaseHdfVisitor {

  private final Map<String, Map<String, AttrSet>> attrs = new HashMap<>();
  private final WritableHdfFile out;

  public HdfSetAttributesVisitor(WritableHdfFile out, List<String> specifiers) {
    this.out = out;
    for (String specifier : specifiers) {
      AttrSet entry = AttrSet.parse(specifier);
      Map<String, AttrSet> nodemap = attrs.computeIfAbsent(entry.attrname().path(),
          k -> new HashMap<>());
      nodemap.put(entry.attrname().attr(), entry);
    }
  }

  @Override
  public void enterNode(Node node) {
    if (attrs.containsKey(node.getName())) {
      out.putAttribute(node.getName(), node.getName());

      Map<String, AttrSet> nodemap = attrs.remove(node.getName());
      for (AttrSet entry : nodemap.values()) {
        System.out.println("setting attr: " + entry);
      }
    }
    super.enterNode(node);
  }

  @Override
  public void finish() {
    if (!attrs.isEmpty()) {
      throw new RuntimeException("Not all attributes were set: " + attrs);
    }

  }


}
