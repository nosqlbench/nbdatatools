package io.nosqlbench.nbvectors.jjq.functions.mappers;

public interface NBIdMapper extends StatefulShutdown {
  void addInstance(String fieldName, String string);
  long lookupId(String fieldName, String text);
}
